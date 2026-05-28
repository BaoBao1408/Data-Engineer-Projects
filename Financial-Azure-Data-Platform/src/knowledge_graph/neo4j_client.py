"""
Neo4j Knowledge Graph Client.
Local dev: Neo4j Community Edition (bolt://neo4j:7687)
Production: Azure Cosmos DB for Apache Gremlin OR Neo4j AuraDB

Node types  : Entity, Document, Person, Organization, Location, Concept
Relationship: MENTIONS, RELATED_TO, BELONGS_TO, AUTHORED_BY, LOCATED_IN
"""
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Generator, Optional

from loguru import logger
from neo4j import GraphDatabase, Session
from neo4j.exceptions import ServiceUnavailable, SessionExpired
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config import get_settings

settings = get_settings()


@dataclass
class GraphNode:
    id: str
    label: str
    properties: dict[str, Any] = field(default_factory=dict)


@dataclass
class GraphRelationship:
    from_id: str
    to_id: str
    rel_type: str
    properties: dict[str, Any] = field(default_factory=dict)


class Neo4jClient:
    """
    Thin wrapper around the Neo4j Python driver.
    Provides CRUD operations for graph nodes and relationships.
    """

    def __init__(
        self,
        uri: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self._uri = uri or settings.neo4j.uri
        self._user = user or settings.neo4j.user
        self._password = password or settings.neo4j.password
        self._driver = None
        self._connect()

    def _connect(self) -> None:
        self._driver = GraphDatabase.driver(
            self._uri,
            auth=(self._user, self._password),
            max_connection_pool_size=settings.neo4j.max_connection_pool_size,
        )
        logger.info(f"Neo4j driver connected: {self._uri}")

    @contextmanager
    def session(self) -> Generator[Session, None, None]:
        with self._driver.session() as session:
            yield session

    def close(self) -> None:
        if self._driver:
            self._driver.close()

    # ─── Node Operations ────────────────────────────────────────────────────

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=5),
        reraise=True,
    )
    def upsert_node(self, node: GraphNode) -> dict:
        """Create or update a node. Uses MERGE to prevent duplicates."""
        props_str = ", ".join(
            [f"n.{k} = ${k}" for k in node.properties.keys()]
        )
        set_clause = f"SET {props_str}" if props_str else ""
        query = f"""
            MERGE (n:{node.label} {{id: $id}})
            {set_clause}
            RETURN n
        """
        params = {"id": node.id, **node.properties}

        with self.session() as sess:
            result = sess.run(query, params)
            record = result.single()
            return dict(record["n"]) if record else {}

    def upsert_nodes_batch(self, nodes: list[GraphNode]) -> int:
        """Batch upsert nodes using UNWIND for performance."""
        if not nodes:
            return 0

        # Group by label for batch operations
        by_label: dict[str, list[dict]] = {}
        for node in nodes:
            key = node.label
            if key not in by_label:
                by_label[key] = []
            by_label[key].append({"id": node.id, **node.properties})

        total = 0
        with self.session() as sess:
            for label, batch in by_label.items():
                query = f"""
                    UNWIND $nodes AS props
                    MERGE (n:{label} {{id: props.id}})
                    SET n += props
                    RETURN count(n) AS cnt
                """
                result = sess.run(query, {"nodes": batch})
                record = result.single()
                count = record["cnt"] if record else 0
                total += count
                logger.debug(f"Upserted {count} {label} nodes")

        return total

    def get_node(self, node_id: str, label: str) -> Optional[dict]:
        query = f"MATCH (n:{label} {{id: $id}}) RETURN n"
        with self.session() as sess:
            result = sess.run(query, {"id": node_id})
            record = result.single()
            return dict(record["n"]) if record else None

    def delete_node(self, node_id: str, label: str) -> bool:
        query = f"MATCH (n:{label} {{id: $id}}) DETACH DELETE n RETURN count(n) AS cnt"
        with self.session() as sess:
            result = sess.run(query, {"id": node_id})
            record = result.single()
            return (record["cnt"] if record else 0) > 0

    # ─── Relationship Operations ─────────────────────────────────────────────

    def upsert_relationship(self, rel: GraphRelationship) -> dict:
        """Create or update a relationship between two nodes."""
        props_str = ", ".join([f"r.{k} = ${k}" for k in rel.properties.keys()])
        set_clause = f"SET {props_str}" if props_str else ""
        query = f"""
            MATCH (a {{id: $from_id}})
            MATCH (b {{id: $to_id}})
            MERGE (a)-[r:{rel.rel_type}]->(b)
            {set_clause}
            RETURN r
        """
        params = {
            "from_id": rel.from_id,
            "to_id": rel.to_id,
            **rel.properties,
        }
        with self.session() as sess:
            result = sess.run(query, params)
            record = result.single()
            return dict(record["r"]) if record else {}

    def upsert_relationships_batch(self, rels: list[GraphRelationship]) -> int:
        if not rels:
            return 0

        # Group by relationship type
        by_type: dict[str, list[dict]] = {}
        for rel in rels:
            if rel.rel_type not in by_type:
                by_type[rel.rel_type] = []
            by_type[rel.rel_type].append({
                "from_id": rel.from_id,
                "to_id": rel.to_id,
                **rel.properties,
            })

        total = 0
        with self.session() as sess:
            for rel_type, batch in by_type.items():
                query = f"""
                    UNWIND $rels AS rel
                    MATCH (a {{id: rel.from_id}})
                    MATCH (b {{id: rel.to_id}})
                    MERGE (a)-[r:{rel_type}]->(b)
                    SET r += rel
                    RETURN count(r) AS cnt
                """
                result = sess.run(query, {"rels": batch})
                record = result.single()
                total += record["cnt"] if record else 0

        return total

    # ─── Graph Queries ───────────────────────────────────────────────────────

    def find_related_entities(
        self,
        entity_id: str,
        max_hops: int = 2,
        limit: int = 50,
    ) -> list[dict]:
        """Find all entities within N hops of a given entity."""
        query = f"""
            MATCH path = (start {{id: $entity_id}})-[*1..{max_hops}]-(related)
            WITH DISTINCT related, length(path) AS hops
            RETURN related.id AS id,
                   labels(related) AS labels,
                   related.name AS name,
                   hops
            ORDER BY hops ASC
            LIMIT $limit
        """
        with self.session() as sess:
            result = sess.run(query, {"entity_id": entity_id, "limit": limit})
            return [dict(r) for r in result]

    def find_path(
        self, from_id: str, to_id: str, max_hops: int = 5
    ) -> Optional[list[dict]]:
        """Find shortest path between two entities."""
        query = f"""
            MATCH path = shortestPath(
                (a {{id: $from_id}})-[*..{max_hops}]-(b {{id: $to_id}})
            )
            RETURN [node IN nodes(path) | node.id] AS node_ids,
                   [rel IN relationships(path) | type(rel)] AS rel_types,
                   length(path) AS hops
        """
        with self.session() as sess:
            result = sess.run(query, {"from_id": from_id, "to_id": to_id})
            record = result.single()
            if record:
                return {
                    "path": list(record["node_ids"]),
                    "relationships": list(record["rel_types"]),
                    "hops": record["hops"],
                }
            return None

    def search_entities(
        self, keyword: str, label: Optional[str] = None, limit: int = 20
    ) -> list[dict]:
        """Full-text search over entity names."""
        label_filter = f":{label}" if label else ""
        query = f"""
            MATCH (n{label_filter})
            WHERE toLower(n.name) CONTAINS toLower($keyword)
               OR toLower(n.description) CONTAINS toLower($keyword)
            RETURN n.id AS id, labels(n) AS labels, n.name AS name
            LIMIT $limit
        """
        with self.session() as sess:
            result = sess.run(query, {"keyword": keyword, "limit": limit})
            return [dict(r) for r in result]

    def get_graph_stats(self) -> dict:
        """Return node and relationship counts by type."""
        with self.session() as sess:
            nodes_result = sess.run("""
                CALL apoc.meta.stats()
                YIELD labels, relTypesCount
                RETURN labels, relTypesCount
            """)
            record = nodes_result.single()
            if record:
                return {
                    "node_counts": dict(record["labels"]),
                    "relationship_counts": dict(record["relTypesCount"]),
                }
            # Fallback if APOC not available
            count_result = sess.run("MATCH (n) RETURN count(n) AS total_nodes")
            c = count_result.single()
            return {"total_nodes": c["total_nodes"] if c else 0}

    def health_check(self) -> bool:
        try:
            with self.session() as sess:
                result = sess.run("RETURN 1 AS ok")
                record = result.single()
                return record["ok"] == 1
        except Exception as e:
            logger.error(f"Neo4j health check failed: {e}")
            return False

    # ─── Schema / Indexes ────────────────────────────────────────────────────

    def create_indexes(self) -> None:
        """Create indexes for common lookup patterns."""
        indexes = [
            "CREATE INDEX entity_id IF NOT EXISTS FOR (n:Entity) ON (n.id)",
            "CREATE INDEX document_id IF NOT EXISTS FOR (n:Document) ON (n.id)",
            "CREATE INDEX person_id IF NOT EXISTS FOR (n:Person) ON (n.id)",
            "CREATE INDEX org_id IF NOT EXISTS FOR (n:Organization) ON (n.id)",
            "CREATE INDEX entity_name IF NOT EXISTS FOR (n:Entity) ON (n.name)",
            # Full-text search index (requires APOC)
            """
            CALL db.index.fulltext.createNodeIndex(
                'entity_fulltext', ['Entity', 'Document', 'Person', 'Organization'],
                ['name', 'description']
            )
            """,
        ]
        with self.session() as sess:
            for idx in indexes:
                try:
                    sess.run(idx)
                except Exception as e:
                    logger.warning(f"Index creation: {e}")
        logger.info("Neo4j indexes created")


# ─── Singleton ────────────────────────────────────────────────────────────────
_neo4j_client: Optional[Neo4jClient] = None


def get_neo4j() -> Neo4jClient:
    global _neo4j_client
    if _neo4j_client is None:
        _neo4j_client = Neo4jClient()
    return _neo4j_client
