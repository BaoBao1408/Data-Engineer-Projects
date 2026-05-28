"""Knowledge Graph router – Neo4j entity and relationship operations."""
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter()


class NodeRequest(BaseModel):
    id: str
    label: str
    properties: dict = {}


class RelationshipRequest(BaseModel):
    from_id: str
    to_id: str
    rel_type: str
    properties: dict = {}


class SearchRequest(BaseModel):
    keyword: str
    label: Optional[str] = None
    limit: int = 20


@router.post("/nodes", status_code=201)
async def create_node(request: NodeRequest):
    """Upsert a node in the Knowledge Graph."""
    from src.knowledge_graph.neo4j_client import GraphNode, get_neo4j
    neo4j = get_neo4j()
    node = GraphNode(id=request.id, label=request.label, properties=request.properties)
    result = neo4j.upsert_node(node)
    return {"status": "upserted", "node": result}


@router.get("/nodes/{label}/{node_id}")
async def get_node(label: str, node_id: str):
    """Retrieve a node by ID and label."""
    from src.knowledge_graph.neo4j_client import get_neo4j
    neo4j = get_neo4j()
    node = neo4j.get_node(node_id, label)
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    return node


@router.post("/relationships", status_code=201)
async def create_relationship(request: RelationshipRequest):
    """Create or update a relationship between two nodes."""
    from src.knowledge_graph.neo4j_client import GraphRelationship, get_neo4j
    neo4j = get_neo4j()
    rel = GraphRelationship(
        from_id=request.from_id,
        to_id=request.to_id,
        rel_type=request.rel_type,
        properties=request.properties,
    )
    result = neo4j.upsert_relationship(rel)
    return {"status": "upserted", "relationship": result}


@router.get("/related/{entity_id}")
async def find_related(entity_id: str, max_hops: int = 2, limit: int = 50):
    """Find all entities related to a given entity within N hops."""
    from src.knowledge_graph.neo4j_client import get_neo4j
    neo4j = get_neo4j()
    related = neo4j.find_related_entities(entity_id, max_hops, limit)
    return {"entity_id": entity_id, "max_hops": max_hops, "related": related}


@router.get("/path")
async def find_path(from_id: str, to_id: str, max_hops: int = 5):
    """Find shortest path between two entities."""
    from src.knowledge_graph.neo4j_client import get_neo4j
    neo4j = get_neo4j()
    path = neo4j.find_path(from_id, to_id, max_hops)
    if not path:
        raise HTTPException(status_code=404, detail="No path found")
    return path


@router.post("/search")
async def search_entities(request: SearchRequest):
    """Full-text search across entity names."""
    from src.knowledge_graph.neo4j_client import get_neo4j
    neo4j = get_neo4j()
    results = neo4j.search_entities(request.keyword, request.label, request.limit)
    return {"keyword": request.keyword, "results": results}


@router.get("/stats")
async def graph_stats():
    """Return Knowledge Graph statistics."""
    from src.knowledge_graph.neo4j_client import get_neo4j
    neo4j = get_neo4j()
    return neo4j.get_graph_stats()
