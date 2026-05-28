from neo4j import GraphDatabase
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
load_dotenv()

# --- Connections ---
engine = create_engine(
    f"postgresql://{os.getenv('WAREHOUSE_DB_USER', 'edp_user')}:{os.getenv('WAREHOUSE_DB_PASSWORD', 'edp_pass')}"
    f"@{os.getenv('WAREHOUSE_DB_HOST', 'postgres-warehouse')}:{os.getenv('WAREHOUSE_DB_PORT', '5432')}/{os.getenv('WAREHOUSE_DB_NAME', 'edp_warehouse')}"
)
driver = GraphDatabase.driver(
    os.getenv('NEO4J_URI'),
    auth=(os.getenv('NEO4J_USER'), os.getenv('NEO4J_PASSWORD'))
)

with engine.connect() as conn:
    entities = conn.execute(text("SELECT entity_code, legal_name, industry_code FROM financial.entities")).fetchall()
    engagements = conn.execute(text("""
        SELECT e.engagement_code, ent.entity_code, e.fiscal_year,
               e.partner_in_charge, e.manager, e.status,
               e.audit_opinion, e.contracted_fee
        FROM audit.engagements e
        JOIN financial.entities ent ON e.entity_id = ent.entity_id
    """)).fetchall()

with driver.session() as s:
    # Clear existing
    s.run("MATCH (n) DETACH DELETE n")

    # Create Entity nodes
    for row in entities:
        s.run("""
            MERGE (e:Entity {code: $code})
            SET e.name = $name, e.industry = $industry
        """, code=row.entity_code, name=row.legal_name, industry=row.industry_code)

    # Create Partner + Manager nodes + relationships from real engagements
    for row in engagements:
        s.run("""
            MERGE (partner:Partner {name: $partner})
            MERGE (manager:Manager {name: $manager})
            MERGE (ent:Entity {code: $entity_code})
            MERGE (eng:Engagement {code: $eng_code})
                SET eng.year = $year, eng.status = $status,
                    eng.opinion = $opinion, eng.fee = $fee
            MERGE (ent)-[:HAS_ENGAGEMENT]->(eng)
            MERGE (eng)-[:LED_BY]->(partner)
            MERGE (eng)-[:MANAGED_BY]->(manager)
        """,
        partner=row.partner_in_charge,
        manager=row.manager,
        entity_code=row.entity_code,
        eng_code=row.engagement_code,
        year=row.fiscal_year,
        status=row.status,
        opinion=row.audit_opinion or 'PENDING',
        fee=float(row.contracted_fee) if row.contracted_fee else 0
        )

    stats = s.run("MATCH (n) RETURN labels(n)[0] as label, count(n) as cnt")
    print("\nGraph seeded from real DB data:")
    for r in stats:
        print(f"  {r['label']:20s}: {r['cnt']} nodes")

driver.close()