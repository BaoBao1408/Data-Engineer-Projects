from neo4j import GraphDatabase
import os
from dotenv import load_dotenv
load_dotenv()

driver = GraphDatabase.driver(
    os.getenv('NEO4J_URI'),
    auth=(os.getenv('NEO4J_USER'), os.getenv('NEO4J_PASSWORD'))
)

with driver.session() as s:
    for code, name in [
        ('VCB','Vietcombank'), ('VIC','Vingroup'), ('HPG','Hoa Phat'),
        ('FPT','FPT Corp'), ('MBB','MB Bank'), ('BVH','Bao Viet'),
        ('PVN','PetroVietnam'), ('SAMSUNG_VN','Samsung Vietnam')
    ]:
        s.run('MERGE (e:Entity {code:$code}) SET e.name=$name, e.country="VN"',
              code=code, name=name)

    for a, b in [('VCB','VIC'), ('HPG','FPT'), ('MBB','BVH'), ('PVN','SAMSUNG_VN')]:
        s.run('MATCH (a:Entity{code:$a}),(b:Entity{code:$b}) MERGE (a)-[:AUDIT_RELATED]->(b)',
              a=a, b=b)

    r = s.run('MATCH (n) RETURN count(n) as total')
    print(f'Neo4j AuraDB connected! Nodes: {r.single()["total"]}')

driver.close()