from neo4j import GraphDatabase
from functions.utils import add_entity_type

# Create a connection class
class Neo4jConnection:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.driver.verify_connectivity()
        print("Connection to Neo4j AuraDB Established!")

    def close(self):
        self.driver.close()

    def run_query(self, query, parameters=None):
        with self.driver.session() as session:
            return session.run(query, parameters)

    def write_entities(self, entities_df):
        with self.driver.session() as session:
            print("Writing entities to database...")
            for _, row in entities_df.iterrows():
                entity_label = row["entity_type"]  # Get entity type
                query = f"""
                MERGE (e:{entity_label} {{name: $name}})
                """

                session.execute_write(lambda tx: tx.run(query, name=row["entity"], database_="neo4j"))
        
        print(f"Write completed! {len(entities_df)} entities added to database.")

    def write_relationships(self, relationships_df):
        print("Writing relationships to database...")

        with self.driver.session() as session:
            for _, row in relationships_df.iterrows():
                query = f"""
                MERGE (h:{row['subject_entity_type']} {{name: $subject}})
                MERGE (t:{row['object_entity_type']} {{name: $object}})
                MERGE (h)-[r:{row['relationship']}]->(t)
                ON CREATE SET r.confidence = $confidence
                """

                session.execute_write(lambda tx: tx.run(
                    query,
                    subject=row["subject"],
                    relationship=row["relationship"],
                    object=row["object"],
                    confidence=row["confidence"],
                    database_="neo4j"
                ))

        print(f"Write completed! {len(relationships_df)} relationships added to database.")