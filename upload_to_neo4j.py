from functions.neo4j_utils import Neo4jConnection
from functions.aws_utils import S3
import pandas as pd
from dotenv import load_dotenv
import os
import argparse

# Create argument parser
parser = argparse.ArgumentParser(description="Upload to Neo4j.")

# Define arguments
parser.add_argument(
    "-m",
    "--method",
    type=str,
    default='relik',
    help="Currently Available Methods: relik, mrebel"
    )

# Parse arguments
args = parser.parse_args()
method = args.method

if __name__ == "__main__":

    load_dotenv()

    # URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
    URI = os.getenv("URI")
    USERNAME = os.getenv("USER_NAME")
    PASSWORD = os.getenv("PASSWORD")
    AUTH = (USERNAME, PASSWORD) 

    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")

    # Create S3 instance
    s3 = S3(aws_access_key_id=AWS_ACCESS_KEY_ID, 
            aws_secret_access_key=AWS_SECRET_KEY)
    
    # Retrieve dfs
    entities_df = s3.read_from_s3('datathon2025',
                              f'data/validation/entities_df_{method}.csv')

    relationships_df = s3.read_from_s3('datathon2025',
                              f'data/validation/relationships_df_{method}.csv')
    
    # Initialize connection to Neo4j
    dbconn = Neo4jConnection(URI, AUTH[0], AUTH[1])

    dbconn.write_entities(entities_df)
    dbconn.write_relationships(relationships_df)

    dbconn.close()
    
