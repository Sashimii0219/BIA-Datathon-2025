import argparse
import pandas as pd
from functions.utils import clean_text
from functions.relik_utils import *
from functions.neo4j_utils import Neo4jConnection
import time

# Create argument parser
parser = argparse.ArgumentParser(description="Process some inputs.")

# Define arguments
parser.add_argument(
    "-m",
    "--method", 
    type=str, 
    default='relik',
    help="Currently Available Methods: relik, XXX"
    )

# Parse arguments
args = parser.parse_args()
method = args.method

if __name__ == "__main__":
    # merged_df = pd.read_csv('datasets/clean/merged_df.csv')
    # print(merged_df)

    # relik = Relik.from_pretrained("relik-ie/relik-relation-extraction-large")
    # text_col = merged_df['coref_text'].tolist()[:2]

    # entities_df, relationships_df = extract_entity_relationship(text_col, relik)

    # print(entities_df)
    # entities_df.to_csv('datasets/clean/entities_df.csv',index=False)
    # relationships_df.to_csv('datasets/clean/relationships_df.csv',index=False)

    # URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
    URI = "neo4j+s://c428f489.databases.neo4j.io"
    AUTH = ("neo4j", "DnXVA8ZWVpf5nHYvwgw_oJzRSYfDkUBhiK4yfSSED0U")

    # Initialize connection
    dbconn = Neo4jConnection(URI, AUTH[0], AUTH[1])

    entities_df = pd.read_csv('./datasets/clean/entities_df.csv')
    relationships_df = pd.read_csv('./datasets/clean/relationships_df.csv')

    dbconn.write_relationships(entities_df, relationships_df)

    dbconn.close()