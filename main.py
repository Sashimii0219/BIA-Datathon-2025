import argparse
import pandas as pd
from functions.utils import clean_text
from functions.relik_utils import *
from functions.rebel_utils import *
from functions.neo4j_utils import Neo4jConnection
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer

from dotenv import load_dotenv
import os
import time

# Create argument parser
parser = argparse.ArgumentParser(description="Process some inputs.")

# Define arguments
parser.add_argument(
    "-m",
    "--method", 
    type=str, 
    default='relik',
    help="Currently Available Methods: relik, mrebel"
    )

# Define arguments
parser.add_argument(
    "-up",
    "--upload_auradb", 
    type=bool, 
    default=True,
    help="Upload to AuraDB"
    )

# Parse arguments
args = parser.parse_args()
method = args.method
upload_auradb = args.upload_auradb

if __name__ == "__main__":

    load_dotenv()

    # URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
    URI = os.getenv("URI")
    USERNAME = os.getenv("USER_NAME")
    PASSWORD = os.getenv("PASSWORD")
    AUTH = (USERNAME, PASSWORD) 

    merged_df = pd.read_csv('datasets/clean/merged_df.csv')
    text_col = merged_df['coref_text'].tolist()

    if method == 'relik':    
        model = Relik.from_pretrained("relik-ie/relik-relation-extraction-large")
        entities_df, relationships_df = relik_extract_entity_relationship(text_col, model)

    elif method == 'mrebel':
        # Load model and tokenizer
        tokenizer = AutoTokenizer.from_pretrained("Babelscape/mrebel-large", src_lang="en_XX", tgt_lang="tp_XX") 
        model = AutoModelForSeq2SeqLM.from_pretrained("Babelscape/mrebel-large")

        entities_df, relationships_df = rebel_extract_entity_relationship(text_col, tokenizer, model)
    
    else:
        raise Exception("No such model!")

    if upload_auradb:
        # Initialize connection to Neo4j
        dbconn = Neo4jConnection(URI, AUTH[0], AUTH[1])

        entities_df = pd.read_csv('./datasets/clean/entities_df.csv')
        relationships_df = pd.read_csv('./datasets/clean/relationships_df.csv')

        dbconn.write_entities(entities_df)
        dbconn.write_relationships(relationships_df)

        dbconn.close()