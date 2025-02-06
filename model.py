import pandas as pd
import argparse
from functions.relik_utils import *
from functions.rebel_utils import *
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer
from dotenv import load_dotenv
from functions.aws_utils import S3
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

# Parse arguments
args = parser.parse_args()
method = args.method

if __name__ == "__main__":
    load_dotenv()
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")

    # Create S3 instance
    s3 = S3(aws_access_key_id=AWS_ACCESS_KEY_ID, 
            aws_secret_access_key=AWS_SECRET_KEY)

    # Read data from S3
    input_data = s3.read_from_s3('datathon2025',
                              'data/clean-data/merged_df.csv')
    text_col = input_data['coref_text'].tolist()

    start_time = time.time()
    # Choose model
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
    
    end_time = time.time()
    print(f"Execution time: {end_time - start_time:.4f} seconds")

    # Upload output back to S3
    s3.upload_to_s3('datathon2025',
            'data/model-output',
            f'entities_df_{method}.csv',
            entities_df)

    s3.upload_to_s3('datathon2025',
            'data/model-output',
            f'relationships_df_{method}.csv',
            relationships_df)