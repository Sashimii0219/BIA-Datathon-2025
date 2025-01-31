import argparse
import pandas as pd
from functions.data_cleaning import clean_text
from functions.relik_functions import *
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
    merged_df = pd.read_csv('datasets/clean/merged_df.csv')
    print(merged_df)

    relik = Relik.from_pretrained("relik-ie/relik-relation-extraction-large")
    text_col = merged_df['coref_text'].tolist()

    entities_df, relationships_df = extract_entity_relationship(text_col, relik)

    entities_df.to_csv('datasets/clean/entities_df.csv')
    relationships_df.to_csv('datasets/clean/relationships_df.csv')