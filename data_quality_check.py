import pandas as pd
import re
import argparse
from functions.aws_utils import *

from dotenv import load_dotenv
import os

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

class TripletValidationPipeline:
    def __init__(self, relationships_df):
        """
        Initialize the pipeline with the input DataFrame of triplets.
        """
        self.df = relationships_df

    def check_null(
        self,
        cols=['subject', 'object', 'object_entity_type', 'subject_entity_type'],
        how='any'
    ):
        """
        Removes rows that contain at least one null value in the specified columns.
        """
        initial_count = self.df.shape[0]
        self.df = self.df.dropna(subset=cols, how=how)
        new_count = self.df.shape[0]

        if new_count < initial_count:
            print(f'Out of {initial_count} rows, a total of {initial_count - new_count} ({round((initial_count - new_count)/initial_count*100,1)}%) rows have been dropped due to missing values in at least one of the following columns: {cols}.')
        else:
            print('No rows were dropped.')
        return self

    def remove_redundant_entities(
        self,
        object_col='object',
        subject_col='subject'
    ):
        """
        Removes rows where the subject or object contains:
        1) Numbers (including dates)
        2) Strings with 3 characters or less
        """
        def redundant_logic_wrap(inputString):
            return (has_numbers(inputString) | char_check(inputString))

        def has_numbers(inputString):
            return bool(re.search(r'\d', inputString))

        def char_check(inputString):
            return len(inputString) <= 3

        initial_count = self.df.shape[0]
        self.df = self.df[(self.df[object_col].apply(redundant_logic_wrap) == False) & (self.df[subject_col].apply(redundant_logic_wrap) == False)]
        new_count = self.df.shape[0]

        if new_count < initial_count:
            print(f'Out of {initial_count} rows, a total of {initial_count - new_count} ({round((initial_count - new_count)/initial_count*100,1)}%) rows have been dropped due to not passing quality checks.')
        else:
            print('No rows were dropped.')
        return self

    def replace_special_char(self):
        """
        Replaces special characters in the subject and object columns with underscores.
        """
        total = self.df.shape[0]
        num_object_changed = self.df[self.df['object'].str.count(r'[\W_]+', re.I) > 0].shape[0]
        num_subject_changed = self.df[self.df['subject'].str.count(r'[\W_]+', re.I) > 0].shape[0]

        print(f'Out of a total {total} rows, {num_object_changed} ({round(num_object_changed/total*100,1)}%) object names and {num_subject_changed} ({round(num_subject_changed/total*100,1)}%) subject names were changed.')

        self.df = self.df.replace(r'[\W_]+', '_', regex=True)
        return self

    def get_cleaned_data(self):
        """
        Returns the cleaned DataFrame after all transformations.
        """
        return self.df


# Example usage
if __name__ == "__main__":

    # Sample DataFrame
    # data = {
    #     'subject': ['Asia', '123', 'USA', 'AI', 'Europe', "", "twin towers"],
    #     'object': ['continent', '456', 'country', 'technology', 'region', "food", "America"],
    #     'subject_entity_type': ['LOC', 'NUM', 'LOC', 'PRODUCT', 'LOC', "UNDEFINED", "OBJECT"],
    #     'object_entity_type': ['LOC', 'NUM', 'LOC', 'PRODUCT', 'LOC', "PRODUCT", "LOC"]
    # }
    # df = pd.DataFrame(data)

    load_dotenv()
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")

        # Create S3 instance
    s3 = S3(aws_access_key_id=AWS_ACCESS_KEY_ID, 
            aws_secret_access_key=AWS_SECRET_KEY)
    
        # Retrieve dfs
    entities_df = s3.read_from_s3('datathon2025',
                              f'data/model-output/entities_df_{method}.csv')

    relationships_df = s3.read_from_s3('datathon2025',
                              f'data/model-output/relationships_df_{method}.csv')

    # Create pipeline and apply transformations
    pipeline = TripletValidationPipeline(relationships_df)
    cleaned_df = (
        pipeline
        .check_null()
        .remove_redundant_entities()
        .replace_special_char()
        .get_cleaned_data()
    )
    
            # Upload output to S3
    s3.upload_to_s3('datathon2025',
                    'data/validation',
                    'clean_relationships_df.csv',
                    cleaned_df)
    
    s3.upload_to_s3('datathon2025',
                    'data/validation',
                    'clean_entities_df.csv',
                    entities_df)