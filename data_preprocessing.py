import pandas as pd
import spacy, coreferee
from functions.utils import coref_text, clean_text
from functions.aws_utils import S3
import os
from dotenv import load_dotenv

# Usage
if __name__ == "__main__":

        load_dotenv()
        AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
        AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")

        # Create S3 instance
        s3 = S3(aws_access_key_id=AWS_ACCESS_KEY_ID, 
                aws_secret_access_key=AWS_SECRET_KEY)

        # Read data from S3
        news_df = s3.read_from_s3('datathon2025',
                                'data/raw-data/news_excerpts_parsed.csv')

        wikileaks_df = s3.read_from_s3('datathon2025',
                                        'data/raw-data/wikileaks_parsed.csv')

        # Rename columns to keep column name consistent
        news_df = news_df.rename(columns={"Link":"source", "Text":"text"})
        wikileaks_df = wikileaks_df.rename(columns={"PDF Path":"source", "Text":"text"})

        # Merge data from both source and give them ids
        merged_df = pd.concat([news_df, wikileaks_df])
        merged_df['source_id'] = pd.factorize(merged_df['source'])[0]
        merged_df['text_id'] = pd.factorize(merged_df['text'])[0]
        merged_df = merged_df.reset_index()

        # Coref text
        coref_nlp = spacy.load('en_core_web_sm')
        coref_nlp.add_pipe('coreferee')
        merged_df['coref_text'] = merged_df['text'].apply(lambda x: coref_text(coref_nlp, x))

        # Clean text
        merged_df['coref_text'] = merged_df['coref_text'].apply(clean_text)

        # Upload output to S3
        s3.upload_to_s3('datathon2025',
                        'data/preprocess',
                        'merged_df.csv',
                        merged_df)
