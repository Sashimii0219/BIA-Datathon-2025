import pandas as pd
import spacy, coreferee
from functions.utils import coref_text, clean_text

news_df = pd.read_excel('news_excerpts_parsed.xlsx')
wikileaks_df = pd.read_excel('wikileaks_parsed.xlsx')

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

merged_df.to_csv('merged_df.csv', index=False)