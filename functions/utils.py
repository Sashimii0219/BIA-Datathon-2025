import spacy
import pandas as pd
import re

def coref_text(coref_nlp, text):
    import coreferee
    coref_doc = coref_nlp(text)
    resolved_text = ""

    for token in coref_doc:
        repres = coref_doc._.coref_chains.resolve(token)
        if repres:
            resolved_text += " " + " and ".join(
                [
                    t.text
                    if t.ent_type_ == ""
                    else [e.text for e in coref_doc.ents if t in e][0]
                    for t in repres
                ]
            )
        else:
            resolved_text += " " + token.text

    return resolved_text

# Function to clean raw text
def clean_text(text):
    text = re.sub("\n", " ", text)
    text = re.sub(r'\b[A-Z]+\b', '', text).strip()
    text = re.sub(" +", " ", text)

    return text

# Function to clean relationship type
def clean_relationship_type(rel_type):
    rel_type = rel_type.upper()  # Convert to uppercase
    rel_type = rel_type.replace(" ", "_")  # Replace spaces with underscores
    rel_type = re.sub(r"[^A-Z0-9_]", "", rel_type)  # Remove special characters (keep A-Z, 0-9, _)
    return rel_type

def add_entity_type(entities_df, relationships_df):
    relationships_df = pd.merge(
        relationships_df,
        entities_df,
        how='left',
        left_on='object',
        right_on='entity')
        
    relationships_df = pd.merge(
        relationships_df,
        entities_df,
        how='left',
        left_on='subject',
        right_on='entity')
    
    relationships_df.drop(['entity_x', 'entity_y'], axis=1, inplace=True)
    relationships_df.rename(columns={"entity_type_x":"object_entity_type",
                                     "entity_type_y":"subject_entity_type"},
                            inplace=True)
    return relationships_df

    

