import pandas as pd
import spacy
import time
from relik import Relik
from functions.utils import clean_relationship_type

def extract_entity_relationship(text_list, relik):
    entities = []
    relationships = []

    start_time = time.time()

    relik_out = relik(text_list, 
                      top_k = 3)
    print(relik_out)
    index = 1
    list_len = len(relik_out)

    # Go through every sentence in one paragraph
    for sentence in relik_out:
        print(f"Extracting Entity-Relationship for {index}/{list_len}...")
        spans = sentence.spans
        ent_list = [(span.text, span.label) for span in spans]
        entities.append(ent_list)
        
        triplets = sentence.triplets
        if len(triplets) != 0:
            rel_list = [(triplet.subject.text, triplet.label, triplet.object.text, triplet.confidence) for triplet in triplets]
            relationships.append(rel_list)
        
        index += 1

    # Flatten and remove duplicates using a set
    entities_list = [(entity.lower(), label) for sublist in entities for entity, label in sublist]
    relationships_list = [(subject.lower(), label.lower(), object.lower(), float(confidence))
                                   for sublist in relationships for subject, label, object, confidence in sublist]

    # Create DataFrame
    entities_df = pd.DataFrame(entities_list, columns=['entity', 'label'])
    relationships_df = pd.DataFrame(relationships_list, columns=['subject', 'relationship', 'object', 'confidence'])

    # Keep unique entities and relationships
    entities_df.drop('label', axis=1, inplace=True)
    entities_df.drop_duplicates(subset=['entity'], inplace=True)
    entities_df = extract_entity_type(entities_df)
    entities_df.fillna('UNDEFINED', inplace=True)

    relationships_df.drop_duplicates(subset=['subject', 'relationship', 'object'], inplace=True)

    end_time = time.time()
    print(f"Execution time: {end_time - start_time:.4f} seconds")

    relationships_df['relationship'] = relationships_df['relationship'].apply(clean_relationship_type)

    return entities_df, relationships_df

def extract_entity_type(entity_df):
    # List to store extracted entities
    entity_type = {"entity":[],
                  "entity_type": []}
    
    # Load spaCy's English model
    nlp = spacy.load("en_core_web_lg")
    for entity in entity_df['entity']:
        doc = nlp(entity)  # Process each word
        for ent in doc.ents:
            entity_type['entity'].append(ent.text)
            entity_type['entity_type'].append(ent.label_)

    # Add entity type back to entity_df
    entity_df = entity_df.merge(
            pd.DataFrame({
            "entity":entity_type['entity'],
            "entity_type": entity_type['entity_type']}
                    ),
            how='left',
            on='entity'
    )

    return entity_df