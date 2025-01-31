import pandas as pd
import spacy
import time
from relik import Relik
from relik.inference.data.objects import RelikOutput

def extract_entity_relationship(text_list, relik):
    entities = []
    relationships = []
    relik_out = relik(text_list, top_k = 3)
    print(relik_out)
    
    index = 1
    list_len = len(relik_out)

    start_time = time.time()

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
    
    # Flatten and remove duplicates using a set
    entities_list = [(entity.lower(), label) for sublist in entities for entity, label in sublist]
    relationships_list = [(subject.lower(), label.lower(), object.lower(), float(confidence))
                                   for sublist in relationships for subject, label, object, confidence in sublist]

    # Create DataFrame
    entities_df = pd.DataFrame(entities_list, columns=['Entity', 'Label'])
    relationships_df = pd.DataFrame(relationships_list, columns=['Subject', 'Relationship', 'Object', 'Confidence'])

    # Keep unique entities
    entities_df.drop('Label', axis=1, inplace=True)
    entities_df.drop_duplicates(subset=['Entity'], inplace=True)
    entities_df = extract_entity_type(entities_df)

    end_time = time.time()
    print(f"Execution time: {end_time - start_time:.4f} seconds")

    return entities_df, relationships_df

def extract_entity_type(entity_df):
    # List to store extracted entities
    entity_type = {"Entity":[],
                  "Entity Type": []}
    
    # Load spaCy's English model
    nlp = spacy.load("en_core_web_lg")
    for entity in entity_df['Entity']:
        doc = nlp(entity)  # Process each word
        for ent in doc.ents:
            entity_type['Entity'].append(ent.text)
            entity_type['Entity Type'].append(ent.label_)

    # Add entity type back to entity_df
    entity_df = entity_df.merge(
            pd.DataFrame({
            "Entity":entity_type['Entity'],
            "Entity Type": entity_type['Entity Type']}
                    ),
            how='left',
            on='Entity'
    )

    return entity_df

if __name__ == "__main__":
    relik = Relik.from_pretrained("relik-ie/relik-relation-extraction-large")
    merged_df = pd.read_csv('datasets/clean/merged_df.csv')

    text_col = merged_df['coref_text'].tolist()[:2]

    # For sentences in news_df['Text'].tolist()[:2]
    entities_df, relationships_df = extract_entity_relationship(text_col, relik)
    
    print(entities_df)
    print(relationships_df)
