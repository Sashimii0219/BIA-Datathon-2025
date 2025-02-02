import spacy
import pandas as pd
from functions.utils import clean_relationship_type

def extract_triplets_typed(text):
    triplets = []
    relation = ''
    text = text.strip()
    current = 'x'
    subject, relation, object_, object_type, subject_type = '','','','',''

    for token in text.replace("<s>", "").replace("<pad>", "").replace("</s>", "").replace("tp_XX", "").replace("__en__", "").split():
        if token == "<triplet>" or token == "<relation>":
            current = 't'
            if relation != '':
                triplets.append({'head': subject.strip(), 'head_type': subject_type, 'type': relation.strip(),'tail': object_.strip(), 'tail_type': object_type})
                relation = ''
            subject = ''
        elif token.startswith("<") and token.endswith(">"):
            if current == 't' or current == 'o':
                current = 's'
                if relation != '':
                    triplets.append({'head': subject.strip(), 'head_type': subject_type, 'type': relation.strip(),'tail': object_.strip(), 'tail_type': object_type})
                object_ = ''
                subject_type = token[1:-1]
            else:
                current = 'o'
                object_type = token[1:-1]
                relation = ''
        else:
            if current == 't':
                subject += ' ' + token
            elif current == 's':
                object_ += ' ' + token
            elif current == 'o':
                relation += ' ' + token
    if subject != '' and relation != '' and object_ != '' and object_type != '' and subject_type != '':
        triplets.append({'head': subject.strip(), 'head_type': subject_type, 'type': relation.strip(),'tail': object_.strip(), 'tail_type': object_type})
    return triplets


def rebel_extract_entity_relationship(text_col, tokenizer, model):

    all_triplets = []

    gen_kwargs = {
    "max_length": 256,
    "length_penalty": 0,
    "num_beams": 5,
    "num_return_sequences": 5,
    "forced_bos_token_id": None,
    }

    for text in text_col:
        #Tokenizer text
        model_inputs = tokenizer(text, 
                                 max_length=256, 
                                 padding=True, 
                                 truncation=True, 
                                 return_tensors = 'pt')
        
        # Generate Tokens
        generated_tokens = model.generate(
            model_inputs["input_ids"].to(model.device),
            attention_mask=model_inputs["attention_mask"].to(model.device),
            decoder_start_token_id = tokenizer.convert_tokens_to_ids("tp_XX"),
            **gen_kwargs,
        )

        # Extract text
        decoded_preds = tokenizer.batch_decode(generated_tokens, skip_special_tokens=False)

        for idx, sentence in enumerate(decoded_preds):
            triplets = extract_triplets_typed(sentence)

            for triplet in triplets:
                all_triplets.append(triplet)
    
    # For aligning output with other models
    relationships_df = pd.DataFrame(
    [{
        'subject': triplet['head'],
        'subject_entity_type': triplet['head_type'].upper(),
        'relationship': triplet['type'],
        'object': triplet['tail'],
        'object_entity_type': triplet['tail_type'].upper(),
        'confidence': 'UNDEFINED'  # Adding a confidence column with default value
    } for triplet in all_triplets]
        )
    
    relationships_df['confidence'] = 'UNDEFINED'
    relationships_df['relationship'] = relationships_df['relationship'].apply(clean_relationship_type)

    entities_df = pd.concat(
        [
            relationships_df[["subject", "subject_entity_type"]].rename(
                columns={"subject":"entity", "subject_entity_type":"entity_type"}),
            relationships_df[["object", "object_entity_type"]].rename(
                columns={"object":"entity", "object_entity_type":"entity_type"})
        ]
    ).drop_duplicates(subset=['entity'])

    return entities_df, relationships_df