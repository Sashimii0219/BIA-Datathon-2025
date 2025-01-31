import spacy, coreferee
import re

def coref_text(coref_nlp, text):
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

def clean_text(text):
    text = re.sub("\n", " ", text)
    text = re.sub(r'\b[A-Z]+\b', '', text).strip()
    text = re.sub(" +", " ", text)

    return text