import argparse
import json
import os
from typing import Union

import nltk
import pandas as pd
from keybert import KeyBERT
from keyphrase_vectorizers import KeyphraseCountVectorizer

finance_stop_words = [
    "company",
    "companies",
    "stock",
    "stocks",
    "share",
    "shares",
    "market",
    "markets",
    "exchange",
    "exchanges",
    "price",
    "prices",
    "pricing",
    "trade",
    "trades",
    "trading",
    "financial",
    "finance",
    "finances",
    "fiscal",
    "monetary",
    "investor",
    "investors",
    "investment",
    "investments",
    "fund",
    "funds",
    "funding",
    "economy",
    "economic",
    "economies",
    "bank",
    "banks",
    "banking",
    "revenue",
    "revenues",
    "profit",
    "profits",
    "loss",
    "losses",
    "dividend",
    "dividends",
    "quarter",
    "quarterly",
    "annual",
    "annually",
    "report",
    "reports",
    "reported",
    "earning",
    "earnings",
    "forecast",
    "forecasts",
    "forecasting",
    "growth",
    "decline",
    "percentage",
    "percent",
    "rise",
    "rises",
    "rising",
    "fall",
    "falls",
    "falling",
    "increase",
    "increases",
    "increasing",
    "decrease",
    "decreases",
    "decreasing",
    "up",
    "down",
    "gain",
    "gains",
    "gaining",
    "drop",
    "drops",
    "dropping",
    "billion",
    "million",
    "trillion",
    "dollar",
    "dollars",
    "year",
    "years",
    "month",
    "months",
    "day",
    "days",
    "time",
    "period",
    "vietnamese business",
    "vietnamese enterprise",
    "vietnamese economy",
    "vietnamese market",
    "asean",
    "vietnamese exporter",
    "vietnamese export",
]

parser = argparse.ArgumentParser(
    description="Extract keyword from json file and store to data_with_keywords folder "
)
parser.add_argument("json_file", type=str, help="The input JSON file name")
args = parser.parse_args()

nltk.download("stopwords")
nltk.download("wordnet")
nltk.download("averaged_perceptron_tagger")
nltk.download("punkt")

kw_model = KeyBERT()
stop_words = nltk.corpus.stopwords.words("english")

stop_words.extend(["from", "re", "also"])
stop_words.extend(finance_stop_words)

data_dir = os.path.join(os.path.dirname(__file__), "data", args.json_file)

# Load JSON data
with open(data_dir, "r") as file:
    data = json.load(file)

# Convert JSON data to pandas DataFrame
df = pd.json_normalize(data)


def lemmatize(
    text: str, lemmatizer=nltk.stem.WordNetLemmatizer(), to_text: bool = True
) -> Union[pd.DataFrame, str]:
    """Lemmatize the words in a sentence by:
         - mapping the POS tag to each word,
         - lemmatize the word.

     Arguments:
         sentence (str):
             The sentence in which the words need to be lemmatized
         lemmatizer:
             Lemmatizer function

    Raises:
         TypeError: if input parameters are not of the expected type

     Returns:
         Lemmatized text
     ----------------------------------------------------------------------------------------
    """

    if not isinstance(text, str):
        raise TypeError("Argument 'text' must be a string.")

    lemmas = []
    tag_dict = {
        "J": nltk.corpus.wordnet.ADJ,
        "N": nltk.corpus.wordnet.NOUN,
        "V": nltk.corpus.wordnet.VERB,
        "R": nltk.corpus.wordnet.ADV,
    }

    tokenized_words = nltk.word_tokenize(text)
    for tokenized_word in tokenized_words:
        tag = nltk.tag.pos_tag([tokenized_word])[0][1][
            0
        ].upper()  # Map POS tag to first character lemmatize() accepts
        wordnet_pos = tag_dict.get(tag, nltk.corpus.wordnet.NOUN)
        lemma = lemmatizer.lemmatize(tokenized_word, wordnet_pos)
        lemmas.append(lemma)

    df = pd.DataFrame({"Original": tokenized_words, "Lemmatized": lemmas})

    if to_text:
        lemmatized_str = df["Lemmatized"].str.cat(sep=" ")
        return lemmatized_str
    else:
        return df


def keybert_keywords(list_of_text) -> list:
    if isinstance(list_of_text, list):
        text = " ".join(list_of_text)
        try:
            print("trying to extract keywords")
            keywords = kw_model.extract_keywords(
                lemmatize(text),
                keyphrase_ngram_range=(2, 4),
                vectorizer=KeyphraseCountVectorizer(),
                use_mmr=True,
                diversity=0.4,
                top_n=200,
            )
            print(f"Found keywords: {keywords}")
            # lst = []
            # for key, score in keywords:
            #     lst.append(key)
            return keywords
        except Exception as e:
            print(f"Exception during keyword extraction: {e}")
            return []
    else:
        return []


def process_row(row):
    content = row["content"]
    keywords = keybert_keywords(content)
    row["keywords"] = keywords
    return row.to_dict()


output_file = os.path.join(
    os.path.dirname(__file__),
    "data_with_keywords",
    args.json_file.strip(".json") + "_keywords.json",
)

open(output_file, "w").close()

results = []
for _, row in df.iterrows():
    results.append(process_row(row))

with open(output_file, "w") as f:
    json.dump(results, f, indent=2)
