import argparse
import json
import os
import re

import pandas as pd

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
    description="Filter keyword from extracted json file"
)
parser.add_argument("json_file", type=str, help="The input JSON file name")
args = parser.parse_args()

data_dir = os.path.join(os.path.dirname(__file__), "data_with_keywords", args.json_file)

# Load JSON data
with open(data_dir, "r") as file:
    data = json.load(file)

# Convert JSON data to pandas DataFrame
df = pd.json_normalize(data)

processed_df = pd.DataFrame(columns=df.columns)

results = []

for index, row in df.iterrows():
    new_row = row.copy()
    filterred_keywords = []
    for keyword_with_score in row["keywords"]:
        if keyword_with_score[1] > 0.5 and (
            keyword_with_score[0] not in finance_stop_words
        ):
            filterred_keywords.append(keyword_with_score[0])

    new_row["filterred_keywords"] = filterred_keywords

    processed_df = processed_df.append(new_row, ignore_index=True)

# Function to handle different date formats and timestamp
def parse_date(date):
    try:
        # Try the first format
        return pd.to_datetime(date, format='%d/%m/%Y %H:%M')
    except (ValueError, TypeError):
        try:
            # Try the second format
            return pd.to_datetime(date)
        except (ValueError, TypeError):
            try:
                # If the above fails, try to parse as a timestamp
                return pd.to_datetime(date, unit='s')
            except (ValueError, TypeError):
                try:
                    # Handle the format "27/03/2024 \u00a0 14:24 (GMT+07:00)"
                    cleaned_date = re.sub(r'\s+\(GMT[+-]\d{2}:\d{2}\)', '', date).strip()
                    return pd.to_datetime(cleaned_date, format='%d/%m/%Y %H:%M')
                except (ValueError, TypeError):
                    return pd.NaT

# Apply the function to the date column
processed_df['date'] = processed_df['date'].apply(parse_date)

# Create the 'year_month' column
processed_df['year_month'] = processed_df['date'].dt.strftime('%Y-%m')

json_str = processed_df.to_json(orient='records')

json_obj = json.loads(json_str)

output_file = os.path.join(
    os.path.dirname(__file__),
    "data_with_keywords",
    args.json_file.strip(".json") + "_filterred.json",
)

with open(output_file, "w") as file:
    json.dump(json_obj, file, indent=2)

