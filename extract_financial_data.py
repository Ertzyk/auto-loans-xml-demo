import re
import os
import pandas as pd
from bs4 import BeautifulSoup
from settings import RAW_DATA_PATH_424_B5

patterns = {
    "Total amount of loans": r"(?:Aggregate\s*Starting\s*Principal\s*Balance\s*of\s*the\s*Receivables|Total\s*Principal\s*Balance)\s*\$?\s*([\d,]+\.\d+)",
    "Total face of value of (public) bonds": r"Total\s*\$?\s*([\d,]+)",
    "Amount of proceeds from public bonds": r"proceeds\s*to\s*the\s*depositor\s*are\s*estimated\s*to\s*be\s*\$?\s*([\d,]+)",
    "Collect expenses by depositor": r"expenses\s*of\s*\$?\s*([\d,]+)",
    "Total value of public + private notes": r"fair\s*value\s*of\s*approximately\s*\$?\s*([\d,]+)",
    "Value of private notes (issuing entity certificate)": r"fair\s*value\s*of\s*approximately\s*\$?\s*([\d,]+)[^$]*\s*reserve\s*account",
    "Reserve Account value": r"Reserve\s*Account\s*\$?\s*([\d,]+\.\d+)"
}

patterns_2 = {
    "Total value of public + private notes": r"Total\s*\$?\s*([\d,]+\.\d+)\s*",
    "Value of private notes (issuing entity certificate)": r"Certificates\s*\$?\s*([\d,]+\.\d+)\s*"
}

def html_financial_data_extractor():
    data = []
    # Process each HTML file in the directory
    for filename in os.listdir(RAW_DATA_PATH_424_B5):
        if filename.endswith(".htm"):
            file_path = os.path.join(RAW_DATA_PATH_424_B5, filename)
            print(f"Processing file: {file_path}")
            data.append(process_file(file_path))
    # Create a DataFrame and save it to a CSV file
    df = pd.DataFrame(data)
    csv_output_path = os.path.join(RAW_DATA_PATH_424_B5, "extracted_financial_data.csv")
    df.to_csv(csv_output_path, index=False)

def process_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        # Read and parse HTML content
        soup = BeautifulSoup(file.read(), 'html.parser')
    text = soup.get_text()
    # Initialize results dictionary with filename
    results = {"Filename": os.path.basename(file_path)}
    # Extract data using defined patterns
    for key, pattern in patterns.items():
        match = re.search(pattern, text, re.IGNORECASE)
        results[key] = match.group(1).replace(',', '') if match else None
    # Check if additional extraction from table is needed
    if not results["Total value of public + private notes"] or not results[
        "Value of private notes (issuing entity certificate)"]:
        total, private = extract_from_second_form(text)
        if not results["Total value of public + private notes"]:
            results["Total value of public + private notes"] = total
        if not results["Value of private notes (issuing entity certificate)"]:
            results["Value of private notes (issuing entity certificate)"] = private
    return results

def extract_from_second_form(text):
    # Define the pattern to locate the table section
    pattern = re.compile(r"The\s+fair\s+value\s+of\s+the\s+notes\s+and\s+the\s+certificates\s+is\s+summarized\s+below",
                         re.IGNORECASE)
    match = pattern.search(text)
    if match:
        # Extract text following the identified section
        table_section = text[match.end():]
        results = {}
        # Extract values using defined patterns
        for key, pat in patterns_2.items():
            found = re.search(pat, table_section, re.IGNORECASE)
            if found:
                # Convert extracted value from string to float (in millions)
                value = float(found.group(1).replace(',', '')) * 1_000_000
                results[key] = str(value)
            else:
                results[key] = None
        return results.get("Total value of public + private notes"), results.get(
            "Value of private notes (issuing entity certificate)")
    return None, None