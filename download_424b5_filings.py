from bs4 import BeautifulSoup
import pandas as pd
import requests
import time
import os

from settings import CMBS_PICKLE_PATH, RAW_DATA_PATH_424_B5

BASE_URL_LIMITED = 'https://www.sec.gov'
JSON_DF_424_B5 = os.path.join(CMBS_PICKLE_PATH, 'json_df_424_b5.pickle')

def download_424b5_filings():
    df_name = JSON_DF_424_B5
    text_to_look_for = '424b5'
    save_folder = RAW_DATA_PATH_424_B5
    suffix = '.htm'

    # Load dataframe with URLs
    df = pd.read_pickle(df_name)
    df.to_csv(os.path.join(CMBS_PICKLE_PATH, 'df_with_html_urls.csv'), index=False)
    start = 0
    for i in range(start, df['cik'].count()):
        # Fetch the URL from the dataframe
        html_url = df['html_location'][i]
        cik = df['cik'][i]
        com_name = df['com_name'][i]

        print(f"Starting index for 424_b5 {i}, url {html_url}")

        headers = {
            'User-Agent': '[Name] [Surname] [email]'
        }

        # Fetch HTML content
        response = requests.get(html_url, headers=headers)
        content = response.text

        # Parse the content with BeautifulSoup
        soup = BeautifulSoup(content, 'html.parser')

        # Try to locate the correct document link
        found_link = None
        for row in soup.find_all('tr'):
            link_cell = row.find('a')
            if link_cell and text_to_look_for in link_cell['href']:
                found_link = link_cell['href']
                break

        if found_link:
            # Form the complete URL
            full_url = BASE_URL_LIMITED + found_link

            # Download the document
            document_response = requests.get(full_url, headers=headers)
            filename = f"{com_name}_{cik}_{i}{suffix}"
            with open(os.path.join(save_folder, filename), 'wb') as f:
                f.write(document_response.content)
            print(f"Downloaded {full_url}")
        else:
            print(f"No document found for 424_b5 {i}")

        # Give the server a break
        time.sleep(2)