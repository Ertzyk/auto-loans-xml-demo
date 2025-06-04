"""
Adapted from the example code in the python-edgar project
https://github.com/greedo/python-edgar

Original code © 2017-2025 python-edgar contributors, MIT License.
Modifications © 2025 Your Name, released under the same licence.
"""
import pandas as pd
import os
import time
import requests
import edgar

from settings import RAW_DATA_PATH_ABS_EE, CMBS_PICKLE_PATH, RAW_DATA_PATH_EDGAR_INDEX, EDGAR_INDEX_TABLE_NAME, \
    EDGAR_CMBS_CIK_TABLE_NAME, INTERMEDIATE_OUTPUT_PATH

#Example directory https://www.sec.gov/Archives/edgar/data/1740040/000105640420009408/index.html

BASE_URL = 'https://www.sec.gov/Archives/'
BASE_URL_LIMITED = 'https://www.sec.gov'
CIK_DF = os.path.join(CMBS_PICKLE_PATH,'cik_df.pickle')
CIK_CSV = os.path.join(INTERMEDIATE_OUTPUT_PATH,'cik_df.csv')
JSON_DF_ABS_EE = os.path.join(CMBS_PICKLE_PATH, 'json_df_abs_ee.pickle')
JSON_DF_424_B5 = os.path.join(CMBS_PICKLE_PATH, 'json_df_424_b5.pickle')

# Remove these exact matches
EXACT_MATCHES = ['afs sensub corp.', 'fifth third holdings funding, llc', \
        'gs mortgage securities corp ii','deutsche mortgage & asset receiving corp', \
        'jp morgan chase commercial mortgage securities corp', \
        'citigroup commercial mortgage securities inc', \
        'ubs commercial mortgage securitization corp.', \
        'credit suisse commercial mortgage securities corp.', \
        'wells fargo commercial mortgage securities inc', \
        'bank of america merrill lynch commercial mortgage inc.', \
        'barclays commercial mortgage securities llc', \
        'ccre commercial mortgage securities, l.p.', \
        'morgan stanley capital i inc.', \
        'california republic funding llc', \
        'banc of america merrill lynch commercial mortgage inc.', \
        'gnmag asset backed securitizations, llc', \
        '3650 reit commercial mortgage securities ii llc', \
        'bmo commercial mortgage securities llc', \
        'efcar, llc']

def download_filings(filing_type):
    # JSON_DF, contains string, and save location
    df_name = JSON_DF_ABS_EE
    text_to_look_for = ['exh_102.xml', 'ex102']
    save_folder = RAW_DATA_PATH_ABS_EE
    suffix = '.xml'
    # Read in the urls
    df = pd.read_pickle(df_name)
    df.to_csv(os.path.join(CMBS_PICKLE_PATH, 'df_with_urls.csv'), index=False)
    # In case it fails mid download, can start off where left off before. It might fail because of the request to the server
    start = 0
    for i in range(start, df['cik'].count()):
        # Get relevant information from DB
        json_url = df['json_location'][i]
        cik = df['cik'][i]
        com_name = df['com_name'][i]
        # Print to commandline to see where problems occur
        print('Starting index for ' + filing_type + ' ' + str(i) + ', url ' + json_url)
        headers = {
            'User-Agent': '[Name] [Surname] [email]'
        }
        # Get the content of the .json file
        response = requests.get(json_url, headers=headers)
        content = response.json()
        # Give the server a break for 2 seconds
        time.sleep(2)
        # Loop over files until I find the right one
        for file in content['directory']['item']:
            # only keep file with correct name
            for text in text_to_look_for:
                if text in file['name']:
                    abs_ee_xml_url = BASE_URL_LIMITED + content['directory']['name'] + '/' + file['name']
        # Now I have the url for the xml. From here we will download the xml file
        # Get the accession_no (which is to name the files)
        accession_no = abs_ee_xml_url.split("/")[7]
        file_name = str(cik) + '_' + accession_no + '_' + com_name + suffix
        save_file_loc = os.path.join(save_folder, file_name)

        # Pull the xml from the url
        response = requests.get(abs_ee_xml_url, headers=headers)
        #Check if it has been blocked by the SEC
        if 'Undeclared Automated Tool' in str(response.content,'utf-8'):
            print('SEC blocked the request')
            exit()
        # Save the file
        with open(save_file_loc, 'wb') as file:
            file.write(response.content)
        # Give the server a break for 2 seconds
        time.sleep(2)
        print('Finished ' + save_file_loc)
    print('Finsihed downloading XML files for ' + filing_type)

def download_index(conn,cursor):
    '''Downloads the index files. Then need to run bash code to stich them together
    follow instructions from here https://pypi.org/project/python-edgar/: Below are important exampls parts'''
    #The entirety of this code
    user_agent = "[Name] [Surname] [email]"
    edgar.download_index(RAW_DATA_PATH_EDGAR_INDEX,2020,user_agent)

    #Upload the tsv files to a database so they can be queried
    #Flag it and will change to zero later
    first_tsv = 1
    #Loop over all .tsv files in the folder
    for entry in os.scandir(RAW_DATA_PATH_EDGAR_INDEX):
        if entry.path.endswith(".tsv"):
            #test_file = os.path.join(RAW_DATA_PATH_EDGAR_INDEX,'2016-QTR1.tsv')
            col_names = ['cik','com_name','form','filing_date','txt_location','html_location']
            df = pd.read_csv(entry,sep='|',names =col_names)
            df['json_location'] = BASE_URL + \
                df['html_location'].str.replace('-index.html','/index.json').str.replace('-','')
            df['html_location'] = BASE_URL + df['html_location']
            df = df.drop(columns=['txt_location'])
            # Get the accession_no (which is to name the files)
            df['accession_no'] = df['html_location'].str.split("/").str[7]
            df['accession_no'] = df['accession_no'].str.replace('-index.html','').str.replace('-','')

            if first_tsv == 1:
                #Replace the table
                df.to_sql(name=EDGAR_INDEX_TABLE_NAME, con=conn, if_exists="replace", index=False)
            else:
                # Append this disclosure to the database
                df.to_sql(name=EDGAR_INDEX_TABLE_NAME, con=conn, if_exists="append", index=False)
            first_tsv = 0
    #Make the form an index on the table
    create_index = 'CREATE INDEX form_index ON ' + EDGAR_INDEX_TABLE_NAME  +'(form)'
    cursor.execute(create_index)
    print('Finished uploading index files to DB')

def get_list_of_ciks(conn, companies):
    #Pull all of the firms that have ABS-EE filings and will do filtering by hand
    query = 'SELECT DISTINCT com_name, cik FROM ' + EDGAR_INDEX_TABLE_NAME + ' WHERE form = "ABS-EE";'
    df = pd.read_sql_query(query, conn)
    #Change to lower case
    df['com_name'] = df['com_name'].str.lower().str.strip()
    # Format CIK values with leading zeros
    df['cik'] = df['cik'].apply(lambda x: str(x).zfill(10))
    # Filter DataFrame to only include rows with the specified company names
    df = df[df['com_name'].isin(companies.keys())]
    # Verify and correct CIKs if necessary
    df = df[df['cik'].isin(companies.values())]
    # Save the filtered CIKs to a CSV and pickle file
    df.to_csv(CIK_CSV, index=False)
    df.to_pickle(CIK_DF)
    # Store the filtered CIKs in the database
    df.to_sql(name=EDGAR_CMBS_CIK_TABLE_NAME, con=conn, if_exists="replace", index=False)
    print('Finished getting CIKs')

def get_list_of_ciks_loan(conn):
    #filing_date is in the correct format
    #Pull all of the firms that have ABS-EE filings and will do filtering by hand
    query = 'SELECT  com_name,cik,min(filing_date) FROM ' + EDGAR_INDEX_TABLE_NAME + \
            ' where form = "ABS-EE" group by cik;'
    df = pd.read_sql_query(query, conn)
    #Change to lower case
    df['com_name'] = df['com_name'].str.lower().str.strip()
    #Remove all files with these words
    list_of_words = ['mortgage','lease','leasing','morgan stanley','bnk','llc','corp',\
                     'gnmag asset backed securitizations']
    for word in list_of_words:
        df = df[~df['com_name'].str.contains(word,na=False)]
    for word in EXACT_MATCHES:
        df = df[df['com_name']!=word]
    #Now I have my list of CIKs
    df.to_pickle(CIK_DF)
    df.to_csv(CIK_CSV,index=False)
    df.to_sql(name=EDGAR_CMBS_CIK_TABLE_NAME, con=conn, if_exists="replace", index=False)
    print('Finished getting CIKs')

def get_list_of_urls(conn):
    '''Will use the list of CIKs and query from the database for all ABS-EE, 10-D, and 424B5'''
    for filing_type in ['abs_ee', '424_b5']:
        if filing_type == 'abs_ee':
            form = 'ABS-EE'
            pickle_file = JSON_DF_ABS_EE
            location = 'json'
        elif filing_type == '424_b5':
            form = '424B5'
            pickle_file = JSON_DF_424_B5
            location = 'html'
        sub_query = '(SELECT DISTINCT cik FROM ' + EDGAR_CMBS_CIK_TABLE_NAME + ')'
        query = 'SELECT DISTINCT a.com_name, a.cik, a.filing_date, a.form, a.'+location+'_location FROM ' + EDGAR_INDEX_TABLE_NAME + \
                " as a INNER JOIN " + sub_query + ' as b ON a.cik = b.cik where a.form = "' + form + '"' + ';'
        df = pd.read_sql_query(query, conn)
        df.to_pickle(pickle_file)
    print('Finished getting list of URLs')