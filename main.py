import os
import sqlite3
import logging
import sys
import time
#Import the other python files
import pull_edgar
import import_raw
import convert_xml_htm
import extract_financial_data
import download_424b5_filings
from settings import DIRECTORY_LIST, LOG_OUTPUT, SQLITE_FILE, IMPORT_RAW, PULL_EDGAR_ABSEE, PULL_EDGAR_INDEX, \
    CONVERT_XML, LOAN_LEVEL_TABLE_NAME, LEASE_LEVEL_TABLE_NAME, REMOVE_LOAN_TABLE, \
    REMOVE_LEASE_TABLE, PULL_EDGAR_PROSPECTUS, EXTRACT_FINANCIAL_DATA, PULL_EDGAR_INDEX_LOANS

#Start timer
start_time = time.time()

# Create directories
for path in DIRECTORY_LIST:
    try:
        os.makedirs(path, exist_ok=True)  # Create directories recursively, no error if directory exists
    except Exception as e:
        print ("Creation of the directory " + path + " failed")
    else:
        print ("Successfully created the directory " + path)

# Remove log file if it exists
try:
    os.remove(LOG_OUTPUT)
except OSError:
    print("File " + LOG_OUTPUT + ' doesnt exist')
else:
    print("File " + LOG_OUTPUT + ' removed')

def main():
    '''This program does the main auto loan data cleaning'''
    # Set logging config
    logging.basicConfig(filename=LOG_OUTPUT, level=logging.INFO)

    # Create/Connect to the database
    conn = sqlite3.connect(SQLITE_FILE)
    cursor = conn.cursor()

    if REMOVE_LOAN_TABLE:
        try:
            cursor.execute('DROP TABLE ' + LOAN_LEVEL_TABLE_NAME)
            print("Successfully deleted the " + LOAN_LEVEL_TABLE_NAME + " table")
        except:
            print(LOAN_LEVEL_TABLE_NAME + " table already deleted")

    if REMOVE_LEASE_TABLE:
        try:
            cursor.execute('DROP TABLE ' + LEASE_LEVEL_TABLE_NAME)
            print("Successfully deleted the " + LEASE_LEVEL_TABLE_NAME + " table")
        except:
            print(LEASE_LEVEL_TABLE_NAME + " table already deleted")

    # Need to only run  once and never again
    if PULL_EDGAR_INDEX == 1:
        # Filter to include only the specific companies
        companies = {
            "World Omni Auto Receivables LLC": "0001083199"
        }
        companies = {key.lower(): value for key, value in companies.items()}

        pull_edgar.download_index(conn, cursor)
        pull_edgar.get_list_of_ciks(conn, companies)
        pull_edgar.get_list_of_urls(conn)

    if PULL_EDGAR_INDEX_LOANS == 1:
        pull_edgar.download_index(conn, cursor)
        pull_edgar.get_list_of_ciks_loan(conn)
        pull_edgar.get_list_of_urls(conn)

    if PULL_EDGAR_ABSEE == 1:
        pull_edgar.download_filings('abs_ee')

    if PULL_EDGAR_PROSPECTUS == 1:
        download_424b5_filings.download_424b5_filings()

    if EXTRACT_FINANCIAL_DATA == 1:
        extract_financial_data.html_financial_data_extractor()

    #Converts abs_ee xml files to pickle files to be used
    if CONVERT_XML == 1:
        convert_xml_htm.convert_xml_to_df()

    #Note this requires you  to have already pulled all of the data and converted to XML
    if IMPORT_RAW ==1:
        import_raw.pull_raw(conn)

    # Commit and close the connection
    conn.commit()
    conn.close()

    #Stop timer
    end_time = time.time()
    print(f"The code took {end_time - start_time} seconds to execute.")

    print("finished main")

if __name__ == '__main__':
    main()