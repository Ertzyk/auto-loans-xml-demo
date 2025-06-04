import os
import shutil
import pandas as pd
from xml.etree import ElementTree as ET
from settings import RAW_DATA_PATH, RAW_DATA_PATH_ABS_EE,CMBS_PICKLE_PATH, RAW_DATA_PATH_ABS_EE_CONVERTED

def convert_xml_to_df():
    # Path to your XML file
    for entry in os.scandir(RAW_DATA_PATH_ABS_EE):
        print('Beginning entry ' + entry.name)
        file_path = os.path.join(RAW_DATA_PATH_ABS_EE, entry.name)

        # Parse the XML
        tree = ET.parse(entry)
        root = tree.getroot()

        # Namespace to be used for finding elements
        # An element's name is a combination of its local name and its namespace
        namespace = {'ns': ET.ElementTree(root).getroot().tag.split('}')[0].strip('{')}

        # List to store each asset's data
        data = []

        #List to collect all the unique column names
        column_names = []

        #Search for all <assets> elements in the XML document
        for asset in root.findall('ns:assets', namespace):
            #Store the data for each asset in a dictionary
            obs = {}
            #Loop over each child element
            for elem in asset:
                #Extract the tag name without the namespace
                #For example elem.tag = {http://www.sec.gov/edgar/document/absee/autoloan/assetdata}assetTypeNumber
                tag = elem.tag.split('}')[1]
                #Collect column names
                if tag not in column_names:
                    column_names.append(tag)
                #Add element data to the dictionary
                obs[tag] = elem.text
            #Append the asset data to the data list:
            data.append(obs)

        # Create DataFrame from the list of dictionaries
        df = pd.DataFrame(data, columns=column_names)

        # Save to pickle file
        file_name_pickle = entry.name.replace('.xml', '') + '_loan' + '.pickle'
        save_location = os.path.join(CMBS_PICKLE_PATH, file_name_pickle)
        df.to_pickle(save_location)

        # Save to .csv file
        file_name_csv = entry.name.replace('.xml', '') + '_df' + '.csv'
        save_location_csv = os.path.join(RAW_DATA_PATH, file_name_csv)
        df.to_csv(save_location_csv, index=False)

        #Once I am done with the xml file, move it to the uploaded folder
        new_file_path = os.path.join(RAW_DATA_PATH_ABS_EE_CONVERTED, entry.name)
        shutil.move(file_path, new_file_path)