import os, platform, time

if platform.system() == "Windows":
    ROOT = ""
    ROOT_CODE = ""
else:  
    ROOT = ""
    ROOT_CODE = ""

RAW_DATA_PATH             = os.path.join(ROOT, "raw")
RAW_DATA_PATH_ABS_EE      = os.path.join(RAW_DATA_PATH, "edgar/abs_ee")
RAW_DATA_PATH_ABS_EE_CONVERTED = os.path.join(RAW_DATA_PATH, "edgar/abs_ee_converted")
RAW_DATA_PATH_EDGAR_INDEX = os.path.join(RAW_DATA_PATH, "edgar/index")
RAW_DATA_PATH_424_B5      = os.path.join(RAW_DATA_PATH, "edgar/424_b5")
RAW_DATA_PATH_LOAN        = os.path.join(RAW_DATA_PATH, "loan")
RAW_DATA_PATH_LEASE       = os.path.join(RAW_DATA_PATH, "lease")

INTERMEDIATE_OUTPUT_PATH  = os.path.join(ROOT, "intermediate_output")
LOG_PATH                  = os.path.join(ROOT, "log")
CMBS_PICKLE_PATH          = os.path.join(ROOT, "intermediate_data/pickle/cmbs")

SQLITE_PATH               = os.path.join(ROOT, "sql_lite")
SQLITE_FILE               = os.path.join(SQLITE_PATH, "cmbs.sqlite")

DIRECTORY_LIST = [
    RAW_DATA_PATH, RAW_DATA_PATH_ABS_EE, RAW_DATA_PATH_ABS_EE_CONVERTED,
    RAW_DATA_PATH_EDGAR_INDEX, RAW_DATA_PATH_424_B5,
    RAW_DATA_PATH_LOAN, RAW_DATA_PATH_LEASE,
    CMBS_PICKLE_PATH, INTERMEDIATE_OUTPUT_PATH, LOG_PATH, SQLITE_PATH
]

date_str   = time.strftime("%Y%m%d")
LOG_OUTPUT = os.path.join(LOG_PATH, f"cmbs_log_{date_str}.log")

LOAN_LEVEL_TABLE_NAME  = "loan_table"
LEASE_LEVEL_TABLE_NAME = "lease_table"
EDGAR_INDEX_TABLE_NAME = "edgar_index"
EDGAR_CMBS_CIK_TABLE_NAME = "cmbs_cik"

IMPORT_RAW              = 0
PULL_EDGAR_ABSEE        = 0
PULL_EDGAR_INDEX        = 0
PULL_EDGAR_INDEX_LOANS  = 0
PULL_EDGAR_PROSPECTUS   = 0
CONVERT_XML             = 0
EXTRACT_FINANCIAL_DATA  = 1

REMOVE_LOAN_TABLE       = 0
REMOVE_LEASE_TABLE      = 0
SAVE_INTERMEDIATE       = 0

STRUCTURAL_ESTIMATION_CODE_PATH = os.path.join(ROOT_CODE, "structural_estimation")