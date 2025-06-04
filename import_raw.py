import os
import numpy as np
import pandas as pd
import logging
import shutil
from settings import RAW_DATA_PATH, INTERMEDIATE_OUTPUT_PATH, RAW_DATA_PATH_LOAN, RAW_DATA_PATH_LEASE, \
    LOAN_LEVEL_TABLE_NAME, LEASE_LEVEL_TABLE_NAME, SAVE_INTERMEDIATE

#These variables are stored as floats
SQL_LL_VARS_FLOAT = ["orig_term","orig_amount", "orig_int_rate", \
            "orig_int_only_term","underwriting", "grace_period_months","origination_value", \
            "credit_score","cosigner", "payment_to_income","remaining_term", \
            "principal_outstanding_beg", "amount_due_next_period","subject_to_demand", \
            "int_rate", "next_int_rate", "servicing_fee_rate", "servicing_flat_fee", \
            "servicing_fee_other", "servicing_fee_uncollected", "interest_amount", \
            "principal_amount", "principal_adjustments", "balance_remaining_end", \
            "scheduled_payment_amount", "payment_collected", "interest_collected", \
            "principal_collected", "other_collected", "advanced_amount", \
            "charge_off_amount", "charge_off_recovered","num_months_extended","repossessed", \
            "lease_resid_value","lease_contract_resid_value","lease_resid_value_type",  \
            "lease_secur_value_amount","lease_secur_discount_rate","balance_remaining_end", \
            "lease_secur_remaining_end","lease_excess_fee","repossessed_proceeds", \
            "lease_termination","lease_liquid_proceeds","asset_added", "modification", \
            "sub_to_dem_repurchase_amount","delinquency_status"]

ORIG_CODE_VARS = ['paymentTypeCode', 'assetSubjectDemandStatusCode', \
            'servicingAdvanceMethodCode', 'vehicleNewUsedCode', 'vehicleTypeCode', \
            'vehicleValueSourceCode''originalInterestRateTypeCode', 'interestCalculationTypeCode', \
            'obligorIncomeVerificationLevelCode', 'obligorEmploymentVerificationCode', \
            'repurchaseReplacementReasonCode', \
            'modificationTypeCode', 'zeroBalanceCode','baseResidualSourceCode','terminationIndicator', \
            'lesseeIncomeVerificationLevelCode','lesseeEmploymentVerificationCode', \
            'repurchaseOrReplacementReasonCode','modificationTypeCode']

BOOLEAN_VARS = ['underwriting','subject_to_demand','cosigner','asset_added', 'modification','repossessed']

ALL_LOAN_COLUMNS = ['subsidy', 'maturity_date', 'model_name', 'credit_score', 'balance_remaining_end',
                    'interest_amount', 'interest_collected', 'payment_to_income', 'int_rate', 'delinquency_status',
                    'report_beg_date', 'repossessed', 'manufacturer', 'remaining_term', 'payment_collected',
                    'principal_collected', 'principal_adjustments', 'charge_off_recovered', 'origination_date',
                    'orig_amount', 'origination_value_source', 'orig_file_name', 'orig_int_rate_type',
                    'principal_outstanding_beg', 'orig_first_payment_date', 'servicing_fee_rate', 'subject_to_demand',
                    'zero_balance', 'originator', 'state', 'new_used', 'orig_term', 'credit_score_type', 'accrual_type',
                    'principal_amount', 'underwriting', 'orig_int_rate', 'origination_value', 'lease_ind', 'type',
                    'asset_num', 'amount_due_next_period', 'other_collected', 'scheduled_payment_amount',
                    'repossessed_proceeds', 'cosigner', 'next_int_rate', 'income_verification', 'servicer',
                    'advanced_amount', 'zero_balance_date', 'servicing_fee_uncollected', 'payment_type',
                    'servicing_advance', 'grace_period_months', 'asset_num_type', 'report_end_date',
                    'paid_through_date', 'charge_off_amount', 'num_months_extended', 'modification',
                    'employment_verification', 'modification_type', 'asset_added', 'model_year']

ALL_LEASE_COLUMNS = ['subsidy', 'maturity_date', 'model_name', 'credit_score', 'balance_remaining_end',
                     'payment_to_income', 'lease_secur_value_amount', 'delinquency_status', 'report_beg_date',
                     'manufacturer', 'remaining_term', 'lease_liquid_proceeds', 'payment_collected', 'origination_date',
                     'orig_amount', 'origination_value_source', 'orig_file_name', 'subject_to_demand',
                     'orig_first_payment_date', 'servicing_fee_rate', 'zero_balance', 'originator', 'state',
                     'lease_secur_remaining_end', 'orig_term', 'credit_score_type', 'new_used',
                     'lease_resid_value_type', 'underwriting', 'lease_secur_discount_rate', 'origination_value',
                     'lease_ind', 'type', 'asset_num', 'amount_due_next_period', 'other_collected',
                     'scheduled_payment_amount', 'cosigner', 'income_verification', 'servicer', 'zero_balance_date',
                     'servicing_fee_uncollected', 'payment_type', 'servicing_advance', 'grace_period_months',
                     'lease_termination', 'lease_contract_resid_value', 'asset_num_type', 'report_end_date',
                     'lease_excess_fee', 'paid_through_date', 'charge_off_amount', 'num_months_extended',
                     'modification', 'employment_verification', 'modification_type', 'asset_added', 'model_year',
                     'lease_resid_value']
def pull_raw(conn):
    # Loop over all of the files in the raw data directory
    for entry in os.scandir(RAW_DATA_PATH):
        if entry.path.endswith(".csv") & entry.is_file():
            # Import data
            file_path = os.path.join(RAW_DATA_PATH, entry.name)
            #If the CSV is empty then catch the exception
            if os.stat(file_path).st_size != 0:
                #on_bad_lines='skip' tells pd.read_csv to skip lines with too many fields.
                df = pd.read_csv(file_path, on_bad_lines='skip')
                #Only the auto loans files have subvented variable
                if 'subvented' in df.columns:
                    #Some mortgage files accidentally have subvented variable but always nan. So exclude those
                    if ((all(elem == "-" for elem in df['subvented']) == False) \
                           and (all(elem == "" for elem in df['subvented']) ==False)):
                        #Clean the raw data file
                        df = clean_raw_disclosures(df,file_path,entry.name)
                        if df['lease_ind'].mean() == 0:
                            # Add missing loan columns
                            for col in ALL_LOAN_COLUMNS:
                                if col not in df.columns:
                                    df[col] = np.nan
                            # Load loan DataFrame into the database
                            df.to_sql(name=LOAN_LEVEL_TABLE_NAME, con=conn, if_exists="append", index=False,
                                      index_label=['orig_file_name', 'asset_num'])
                            # Move the processed file to the loan folder
                            new_file_path = os.path.join(RAW_DATA_PATH_LOAN, entry.name)
                            shutil.move(file_path, new_file_path)
                        elif df['lease_ind'].mean() == 1:
                            # Add missing lease columns
                            for col in ALL_LEASE_COLUMNS:
                                if col not in df.columns:
                                    df[col] = np.nan
                            # Load lease DataFrame into the database
                            df.to_sql(name=LEASE_LEVEL_TABLE_NAME, con=conn, if_exists="append", index=False,
                                      index_label=['orig_file_name', 'asset_num'])
                            # Move the processed file to the lease folder
                            new_file_path = os.path.join(RAW_DATA_PATH_LEASE, entry.name)
                            shutil.move(file_path, new_file_path)
                        if SAVE_INTERMEDIATE == 1:
                            file_name = "intermediate_" + entry.name
                            intermediate_data_path = os.path.join(INTERMEDIATE_OUTPUT_PATH, file_name)
                            df = df[:100]
                            df.to_csv(intermediate_data_path, index=False)
                            print("successfully saved " + file_name)

    # Table name to query
    loan_table_name = 'loan_table'
    loan_query_all = f"SELECT * FROM {loan_table_name}"
    loan_specific_asset_numbers = [714052, 748653, 632930]
    loan_query_filtered = f"SELECT * FROM {loan_table_name} WHERE asset_num IN ({', '.join(map(str, loan_specific_asset_numbers))})"
    loan_query = loan_query_filtered
    # Execute the query and load the result into a DataFrame
    df = pd.read_sql(loan_query, conn)
    # Save the result to a CSV file
    df.to_csv('loan_query_result.csv', index=False)
    print("Query completed and results saved to 'loan_query_result.csv'")

    # Table name to query
    lease_table_name = 'lease_table'
    lease_query_all = f"SELECT * FROM {lease_table_name}"
    lease_specific_asset_numbers = [237278630, 267663368, 271106294]
    lease_query_filtered = f"SELECT * FROM {lease_table_name} WHERE asset_num IN ({', '.join(map(str, lease_specific_asset_numbers))})"
    lease_query = lease_query_filtered
    # Execute the query and load the result into a DataFrame
    df = pd.read_sql(lease_query, conn)
    # Save the result to a CSV file
    df.to_csv('lease_query_result.csv', index=False)
    print("Query completed and results saved to 'lease_query_result.csv'")

def clean_raw_disclosures(raw_df,file,file_name):
    '''This program takes a raw disclosure csv and cleans the data
    Will probably upload to a database later
    base on https://www.law.cornell.edu/cfr/text/17/229.1125
    #downloaded from https://www.sec.gov/info/edgar/specifications/absxml.htm'''
    print("Beginning to clean " + file)
    logging.warning("Beginning to clean " + file)
    #Set the df we work with to the raw data frame
    df = raw_df

    #Creates an indicator for whether it is a lease or loan file
    df = create_lease_ind(df)

    #Confirm it is either a loan or lease file, not both!
    assert (df['lease_ind'].mean() == 0 or df['lease_ind'].mean() == 1), \
        "Warning, not all observations are either lease or loan"
    #Convert subvent to string because of how some entries are given
    df['subvented']= df['subvented'].astype(str)
    #For code variables, need to convert them to numeric in case there are missing values,
    #causing pandas to read as string
    for var in ORIG_CODE_VARS:
        if var in df.columns:
            df[var] = pd.to_numeric(df[var],errors='coerce')
    #Turn codes into usable variables
    df = define_codes(df)
    #Rename other variables into a better convention
    df = relabel(df)
    df = convert_booleans(df)
    #Convert numeric variables to numeric
    df = convert_numeric(df)

    #Normalize variables to percentages
    #Only loan files have these variables
    if df['lease_ind'].mean() == 0:
        if 'int_rate' in df.columns:
            df['int_rate'] = 100 * df['int_rate']
        if 'orig_int_rate' in df.columns:
            df['orig_int_rate'] = 100 * df['orig_int_rate']
        if 'next_int_rate' in df.columns:
            df['next_int_rate'] = 100 * df['next_int_rate']
    if 'servicing_fee_rate' in df.columns:
        df['servicing_fee_rate'] = 100 * df['servicing_fee_rate']

    #Add file name to the dataframe
    df['orig_file_name'] = file_name
    return df

def adjust_misreported_percentages(df):
    '''There are four variables that are occasionally reported as percentages, and
    thus need to be adjusted because we account for them being reported as decimals. Also makes annual_income
    Note this is used in later programs because
    the database was already fully created when I added this'''
    #We use 50 as the cutoff. An interest rate (servicing rate) above 50% is most definitely misreported
    #Only loan files have these variables
    if 'int_rate' in df.columns:
        df.loc[df['int_rate'] >50,'int_rate'] *= 1/100
        df.loc[df['orig_int_rate'] >50,'orig_int_rate'] *= 1/100
        df.loc[df['next_int_rate'] >50,'next_int_rate'] *= 1/100
    df.loc[df['servicing_fee_rate'] > 50, 'servicing_fee_rate'] *= 1/100

    #A lot of observations will have payment_to_income as 999.99 originally
    #This probably means missing
    df.loc[(df['payment_to_income'] >= 999), 'payment_to_income'] = np.nan
    #Sometimes we  have payment_to_income = scheduled_payment_amount, which probably means payment_to_income is wrong
    df.loc[(df['payment_to_income'] == df['scheduled_payment_amount']), 'payment_to_income'] = np.nan
    #If payment to income is zero, it is probably missing
    df.loc[(df['payment_to_income'] == 0), 'payment_to_income'] = np.nan
    #If it is greater than 1, it is must be a percentage
    df.loc[df['payment_to_income'] > 1, 'payment_to_income'] *= 1 / 100
    #Create annual_income variable
    df['annual_income'] = round(12*df['scheduled_payment_amount']/df['payment_to_income'])
    df.loc[(df['amount_due_next_period'] == 0) | (np.isnan(df['amount_due_next_period'])), 'annual_income'] = np.nan
    df.loc[(df['scheduled_payment_amount'] == 0), 'annual_income'] = np.nan
    df.loc[(df['scheduled_payment_amount'] == 0), 'annual_income'] = np.nan

    return df

def convert_booleans(df):
    '''This converts booleans to 0/1 indicators'''
    for var in BOOLEAN_VARS:
        if var in df.columns:
            #First convert the variable to a string
            df[var] =  df[var].apply(str)
            #Then use a dictionary to convert it to string. Later in the code I will force it into a numeric variable
            df[var] = df[var].replace({'True': '1','TRUE': '1','true': '1','False': '0','FALSE': '0','false': '0'})
    return df

def convert_numeric(df):
    '''This converts variables to numeric if they are object types'''
    #Force to numeric for numeric vars
    for var in SQL_LL_VARS_FLOAT:
        if var in df.columns:
            df[var] = pd.to_numeric(df[var],errors='coerce')
    return df

def relabel(df):
    #Relabels variables into a naming convention I like
    # These are the variables for the loan files
    df = df.rename(columns={"assetTypeNumber": "asset_num_type", \
                            "assetNumber": "asset_num", \
                            "reportingPeriodBeginningDate": "report_beg_date", \
                            "reportingPeriodEndingDate": "report_end_date", \
                            "originatorName": "originator", \
                            "originationDate": "origination_date", \
                            "originalLoanTerm": "orig_term", \
                            "originalLoanAmount": "orig_amount", \
                            "loanMaturityDate": "maturity_date", \
                            "originalInterestRatePercentage": "orig_int_rate", \
                            "originalInterestOnlyTermNumber": "orig_int_only_term", \
                            "originalFirstPaymentDate": "orig_first_payment_date", \
                            "underwritingIndicator": "underwriting", \
                            "gracePeriodNumber": "grace_period_months", \
                            "vehicleManufacturerName": "manufacturer", \
                            "vehicleModelName": "model_name", \
                            "vehicleModelYear": "model_year", \
                            "vehicleValueAmount": "origination_value", \
                            "obligorCreditScoreType": "credit_score_type", \
                            "obligorCreditScore": "credit_score", \
                            "coObligorIndicator": "cosigner", \
                            "paymentToIncomePercentage": "payment_to_income", \
                            "obligorGeographicLocation": "state", \
                            "assetAddedIndicator": "asset_added", \
                            "remainingTermToMaturityNumber": "remaining_term", \
                            "reportingPeriodModificationIndicator": "modification", \
                            "reportingPeriodBeginningLoanBalanceAmount": "principal_outstanding_beg", \
                            "nextReportingPeriodPaymentAmountDue": "amount_due_next_period", \
                            "reportingPeriodInterestRatePercentage": "int_rate", \
                            "nextInterestRatePercentage": "next_int_rate", \
                            "servicingFeePercentage": "servicing_fee_rate", \
                            "servicingFlatFeeAmount": "servicing_flat_fee", \
                            "otherServicerFeeRetainedByServicer": "servicing_fee_other", \
                            "otherAssessedUncollectedServicerFeeAmount": "servicing_fee_uncollected", \
                            "scheduledInterestAmount": "interest_amount", \
                            "scheduledPrincipalAmount": "principal_amount", \
                            "otherPrincipalAdjustmentAmount": "principal_adjustments", \
                            "reportingPeriodActualEndBalanceAmount": "balance_remaining_end", \
                            "reportingPeriodScheduledPaymentAmount": "scheduled_payment_amount", \
                            "totalActualAmountPaid": "payment_collected", \
                            "actualInterestCollectedAmount": "interest_collected", \
                            "actualPrincipalCollectedAmount": "principal_collected", \
                            "actualOtherCollectedAmount": "other_collected", \
                            "servicerAdvancedAmount": "advanced_amount", \
                            "interestPaidThroughDate": "paid_through_date", \
                            "zeroBalanceEffectiveDate": "zero_balance_date", \
                            "currentDelinquencyStatus": "delinquency_status", \
                            "primaryLoanServicerName": "servicer", \
                            "mostRecentServicingTransferReceivedDate": "servicing_transfer_date", \
                            "assetSubjectDemandIndicator": "subject_to_demand", \
                            "repurchaseAmount": "sub_to_dem_repurchase_amount", \
                            "DemandResolutionDate": "sub_to_dem_resolution_date", \
                            "repurchaserName": "sub_to_dem_repurchaser", \
                            "chargedoffPrincipalAmount": "charge_off_amount", \
                            "recoveredAmount": "charge_off_recovered", \
                            "paymentExtendedNumber": "num_months_extended", \
                            "repossessedIndicator": "repossessed", \
                            "repossessedProceedsAmount": "repossessed_proceeds" \
                            })

    # Variable names for automobile leases
    df = df.rename(columns={"reportingPeriodBeginDate": "report_beg_date", \
                            "reportingPeriodEndDate": "report_end_date", \
                            "acquisitionCost": "orig_amount", \
                            "originalLeaseTermNumber": "orig_term", \
                            "scheduledTerminationDate": "maturity_date", \
                            "gracePeriod": "grace_period_months", \
                            "baseResidualValue": "lease_resid_value", \
                            "contractResidualValue": "lease_contract_resid_value", \
                            "lesseeCreditScoreType": "credit_score_type", \
                            "lesseeCreditScore": "credit_score", \
                            "coLesseePresentIndicator": "cosigner", \
                            "lesseeGeographicLocation": "state", \
                            "remainingTermNumber": "remaining_term", \
                            "reportingPeriodSecuritizationValueAmount": "lease_secur_value_amount", \
                            "securitizationDiscountRate": "lease_secur_discount_rate", \
                            "otherLeaseLevelServicingFeesRetainedAmount": "servicing_fee_other", \
                            "reportingPeriodEndingActualBalanceAmount": "balance_remaining_end", \
                            "reportingPeriodEndActualSecuritizationAmount": "lease_secur_remaining_end", \
                            "paidThroughDate": "paid_through_date", \
                            "primaryLeaseServicerName": "servicer", \
                            "demandResolutionDate": "sub_to_dem_resolution_date", \
                            "chargedOffAmount": "charge_off_amount", \
                            "leaseExtended": "num_months_extended", \
                            "excessFeeAmount": "lease_excess_fee", \
                            "liquidationProceedsAmount": "lease_liquid_proceeds", \
                            })
    return df

def define_codes(df):
    # Define the dictionaries for mapping
    payment_type_dict = {1: 'Bi-Weekly', 2: 'Monthly', 3: 'Quarterly', 4: 'Balloon', 98: 'Other'}
    sub_to_dem_status_dict = {0: 'Asset Pending Repurchase', 1: 'Asset Was Repurchased', 2: 'Demand in Dispute',
                              3: 'Demand Withdrawn', 4: 'Demand Rejected', 98: 'Other'}
    servicing_advance_dict = {1: 'No advancing', 2: 'Interest only', 3: 'Principal only',
                              4: 'Principal and Interest', 99: 'Unavailable'}
    new_used_dict = {1: 'New', 2: 'Used'}
    type_dict = {1: 'Car', 2: 'Truck', 3: 'SUV', 4: 'Motorcycle', 98: 'Other', 99: 'Unknown'}
    origination_value_source_dict = {1: 'Invoice Price', 2: 'MSRP', 3: 'Kelly Blue Book', 98: 'Other'}

    # Apply the mappings
    df['payment_type'] = df['paymentTypeCode'].map(payment_type_dict)

    if 'assetSubjectDemandStatusCode' in df.columns:
        print("Column 'assetSubjectDemandStatusCode' is present in the DataFrame.")
        df['sub_to_dem_status'] = df['assetSubjectDemandStatusCode'].map(sub_to_dem_status_dict)
    else:
        print("Column 'assetSubjectDemandStatusCode' is not present in the DataFrame.")

    df['servicing_advance'] = df['servicingAdvanceMethodCode'].map(servicing_advance_dict)
    df['new_used'] = df['vehicleNewUsedCode'].map(new_used_dict)
    df['type'] = df['vehicleTypeCode'].map(type_dict)
    df['origination_value_source'] = df['vehicleValueSourceCode'].map(origination_value_source_dict)

    columns_to_drop = ['paymentTypeCode', 'assetSubjectDemandStatusCode', 'servicingAdvanceMethodCode',
                        'vehicleNewUsedCode', 'vehicleTypeCode', 'vehicleValueSourceCode']

    # Drop the columns only if they exist
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    #Recode variables for loan files only
    if df['lease_ind'].mean() == 0:
        # Dictionary for variable mappings
        accrual_type_dict = {1: 'Simple', 98: 'Other'}
        orig_int_rate_type_dict = {1: 'Fixed', 2: 'Adjustable', 98: 'Other'}
        subsidy_dict = {'0': 'None', '1': 'Rate Subvention', '2': 'Cash Rebate', '98': 'Other',
                        '2; 1': 'Rate Subvention and Cash Rebate', '1; 2': 'Rate Subvention and Cash Rebate'}
        income_verification_dict = {1: 'Not Stated, Not Verified', 2: 'Stated, Not Verified',
                                    3: 'Stated, Verified', 4: 'Stated, Verified - Lvl 4',
                                    5: 'Stated, Verified - Lvl 5'}
        employment_verification_dict = {1: 'Not Stated, Not Verified', 2: 'Stated, Not Verified',
                                        3: 'Stated, Verified'}
        sub_to_dem_replacement_dict = {1: 'Fraud', 2: 'Early Payment Default',
                                       3: 'Other Recourse Obligation', 4: 'Reps/ Warants Breach',
                                       5: 'Servicer Breach', 98: 'Other', 99: 'Unknown'}
        modification_type_dict = {1: 'APR', 2: 'Principal', 3: 'Term', 4: 'Extension', 98: 'Other'}
        zero_balance_dict = {1: 'Prepaid or Matured', 2: 'Third-party Sale',
                             3: 'Repurchased or Replaced', 4: 'Charged-off',
                             5: 'Servicing Transfer', 99: 'Unavailable'}

        # Apply the mappings
        df['accrual_type'] = df['interestCalculationTypeCode'].map(accrual_type_dict)
        df['orig_int_rate_type'] = df['originalInterestRateTypeCode'].map(orig_int_rate_type_dict)
        df['subsidy'] = df['subvented'].map(subsidy_dict)
        df['income_verification'] = df['obligorIncomeVerificationLevelCode'].map(income_verification_dict)
        df['employment_verification'] = df['obligorEmploymentVerificationCode'].map(employment_verification_dict)

        if 'repurchaseReplacementReasonCode' in df.columns:
            print("Column 'repurchaseReplacementReasonCode' is present in the DataFrame.")
            df['sub_to_dem_replacement'] = df['repurchaseReplacementReasonCode'].map(sub_to_dem_replacement_dict)
        else:
            print("Column 'repurchaseReplacementReasonCode' is not present in the DataFrame.")

        df['modification_type'] = df['modificationTypeCode'].map(modification_type_dict)

        if 'zeroBalanceCode' in df.columns:
            print("Column 'zeroBalanceCode' is present in the DataFrame.")
            df['zero_balance'] = df['zeroBalanceCode'].map(zero_balance_dict)
        else:
            print("Column 'zeroBalanceCode' is not present in the DataFrame.")

        columns_to_drop = ['originalInterestRateTypeCode', 'interestCalculationTypeCode', 'subvented',
                            'obligorIncomeVerificationLevelCode', 'obligorEmploymentVerificationCode',
                            'repurchaseReplacementReasonCode', 'modificationTypeCode', 'zeroBalanceCode']

        # Drop the columns only if they exist
        df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    #Make lease codes useful
    elif df['lease_ind'].mean() == 1:

        base_residual_dict = {1: 'Black Book', 2: 'Automotive Lease Guide', 98: 'Other'}
        termination_dict = {1: 'Payoff', 2: 'Return', 3: 'Repossession', 4: 'Repurchase', 98: 'Other'}
        income_verification_dict = {1: 'Not Stated, Not Verified', 2: 'Stated, Not Verified', 3: 'Stated, Verified',
                                    4: 'Stated, Verified - Lvl 4', 5: 'Stated, Verified - Lvl 5'}
        employment_verification_dict = {1: 'Not Stated, Not Verified', 2: 'Stated, Not Verified', 3: 'Stated, Verified'}
        sub_to_dem_replacement_dict = {1: 'Fraud', 2: 'Early Payment Default', 3: 'Other Recourse Obligation',
                                       4: 'Reps/ Warants Breach', 5: 'Servicer Breach', 98: 'Other', 99: 'Unknown'}
        subsidy_dict = {'0': 'None', '1': 'Rate or Finance Charge Subvention', '2': 'Residual Subvention',
                        '98': 'Other', '2; 1': 'Rate and Residual Subvention', '1; 2': 'Rate and Residual Subvention',
                        '1; 98; 2': 'Rate, Residual, and Other', '98; 1; 2': 'Rate, Residual, and Other',
                        '1; 2; 98': 'Rate, Residual, and Other', '2; 1; 98': 'Rate, Residual, and Other',
                        '98; 2; 1': 'Rate, Residual, and Other', '2; 98; 1': 'Rate, Residual, and Other',
                        '1; 98': 'Rate and Other', '98; 1': 'Rate and Other', '2; 98': 'Residual and Other',
                        '98; 2': 'Residual and Other'}
        modification_type_dict = {1: 'Payment Amount', 2: 'Term', 3: 'Extension', 98: 'Other'}
        zero_balance_dict = {1: 'Terminated', 2: 'Repurchased or Replaced', 3: 'Charged-off', 4: 'Servicing Transfer',
                             99: 'Unavailable'}

        # Apply mappings
        df['lease_resid_value_type'] = df['baseResidualSourceCode'].map(base_residual_dict)

        if 'terminationIndicator' in df.columns:
            print("Column 'terminationIndicator' is present in the DataFrame.")
            df['lease_termination'] = df['terminationIndicator'].map(termination_dict)
        else:
            print("Column 'terminationIndicator' is not present in the DataFrame.")

        df['income_verification'] = df['lesseeIncomeVerificationLevelCode'].map(income_verification_dict)
        df['employment_verification'] = df['lesseeEmploymentVerificationCode'].map(employment_verification_dict)

        if 'repurchaseOrReplacementReasonCode' in df.columns:
            print("Column 'repurchaseOrReplacementReasonCode' is present in the DataFrame.")
            df['sub_to_dem_replacement'] = df['repurchaseOrReplacementReasonCode'].map(sub_to_dem_replacement_dict)
        else:
            print("Column 'repurchaseOrReplacementReasonCode' is not present in the DataFrame.")

        df['subsidy'] = df['subvented'].map(subsidy_dict)
        df['modification_type'] = df['modificationTypeCode'].map(modification_type_dict)

        if 'zeroBalanceCode' in df.columns:
            print("Column 'zeroBalanceCode' is present in the DataFrame.")
            df['zero_balance'] = df['zeroBalanceCode'].map(zero_balance_dict)
        else:
            print("Column 'zeroBalanceCode' is not present in the DataFrame.")


        columns_to_drop = ['baseResidualSourceCode', 'terminationIndicator', 'lesseeIncomeVerificationLevelCode',
                           'lesseeEmploymentVerificationCode', 'repurchaseOrReplacementReasonCode', 'subvented',
                           'modificationTypeCode', 'zeroBalanceCode']

        # Drop the columns only if they exist
        df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    return df

def create_lease_ind(df):
    '''This creates an indicator for whether this is a lease are loan'''
    # Note if one file has both loans and leases, will need to adjust method
    assert not ('originalLeaseTermNumber' in df.columns and 'originalLoanTerm' in df.columns), \
        "File contains both leases and loans, need to recode this"

    # Make an indicator for whether it is a lease  or not
    df['lease_ind'] = np.nan
    # If the csv has the 'originalLeaseTermNumber'
    if 'originalLeaseTermNumber' in df.columns:
        df['lease_ind']=1
    else:
        df['lease_ind']=0

    assert not df['lease_ind'].isnull().values.any()
    return df