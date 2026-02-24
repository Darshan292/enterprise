import pandas as pd
from Converting_Data_Create_EIB_Final import mapping_data
from Converting_Data_Create_EIB_Final import generate_row_id
from Converting_Data_Create_EIB_Final import convert_column_to_row,remove_duplicate_details,set_primary_flag
from Converting_Data_Create_EIB_Final import remove_non_numeric_char, increment_row_id_for_same_sort_col, personal_contact_wipe_address, personal_contact_wipe_local_address
import numpy as np
import os


organization_column_mapping = {'Organization Reference ID': 'Supervisory Organization ID',
                               'Organization Name': 'Supervisory Organization Name',
                               'Organization Code': 'Supervisory Organization ID',
                               'Availability Date': 'Effective Date',
                               'Effective Date': 'Effective Date',
                               'Include Organization Code In Name': 'Include Organization Code in Name?',
                               'Include Leader In Name': 'Include Manager in Name?',
                               'Position Management Enabled': 'Staffing Model',
                               'Superior Organization': 'Superior Supervisory Organization ID',
                               'Organization Subtype Name*': 'Organization Subtype',
                               'Primary Business Site': 'Supervisory Organization Location Name'}

job_requisitions_column_mapping = {'Create Job Requisition Reason*': 'Job Requisition Reason*'}


def get_mapped_supervisory_org_data(input_data, mapping_file,
                                    all_unique_data_list,
                                    unavailable_reference_id_list):
    convert_and_format_date(input_data, 'Effective Date')
    columns_mapping = {
        'key': 2,
        'Effective Date': [3, 9],
        'Supervisory Organization ID': [4, 11],
        'Include Organization Code in Name?': [5, 10],
        'Supervisory Organization Name': 8,
        'Include Manager in Name?': 12,
        'Staffing Model': 15,
        'Superior Supervisory Organization ID': 16,
        # 'Organization Subtype': 18,
        'Supervisory Organization Location Name': 20

    }

    data_transform_mapping = {'Supervisory Organization Location Name': 'Location_ID',
                              'Include Manager in Name?': 'Numeric_Boolean_ID', 
                              'Include Organization Code in Name?': 'Numeric_Boolean_ID'}
    input_data, all_unique_values, un_available_reference_type_id = mapping_data(input_data, data_transform_mapping,
                                                                                 mapping_file, all_unique_data_list,
                                                                                 unavailable_reference_id_list,
                                                                                 'supervisory_org')
    return input_data, columns_mapping, all_unique_values, un_available_reference_type_id


def convert_and_format_date(input_data, column):
    # Convert column to datetime format
    input_data[column] = pd.to_datetime(input_data[column], errors='coerce')
    # Format column as "YYYY-MM-DD"
    # input_data[column] = input_data[column].dt.strftime('%Y-%b-%d')
    input_data[column] = input_data[column].dt.strftime('%Y-%m-%d')


def get_mapped_personal_info(input_data, mapping_file):
    # format columns to date
    convert_and_format_date(input_data, 'Date of Birth')
    convert_and_format_date(input_data, 'Marital Status Date')

    columns_mapping = {
        'key': 2,
        'DJ Employee ID': 3,
        'Date of Birth': 6,
        'Gender': 9,
        'Disability Status': 14,
        'Marital Status': 36,
        'Citizenship Status': 38,
        'Race/Ethnicity': 41,
        'Marital Status Date': 71
    }

    data_transform_mapping = {
        'Gender': 'Gender_Code',
        'Marital Status': 'Marital_Status_ID',
        'Citizenship Status': 'Citizenship_Status_Code',
        'Race/Ethnicity': 'Ethnicity_ID',
        'Disability Status': 'Disability_ID'
    }
    # 'Race/Ethnicity':'Ethnicity_ID','Disability Status':'Disability_ID',

    personal_info_data = mapping_data(input_data, data_transform_mapping, mapping_file)
    return personal_info_data, columns_mapping


def map_unique_record_number(input_data, column_name):
    duplicate_value = {}
    unique_number_list = []
    n = 0
    for inx, row in input_data.iterrows():
        p_row = row.get(column_name)
        if p_row not in duplicate_value.keys():
            duplicate_value[p_row] = 1
            n += 1
            unique_number_list.append(n)
        else:
            duplicate_value[p_row] += 1
            unique_number_list.append(n)

    input_data['unique_number'] = unique_number_list

    return input_data


def get_mapped_job_requisitions_data(input_data, mapping_file, all_unique_data_list,
                                     unavailable_reference_id_list):
    convert_and_format_date(input_data, 'Recruiting Start Date')
    convert_and_format_date(input_data, 'Target Hire Date')
    convert_and_format_date(input_data, 'Target End Date')
    input_data = input_data.drop_duplicates().reset_index(drop=True)
    #create spreadsheet key for the load
    input_data['spreadsheet_key'] = (input_data['Job Requisition ID'] != input_data['Job Requisition ID'].shift()).cumsum()
    #clone Position into new column so that we can can write into different column
    input_data['New Position ID'] = input_data['Position ID']

    # Compare 'Job Requisition ID' with the previous row
    is_same_req = input_data['Job Requisition ID'] == input_data['Job Requisition ID'].shift(1)
    is_same_pos = input_data['Position ID'] == input_data['Position ID'].shift(1)

    # if req is same then wipe out Position ID in Position ID column
    input_data['Position ID'] = input_data.apply(lambda x: '' if (is_same_req[x.name] and not is_same_pos[x.name])  else x['Position ID'], axis=1)
    # if req is not same then wipe out Position ID in New Position ID column
    input_data['New Position ID'] = input_data.apply(lambda x: '' if (is_same_req[x.name] and is_same_pos[x.name]) else ('' if not is_same_req[x.name] else x['New Position ID']), axis=1)
    
    columns_mapping = {
        'spreadsheet_key': 2,
        'Position ID': 3,
        "New Position ID": 4,
        'Supervisory Organization': 5,
        'Job Requisition Reason*': 6,
        #'Number of Openings': 7,
        'Job Requisition ID': 8,
        'Job Posting Title': 9,
        'Recruiting Instruction': 10,
        'Job Description':12,
        'Job Description Summary': 11,
        'Justification': 14,
        'Recruiting Start Date': 20,
        'Target Hire Date': 21,
        'Target End Date': 22,
        'Job Profile': 23,
        'Referral Payment Plan': 25,
        'Worker Type': 26,
        'Worker SubType': 27,
        'Primary Location': 29,
        'Primary Job Posting Location': 30,
        'Position Time Type': 33,
        'Employee Contract Type': 34,
        'Scheduled Weekly Hours': 35,
        'Questionnaire for External Career Sites': 42,
        'Supplier Id': 97,
        'Currency ID': 98,
        'Pay Rate': 100,
        'Frequency': 101,
        'Maximum Amount': 102,
        'Replacement for Worker+': 105,
        'Company Assignment': 108,
        'Cost Center Assignment': 109,
        'Additional Locations+':31,
        'Additional Job Posting Locations+':32,
        'Spotlight Job': 37,
        'Business Hierarchy': 116,
        'Job Application Template': 118,        
    }
    data_transform_mapping = {#'Supervisory Organization': 'Organization_Reference_ID',
                              'Job Requisition Reason*': 'General_Event_Subcategory_ID',
                              'Job Profile': 'Job_Profile_ID', 
                              'Worker SubType': 'Position_Worker_Type_WID',
                              #'Recruiting Instruction': 'Recruiting_Instruction_ID',
                              'Questionnaire for External Career Sites': 'Questionnaire_ID',
                              'Spotlight Job': 'Numeric_Boolean_ID',
                              'Primary Location': 'Location_ID',
                              'Additional Locations+': 'Location_ID',
                                'Additional Job Posting Locations+': 'Location_ID',
                              'Primary Job Posting Location': 'Location_ID',
                              'Position Time Type': 'Position_Time_Type_ID', 
                              "Frequency": "Frequency_ID",
                               'Replacement for Worker+':'Worker_WID'}
   
    input_data, all_unique_values, un_available_reference_type_id = mapping_data(input_data, data_transform_mapping,
                                                                                 mapping_file, all_unique_data_list,
                                                                                 unavailable_reference_id_list,
                                                                                 "job_requisitions")
    return input_data, columns_mapping, all_unique_values, un_available_reference_type_id


def get_mapped_create_position_data(input_data, mapping_file,
                                    all_unique_data_list,
                                    unavailable_reference_id_list):
    convert_and_format_date(input_data, 'Availability Date')
    convert_and_format_date(input_data, 'Earliest Hire Date')

    columns_mapping = {
        'key': 2,
        'Organization_Reference_ID': 3,
        'Position_Request_Reason_ID': 4,
        'Position ID': 5,
        'Job Posting Title': 6,
        'Is Overlapped Position': 11,
        'Availability Date': 61,
        'Earliest Hire Date': 62,
        # 'Job_Family_ID': 63,
        'Job_Profile_ID': 64,
        'Location_ID': 65,
        'Worker_Type_ID': 66,
        'Position_Time_Type_ID': 67,
        'Worker Sub-Type': 68,
        # 'Employee_Type_ID': 68,
        'Scheduled Weekly Hours': 76,
        'Default Weekly Hours' : 75
    }
    data_transform_mapping = {#'Organization_Reference_ID': 'Organization_Reference_ID',
                              #'Position_Request_Reason_ID': 'General_Event_Subcategory_ID',
                              'Job_Profile_ID': 'Job_Profile_ID',
                               'Location_ID': 'Location_ID',
                              'Position_Time_Type_ID': 'Position_Time_Type_ID',
                              'Is Overlapped Position': 'Numeric_Boolean_ID', 
                              'Worker Sub-Type': 'Position_Worker_Type_WID'}

    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "position")

    # input_data.drop_duplicates("Position ID", keep='first', inplace=True)

    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id

def mapping_spread_sheet_key(input_data, additional_data):
    mapped_dict = {str(k): g["Spreadsheet Key*"].values[0] for k, g in additional_data.groupby(["Position ID", 'Available for Overlap'])}
    input_data["Row ID"] = input_data["Position ID"].map(mapped_dict)
    return input_data

def get_request_default_compensation(input_data, mapping_file,
                                     all_unique_data_list,
                                     unavailable_reference_id_list, eib_file_name):
    # create_position_sheet = "Create Position"
    # additional_data = pd.read_excel(eib_file_name, sheet_name=create_position_sheet, skiprows=[0, 1, 2, 3])

    # additional_data = additional_data[["Spreadsheet Key*", "Position ID","Available for Overlap"]]

    # input_data = mapping_spread_sheet_key(input_data, additional_data)
    #input_data['spreadsheet_key'] = (input_data['Position ID'] != input_data['Position ID'].shift()).cumsum()

    columns_mapping = {
        'spreadsheet_key': 2,
        'Compensation Package': 6,
        'Compensation Grade': 7,
        'Compensation Grade Profile': 8,
        'Compensation Step ID': 9,
        'Salary Plan': 12,
        'Salary Amount': 13,
        'Salary Currency': 16,
        'Salary Frequency': 17
    }

    data_transform_mapping = {##'Salary Currency': 'Currency_ID', 
                              
                             'Salary Frequency': 'Frequency_ID',
                              'Salary Plan': 'Compensation_Plan_ID',
                              'Hourly Plan': 'Compensation_Plan_ID',
                              'Bonus Plan': 'Compensation_Plan_ID',
                              'Allowance Plan': 'Compensation_Plan_ID',
                              'Compensation Step ID':'Compensation_Step_ID',
                              'Allowance Frequency': 'Frequency_ID',
                              'Hourly Frequency': 'Frequency_ID',
                              'Compensation Grade Profile': 'Compensation_Grade_Profile_ID',
                              'Compensation Grade': 'Compensation_Grade_ID',
                              'Compensation Package': 'Compensation_Package_ID'}
    
    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "position")
    updated_input_data = input_data.drop(input_data.loc[input_data['Worker_Type_ID'] == 'Contingent Worker'].index)

    #Wipe out the step if grade profile is NCA_No Grade
    updated_input_data.loc[updated_input_data['Compensation Grade Profile'] == 'COMPENSATION_GRADE_PROFILE-6-1', 'Compensation Step ID'] = ""

    #if grade profile is NCA_No Grade then we also grade to be no grade.. 
    updated_input_data.loc[updated_input_data['Compensation Grade Profile'] == 'COMPENSATION_GRADE_PROFILE-6-1', 'Compensation Grade'] = "COMPENSATION_GRADE-6-1"

    
    return updated_input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id


def get_assign_org_data(input_data, mapping_file,
                        all_unique_data_list,
                        unavailable_reference_id_list, eib_file_name):
    


    input_data = input_data[["spreadsheet_key",'Company Name','Department','Division','Business Area', 'Industrial Code']]
    
     # columns that i need to convert to a row
    columns_to_melt = ['Division','Business Area', 'Department', 'Industrial Code']
        # define the key
    id_vars = ["spreadsheet_key",'Company Name','Department']

    var_name = "Org_Type"
    value_name = "Custom Org"

    input_data = convert_column_to_row(input_data, id_vars, columns_to_melt, var_name, value_name)
   
      # Apply the custom function to each group
    input_data = generate_row_id(input_data, 'spreadsheet_key', "Custom Org","Row ID")
    input_data['Delete'] = "N"

    columns_mapping = {
            'spreadsheet_key': 2,
            'Company Name': 3,
            'Department':4,
            'Row ID': 6,
            "Delete":7,
            "Custom Org":8
        }
    data_transform_mapping = {'Company Name': 'Company_Reference_ID'}

    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Position Org")
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id




def get_mapped_prehire_data(input_data, mapping_data_dict, all_unique_values, un_available_reference_type_id):
    #input_data = input_data[input_data["Applicant ID"].isin(['NCA_APPLICANT-3-33720', 'NCA_A707326', 'NCA_APPLICANT-3-33703'])]
    input_data['Add_Only'] = "Y"
    columns_mapping = {
        'key': 2,
        'Add_Only': 3,
        'Applicant ID': 5,
        # 'Legacy Worker ID': 6,
        
        # 'Applicant Source Category Name': 8,
        'Country': [9, 39],
        'Title - Reference ID': 10,
        #'Prefix': 11,
        'Legal First Name': 13,
        'Legal Middle Name': 14,
        'Legal Last Name': 15,
        'Legal Secondary Name': 16,
        'Preferred First Name': 43,
        'Preferred Middle Name': 44,
        'Preferred Last Name': 45,
        'Preferred Secondary Name': 46,
        'Local Script': 49,
        'Local Script First Name': 50,
        'Local Script Middle Name': 51,
        'Local Script Last Name': 52,
        'Local Script Secondary Name': 53,
        'Local First Name 2': 54,
        'Local Primary 2': 55,
        'Email Address': 356,
        'Visibility (Email)': 359,
        'Primary (Email)': 361,
        'Usage Type (Email)': 362,
        'Applicant Source Name': 649
    }

    data_transform_mapping = {'Usage Type (Email)': 'Communication_Usage_Type_ID', 
                              "Visibility (Email)": "Visibility_ID", 
                              'Primary (Email)': "Numeric_Boolean_ID",
                              "Country": "ISO_3166-1_Alpha-3_Code",
                              "Title - Reference ID": 'Predefined_Name_Component_ID',
                              'Applicant Source Name':'Applicant_Source_ID2'
                              }
    input_data, all_unique_values, un_available_reference_type_id = mapping_data(input_data, data_transform_mapping,
                                                                                 mapping_data_dict, all_unique_values,
                                                                                 un_available_reference_type_id,
                                                                                 "pre_hire")
    return input_data, columns_mapping, all_unique_values, un_available_reference_type_id


def get_mapped_hire_employee_data(input_data, mapping_data_dict, all_unique_values, un_available_reference_type_id):
    
    # columns that i need to convert to a row
    columns_to_melt = ['Job Classification - Annual Leave Entitlement','Job Classification - Personal Leave Entitlement',
                       'Job Classification - Public Holiday Entitlement','Job Class - WGEA']
    # define the key
    id_vars = ['spreadsheet_key','Applicant ID','Supervisory Organization','Position ID','Hire Date','Legacy Worker ID','Employee Type Name',"Original Hire Date",
                       'Continuous Service Date','End Employment Date','Job Profile ID','Job Title', 'Business Title','Location','Work Space','Time Type',
                       'Work Shift','Default Weekly Hours','Scheduled Weekly Hours','Pay Rate Type','Position Start Date for Conversion']

    var_name = "Org_Type"
    value_name = "Job Classifications"
    
    input_data = convert_column_to_row(input_data, id_vars, columns_to_melt, var_name, value_name)

    convert_and_format_date(input_data, 'Hire Date')
    convert_and_format_date(input_data, "Original Hire Date")
    convert_and_format_date(input_data, 'End Employment Date')
    convert_and_format_date(input_data, 'Continuous Service Date')
    convert_and_format_date(input_data, 'Position Start Date for Conversion')
    
    columns_mapping = {
        'spreadsheet_key': 2,
        'Applicant ID': 3,
        'Supervisory Organization': 404,
        'Position ID': 405,
        'Hire Date': 407,
        'Legacy Worker ID': 408,
        #'Hire Reason': 410,
        'Employee Type Name': 411,
        'Continuous Service Date': 414,
        'End Employment Date': 417,
        'Position Start Date for Conversion': 420,
        'Job Profile ID': 421,
        'Job Title': 422,
        'Business Title': 423,
        'Location': 424,
        'Work Space': 425,
        'Time Type': 426,
        'Work Shift': 428,
        'Default Weekly Hours': 430,
        'Scheduled Weekly Hours': 431,
        'Pay Rate Type': 439,
        'Job Classifications': 440
    }

    data_transform_mapping = {#'Supervisory Organization': 'Organization_Reference_ID',
                              #'Hire Reason': 'General_Event_Subcategory_ID',
                              'Employee Type Name': 'Position_Worker_Type_WID',
                              'Time Type': "Position_Time_Type_ID",
                              'Location': 'Location_ID', 'Pay Rate Type': 'Pay_Rate_Type_ID',
                              'Job Profile ID': 'Job_Profile_ID',
                              'Legacy Worker ID':"Worker_ID",
                              'Job Classifications':'Job_Classification_ID'}
    input_data, all_unique_values, un_available_reference_type_id = mapping_data(input_data, data_transform_mapping,
                                                                                 mapping_data_dict, all_unique_values,
                                                                                 un_available_reference_type_id,
                                                                                 "Hire Employee")
    return input_data, columns_mapping, all_unique_values, un_available_reference_type_id


def get_mapped_hire_employee_compensation_data(input_data, mapping_data_dict, all_unique_values_list,
                                               un_available_reference_type_id):
    columns_mapping = {
        'spreadsheet_key': 2,
        'Compensation Package ID': 4,
        'Compensation Grade ID': 5,
        # 'Compensation Profile ID': 399,
        'Compensation Plan Name': 11,
        'Compensation Currency': 13,
        'Compensation Amount': 12,
        'Frequency': 14
    }
    

    # data_transform_mapping = {'Visa Type Name': 'Visa_ID_Type_ID'}
    # input_data = mapping_data(input_data, data_transform_mapping, mapping_file)
    #
    data_transform_mapping = {'Compensation Plan Name': 'Compensation_Plan_ID', 'Compensation Currency': 'Currency_ID',
                              'Frequency': 'Frequency_ID'}
    # get_transformation_data(input_data, transformation_filed, mapping_data_dict)
    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_data_dict,
                                                                                      all_unique_values_list,
                                                                                      un_available_reference_type_id,
                                                                                      "Hire Employee Comp")

    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id

def hire_mapping_spread_sheet_key(input_data, additional_data, groupby):
    mapped_dict = {str(k): g["Spreadsheet Key*"].values[0] for k, g in additional_data.groupby(groupby)}
    input_data["Spreadsheet Key*"] = input_data["Legacy Worker ID"].map(mapped_dict)
    return input_data

def get_mapped_hire_employee_assign_pay_group_data(input_data, mapping_data_dict,
                                                   all_unique_values_list, un_available_reference_type_id,
                                                   eib_file_name):
    # hire_employee_sheet = "Hire Employee"
    # additional_data = pd.read_excel(eib_file_name, sheet_name=hire_employee_sheet, skiprows=[0, 1, 2, 3])
    # #print(additional_data.head())

    # additional_data = additional_data[["Spreadsheet Key*", "Employee ID"]]

    # additional_data.drop_duplicates(subset=["Spreadsheet Key*", "Employee ID"], inplace=True)

    # input_data = hire_mapping_spread_sheet_key(input_data, additional_data,"Employee ID")
    #input_data['spreadsheet_key'] = (input_data['Legacy Worker ID'] != input_data['Legacy Worker ID'].shift()).cumsum()

    columns_mapping = {
        'spreadsheet_key': 2,
        'Pay Group': 3
    }

    data_transform_mapping = {'Pay Group': 'Organization_Reference_ID'}
    # get_transformation_data(input_data, transformation_filed, mapping_data_dict)
    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_data_dict,
                                                                                      all_unique_values_list,
                                                                                      un_available_reference_type_id,
                                                                                      "Hire Pay Group")

    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id


def get_hire_assign_org_data(input_data, mapping_file,all_unique_data_list, unavailable_reference_id_list, eib_file_name):
    
    #input_data['spreadsheet_key'] = (input_data['Legacy Worker ID'] != input_data['Legacy Worker ID'].shift()).cumsum()

    input_data = input_data[["spreadsheet_key",'Company Name','Department','Product','Division','Business Area','Job Classification - Pay Policy', 'Industrial Code']]
    
     # columns that i need to convert to a row
    columns_to_melt = ['Product','Division','Business Area','Job Classification - Pay Policy', 'Industrial Code']
        # define the key
    id_vars = ["spreadsheet_key",'Company Name','Department',]

    var_name = "Org_Type"
    value_name = "Custom Org"

    input_data = convert_column_to_row(input_data, id_vars, columns_to_melt, var_name, value_name)
   
      # Apply the custom function to each group
    input_data = generate_row_id(input_data, 'spreadsheet_key', "Custom Org","Row ID")
    input_data['Delete'] = "N"

    columns_mapping = {
            'spreadsheet_key': 2,
            'Company Name': 3,
            'Department':4,
            'Row ID': 6,
            "Delete":7,
            "Custom Org":8
        }
    data_transform_mapping = {'Company Name': 'Company_Reference_ID'}

    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Hire Assign Org")
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id


def get_input_data_from_file_with_sheet_name(input_file, input_sheet):
    input_data = pd.read_excel(input_file, sheet_name=input_sheet, skiprows=[1, 2], dtype=object)

    return input_data


def get_mapped_hire_cwr_data(input_data, mapping_file, all_unique_data_list, unavailable_reference_id_list):
    
    input_data['spreadsheet_key'] = (input_data['Legacy Worker ID'] != input_data['Legacy Worker ID'].shift()).cumsum()

    convert_and_format_date(input_data, 'Contract Begin Date')
    #convert_and_format_date(input_data, 'Contract End Date')

    columns_mapping = {
        'spreadsheet_key': 2,
        'Applicant ID': 3,
        'Supervisory Organization': 403,
        'Position ID': 404,
        'Contract Begin Date': 406,
        'Legacy Worker ID': 407,
        'Contingent Worker Type Name': 410,
        'Legacy Worker ID': 407,
        'Contract End Date': 412,
        'Supplier Name': 415,
        'Job Profile': 417,
        # 'Position Start Date for Conversion': 410,
        'Job Title': 418,
        'Business Title': 419,
        'Location': 420,
        # 'Work Space': 415,
        'Time Type': 422,
        'Default Weekly Hours': 426,
        'Scheduled Weekly Hours': 427,
        'Job Class - WGEA': 436,
        'Pay Rate':448,
        'Currency':449,
        'Fequency':450,
        'Spend Category':454
    }

    data_transform_mapping = {#'Supervisory Organization': 'Organization_Reference_ID',
                              'Job Profile': 'Job_Profile_ID',
                              'Contingent Worker Type Name': 'Position_Worker_Type_WID',
                              'Location': 'Location_ID', 'Time Type': 'Position_Time_Type_ID',
                              'Legacy Worker ID':"Worker_ID",
                              "Fequency":"Frequency_ID",
                              'Job Class - WGEA':'Job_Classification_ID'}

    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Hire-CWR")

    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id

def get_hire_cwr_assign_org_data(input_data, mapping_file,all_unique_data_list, unavailable_reference_id_list, eib_file_name):
    
    # hire_employee_sheet = "Contract Contingent Worker"
    # additional_data = pd.read_excel(eib_file_name, sheet_name=hire_employee_sheet, skiprows=[0, 1, 2, 3])
    # #print(additional_data.head())
    # additional_data = additional_data[["Spreadsheet Key*", "Contingent Worker ID"]]

    # additional_data.drop_duplicates(subset=["Spreadsheet Key*", "Contingent Worker ID"], inplace=True)

    # input_data = hire_mapping_spread_sheet_key(input_data, additional_data,"Contingent Worker ID")
    input_data['spreadsheet_key'] = (input_data['Legacy Worker ID'] != input_data['Legacy Worker ID'].shift()).cumsum()
  
    input_data = input_data[["spreadsheet_key",'Company Name','Department','Division','Business Area', 'Industrial Code']]
    
     # columns that i need to convert to a row
    columns_to_melt = ['Division','Business Area', 'Industrial Code']
        # define the key
    id_vars = ["spreadsheet_key",'Company Name','Department']

    var_name = "Org_Type"
    value_name = "Custom Org"

    input_data = convert_column_to_row(input_data, id_vars, columns_to_melt, var_name, value_name)

    # Apply the custom function to each group
    input_data = generate_row_id(input_data, 'spreadsheet_key', "Custom Org","Row ID")
    input_data['Delete'] = "N"

    columns_mapping = {
            'spreadsheet_key': 2,
            'Company Name': 3,
            'Department':4,
            'Row ID': 6,
            "Delete":7,
            "Custom Org":8
        }
    data_transform_mapping = {'Company Name': 'Company_Reference_ID'}

    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Hire CWR Assign Org")
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id



def get_mapped_change_job_data(input_data, mapping_file, all_unique_data_list, unavailable_reference_id_list):
    convert_and_format_date(input_data, 'Effective Date')
    convert_and_format_date(input_data, 'End Employment Date')

    # columns that i need to convert to a row
    columns_to_melt = ['Job Classification - Pay Policy', 'Job Classification - Annual Leave Entitlement','Job Classification - Personal Leave Entitlement',
                       'Job Classification - Public Holiday Entitlement', 'Job Classification - Leave Loading Entitlement', 'Job Class - WGEA']
    
    # define the key
    id_vars = ['spreadsheet_key','Legacy Worker ID','Position Id','Effective Date', 'Job Change Reason', 'Supervisory Org Id', 'Job Req Id',
               'Employee Type', 'Job Profile', 'Position Title', 'Business Title', 'Work Location', 'Time Type', 'Default Weekly Hours',
               'Scheduled Weekly Hours', 'Pay Rate Type', 'End Employment Date']


    var_name = "Org_Type"
    value_name = "Job Classifications"

    input_data = convert_column_to_row(input_data, id_vars, columns_to_melt, var_name, value_name)
    columns_mapping = {
        'spreadsheet_key': 2,
        'Legacy Worker ID': 3,
        'Effective Date': 5,
        'Job Change Reason': 6,
        'Supervisory Org Id': 7,
        'Position Id': 10,
        'Job Req Id': 11,
        'Employee Type': 13,
        'Job Profile': 15,
        'Position Title': 16,
        'Business Title': 17,
        'Work Location': 18,
        'Time Type': 20,
        'Default Weekly Hours': 24,
        'Scheduled Weekly Hours': 25,
        'Pay Rate Type': 33,
        "Job Classifications": 34,
        'End Employment Date': 47,
    }

    data_transform_mapping = {'Time Type': 'Time_Type_ID', "Job Profile": "Job_Profile_ID",
                              'Legacy Worker ID': "Worker_ID", "Employee Type": "Employee_Type_ID",
                              "Supervisory Org Id": "Organization_Reference_ID", 
                              "Work Location": "Location_ID", "Job Classifications": "Job_Classification_Reference_ID"}
    
    input_data, all_unique_values, un_available_reference_type_id = mapping_data(input_data, data_transform_mapping,
                                                                                 mapping_file, all_unique_data_list,
                                                                                 unavailable_reference_id_list,
                                                                                 'Change Job')
    return input_data, columns_mapping, all_unique_values, un_available_reference_type_id

def get_mapped_change_job_change_organization_data(input_data, mapping_file, all_unique_data_list, unavailable_reference_id_list):
    
    # columns that i need to convert to a row
    columns_to_melt = ['Current Division', 'Current Business Area','Industrial Code']
    
    # define the key
    id_vars = ['spreadsheet_key','Company Code','Cost Center Code']


    var_name = "Org_Type"
    value_name = "Custom Classifications"

    columns_mapping = {
        'spreadsheet_key': 2,
        'Company Code': 3,
        'Cost Center Code': 4,
        'Custom Classifications': 8

    }

    data_transform_mapping = {'Cost Center Code': "Cost_Center_Reference_ID"}
    
    input_data, all_unique_values, un_available_reference_type_id = mapping_data(input_data, data_transform_mapping,
                                                                                 mapping_file, all_unique_data_list,
                                                                                 unavailable_reference_id_list,
                                                                                 'Change Job')
    return input_data, columns_mapping, all_unique_values, un_available_reference_type_id

def get_mapped_change_job_assign_pay_group_data(input_data, mapping_file, all_unique_data_list, unavailable_reference_id_list):
    
    columns_mapping = {
        'spreadsheet_key': 2,
        'Pay Group': 3

    }

    data_transform_mapping = {'Pay Group': "Organization_Reference_ID"}
    
    input_data, all_unique_values, un_available_reference_type_id = mapping_data(input_data, data_transform_mapping,
                                                                                 mapping_file, all_unique_data_list,
                                                                                 unavailable_reference_id_list,
                                                                                 'Change Job')
    return input_data, columns_mapping, all_unique_values, un_available_reference_type_id

def get_mapped_terminate_emp_data(input_data, mapping_file, all_unique_values_list,
                                  un_available_reference_type_id):
    convert_and_format_date(input_data, 'Termination Date')
    convert_and_format_date(input_data, 'Last Day of Work (Date)')
    convert_and_format_date(input_data, 'Pay Through Date')
    convert_and_format_date(input_data, 'Resignation Date')
    convert_and_format_date(input_data, 'Expected Date of Return')
    convert_and_format_date(input_data, 'Notify Employee By Date')

    columns_mapping = {
        'key': 2,
        'Legacy Worker ID': 3,
        'Termination Date': 4,
        'Last Day of Work (Date)': 5,
        'Primary Reason': 6,
        'Local Termination Reason': 8,
        'Pay Through Date': 9,
        'Resignation Date': 10,
        'Notify Employee By Date': 13,
        'Close Position': 17,
        'Expected Date of Return': 20
    }

    data_transform_mapping = {'Local Termination Reason': 'Local_Termination_Reason_ID',
                              'Legacy Worker ID':"Worker_ID"}
    input_data, all_unique_values, un_available_reference_type_id = mapping_data(input_data, data_transform_mapping,
                                                                                 mapping_file, all_unique_values_list,
                                                                                 un_available_reference_type_id,
                                                                                 'Terminate Emp')
    return input_data, columns_mapping, all_unique_values, un_available_reference_type_id


def get_mapped_end_contingent_worker_contracts_data(input_data, mapping_file, all_unique_data_list,
                                                    unavailable_reference_id_list):
    convert_and_format_date(input_data, 'Contract End Date*')
    convert_and_format_date(input_data, 'Notify Worker By Date')
    convert_and_format_date(input_data, 'Last Day of Work (Date)')

    columns_mapping = {
        'key': 2,
        'Legacy Worker ID': 3,
        'Contract End Date*': 4,
        'Last Day of Work (Date)': 5,
        'Primary Reason*': 6,
        'Notify Worker By Date': 9,
        'Regrettable': 10,
        'Close Position': 11,

    }

    data_transform_mapping = {'Legacy Worker ID':"Worker_ID"}
    input_data, all_unique_values, un_available_reference_type_id = mapping_data(input_data, data_transform_mapping,
                                                                                 mapping_file, all_unique_data_list,
                                                                                 un_available_reference_type_id,
                                                                                 'CWR Contracts')
    return input_data, columns_mapping, all_unique_values, un_available_reference_type_id

def get_mapped_one_time_payments_data(input_data, mapping_file, all_unique_data_list, unavailable_reference_id_list):
    convert_and_format_date(input_data, 'Effective Date')

    columns_mapping = {
        'key': 2,
        # 'Legacy Employee ID': 0,
        'Legacy Worker ID': 3,
        'Effective Date': 5,
        'OTP Plan Name': 9,
        'OTP Payment Amount (In Local Currency)': 14,
        'OTP Currency': 16
    }

    data_transform_mapping = {'Legacy Worker ID':"Worker_ID"}
    input_data, all_unique_values, un_available_reference_type_id = mapping_data(input_data, data_transform_mapping,
                                                                                 mapping_file, all_unique_data_list,
                                                                                 un_available_reference_type_id,
                                                                                 'One Time payment')
    return input_data, columns_mapping, all_unique_values, un_available_reference_type_id

def get_mapped_compensation_data(
        input_data, mapping_file, all_unique_data_list, unavailable_reference_id_list):
    convert_and_format_date(input_data, 'Effective Date')
    convert_and_format_date(input_data, 'Expected End Date')

    columns_mapping = {
        'key': 2,
        # 'DJ Employee ID': 3,
        'Effective Date': 5,
        'Compensation Reason': 8,
        'Compensation Plan Name': 20,
        'Compensation Amount': 21,
        'Bonus or Superannuation %': 22,
        'Compensation Currency': 24,
        'Frequency': 25,
        'Expected End Date': 26,
        "Compensation Plan Type": ''
    }

    # data_transform_mapping = {}
    # input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
    #                                                                                   data_transform_mapping,
    #                                                                                   mapping_file,
    #                                                                                   all_unique_data_list,
    #                                                                                   unavailable_reference_id_list)

    return input_data, columns_mapping, all_unique_data_list, unavailable_reference_id_list


def get_mapped_put_candidate_data(input_data, mapping_file,
                                  all_unique_data_list, unavailable_reference_id_list):
    
    ##PLEASE TEST THIS: WE WANT TO PASS ONLY OPEN AND FROZEN Records
    input_data = input_data[~input_data['Job Requisition Status'].isin(['Closed', 'Filled'," "])]
    input_data.reset_index(drop=True, inplace=True)

    input_data = input_data.sort_values(by='Legacy Candidate ID').reset_index(drop=True)
    input_data['spreadsheet_key'] = (input_data['Legacy Candidate ID'] != input_data['Legacy Candidate ID'].shift()).cumsum()
    
    #input_data = increment_row_id_for_same_sort_col(input_data, ['spreadsheet_key'], "Job Requisition*", "Put Candidate Row ID")
    input_data["Put Candidate Row ID"] = input_data.groupby("Legacy Candidate ID").cumcount() + 1

    input_data.loc[(input_data['Phone Number'] == '') | input_data['Phone Number'].isnull(), 'Phone Device Type'] = ''
    
    input_data['Add_Only'] = "Y"
    columns_mapping = {
        'spreadsheet_key': 2,
        'Add_Only': 3,
        'Legacy Candidate ID': 5,
        # 'Candidate ID':0,
        'First Name': 11,
        'Middle Name': 12,
        'Last Name': 14,
        'Secondary Last Name': 15,
        'Tertiary Last Name': 16,
        'Phone Device Type': 41,
        'Country Phone Code': 42,
        'Phone Number': 43,
        'Phone Extension': 44,
        'Email Address': 45,
        'Country': 47,
        'Address Line 1': 48,
        'Address Line 2': 49,
        'Address Line 3': 50,
        'Address Line 4': 51,
        'Address Line 5': 52,
        'Address Line 6': 53,
        'Address Line 7': 54,
        'Address Line 8': 55,
        'Address Line 9': 56,
        'Address Line 1 - Local': 57,
        'Address Line 2 - Local': 58,
        'Address Line 3 - Local': 59,
        'Address Line 4 - Local': 60,
        'Address Line 5 - Local': 61,
        'City': 62,
        'City - Local': 63,
        'City Subdivision 1': 64,
        'City Subdivision 1 - Local': 65,
        'Country Region':68,
        'Region Subdivision 1': 69,
        'Region Subdivision 2': 70,
        'Region Subdivision 1 - Local': 71,
        'Postal Code': 72,
        'Row ID*': 79,
        "Put Candidate Row ID":81,
        'Job Requisition*': 83,
        'Stage*': 85,
        'Disposition': 87,
        'Source': 89,
        'Referred By Worker': 90,
        'Added By Worker': 91,
        'Gender': 93,
        'Ethnicity+': 94,
        'Veterans Status+': 95,
        'Hispanic or Latino': 96,
        'Disability Status': 97
    }

    data_transform_mapping = {"Phone Device Type": "Phone_Device_Type_ID", 
                              "Source": "Applicant_Source_ID",
                              'Country Phone Code':'Country_Phone_Code_ID',
                              "Disposition":"Recruiting_Disposition_ID",
                              'Country Region': "Country_Region_ID",
                              "Referred By Worker":"Worker_WID",
                              'Added By Worker':"Worker_WID"}
    
    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "put_candidate")
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id


def get_mapped_job_requisition_roles_edit_job_data(input_data, mapping_file, all_unique_data_list,
                                                   unavailable_reference_id_list):
   

    columns_mapping = {
        'spreadsheet_key': 2,
        'Job Requisition ID': 3
    }

    return input_data, columns_mapping, all_unique_data_list, unavailable_reference_id_list

def get_mapped_role_based_requisition_roles_data(input_data, mapping_file,all_unique_data_list, unavailable_reference_id_list):
    
    convert_and_format_date(input_data, 'Effective Date')

     # columns that i need to convert to a row
    columns_to_melt = ['Secondary Screening Manager','Primary Recruiters']
        # define the key
    id_vars = ['Job Requisition ID','spreadsheet_key','Effective Date']

    var_name = "Organization Role*"
    value_name = "Role Assignee+"

    input_data = convert_column_to_row(input_data, id_vars, columns_to_melt, var_name, value_name)

    #input_data = input_data.drop_duplicates().reset_index(drop=True)
    input_data = input_data.sort_values(by=['spreadsheet_key','Organization Role*'])
    print(input_data)
    input_data = input_data.drop_duplicates().reset_index(drop=True)
    print(input_data)
    input_data["CompositeKeyRowID"] = input_data['Role Assignee+'].astype(str) + input_data['Organization Role*'].astype(str)
    # Apply the custom function to each group
    input_data = generate_row_id(input_data, 'spreadsheet_key', 'CompositeKeyRowID', "Roles Row ID")
    print(input_data)
    
    # Exclude records where "Organization Role*" is 'Primary Recruiters' and "Roles Row Id" is 2.0
    #input_data = input_data[~((input_data["Organization Role*"] == 'Primary Recruiters') & (input_data["Roles Row ID"] == 2.0))]
    input_data['Hardcoded Value_Y'] = 'Y'
    input_data['Hardcoded Value_N'] = 'N'

    columns_mapping = {
        'spreadsheet_key': 2,
        'Effective Date': 3,
        'Roles Row ID': 7,
        'Hardcoded Value_N': [6],
        'Job Requisition ID': 8,
        #'External Supplier Invoice Source': 8,
        'Organization Role*': 10,
        'Role Assignee+': [5,13],
        #'Single Assignment Manager': 11
    }

    data_transform_mapping = {'Organization Role*': 'Organization_Role_ID','Job Requisition ID':"Supervisory_to_BA_WID"}
    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Create Job Requisition Roles")
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id


def get_mapped_job_requisition_roles_data(input_data, mapping_file,
                                          all_unique_data_list, unavailable_reference_id_list):
    
    convert_and_format_date(input_data, 'Effective Date')

     # columns that i need to convert to a row
    columns_to_melt = ['Secondary Screening Manager','Primary Recruiters']
        # define the key
    id_vars = ['Job Requisition ID','spreadsheet_key','Effective Date']

    var_name = "Organization Role*"
    value_name = "Role Assignee+"

    input_data = convert_column_to_row(input_data, id_vars, columns_to_melt, var_name, value_name)
    input_data = input_data.sort_values(by='spreadsheet_key')
    # Apply the custom function to each group
    input_data = generate_row_id(input_data, 'spreadsheet_key', "Organization Role*", "Roles Row ID")

    columns_mapping = {
        'spreadsheet_key': 2,
        'Effective Date': 4,
        'Roles Row ID': 5,
        #'Role Assigner': 7,
        #'External Supplier Invoice Source': 8,
        'Organization Role*': 9,
        'Role Assignee+': 10,
        #'Single Assignment Manager': 11
    }
    data_transform_mapping = {'Organization Role*': 'Organization_Role_ID'}
    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Job Requisition Roles")
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id


def get_mapped_start_performance_reviews_data(input_data, mapping_file,
                                              all_unique_data_list, unavailable_reference_id_list):
    convert_and_format_date(input_data, 'Review Period End Date')
    convert_and_format_date(input_data, 'Review Period Start Date')

    columns_mapping = {
        'spreadsheet_key': 2,
        'Legacy Worker ID': 3,
        'Review Template': 4,
        'Review Period Start Date': 5,
        'Review Period End Date': 6

    }
    data_transform_mapping = {'Review Template': 'Employee_Review_Template_ID','Legacy Worker ID':"Worker_WID"}
    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Performance Reviews")
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id


def get_transformation_data(input_data, transformation_filed, mapping_data_dict):
    for key, value in transformation_filed.items():
        for row in input_data[key]:
            if row in mapping_data_dict.keys():
                if value in mapping_data_dict[row].keys():
                    input_data[key] = mapping_data_dict[row][value]


def get_mapped_performance_manager_evaluation_data(input_data, mapping_file, all_unique_data_list,
                                                   unavailable_reference_id_list):
    columns_mapping = {
        'spreadsheet_key': 2,
        'Manager ID': 3,
        'Manager Review Rating': 5

    }

    data_transform_mapping = {'Manager Review Rating': 'Review_Rating_ID', 'Manager ID': "Worker_WID"}
    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Performance Reviews")
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id


def get_mapped_probation_info_data(input_data, mapping_file,
                                   all_unique_data_list, unavailable_reference_id_list):
    convert_and_format_date(input_data, 'Start Date')
    convert_and_format_date(input_data, 'End Date')
    #convert_and_format_date(input_data, 'Extended End Date')
    convert_and_format_date(input_data, 'Probation Review Date')
    
    input_data['Schedule*'] = input_data['Schedule*'].replace('0','')
    
    columns_mapping = {
        'key': 2,
        'Legacy Worker ID': 3,
        'Start Date': 6,
        'End Date': 7,
        'Probation Type': 8,
        'Duration*': 10,
        'Unit*': 11,
        'Probation Reason': 14,
        #'Extended End Date': 15,
        #'Probation Review': 16,
        'Probation Review Date': 17,
        'Schedule*': 18,
        'Unit': 19,
        'Note': 20

    }

    data_transform_mapping = {"Probation Type": "Employee_Probation_Period_Type_ID",
                              "Probation Reason": "Employee_Probation_Period_Reason_ID",
                              'Unit*': "Date_And_Time_Unit_ID", "Unit": "Date_And_Time_Unit_ID",
                              'Legacy Worker ID':"Worker_WID"}
    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Probation Info")
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id

def remove_html_tags(text):
    import re
    # Remove HTML tags using regular expressions
    clean_text = re.sub('<.*?>', '', str(text)) 
    return clean_text


def get_mapped_manage_goals_data(input_data, mapping_file,all_unique_data_list, unavailable_reference_id_list):
    convert_and_format_date(input_data, 'Due Date')

    input_data['spreadsheet_key'] = (input_data['Legacy Worker ID'] != input_data['Legacy Worker ID'].shift()).cumsum()
        # columns that i need to convert to a row
    columns_to_melt = ['Performance Measure Goal Category','General Measure Goal Category']
        # define the key
    id_vars = ['spreadsheet_key','Legacy Worker ID','Name','Description','Goal Weight','Due Date',	'Status']

    var_name = "Goal_Type"
    value_name = "Category"

    input_data = convert_column_to_row(input_data, id_vars, columns_to_melt, var_name, value_name)

    input_data = generate_row_id(input_data, 'spreadsheet_key', "Name", "Goal Type ID")
    
    # Apply the function to remove HTML tags from the 'text' column 
    input_data['Name'] = input_data['Name'].apply(remove_html_tags)
    input_data['Description'] = input_data['Description'].apply(remove_html_tags)

    # #ADDDING THESE REPLACES TO ACCOUNT FOR DATA ISSUE WITH HTML TAG IN OUTPUT FILE
    input_data[['Name', 'Description']] = input_data[['Name', 'Description']].apply(lambda x: x.str.replace('style=font-size','style="font-size'))
    input_data[['Name', 'Description']] = input_data[['Name', 'Description']].apply(lambda x: x.str.replace('href=http','href="http'))
    input_data[['Name', 'Description']] = input_data[['Name', 'Description']].apply(lambda x: x.str.replace('href=mailto','href="mailto'))
    input_data[['Name', 'Description']] = input_data[['Name', 'Description']].apply(lambda x: x.str.replace('class=emphasis','class="emphasis'))
    input_data[['Name', 'Description']] = input_data[['Name', 'Description']].apply(lambda x: x.str.replace('class=emphasis','class="emphasis'))
    #ONLY CLEANUP THE <P> FOR 717998
    mask = input_data['Legacy Worker ID'] == '717998'
    input_data.loc[mask, 'Name'] = input_data.loc[mask, 'Name'].apply(lambda x: x.replace('<p>', ''))
    input_data.loc[mask, 'Description'] = input_data.loc[mask, 'Description'].apply(lambda x: x.replace('<p>', ''))
    columns_mapping = {
        'spreadsheet_key': 2,
        'Legacy Worker ID': 3,
        "Goal Type ID":4,
        'Name': 7,
        'Description': 8,
        'Category': 10,
        'Goal Weight': 12,
        'Due Date': 13,
        'Status': 14
    }

    data_transform_mapping = {"Category": "Goal_Category_ID", "Status": "Component_Completion_ID",'Legacy Worker ID':"Worker_WID"}

    # data_transform_mapping = {"Status": "Component_Completion_ID"}
    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Manage Goals")
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id


def get_mapped_supplier_data(input_data, mapping_file,
                             all_unique_data_list, unavailable_reference_id_list):
    columns_mapping = {
        'key': 2,
        #'Supplier ID': 8,
        'Supplier Reference ID': 9,
        'Supplier Name*': 11,
        'Payment_types_accepted_id': 84,
        'Default_payment_Type_ID': 85,
        'Business Entity Name*': 90,
        'Supplier_status_ID': 332,
        # 'Default_payment_Type': 0,
        # 'Payment_types_accepted_type': 0,
        # 'Supplier_status_type': 0,
        # 'Approval Status Type': 10,
        'Approved_Status_ID': 14,
        # 'Suplier Category Type': 13,
        'Suplier Category ID': 44,
        'Accepted Currencies':344,

    }
    data_transform_mapping = {"Default_payment_Type_ID": "Payment_Type_ID", 
                            "Payment_types_accepted_id":"Payment_Type_ID"}
    
    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Supplier")
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id


def get_mapped_assign_work_schedule_data(input_data, mapping_file,
                                         all_unique_data_list, unavailable_reference_id_list):
    convert_and_format_date(input_data, 'Start Date*')
    convert_and_format_date(input_data, 'End Date')

    columns_mapping = {
        'key': 2,
        'Legacy Worker ID': 3,
        # 'Worker Type': 10,
        'Start Date*': 4,
        'End Date': 5,
        'Work Schedule Calendar': 6
    }
    data_transform_mapping = {'Legacy Worker ID': "Worker_WID", "Work Schedule Calendar": "Work_Schedule_Calendar_ID"}

    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Assign Work Schedule")
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id


def get_mapped_international_add_additional_job_data(input_data, mapping_file,
                                                     all_unique_data_list, unavailable_reference_id_list):
    convert_and_format_date(input_data, 'Effective Date')

    columns_mapping = {
        'key': 2,
        'Effective Date': 3,
        'Legacy Worker ID': 4,
        'Business Hierarchy': 5,
        'Position ID': 6,
        # 'Company ID': 6,
        # 'Cost Center ID': 0

    }
    data_transform_mapping = {"Business Hierarchy": "Organization_Reference_ID",'Legacy Worker ID':"Worker_ID"}
    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "put_candidate")
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id


def get_mapped_international_edit_assign_org_data(input_data, mapping_file,
                                                  all_unique_data_list,
                                                  unavailable_reference_id_list):
    columns_mapping = {
        'key': 2,
        'Company ID': 3,
        'Cost Center ID': 4

    }
    data_transform_mapping = {"Company ID": "Company_Reference_ID", "Cost Center ID": "Cost_Center_Reference_ID"}
    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "put_candidate")
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id


def get_mapped_leave_of_absence_event_data(input_data, mapping_file,
                                           all_unique_data_list, unavailable_reference_id_list):
    convert_and_format_date(input_data, 'Expected Due Date')
    convert_and_format_date(input_data, 'Childs Birth Date')
    convert_and_format_date(input_data, 'First Day of Leave (Date)')
    convert_and_format_date(input_data, 'Last Day of Work (Date)')
    convert_and_format_date(input_data, 'Estimated Last Day of Leave (Date)')
    convert_and_format_date(input_data, 'Adoption Placement Date')
    convert_and_format_date(input_data, 'Last Date for Which Paid')

    columns_mapping = {
        'key': 2,
        'Legacy Worker ID': 3,
        'Leave Type Name': 6,
        'Leave Reason': 7,
        #'Position Reference ID': 8,
        'First Day of Leave (Date)': 9,
        'Last Day of Work (Date)': 10,
        'Estimated Last Day of Leave (Date)': 11,
        # 'Job Overlap Allowed': 0,
        'Last Date for Which Paid': 14,
        'Expected Due Date': 15,
        'Childs Birth Date': 16,
        'Date Baby Arrived Home From Hospital': 19,
        'Adoption Placement Date': 20,
        'Number of Child Dependents': 27
        # 'Country ISO Code': 0

    }

    data_transform_mapping = {'Leave Type Name': "Leave_of_Absence_Type_ID",
                              'Legacy Worker ID': 'Worker_ID',
                              'Leave Reason': 'Leave_Reason_ID'
                              }
    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Leave Of Absence Events")
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id


def get_mapped_add_workday_event_data(input_data, mapping_file,
                                      all_unique_data_list, unavailable_reference_id_list):
    
    input_data['spreadsheet_key'] = (input_data['Legacy Worker ID'] != input_data['Legacy Worker ID'].shift()).cumsum()

    input_data['Password'] = "NewsCorpAustraliaisGreat1$"
    
    columns_mapping = {
        'spreadsheet_key': 2,
        'Worker Type':7,
        #'Employee Type':3,
        'Legacy Worker ID': 8,
        # 'CWR Type':5,
        # 'CWR Worker ID': 6,
        'User Name': 9,
        'Password': 10,
        'Generate Random Password': 11,
        'Required New Password At Next Login': 12,
        "Passcode Exempt":13,
        "Passcode Grace Period Enabled":14,
        "Grace Period Signins Remaining":15,
        'Account Disabled': 19,
        #'Account Locked': 15,
        'Session Timeout Minutes': 25,
        'Show User Name in Browser Window': 26,
        'Display XML Icon on Reports': 27,
        'Enable Workbox': 28,
        'Locale':29,
        'Display Language':30,
        'Allow Mixed-Language Transactions': 33,
        'Exempt from Delegated Authentication': 40,
    }

    data_transform_mapping = {
                              'Legacy Worker ID': "Worker_ID",
                              #'CWR Worker ID': "Worker_ID",
                             'Generate Random Password':'Boolean_ID',
                              'Account Disabled':'Numeric_Boolean_ID',
                            'Account Locked':'Numeric_Boolean_ID',
                            'Passcode Exempt':'Numeric_Boolean_ID',
                            'Passcode Grace Period Enabled':'Reverse_Numeric_Boolean_ID',
                             'Required New Password At Next Login': 'Numeric_Boolean_ID',
                              'Show User Name in Browser Window': 'Numeric_Boolean_ID',
                              'Display XML Icon on Reports': 'Numeric_Boolean_ID',
                              'Enable Workbox': 'Boolean_ID',
                              'Allow Mixed-Language Transactions': 'Numeric_Boolean_ID',
                              'Exempt from Delegated Authentication': 'Numeric_Boolean_ID',
                              }

    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Add Workday Account")

    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id

def get_mapped_role_based_assignments_data(input_data, mapping_file,
                                           all_unique_data_list, unavailable_reference_id_list, flag):
   

    convert_and_format_date(input_data, 'Effective Date')
    
    input_data['Hardcoded Value_Y'] = 'Y'
    input_data['Hardcoded Value_N'] = 'N'

    columns_mapping = {
        'spreadsheet_key': 2,
        'Effective Date': 3,
        'Position ID': 5,
        'Hardcoded Value_N': [6],
        "Role Row ID":7,
        'Role Assigner*': 8,
        'Assignable Role*': 10,
        #'Remove Existing Assignees for Assignable Role on Role Assigner':11,
        'Update Later Dated Assignments':12,
        'Assignees to Add+':13,
        'Remove Supervisory Organization Single Assignment Manager':16
    }
    if flag == 'Supervisory':
        data_transform_mapping = {"Assignable Role*": "Organization_Role_ID",'Role Assigner*': "Supervisory_WID"}
    else: 
        data_transform_mapping = {"Assignable Role*": "Organization_Role_ID", 
                                  'Role Assigner*': "Supervisory_to_BA_WID"}

    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Role Based Assignments")
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id

def get_mapped_edit_worker_additional_data(input_data, mapping_file,
                                           all_unique_data_list, unavailable_reference_id_list):
    
    convert_and_format_date(input_data, 'Effective Date-ANC Subledger Code')
    convert_and_format_date(input_data, 'Effective Date-Leave Loading Entitlement')
    convert_and_format_date(input_data, 'Effective Date-Sighted Worker Document')
  
    

    input_data['spreadsheet_key'] = (input_data['Legacy Worker ID'] != input_data['Legacy Worker ID'].shift()).cumsum()
    input_data.replace('', pd.NA, inplace=True)
    # Step 1: Create three separate DataFrames
    subledger_df = input_data[["spreadsheet_key","Legacy Worker ID","Effective Date-ANC Subledger Code", "ANC Subledge Code"]]
    leaveloading_df = input_data[["spreadsheet_key","Legacy Worker ID","Effective Date-Leave Loading Entitlement","Leave Loading Entitlement"]]
    sighted_df = input_data[['spreadsheet_key','Legacy Worker ID','Effective Date-Sighted Worker Document','Passport Sighted?', 'Visa Sighted?','Citizenship Sighted?', 'Birth Certificate Sighted?',
                                'Drivers License Sighted?', 'COVID Vaccination Sighted?','COVID Booster 1 Sighted?', 'Other - Please Specify']]
    
    # Step 2: Rename the columns to 'Effective Date' so that i can join using those

    subledger_df.rename(columns={'Effective Date-ANC Subledger Code': 'Effective Date'}, inplace=True)
    leaveloading_df.rename(columns={'Effective Date-Leave Loading Entitlement': 'Effective Date'}, inplace=True)
    sighted_df.rename(columns={'Effective Date-Sighted Worker Document': 'Effective Date'}, inplace=True)

    # Merge the DataFrames
    merged_df = pd.merge(subledger_df, leaveloading_df, how='outer', on=['spreadsheet_key','Legacy Worker ID', 'Effective Date'])
    merged_df = pd.merge(merged_df, sighted_df, how='outer', on=['spreadsheet_key','Legacy Worker ID', 'Effective Date'])
    print(len(merged_df))
    #IF NONE OF THE DATA POINTS are populated besides Effective date, i don't want to process
    merged_df= merged_df.dropna(subset=['ANC Subledge Code','Leave Loading Entitlement', 'Passport Sighted?',
    'Visa Sighted?', 'Citizenship Sighted?', 'Birth Certificate Sighted?',
    'Drivers License Sighted?', 'COVID Vaccination Sighted?',
    'COVID Booster 1 Sighted?', 'Other - Please Specify'], how='all')
    merged_df.reset_index(inplace=True, drop = True)
    print(len(merged_df))
    input_data = merged_df.fillna('')

    columns_mapping = {
        'key': 2,
        'Legacy Worker ID': 4,
        'Effective Date': 3,
        'ANC Subledge Code': 62,
        'Leave Loading Entitlement': 61,
        'Passport Sighted?': 53,
        'Visa Sighted?': 54,
        'Citizenship Sighted?': 55,
        'Birth Certificate Sighted?': 56,
        'Drivers License Sighted?': 57,
        'COVID Vaccination Sighted?': 58,
        'COVID Booster 1 Sighted?': 59,
        'Other - Please Specify': 60

    }
    data_transform_mapping = {'Legacy Worker ID': "Worker_WID",
                              'Passport Sighted?': "Numeric_Boolean_ID",
                              'Leave Loading Entitlement':'Leave_Loading_ID',
                                'Visa Sighted?': "Numeric_Boolean_ID",
                                'Citizenship Sighted?': "Numeric_Boolean_ID",
                                'Birth Certificate Sighted?': "Numeric_Boolean_ID",
                                'Drivers License Sighted?': "Numeric_Boolean_ID",
                                'COVID Vaccination Sighted?':"Numeric_Boolean_ID",
                                'COVID Booster 1 Sighted?': "Numeric_Boolean_ID",
                                "ANC Subledge Code":"ANC_Subledger_Code"}
    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Worker Additional Data")
    #REMOVED AS THIS IS VERY RESOURCE INTENSIVE
    #input_data = edit_worker_additional_data_helper(input_data, column_dict)

    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id


def get_mapped_nca_additional_data_position(input_data, mapping_file,
                                            all_unique_data_list, unavailable_reference_id_list):
    columns_mapping = {
        'key': 2,
        'positionRestrictions': 3,
        # 'Masthead': 9

    }
    # transformation_filed = {"Role Assigner*": "Organization_Reference_ID"}
    # get_transformation_data(input_data, transformation_filed, mapping_data_dict)

    return input_data, columns_mapping, all_unique_data_list, unavailable_reference_id_list


def get_mapped_edit_position_restrictions_additional_data_position(input_data, mapping_file,
                                                                   all_unique_data_list, unavailable_reference_id_list):
    columns_mapping = {
        'key': 2,
        'Effective Date': 3,
        'Position Restrictions': 4,
        'ncaBudgetFte': 6,
        # 'policeCheck': 0,
        # 'commissionEligible': 0

    }
    # transformation_filed = {"Role Assigner*": "Organization_Reference_ID"}
    # get_transformation_data(input_data, transformation_filed, mapping_data_dict)

    return input_data, columns_mapping, all_unique_data_list, unavailable_reference_id_list


def make_compensation_eib_data_sheet(input_data, sheet, all_unique_data_list, unavailable_reference_id_list):
    start_row = 6
    row_count_dict = {}
    row_count = 6
    input_data['id'] = input_data.groupby(['Legacy Worker ID']).ngroup()
    emp_count = 0
    spreadsheet_key = 1
    for idx, row in input_data.iterrows():
        bonus_row_id = 'bonus_row_id_' + str(row['Legacy Worker ID'])
        salary_row_id = 'Salary_start_row_' + str(row['Legacy Worker ID'])
        allowance_row_id = 'Allowance_start_row_' + str(row['Legacy Worker ID'])

        if emp_count < row['id']:
            row_count = sheet.max_row
            row_count_dict[salary_row_id] = row_count
            row_count_dict[bonus_row_id] = row_count
            row_count_dict[allowance_row_id] = row_count
            row_count_dict['Salary'] = 0
            row_count_dict['Bonus'] = 0
            row_count_dict['Allowance'] = 0
            spreadsheet_key += 1

        if row['Compensation Plan Type'] == 'Salary':
            if 'Salary' in row_count_dict.keys():
                row_count_dict['Salary'] += 1
            else:
                row_count_dict['Salary'] = 1

            if salary_row_id in row_count_dict.keys():
                row_count_dict[salary_row_id] += 1
            else:
                row_count_dict[salary_row_id] = row_count

            sheet.cell(row=row_count_dict[salary_row_id], column=20, value=row['Compensation Plan Name'])
            sheet.cell(row=row_count_dict[salary_row_id], column=21, value=row['Compensation Amount'])
            sheet.cell(row=row_count_dict[salary_row_id], column=22, value=row['Bonus or Superannuation %'])
            sheet.cell(row=row_count_dict[salary_row_id], column=24, value=row['Compensation Currency'])
            sheet.cell(row=row_count_dict[salary_row_id], column=25, value=row['Frequency'])
            sheet.cell(row=row_count_dict[salary_row_id], column=26, value=row['Expected End Date'])

            sheet.cell(row=row_count_dict[salary_row_id], column=19, value=row_count_dict['Salary'])

            sheet.cell(row=row_count_dict[salary_row_id], column=3, value=row['Legacy Worker ID'])
            sheet.cell(row=row_count_dict[salary_row_id], column=5, value=row['Effective Date'])
            sheet.cell(row=row_count_dict[salary_row_id], column=8, value=row['Compensation Reason'])
            sheet.cell(row=row_count_dict[salary_row_id], column=2, value=spreadsheet_key)

        if row['Compensation Plan Type'] == 'Bonus':
            if 'Bonus' in row_count_dict.keys():
                row_count_dict['Bonus'] += 1
            else:
                row_count_dict['Bonus'] = 1

            if bonus_row_id in row_count_dict.keys():
                row_count_dict[bonus_row_id] += 1
            else:
                row_count_dict[bonus_row_id] = row_count

            sheet.cell(row=row_count_dict[bonus_row_id], column=60, value=row['Compensation Plan Name'])
            sheet.cell(row=row_count_dict[bonus_row_id], column=61, value=row['Compensation Amount'])
            sheet.cell(row=row_count_dict[bonus_row_id], column=62, value=row['Bonus or Superannuation %'])

            sheet.cell(row=row_count_dict[bonus_row_id], column=59, value=row_count_dict['Bonus'])

            sheet.cell(row=row_count_dict[bonus_row_id], column=3, value=row['Legacy Worker ID'])
            sheet.cell(row=row_count_dict[bonus_row_id], column=5, value=row['Effective Date'])
            sheet.cell(row=row_count_dict[bonus_row_id], column=8, value=row['Compensation Reason'])
            sheet.cell(row=row_count_dict[bonus_row_id], column=2, value=spreadsheet_key)

        if row['Compensation Plan Type'] == 'Allowance':
            if 'Allowance' in row_count_dict.keys():
                row_count_dict['Allowance'] += 1
            else:
                row_count_dict['Allowance'] = 1

            if allowance_row_id in row_count_dict.keys():
                row_count_dict[allowance_row_id] += 1
            else:
                row_count_dict[allowance_row_id] = row_count

            sheet.cell(row=row_count_dict[allowance_row_id], column=38, value=row['Compensation Plan Name'])
            sheet.cell(row=row_count_dict[allowance_row_id], column=40, value=row['Compensation Amount'])
            sheet.cell(row=row_count_dict[allowance_row_id], column=39, value=row['Bonus or Superannuation %'])
            sheet.cell(row=row_count_dict[allowance_row_id], column=42, value=row['Compensation Currency'])
            sheet.cell(row=row_count_dict[allowance_row_id], column=43, value=row['Frequency'])
            sheet.cell(row=row_count_dict[allowance_row_id], column=44, value=row['Expected End Date'])

            sheet.cell(row=row_count_dict[allowance_row_id], column=37, value=row_count_dict['Allowance'])

            sheet.cell(row=row_count_dict[allowance_row_id], column=3, value=row['Legacy Worker ID'])
            sheet.cell(row=row_count_dict[allowance_row_id], column=5, value=row['Effective Date'])
            sheet.cell(row=row_count_dict[allowance_row_id], column=8, value=row['Compensation Reason'])
            sheet.cell(row=row_count_dict[allowance_row_id], column=2, value=spreadsheet_key)

            row_count += 1

        start_row += 1
        emp_count = row['id']
    return sheet, all_unique_data_list, unavailable_reference_id_list


def get_mapped_company_data(input_data, mapping_file,
                            all_unique_data_list, unavailable_reference_id_list):
    convert_and_format_date(input_data, 'Effective Date')

    input_data['Add_Only'] = 'Y'
    columns_mapping = {
        'key': 2,
        'Effective Date': [5, 13],
        'Company Reference Id': 6,
        'Company Name': 8,
        'Company Code': 10,
        #'Organization Type Name*': 17,
        #'Organization Subtype Name*': 18,
        'Add Container Organization+': 20,
        'Currency': 23

    }
    data_transform_mapping = {"Add Container Organization+": "Organization_Reference_ID",
                              "Currency": "Currency_ID"}
    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Company")
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id

def get_mapped_job_family_data(input_data, mapping_file,
                               all_unique_data_list, unavailable_reference_id_list):
    convert_and_format_date(input_data, 'Effective Date')

    input_data['Add_Only'] = 'Y'
    columns_mapping = {
        'key': 2,
        'Effective Date': 6,
        'Job Family ID': 5,
        'Job Family Name': 7

    }
    # data_transform_mapping = {"Job Family ID": "Job_Family_ID"}
    # input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
    #                                                                                   data_transform_mapping,
    #                                                                                   mapping_file,
    #                                                                                   all_unique_data_list,
    #                                                                                   unavailable_reference_id_list,
    #                                                                                   "Job Family")
    return input_data, columns_mapping, all_unique_data_list, unavailable_reference_id_list


def get_mapped_job_family_group_data(input_data, mapping_file,
                                     all_unique_data_list, unavailable_reference_id_list):
    convert_and_format_date(input_data, 'Effective Date')
    input_data['Add_Only'] = 'Y'

    #overwrite the job family group name with job family name and id with id, so that i can remap them to respective groups
    input_data['Job Family Group Name'] = input_data['Attached Job Family Name']
    input_data['Job Family Group ID'] = input_data['Attached Job Family Id']

    columns_mapping = {
        'key': 2,
        #'Add_Only': 3,
        'Effective Date': 6,
        'Job Family Group Name': 7,
        'Job Family Group ID': 5,
        #'Job Family Group Summary': 8,
        'Attached Job Family Name': 13,
        'Attached Job Family Id': 12

    }
    data_transform_mapping = {"Job Family Group Name": "Job_Family_Group_Name",
                                "Job Family Group ID": "Job_Family_Group_ID",
                                "Attached Job Family Name": "Job_Family_Name",
                                "Attached Job Family Id": "Job_Family_ID"}
    
    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Job Family Group")
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id


def get_mapped_cost_center_data(input_data, mapping_file,
                                all_unique_data_list, unavailable_reference_id_list):
    convert_and_format_date(input_data, 'Effective Date')
    input_data['Add_Only'] = 'Y'
    input_data['spreadsheet_key'] = (input_data['Cost Center ID'] != input_data['Cost Center ID'].shift()).cumsum()
    columns_mapping = {
        'spreadsheet_key': 2,
        'Add_Only': 3,
        'Effective Date': [5,14],
        'Cost Center Name': 9,
        'Cost Center ID': [7],
        #'Organization Type Name*': 8,
        #'Organization Subtype Name*': 9,
        'Organization Code': 11,
        'Cost Center Hierarchy': 21

    }
    # data_transform_mapping = {"Job Family ID": "Job_Family_ID"}
    # input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
    #                                                                                   data_transform_mapping,
    #                                                                                   mapping_file,
    #                                                                                   all_unique_data_list,
    #                                                                                   unavailable_reference_id_list,
    #                                                                                   "Cost Center")
    return input_data, columns_mapping, all_unique_data_list, unavailable_reference_id_list


def get_mapped_location_hierarchy_data(input_data, mapping_file,
                                       all_unique_data_list, unavailable_reference_id_list):
    convert_and_format_date(input_data, 'Effective Date')

    columns_mapping = {
        'key': 2,
        'Effective Date': [3, 9],
        'Location Hierarchy ID': 4,
        'Location Hierarchy Name': 8,
        'Superior Organization Name': 16
    }

    data_transform_mapping = {"Superior Organization Name": "Organization_Reference_ID"}
    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Location Hierarchy")
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id

def get_mapped_custom_organizations_data(input_data, mapping_file,
                                         all_unique_data_list, unavailable_reference_id_list):
    convert_and_format_date(input_data, 'Effective Date')

    columns_mapping = {
        'key': 2,
        'Effective Date': [3, 9],
        'Organization ID': 4,
        'Organization Name': 8,
        'Include Organization Code in Name?': 10,
        'Organization Code': 11,
        'Organization Type': 17,
        'Organization Subtype': 18,
        'Superior Organization Name': 21
    }

    data_transform_mapping = {"Organization Type": "Organization_Type_ID", "Organization Subtype": "Organization_Subtype_ID",
                              "Superior Organization Name": "Custom_Org_Superior_Org_ID"}
    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Custom Organizations")
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id


def get_mapped_cost_center_hierarchy_data(input_data, mapping_file,
                                          all_unique_data_list, unavailable_reference_id_list):
    
    convert_and_format_date(input_data, 'Effective Date')

    columns_mapping = {
        'key': 2,
        'Effective Date': [3, 9],
        'Cost Center ID': 4,
        'Cost Center Hierarchy Name': 8,
        'Organization Code': 11,
        'Superior Org Ref ID': 16,  # Need to make sure for mapping
        'Organization Type Name*': 17,
        'Organization Subtype Name*': 18
    }

    data_transform_mapping = {"Organization Type Name*": "Organization_Type_Name_ID",
                              "Organization Subtype Name*": "Organization_Subtype_Name_ID"
                              }
    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Cost Center Hierarchy")
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id


def get_mapped_collective_agreement_data(input_data, mapping_file,
                                         all_unique_data_list, unavailable_reference_id_list):
    convert_and_format_date(input_data, 'Collective Agreement Start Date')
    input_data['Add_Only'] = 'Y'

    columns_mapping = {
        'key': 2,
        'Add_Only': 3,
        'Collective Agreement': 5,
        'Collective Agreement ID': 6,
        'Collective Agreement Start Date': 8,
        'Collective Agreement Country': 10,
        'Collective Agreement Eligibility Rule ID': 12,  
    }

    data_transform_mapping = {"Collective Agreement Country": "ISO_3166-1_Alpha-3_Code"}
    
    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Collective Agreement")
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id

def get_mapped_job_classification_data(input_data, mapping_file,
                                         all_unique_data_list, unavailable_reference_id_list):
    convert_and_format_date(input_data, 'Effective Date')
    input_data['Add_Only'] = 'Y'

    columns_mapping = {
        'key': 2,
        'Add_Only': 3,
        'Job Classification Group Id': 5,
        'Effective Date': 6,
        'Job Classification Group Name': 7,
        'Country': 9,
        'Job Classification Name': 16,
        'Job Classification ID': 15,
    }

    data_transform_mapping = {"Country": "ISO_3166-1_Alpha-3_Code"}
    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Job Classification")
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id
    
    
def get_mapped_put_supervisory_assignment_restrictions_data(input_data, columns_mapping, all_unique_data_list,
                                                            unavailable_reference_id_list):
    
    #every superbvisiory org should have NCA division restriction, so hardcoding the value to avoid more chnages
    input_data['Default Division'] = "DIVISION_NCA"
    input_data['Allowed Division'] = "DIVISION_NCA"
    
    input_data['spreadsheet_key'] = (input_data['Supervisory Organization'] != input_data['Supervisory Organization'].shift()).cumsum()
    # Reshape the DataFrame for cost center
    df_cost_center = input_data[['spreadsheet_key', 'Supervisory Organization', 'Default Cost Centre', 'Allowed Cost Centre']]
    df_cost_center.columns = ['spreadsheet_key', 'Supervisory Organization','Default Org', 'Allowed Org']
    df_cost_center.loc[:, 'Org Type'] = 'COST_CENTER'
    

    # Reshape the DataFrame for company
    df_company = input_data[['spreadsheet_key', 'Supervisory Organization', 'Default Company', 'Allowed Company']]
    df_company.columns = ['spreadsheet_key', 'Supervisory Organization','Default Org', 'Allowed Org']
    df_company.loc[:, 'Org Type'] = 'COMPANY'

    # Reshape the DataFrame for line of business
    df_line_of_business = input_data[['spreadsheet_key', 'Supervisory Organization', 'Default Line of Business', 'Allowed Line of Business']]
    df_line_of_business.columns = ['spreadsheet_key', 'Supervisory Organization', 'Default Org', 'Allowed Org']
    df_line_of_business.loc[:, 'Org Type'] = 'BUSINESS_AREA'

    # Reshape the DataFrame for Division
    df_division = input_data[['spreadsheet_key', 'Supervisory Organization', 'Default Division', 'Allowed Division']]
    df_division.columns = ['spreadsheet_key', 'Supervisory Organization','Default Org', 'Allowed Org']
    df_division.loc[:, 'Org Type'] = 'DIVISION'

    # Combine the three DataFrames
    final_df = pd.concat([df_cost_center, df_company, df_line_of_business, df_division], ignore_index=True)

    # Drop rows where both default and allowed values are null
    final_df = final_df.dropna(subset=['Default Org', 'Allowed Org'], how='all')

    # Reset the index
    input_data = final_df.sort_values(by=['spreadsheet_key', 'Supervisory Organization','Org Type'])
    input_data.reset_index(drop=True, inplace=True)

    input_data = generate_row_id(input_data, 'spreadsheet_key','Org Type', "Org Type Row ID")
    input_data = generate_row_id(input_data, 'spreadsheet_key','Default Org', "Default Row ID")
    input_data = generate_row_id(input_data, 'spreadsheet_key','Allowed Org', "Allowed Row ID")
    input_data["Replace All"]= "Y"
    columns_mapping = {
        'spreadsheet_key': 2,
        'Replace All': 3,
        'Supervisory Organization': 4,
        "Org Type Row ID":5,
        "Org Type":6,
        "Allowed Row ID":7,
        'Allowed Org':9,
        "Default Row ID":10,
        'Default Org':12,
    }
    # start_row = 6
    # supervisory_records = {}

    # for idx, row in input_data.iterrows():
    #     key_number = row['key_number']
    #     key = row["Supervisory Organization"]
    #     key_number += 1
    #     row_id = 1

    #     for record_type in ['Line of Business', 'Company', 'Cost Centre']:
    #         if pd.notna(row[f'Default {record_type}']) or pd.notna(row[f'Allowed {record_type}']):
    #             sheet.cell(row=start_row, column=2, value=key_number)
    #             sheet.cell(row=start_row, column=4, value=row["Supervisory Organization"])
    #             sheet.cell(row=start_row, column=5, value=row_id)
    #             if record_type == "Line of Business":
    #                 sheet.cell(row=start_row, column=6, value="BUSINESS_AREA")
    #             elif record_type == "Company":
    #                 sheet.cell(row=start_row, column=6, value="COMPANY")
    #             elif record_type == "Cost Centre":
    #                 sheet.cell(row=start_row, column=6, value="COST_CENTER")
                
    #             record_key = f"{key}_{record_type}"
    #             if record_key not in supervisory_records:
    #                 supervisory_records[record_key] = 1
                    
    #             sheet.cell(row=start_row, column=7, value=supervisory_records[record_key])
                
    #             if pd.notna(row[f'Allowed {record_type}']):
    #                 sheet.cell(row=start_row, column=9, value=row[f'Allowed {record_type}'])
    #             if pd.notna(row[f'Default {record_type}']):
    #                 sheet.cell(row=start_row, column=12, value=row[f'Default {record_type}'])
                
    #             supervisory_records[record_key] += 1

    #             start_row += 1
    #             row_id += 1

    return input_data,columns_mapping, all_unique_data_list, unavailable_reference_id_list

def get_mapped_location_data(input_data, mapping_file,
                             all_unique_data_list, unavailable_reference_id_list):
    convert_and_format_date(input_data, 'Effective Date')
    input_data['Add_Only'] = 'Y'

    columns_mapping = {
        'key': 2,
        'Add_Only': 3,
        'Effective Date': 50,
        'Location Name*': 8,
        'Location ID': 6,
        'Location Usage*+*': 9,
        'Location Type+': 10,
        'Time Profile': 18,
        'Display Language': 20,
        'Time Zone': 21,
        'Default Currency':22,
        'Municipality': 57,
        'Country Region': 64,
        'Country': [51, 59],
        'Postal Code': 70
        #'Location Hierarchy': 154
    }

    data_transform_mapping = {"Location ID": "Location_ID", 
                              "Time Profile": "Time_Profile_ID", 
                              "Display Language": "User_Language_ID",
                              "Time Zone": "Time_Zone_ID", 
                              "Country": "ISO_3166-1_Alpha-3_Code",
                              "Location Usage*+*": "Location_Usage_ID", 
                              "Country Region": "Country_Region_ID",
                              'Country':"ISO_3166-1_Alpha-3_Code"
                              #"Location Hierarchy": "Location_Hierarchy_ID"
                              }
    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Location")
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id




def get_mapped_job_category_data(input_data, mapping_file,
                                 all_unique_data_list, unavailable_reference_id_list):
    # convert_and_format_date(input_data, 'Effective Date')
    input_data['Add_Only'] = 'Y'

    columns_mapping = {
        'key': 2,
        'Add_Only': 3,
        'Job Category Name': 6,
        'Job Category ID': 5,
        'Job Category Description': 7
    }

    # data_transform_mapping = {"Job Category ID": "Job_Category_ID"}
    # input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
    #                                                                                   data_transform_mapping,
    #                                                                                   mapping_file,
    #                                                                                   all_unique_data_list,
    #                                                                                   unavailable_reference_id_list,
    #                                                                                   "Job Category")
    return input_data, columns_mapping, all_unique_data_list, unavailable_reference_id_list


def get_mapped_job_profile_data(input_data, mapping_file,
                                all_unique_data_list, unavailable_reference_id_list):
    convert_and_format_date(input_data, 'Effective Date')
    input_data['Add_Only'] = 'Y'

    input_data = generate_row_id(input_data, 'Job Code','Job Profile Exempt - Country/Country Region', "Row ID")
    columns_mapping = {
        'key': 2,
        'Add_Only': 3,
        #'Job Profile ID':4,
        'Job Code': 5,
        'Effective Date': 6,
        'Job Title': 8,
        'Job Profile Summary': 11,
        'Job Description': 12,
        'Work Shift Required': 14,
        'Is Job Public': 15,
        'Inactive': 7,
        'Management Level': 16,
        'Job Category': 17,
        'Job Level': 18,
        'Job Family*': 21,
        'Company Insider Type*': 24,
        'Referral Payment Plan': 25,
        'Critical Job': 26,
        'Difficulty to Fill': 27,
        'Job Classification*': 31,
        'Pay Rate Type Country': 34,
        'Pay Rate Type': 35,
        'Row ID': 36,
        'Job Profile Exempt - Country/Country Region': 38,
        'Job Exempt': 40,
        'Compensation Grade': 96,
        'Compensation Grade Profile': 97,
        'Allowed Unions': 98,
    }

    data_transform_mapping = {"Inactive": "Numeric_Boolean_ID", "Work Shift Required": "Numeric_Boolean_ID",
                              "Is Job Public": "Numeric_Boolean_ID", "Management Level": "Management_Level_ID",
                              "Job Category": "Job_Category_ID", "Job Family*": "Job_Family_ID",
                              "Job Classification*": "Job_Classification_Reference_ID",
                              "Pay Rate Type Country": "ISO_3166-1_Alpha-3_Code", "Pay Rate Type": "Pay_Rate_Type_ID",
                              "Job Exempt": "Numeric_Boolean_ID",
                              'Job Profile Exempt - Country/Country Region':"ISO_3166-1_Alpha-3_Code"
                              }
    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Job Profile")
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id



def get_mapped_comp_grade_and_grade_profile_data(input_data, mapping_file,
                                                 all_unique_data_list, unavailable_reference_id_list):
    convert_and_format_date(input_data, 'Effective Date')
    input_data['Add_Only'] = 'N'

    columns_mapping = {
        'key': 2,
        'Add_Only': 3,
        'Compensation Grade Name': 7,
        'Compensation Grade ID': 5,
        'Effective Date': [6, 40],
        'Grade Description': 8,
        'Compensation Grade Element': 9,
        'Compensation Grade Eligibility Rule': 10,
        'Compensation Grade - Currency': 21,
        'Compensation Grade - Frequency': 22,
        'Compensation Grade - Allow Override': 24,
        'Compensation Grade Profile Name': 41,
        'Compensation Grade Profile ID': 39,
        'Grade Profile Description': 42,
        'Grade Profile Compensation Element': 43,
        'Grade Profile Eligibility Rule': 44,
        'Number of Pay Range Segments': 46,
        'Minimum': 47,
        'Midpoint': 48,
        'Maximum': 49,
        'Segment 1 Top': 51,
        'Segment 2 Top': 52,
        'Segment 3 Top': 53,
        'Segment 4 Top': 54,
        'Grade Profile - Currency': 56,
        'Grade Profile - Frequency': 57,
        'Compensation Plan': 58,
        'Grade Profile - Allow Override (New)': 59,
        'Compensation Step Reference ID': 63,
        'Sequence': 65,
        'Name': 66,
        'Amount': 67,
        'Interval': 68,
        'Period': 69,
        'Progression Rule': 70

    }

    data_transform_mapping = {"Compensation Grade Element": "Compensation_Element_ID",
                              "Compensation Grade - Frequency": "Frequency_ID",
                              "Compensation Grade - Allow Override": "Numeric_Boolean_ID",
                              "Grade Profile Compensation Element": "Compensation_Element_ID",
                              "Grade Profile - Frequency": "Frequency_ID", "Compensation Plan": "Compensation_Plan_ID",
                              "Grade Profile - Allow Override (New)": "Numeric_Boolean_ID", 
                              "Grade Profile Eligibility Rule": "Condition_Rule_ID",
                              "Compensation Grade Eligibility Rule": "Condition_Rule_ID"
                              }
    
    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
       
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Comp Grade and Grade Profile")
    
    #input_data['Grade Profile Eligibility Rule'] ="CONDITION_RULE_Worker_in_Australia"
    #input_data['Compensation Grade Eligibility Rule'] ="CONDITION_RULE_Worker_in_Australia"
    
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id

def job_requisition_additional_data_helper(df):
    new_data = []
    # Iterate over the original DataFrame
    for _, row in df.iterrows():
        # Check if the effective dates are the same
        if row['Effective Date (Who Are We?)'] == row['Effective Date (Police Check)']:
            # Append the row as is to the new DataFrame and blank out 'Effective Date (Who Are We?)'
            new_row = row.copy()
            new_row['Effective Date (Police Check)'] = ''
            new_data.append(new_row)
        else:
            if pd.notna(row['Effective Date (Who Are We?)']) and pd.notna(row['Effective Date (Police Check)']):
                new_row = row.copy()
                new_row['Effective Date (Police Check)'] = ''
                #new_row['Police Check'] = ''
                new_data.append(new_row)
                # Create a new row with the 'Effective Date (Police Check)' moved to 'Effective Date (Who Are We?)'
                new_row = row.copy()
                new_row['Effective Date (Who Are We?)'] = row['Effective Date (Police Check)']
                new_row['Effective Date (Police Check)'] = ''
                #new_row['Who Are We?'] = ''
                new_data.append(new_row)
            elif pd.notna(row['Effective Date (Who Are We?)']) and pd.isna(row['Effective Date (Police Check)']):
                new_row = row.copy()
                new_row['Effective Date (Police Check)'] = ''
                #new_row['Police Check'] = ''
                new_data.append(new_row)
            elif pd.isna(row['Effective Date (Who Are We?)']) and pd.notna(row['Effective Date (Police Check)']):
                new_row = row.copy()
                new_row['Effective Date (Who Are We?)'] = row['Effective Date (Police Check)']
                new_row['Effective Date (Police Check)'] = ''
                #new_row['Who Are We?'] = ''
                new_data.append(new_row)

    # Create a new DataFrame from the modified data
    new_df = pd.DataFrame(new_data)
    return new_df

def get_mapped_job_requisition_additional_data(input_data, mapping_file,
                                                 all_unique_data_list, unavailable_reference_id_list):
        
    #exclude the records that have no Police Check or Who are we
    input_data = input_data.dropna(subset=['Police Check', 'Who Are We?'], how='all').reset_index(drop=True)

    # Convert columns to datetime
    input_data['Effective Date (Who Are We?)'] = pd.to_datetime(input_data['Effective Date (Who Are We?)'])
    input_data['Effective Date (Police Check)'] = pd.to_datetime(input_data['Effective Date (Police Check)'])

    # Create the new 'Effective Date' column with the max date
    input_data['Effective Date'] = input_data[['Effective Date (Who Are We?)', 'Effective Date (Police Check)']].min(axis=1)

    convert_and_format_date(input_data, 'Effective Date')

    input_data['spreadsheet_key'] = (input_data['Job Requisition ID'] != input_data['Job Requisition ID'].shift()).cumsum()
    input_data['req_row_id'] = 1

    columns_mapping = {
        'spreadsheet_key': 2,
        'Effective Date': 3,
        'Job Requisition ID': 4,
        'req_row_id': 5,
        'Police Check': 21,
        'Who Are We?': 20
    }
   
    data_transform_mapping = {"Police Check": "Police_Check_ID"}
    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Job Requisition Additional Data")
    
    #input_data = job_requisition_additional_data_helper(input_data)
    #input_data = input_data.groupby('Job Requisition ID').apply(lambda x: x.sort_values('Effective Date (Who Are We?)')).reset_index(drop=True)
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id

def get_mapped_employee_compensation_data(input_data, mapping_file,
                                     all_unique_data_list,
                                     unavailable_reference_id_list):
    convert_and_format_date(input_data, 'Effective Date')

    input_data['spreadsheet_key'] = (input_data['Legacy Worker ID'] != input_data['Legacy Worker ID'].shift()).cumsum()
    input_data[['Allowance Amount', 'Allowance Percentage']] = input_data[['Allowance Amount', 'Allowance Percentage']].replace("0", "")
    input_data = generate_row_id(input_data, 'spreadsheet_key', "Bonus Plan", "Bonus Row ID")
    input_data = generate_row_id(input_data, 'spreadsheet_key', "Allowance Plan", "Allowance Row ID")
    input_data['Compensation Reason'] = "GES-RCC-CON-BUM"

    columns_mapping = {
        'spreadsheet_key': 2,
        'Legacy Worker ID': 3,
        'Effective Date': 5,
        'Compensation Reason': 8,
        'Compensation Package ID': 13,
        'Compensation Grade ID': 14,
        'Compensation Profile ID': 15,
        'Compensation Step ID':16,
        'Salary Plan': 20,
        'Salary Amount': 21,
        'Salary Currency': 24,
        'Salary Frequency': 25
    }

    data_transform_mapping = { 'Legacy Worker ID':"Worker_ID",
                              'Salary Frequency': 'Frequency_ID',
                              'Hourly Frequency': 'Frequency_ID',
                              'Salary Plan': 'Compensation_Plan_ID',
                              'Hourly Plan': 'Compensation_Plan_ID',
                              'Bonus Plan': 'Compensation_Plan_ID',
                              'Allowance Plan': 'Compensation_Plan_ID',
                              'Allowance Frequency': 'Frequency_ID',
                              'Compensation Step ID':'Compensation_Step_ID',
                              'Compensation Profile ID': 'Compensation_Grade_Profile_ID',
                              'Compensation Grade ID': 'Compensation_Grade_ID',
                              'Compensation Package ID': 'Compensation_Package_ID'}
    
    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Employee Compensation")
    
    #Wipe out the step if grade is NCA_No Grade
    #input_data['Compensation Step ID'] = input_data.loc[input_data['Compensation Profile ID'] == 'COMPENSATION_GRADE_PROFILE-6-559', 'Compensation Step ID'] = ""

    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id

def get_mapped_edit_workday_account_data(input_data, mapping_file, all_unique_data_list, unavailable_reference_id_list):
    
    input_data['spreadsheet_key'] = (input_data['Legacy Worker ID'] != input_data['Legacy Worker ID'].shift()).cumsum()
    #create new column for CWR ID
    input_data['CWR Worker ID'] = input_data['Legacy Worker ID']

    # Blank out 'Legacy Worker ID' if 'Worker Type ID' is 'Contingent_Worker_ID'
    input_data['Legacy Worker ID'] = input_data.apply(lambda row: '' if row['Worker Type'] == 'Contingent_Worker_ID' else row['Legacy Worker ID'], axis=1)
    
    # Blank out 'Legacy Worker ID' if 'Worker Type ID' is 'Contingent_Worker_ID'
    input_data['Employee Type'] = input_data.apply(lambda row: '' if row['Worker Type'] == 'Contingent_Worker_ID' else "WD-EMPLID", axis=1)

    # Blank out 'CWR Worker ID' if 'Worker Type ID' is 'Employee_ID'
    input_data['CWR Type'] = input_data.apply(lambda row: '' if row['Worker Type'] == 'Employee_ID' else "WD-EMPLID", axis=1)
    # Blank out 'CWR Worker ID' if 'Worker Type ID' is 'Employee_ID'
    input_data['CWR Worker ID'] = input_data.apply(lambda row: '' if row['Worker Type'] == 'Employee_ID' else row['CWR Worker ID'], axis=1)
    input_data['Password'] = "NewsCorpAustraliaisGreat5$"
    
    columns_mapping = {
        'spreadsheet_key': 2,
        #'Worker Type':7,
        'Employee Type':3,
        'Legacy Worker ID': 4,
        'CWR Type':5,
        'CWR Worker ID': 6,
        'User Name': 8,
        'Password': 9,
        'Generate Random Password': 10,
        'Required New Password At Next Login': 11,
        #'Account Locked': 15,
        'Account Disabled': 18,
        "Passcode Exempt":12,
        "Passcode Grace Period Enabled":13,
        "Grace Period Signins Remaining":14,
        'Session Timeout Minutes': 24,
        'Show User Name in Browser Window': 25,
        'Display XML Icon on Reports': 26,
        'Enable Workbox': 27,
        'Locale':28,
        'Display Language':29,
        'Allow Mixed-Language Transactions': 32,
        'Exempt from Delegated Authentication': 39,
    }

    data_transform_mapping = {
                              'Legacy Worker ID': "Worker_ID",
                              'CWR Worker ID': "Worker_ID",
                             'Generate Random Password':'Boolean_ID',
                              'Account Disabled':'Numeric_Boolean_ID',
                            'Account Locked':'Numeric_Boolean_ID',
                            "Passcode Exempt":'Numeric_Boolean_ID',
                              "Passcode Grace Period Enabled":'Reverse_Numeric_Boolean_ID',
                             'Required New Password At Next Login': 'Numeric_Boolean_ID',
                              'Show User Name in Browser Window': 'Numeric_Boolean_ID',
                              'Display XML Icon on Reports': 'Numeric_Boolean_ID',
                              'Enable Workbox': 'Boolean_ID',
                              'Allow Mixed-Language Transactions': 'Numeric_Boolean_ID',
                              'Exempt from Delegated Authentication': 'Numeric_Boolean_ID',
                              }

    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Update Workday Account")

    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id

def get_mapped_time_off_events_data(input_data, mapping_file,
                                          all_unique_data_list,
                                          unavailable_reference_id_list):
    convert_and_format_date(input_data, 'Time off Date')
    
    input_data = input_data[input_data["Time Off Status"].isin(['Approved'])]
    
    input_data['Time Off Units'] = input_data['Time Off Units'].astype(float)

    #input_data = input_data[input_data['Time Off Reason Code'].isna()]

    input_data['Time Off Reason Code'] = input_data['Time Off Reason Code'].fillna("")
    
    input_data = input_data.groupby(['Legacy Worker ID', 'Time off Date', 'Time Off Type Name or Code','Time Off Reason Code'],as_index=False).agg({'Time Off Units': 'sum'}).reset_index()
    # Filter rows where the sum is negative
    print(input_data[input_data["Legacy Worker ID"].isin(["708463"])])
    negative_sums_df = input_data[input_data['Time Off Units'] < 0.0]
    negative_sums_df = negative_sums_df.groupby(['Legacy Worker ID', 'Time Off Type Name or Code'],as_index=False).agg({'Time off Date': 'max','Time Off Units': 'sum'})
    # Save negative sums to a new CSV file
    negative_sums_df.to_csv(r"C:\\Source_Code\\negative_time_off_units.csv", index=False)
   
    # Filter out rows where the sum of 'Time Off Units' is 0
    input_data = input_data[input_data['Time Off Units'] > 0.0]
    print(input_data[input_data["Legacy Worker ID"].isin(["708463"])])
    input_data = input_data.reset_index(drop=True)
    
    
    input_data['spreadsheet_key'] = (input_data['Legacy Worker ID'] != input_data['Legacy Worker ID'].shift()).cumsum()
    input_data = generate_row_id(input_data, 'spreadsheet_key', "Time off Date", "Row ID")

    columns_mapping = {
        'key': 2,
        'Legacy Worker ID': 5,
        'Row ID': 6,
        'Time off Date': 8,
        'Time Off Units': 11,
        'Time Off Type Name or Code': 13,
        'Time Off Reason Code': 16,
        #'Country ISO Code': 0,

    }

    data_transform_mapping = {
                              'Legacy Worker ID': "Worker_WID", 'Time Off Type Name or Code':"Time_Off_Code"
                              }

    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Time Off Events")

    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id

def get_mapped_assign_notice_period_data(input_data, mapping_file,
                                        all_unique_data_list,
                                        unavailable_reference_id_list):

    convert_and_format_date(input_data, 'Effective Date*')
 
    # Apply the logic to blank out 'Duration - Employer' and 'Unit - Employer' when 'Derive Notice Period - Employer' is 1
    input_data.loc[input_data['Derive Notice Period - Employer'] == "1", ['Duration - Employer', 'Unit - Employer']] = ""
    # Apply the logic to blank out 'Duration - Employer' and 'Unit - Employer' when 'Derive Notice Period - Employer' is 1
    input_data.loc[input_data['Derive Notice Period - Employee'] == "1", ['Duration - Employee', 'Unit - Employee']] = ""

    columns_mapping = {
        'key': 2,
        'Legacy Worker ID': 3,
        'Effective Date*': 4,
        'Derive Notice Period - Employer': 5,
        'Duration - Employer': 6,
        'Unit - Employer': 7,
        'Adjustment - Employer': 8,
        'Derive Notice Period - Employee': 9,
        'Duration - Employee': 10,
        'Unit - Employee': 11,
        'Adjustment - Employee': 12
    }
    data_transform_mapping = {
                              'Legacy Worker ID': "Worker_WID", 
                              'Derive Notice Period - Employer': "Numeric_Boolean_ID",
                              'Derive Notice Period - Employee': 'Numeric_Boolean_ID', 
                              'Unit - Employee': 'Date_And_Time_Unit_ID',
                              'Unit - Employer': 'Date_And_Time_Unit_ID'
                              }

    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Assign Notice Period")

    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id


def get_mapped_user_based_assignments_data(input_data, mapping_file,
                                           all_unique_data_list,
                                           unavailable_reference_id_list):

    # Drop 'User-Based Security Group*' from the DataFrame
    input_data = input_data.sort_values(by=['Position ID']).reset_index(drop=True)

    # 
    #input_data = input_data[input_data["Position ID"]=='307320']
    # input_data.reset_index(inplace=True)
    input_data['spreadsheet_key'] = (input_data['Position ID'] != input_data['Position ID'].shift()).cumsum()
    input_data['Hardcoded Value_N'] = "N"
    

    columns_mapping = {
        'key': 2,
        'Effective Date': 3,
        'Position ID': [5,13],
        'Hardcoded Value_N': [6],
        "Role Row ID":7,
        'Role Assigner*': 8,
        'User-Based Security Group*': 10,
    }
    
    data_transform_mapping = {'Role Assigner*': "Supervisory_to_BA_WID", 'User-Based Security Group*': "User_Based_Security_Group_ID"}

    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "User Based")
    
    input_data = input_data.drop_duplicates().reset_index(drop=True)

    input_data = generate_row_id(input_data, 'spreadsheet_key', "User-Based Security Group*", "Role Row ID")
   
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id

def get_mapped_licenses_data(input_data, mapping_file,
                             all_unique_data_list,
                             unavailable_reference_id_list):
    input_data['spreadsheet_key'] = (input_data['Legacy Worker ID'] != input_data['Legacy Worker ID'].shift()).cumsum()
    input_data = generate_row_id(input_data, 'spreadsheet_key', "License Type", "Row ID")

    columns_mapping = {
        'spreadsheet_key': 2,
        'Legacy Worker ID': 3,
        'License ID': 9,
        'Row ID': 6,
        'License Type': 10,
        'Issue Date': 16,
        'Expiration Date': 17

    }

    data_transform_mapping = {
        'Legacy Worker ID': "Worker_WID", #"License ID": "License_Identifier_ID",
        'License Type': 'License_ID_Type_ID'
    }

    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Licenses")

    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id


def get_mapped_job_history_data(input_data, mapping_file,
                                           all_unique_data_list,
                                           unavailable_reference_id_list):
    convert_and_format_date(input_data, 'Start Date')
    convert_and_format_date(input_data, 'End Date')

    input_data['spreadsheet_key'] = (input_data['Legacy Worker ID'] != input_data['Legacy Worker ID'].shift()).cumsum()
    input_data = generate_row_id(input_data, 'spreadsheet_key', "Job History", "Row ID")

    columns_mapping = {
        'spreadsheet_key': 2,
        'Legacy Worker ID': 3,
        'Source': 4,
        'Row ID': [5, 7],
        # 'Job History': 6,
        # 'Job History ID': 8,
        'Remove Job History': 9,
        'Job Title': 10,
        'Company': 11,
        'Job History Company': 12,
        'Start Date': 13,
        'End Date': 14,
        'Responsibilities and Achievements': 15,
        'Location': 16,
        'Job Reference': 17,
        'Contact': 18

    }

    data_transform_mapping = {
        'Legacy Worker ID': "Worker_WID", 'Source': 'Skill_Source_Category_ID',
        'Job History Company': 'Job_History_Company_ID',

    }

    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Job History")

    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id


def get_mapped_education_data(input_data, mapping_file,
                                           all_unique_data_list,
                                           unavailable_reference_id_list):

    convert_and_format_date(input_data, 'Date Degree Received')
    convert_and_format_date(input_data, 'First Year Attended')
    convert_and_format_date(input_data, 'First Day Attended')
    convert_and_format_date(input_data, 'Last Year Attended')
    convert_and_format_date(input_data, 'Last Day Attended')

    input_data['spreadsheet_key'] = (input_data['Legacy Worker ID'] != input_data['Legacy Worker ID'].shift()).cumsum()
    input_data = generate_row_id(input_data, 'spreadsheet_key', "Education", "Row ID")

    columns_mapping = {
        'key': 2,
        'Legacy Worker ID': 3,
        'Source': 4,
        'Row ID': [5, 7],
        'Education': 6,
        'Education ID': 8,
        'Remove Education': 9,
        'Country': 10,
        'School': 11,
        'School Name': 12,
        'School Type': 13,
        'Location': 14,
        'Degree': 15,
        'Degree Completed': 16,
        'Date Degree Received': 17,
        'Grade Average': 19,
        'Field Of Study': 18,
        'First Year Attended': 20,
        'Last Year Attended': 21,
        'Is Highest Level of Education': 22,
        'First Day Attended': 23,
        'Last Day Attended': 24

    }

    data_transform_mapping = {
        'Legacy Worker ID': "Worker_WID", 'Source': 'Skill_Source_Category_ID',
        'Education': 'Education_ID', 'Country': 'ISO_3166-1_Alpha-3_Code',
        'School': 'School_ID', 'School Type': 'School_Type_ID', 'Degree': 'Degree_ID',
        'Field Of Study': 'Field_Of_Study_ID',

    }

    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Education")

    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id
		

def get_mapped_skills_data(input_data, mapping_file,
                                           all_unique_data_list,
                                           unavailable_reference_id_list):

    input_data['spreadsheet_key'] = (input_data['Legacy Worker ID'] != input_data['Legacy Worker ID'].shift()).cumsum()
    input_data = generate_row_id(input_data, 'spreadsheet_key', "Skill Item", "Row ID")

    columns_mapping = {
        'spreadsheet_key': 2,
        'Legacy Worker ID': 3,
        'Row ID': 4,
        'Skill Item': 5,
        'Remove Skill': 6,
        'Skill Name': 7
        }

    data_transform_mapping = {'Legacy Worker ID': "Worker_WID", 'Skill Item': 'Skill_ID'}

    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Skills")

    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id


def get_mapped_other_ids_data(input_data, mapping_file,
                                           all_unique_data_list,
                                           unavailable_reference_id_list):
    convert_and_format_date(input_data, 'Issued Date')
    convert_and_format_date(input_data, 'Expiration Date')
    
    input_data['spreadsheet_key'] = (input_data['Legacy Worker ID'] != input_data['Legacy Worker ID'].shift()).cumsum()
    input_data = generate_row_id(input_data, 'spreadsheet_key', "Legacy Worker ID", "Row ID")
    #input_data['ID'] = input_data['ID'].replace('Emergine',"Emerging")

    columns_mapping = {
        'spreadsheet_key': 2,
        'Legacy Worker ID': 3,
        #'Replace All': 5,
        'Row ID': 6,
        #'Delete': 7,
        #'Custom ID': 8,
        'ID': 9,
        'ID Type': 10,
        'Issued Date': 11,
        'Expiration Date': 12,
        'Organization ID': 13,
        #'Custom Description': 14,
        #'Custom ID Shared': 15

    }

    data_transform_mapping = {
        'Legacy Worker ID': "Worker_WID", 'ID Type': 'Custom_ID_Type_ID',
        #'Organization ID': 'Organization_Reference_ID'

    }

    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Other-Ids")
    
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id


def get_mapped_flexible_work_arrangements_data(input_data, mapping_file,
                                           all_unique_data_list,
                                           unavailable_reference_id_list):

    convert_and_format_date(input_data, 'Start Date*')
    convert_and_format_date(input_data, 'Proposed End Date')
    # Generate row_id based on unique combinations of column1 and column2
    input_data['spreadsheet_key'] = input_data.groupby(['Legacy Worker ID', 'Flexible Work Arrangement Type*', 'Start Date*']).ngroup() + 1
    #input_data['spreadsheet_key'] = (input_data['Flexible Work Arrangement Type*'] != input_data['Flexible Work Arrangement Type*'].shift()).cumsum()
  
     # columns that i need to convert to a row
    columns_to_melt = ['Days Of Week 1','Days Of Week 2','Days Of Week 3','Days Of Week 4','Days Of Week 5']
        # define the key
    id_vars = ['spreadsheet_key','Legacy Worker ID','Position ID','Start Date*','Proposed End Date','Reason','Flexible Work Arrangement Type*','Hours Per Week','Days Per Week']

    var_name = "Day_of_Week_Type"
    value_name = "Days Of Week+"

    input_data = convert_column_to_row(input_data, id_vars, columns_to_melt, var_name, value_name)
    
    columns_mapping = {
        'spreadsheet_key': 2,
        'Legacy Worker ID': 3,
        'Position ID': 4,
        'Start Date*': 5,
        'Proposed End Date': 6,
        'Reason': 7,
        'Flexible Work Arrangement Type*': 8,
        'Hours Per Week': 9,
        'Days Per Week': 10,
        'Days Of Week+': 11
    }

    data_transform_mapping = {
        'Legacy Worker ID': "Worker_WID", 'Position ID': 'Position_ID',
        #'Reason': 'Event_Classification_Subcategory_ID',
        #'Flexible Work Arrangement Type*': 'Flexible_Work_Arrangement_Subtype_ID',
        'Days Of Week+': 'Day_of_the_Week_ID'
    }

    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Flexible Work Arrangements")
   
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id


def get_mapped_worker_collective_agreement_data(input_data, mapping_file,
                                           all_unique_data_list,
                                           unavailable_reference_id_list):
    convert_and_format_date(input_data, "Effective Date")

    columns_mapping = {
        'key': 2,
        'Legacy Worker ID': 3,
        #'Position ID': 5,
        'Effective Date': 4,
        'Collective Agreement ID': 6,

    }

    data_transform_mapping = {
        'Legacy Worker ID': "Worker_WID",
    }

    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Worker Collective Agreement")

    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id

def get_mapped_worker_personal_contact_data(input_data, mapping_file,
                                            all_unique_data_list,
                                            unavailable_reference_id_list):
    convert_and_format_date(input_data, 'Effective Date')
    # format columns to date
    remove_non_numeric_char(input_data, "Home Phone Number")

    #input_data = input_data[input_data["Legacy Worker ID"]=="370070"]    
  
    input_data = input_data.sort_values(by=['Legacy Worker ID', 'Primary', 'Home Phone Is Primary', 'Email Primary'], ascending=[True, False, False, False]).reset_index(drop=True)

    input_data['spreadsheet_key'] = (input_data['Legacy Worker ID'] != input_data['Legacy Worker ID'].shift()).cumsum()

    # columns that i need to convert to a row
    columns_to_melt = ['Home Address Usage 1', 'Home Address Usage 2',
                       'Home Address Usage 3', 'Home Address Usage 4',
                       'Home Address Usage 5']

    # define the key
    id_vars = ['spreadsheet_key', 'Legacy Worker ID', 'Worker ID', 'Effective Date', 'Address ID',
               'Home Address Line 1', 'Home Address Line 2', 'Home Address Line 3','Home Address Line 4',
               'Home Address Line 1 - Local','Home Address Line 2 - Local','Home Address Line 3 - Local','Home Address Line 4 - Local',
               'City','City - Local','Region/State', 'Country', 'Postal Code', 'Home Usage Type',
               'Primary', 'Public', 'Home Phone ID', 'Home Phone Number Country Code', 'Home Phone Number',
               'Home Phone Usage Type', 'Home Phone Device Type',
               'Home Phone Is Primary', 'Phone Visibility', 'Home Email',
               'Email Primary', 'Email Visibility', 'Email Usage']

    var_name = "Usage"
    value_name = "Usage Type"

    input_data = convert_column_to_row(input_data, id_vars, columns_to_melt, var_name, value_name)

    # Define custom values
    input_data['Address ID'].fillna('Custom Address ID', inplace=True)

    #Address Processsing
    input_data['Number of Address'] = input_data.groupby(['Legacy Worker ID', 'Address ID'])['Address ID'].transform('count')
    input_data = set_primary_flag(input_data,'Legacy Worker ID','Primary')
    print(input_data)
    input_data = input_data.groupby(['Legacy Worker ID', 'Address ID'], sort=False).apply(personal_contact_wipe_address).reset_index(drop=True)
    
    input_data = generate_row_id(input_data, 'spreadsheet_key', "Address ID", "Address Row ID")
    #IF address ID is custom address, then blank
    input_data['Public'] = np.where(input_data['Address ID'] =='Custom Address ID',"", np.where(input_data['Public'] == 'Public', 'Y', np.where(input_data['Public'].isnull(), 'N', 'N')))

    input_data = input_data.groupby(['Legacy Worker ID', 'Address ID'], sort=False).apply(personal_contact_wipe_local_address).reset_index(drop=True)
    
    #Phone Procesing
    input_data = generate_row_id(input_data, 'spreadsheet_key', "Home Phone ID", "Home Phone Row ID")
    input_data['Phone Visibility'] = np.where(input_data['Phone Visibility'] == 'Public', 'Y', np.where(input_data['Phone Visibility'].isnull(), 'N', 'N'))
    
    #clean up duplicated phone details & set the flag as primary if a worker does not have any primary phone
    input_data = remove_duplicate_details(input_data, 'Legacy Worker ID', 'Home Phone ID', ['Home Phone ID',"Home Phone Row ID",'Home Phone Number Country Code', 
                                                                                            'Home Phone Number','Home Phone Usage Type', 'Home Phone Device Type',
                                                                                            'Home Phone Is Primary', 'Phone Visibility'])
    
    #setting primary as true when there is no primary phone set for the worker
    input_data = set_primary_flag(input_data,'Legacy Worker ID','Home Phone Is Primary')
    
    #Email Processing
    input_data = generate_row_id(input_data, 'spreadsheet_key', "Home Email", "Home Email Row ID")
    input_data['Email Visibility'] = np.where(input_data['Email Visibility'] == 'Public', 'Y', np.where(input_data['Email Visibility'].isnull(), 'N', 'N'))
    #clean up duplicated Email details & set the flag as primary if a worker does not have any primary Email
    input_data = remove_duplicate_details(input_data, 'Legacy Worker ID','Home Email',["Home Email Row ID",'Home Email','Email Primary', 'Email Visibility', 'Email Usage'])
    input_data = set_primary_flag(input_data,'Legacy Worker ID','Email Primary')
    
    input_data['default_value_1'] = np.where(input_data['Home Usage Type'].isnull() | (input_data['Home Usage Type'] == ''), np.nan, 1)
    input_data['default_value_N'] = 'N'
    #Replace the Custom Addres ID in the Column
    input_data['Address Row ID'] = np.where(input_data['Address ID'] =='Custom Address ID',"",input_data['Address Row ID'])
    input_data['Primary'] = np.where(input_data['Address ID'] =='Custom Address ID',"",input_data['Primary'])
    input_data["Address ID"] = input_data["Address ID"].replace("Custom Address ID", " ")
    #print(input_data[["Legacy Worker ID", "Home Address Line 1", "Primary", "Address Row ID", "Public"]])
    columns_mapping = {
        'spreadsheet_key': 2,
        'Legacy Worker ID': 3,
        'Effective Date': 4,
        'Address Row ID': 5,
        'Country': 12,
        #'Address_line_row_ID': 14,
        #'Usage': 16,
        'Home Address Line 1': 17,
        'City': 18,
        'City - Local':41,
        'Region/State': 25,
        'Postal Code': 31,
        'default_value_1':[14,32,34],
        'Public':33,
        'Primary':35,
        'Home Usage Type':36,
        'Usage Type': 37,
        'Home Phone Number Country Code': 55,
        'Home Phone Number': 56,
        'Home Phone Device Type': 58,
        'Home Phone Is Primary': 62,
        'Home Phone Usage Type': 63,
        'Home Email': 72
    }

    data_transform_mapping = {
        'Region/State': 'Country_Region_ID',
        'Legacy Worker ID': "Worker_WID",
        'Primary': "Numeric_Boolean_ID",
        'Home Phone Is Primary': 'Numeric_Boolean_ID',
        'Email Primary': 'Numeric_Boolean_ID',
        'Home Phone Device Type': 'Phone_Device_Type_ID',
        'Home Usage Type':'Communication_Usage_Type_ID',
        'Home Phone Usage Type': "Communication_Usage_Type_ID",
        "Email Usage": 'Communication_Usage_Type_ID',
        'Usage Type': 'Communication_Usage_Behavior_ID'
    }

    
    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Personal Contact Data")
    
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id

def get_mapped_worker_work_contact_data(input_data, mapping_file,
                                        all_unique_data_list,
                                        unavailable_reference_id_list):
    convert_and_format_date(input_data, 'Effective Date')
    remove_non_numeric_char(input_data, "Work Phone Number")

    input_data = input_data.sort_values(by=['Legacy Worker ID', 'Work Phone Is Primary','Email Primary'], ascending=[True, False,False]).reset_index(drop=True)
    input_data['spreadsheet_key'] = (input_data['Legacy Worker ID'] != input_data['Legacy Worker ID'].shift()).cumsum()
    input_data['Work Visibility'] = np.where(input_data['Work Visibility'] == 'Public', 'Y', np.where(input_data['Work Visibility'].isna(), 'N', 'N'))
    #input_data = generate_row_id(input_data, 'spreadsheet_key',"Work Phone Device Type", "Phone Row ID")
    
    # Generate row IDs based on changes in 'Work Phone Number' and 'Work Phone Device Type' within each group of 'spreadsheet_key'
    input_data['Phone Row ID'] = ((input_data['Work Phone Number'] != input_data['Work Phone Number'].shift(1)) | (input_data['Work Phone Device Type'] != input_data['Work Phone Device Type'].shift(1))).groupby(input_data['spreadsheet_key']).cumsum() + 1

    # Adjust row_id where it's the first row within each 'id'
    input_data['Phone Row ID'] = input_data.groupby('spreadsheet_key')['Phone Row ID'].transform(lambda x: x - min(x) + 1)

    input_data = generate_row_id(input_data, 'spreadsheet_key',"Email Address", "Email Row ID")
    input_data['Email Public'] = np.where(input_data['Email Public'] == 'Public', 'Y', np.where(input_data['Email Public'].isna(), 'N', 'N'))
    #
    input_data['Default_1'] = np.where(input_data['Work Phone Usage Type'].isnull() | (input_data['Work Phone Usage Type'] == ''), np.nan, 1)
  
      #CAlculate the number of phone and then overwrite the email primary to 1 when both the row =1 and number of email =1
    input_data['Number of Phone'] = input_data.groupby(['Legacy Worker ID', 'Phone Row ID'])['Phone Row ID'].transform('nunique')
    input_data['Work Phone Is Primary'] = np.where((input_data['Work Phone Is Primary']=='0') & (input_data['Phone Row ID']==1), "1", input_data['Work Phone Is Primary'])

    columns_mapping = {
        'spreadsheet_key': 2,
        'Legacy Worker ID': 3,
        'Effective Date': 4,
        "Phone Row ID":44,
        'Work Phone Number Country Code': 55,
        'Work Phone Number': 56,
        'Work Phone Device Type': 58,
        'Default_1':[59,61],
        'Work Visibility': 60,
        'Work Phone Is Primary': 62,
        'Work Phone Usage Type': 63,
        'Email Address':72
    }

    data_transform_mapping = {
          'Legacy Worker ID': "Worker_WID",
          'Work Phone Is Primary': "Numeric_Boolean_ID",
          "Email Primary": "Numeric_Boolean_ID",
          'Work Phone Device Type': 'Work_Phone_Device_Type_ID',
          'Work Phone Usage Type': "Communication_Usage_Type_ID",
    }

    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Work Contact Change")
    
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id

def get_edit_service_dates_data(input_data, mapping_file,
                                        all_unique_data_list,
                                        unavailable_reference_id_list, load):
   
    # format columns to date
    convert_and_format_date(input_data, 'Original Hire Date')
    convert_and_format_date(input_data, 'Continuous Service Date')
    convert_and_format_date(input_data, 'Company Service Date')
    #convert_and_format_date(input_data, 'End Employment Date (Only for Fixed Term Employees)')
    convert_and_format_date(input_data, 'Seniority Date')
    convert_and_format_date(input_data, 'Severance Date')

    if load in ["Hire Employee","Hire CWR","Overlapping Hire Employee", "Future Hires"]:
        columns_mapping = {
            'spreadsheet_key': 2,
            'Original Hire Date': 3,
            'Continuous Service Date': 4,
            'Company Service Date': 12,
            'Seniority Date': 8,
            'Severance Date': 9
        }

        return input_data, columns_mapping, all_unique_data_list, unavailable_reference_id_list
    else:
        columns_mapping = {
            'spreadsheet_key': 2,
            'Legacy Worker ID': 3,
            'Original Hire Date': 4,
            'Continuous Service Date': 5,
            'Company Service Date': 13,
            #'End Employment Date (Only for Fixed Term Employees)': 8,
            'Seniority Date': 9,
            'Severance Date': 10
        }
        data_transform_mapping = {'Legacy Worker ID': "Worker_WID"}

        input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Service Dates")
    
        return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id

def read_external_excel_file(file_name, sheet_name):

    external_data = pd.read_excel(file_name, sheet_name=sheet_name, dtype=object)

    return external_data

def get_mapped_job_history_company_data(input_data, mapping_file,
                                        all_unique_data_list,
                                        unavailable_reference_id_list):
    
    sheet_name = "Final"
    file_path = os.path.dirname(mapping_file)
    job_history_file_name = file_path + '\\Job History - Companies.xlsx'

    job_history_file_data = read_external_excel_file(job_history_file_name, sheet_name)

    ref_id_list = list(job_history_file_data['NCA8 Ref ID'])
    
    input_data = input_data.loc[input_data['Job History Reference ID'].isin(ref_id_list), :].reset_index(drop=True)
    
    input_data['spreadsheet_key'] = (input_data['Job History Reference ID'] != input_data['Job History Reference ID'].shift()).cumsum()

    input_data['Add_Only'] = 'Y'

    columns_mapping = {
            'spreadsheet_key': 2,
            'Add_Only': 3,
            'Job History Company Name': 7,
            'Job History Reference ID': 6,
            'Industry': 8,
            'Watching': 9
        }
    
    data_transform_mapping = {'Watching': "Numeric_Boolean_ID"}

    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Job History Company")
    
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id

def get_mapped_pronoun_preferences_data(input_data, mapping_file,
                                        all_unique_data_list,
                                        unavailable_reference_id_list):
    
    input_data['spreadsheet_key'] = (input_data['Legacy Worker ID'] != input_data['Legacy Worker ID'].shift()).cumsum()

    input_data['Public Profile Data Type'] = "PRONOUN"
    input_data['Row ID'] = 1
                                
    columns_mapping = {
        'spreadsheet_key': 2,
        'Legacy Worker ID': 3,
        'Row ID': 4,
        'Public Profile Data Type': 5,
        'Pronoun Visibility': 6
    }
    
    data_transform_mapping = {'Legacy Worker ID': "Worker_WID",'Pronoun Visibility': "Public_Profile_Display_Option_ID"}

    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Pronoun Public Preference")
    
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id

def get_mapped_future_termination_data(input_data, mapping_file,
                                        all_unique_data_list,
                                        unavailable_reference_id_list):
    
    input_data['spreadsheet_key'] = (input_data['Legacy Worker ID'] != input_data['Legacy Worker ID'].shift()).cumsum()

    convert_and_format_date(input_data, "Resignation Date")
    convert_and_format_date(input_data, "Termination Date")
                                
    columns_mapping = {
        'spreadsheet_key': 2,
        'Legacy Worker ID': 3,
        'Termination Date': 4,
        'Last Day of Work (Date)': 5,
        'Primary Reason': 6,
        #'Local Termination Reason': 8,
        'Pay Through Date': 9,
        'Resignation Date': 10,
        'Close Position': 17,
        'Expected Date of Return': 20
    }
    
    data_transform_mapping = {'Legacy Worker ID': "Worker_WID",'Primary Reason': "Termination_Subcategory_ID",
                              #"Local Termination Reason": "Local_Termination_Reason_ID", 
                              "Close Position":"Boolean_ID"}

    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Future Termination")
    
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id

def get_mapped_future_hires_data(input_data, mapping_file,
                                        all_unique_data_list,
                                        unavailable_reference_id_list):
    
    input_data['spreadsheet_key'] = (input_data['Legacy Worker ID'] != input_data['Legacy Worker ID'].shift()).cumsum()

    input_data['Public Profile Data Type'] = "PRONOUN"
    input_data['Row ID'] = 1
                                
    columns_mapping = {
        'spreadsheet_key': 2,
        'Legacy Worker ID': 3,
        'Row ID': 4,
        'Public Profile Data Type': 5,
        'Pronoun Visibility': 6
    }
    
    data_transform_mapping = {'Legacy Worker ID': "Worker_WID",'Pronoun Visibility': "Public_Profile_Display_Option_ID"}

    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Future Hires")
    
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id

def get_mapped_future_job_change_data(input_data, mapping_file,
                                        all_unique_data_list,
                                        unavailable_reference_id_list):
    
    input_data['spreadsheet_key'] = (input_data['Legacy Worker ID'] != input_data['Legacy Worker ID'].shift()).cumsum()

    input_data['Public Profile Data Type'] = "PRONOUN"
    input_data['Row ID'] = 1
                                
    columns_mapping = {
        'spreadsheet_key': 2,
        'Legacy Worker ID': 3,
        'Row ID': 4,
        'Public Profile Data Type': 5,
        'Pronoun Visibility': 6
    }
    
    data_transform_mapping = {'Legacy Worker ID': "Worker_WID",'Pronoun Visibility': "Public_Profile_Display_Option_ID"}

    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Future Job Change")
    
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id

def get_mapped_future_comp_change_data(input_data, mapping_file,
                                        all_unique_data_list,
                                        unavailable_reference_id_list):
    
    input_data['spreadsheet_key'] = (input_data['Legacy Worker ID'] != input_data['Legacy Worker ID'].shift()).cumsum()

    input_data['Public Profile Data Type'] = "PRONOUN"
    input_data['Row ID'] = 1
                                
    columns_mapping = {
        'spreadsheet_key': 2,
        'Legacy Worker ID': 3,
        'Row ID': 4,
        'Public Profile Data Type': 5,
        'Pronoun Visibility': 6
    }
    
    data_transform_mapping = {'Legacy Worker ID': "Worker_WID",'Pronoun Visibility': "Public_Profile_Display_Option_ID"}

    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "Future Comp Change")
    
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id

def get_mapped_skills_reference_data(input_data, mapping_file,
                                        all_unique_data_list,
                                        unavailable_reference_id_list):
    
    input_data['Add Only'] = "Y"
    input_data['Delete'] = "N"
                                
    columns_mapping = {
        'key': 2,
        'Add Only': 3,
        'Delete': 4,
        'Skill Name': 6,
        'Skill Reference ID': 9
    }
    
   
    return input_data, columns_mapping, all_unique_data_list, unavailable_reference_id_list

def get_mapped_end_cwr_contract_data(input_data, mapping_file,
                                        all_unique_data_list,
                                        unavailable_reference_id_list):
    
    input_data['spreadsheet_key'] = (input_data['Legacy Worker ID'] != input_data['Legacy Worker ID'].shift()).cumsum()
                                
    columns_mapping = {
        'spreadsheet_key': 2,
        'Legacy Worker ID': 3,
        'Termination Date': 4,
        'Last Day of Work (Date)': 5,
        'Primary Reason': 6,
        'Notify Employee By Date': 9,
        'Close Position': 11
    }
    
    data_transform_mapping = {'Legacy Worker ID': "Worker_WID", 'Primary Reason': "Termination_Subcategory_ID"}

    input_data, all_unique_values_list, un_available_reference_type_id = mapping_data(input_data,
                                                                                      data_transform_mapping,
                                                                                      mapping_file,
                                                                                      all_unique_data_list,
                                                                                      unavailable_reference_id_list,
                                                                                      "End CWR Contract")
    
    return input_data, columns_mapping, all_unique_values_list, un_available_reference_type_id
