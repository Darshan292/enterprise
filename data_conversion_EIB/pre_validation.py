import pandas as pd
import xlsxwriter
import numpy as np
from collections import defaultdict
from datetime import datetime
from openpyxl import load_workbook
import datetime, os


def validate_mapping_fields_by_sheet_with_column(input_data, sheet, column, master_data, sheet_name, row_id,
                                                 column_name, reason):
    for idx, values in input_data.iterrows():  # multiple and all the master dump data
        if values[column] not in master_data['Master_ID']:
            sheet_name.append(sheet)
            row_id.append(idx)
            column_name.append(column)
            reason.append(column + ' is not available in the master data...')


def validate_isalpha(input_data, search_list, sheet, sheet_name, row_id, column_name, reason, column_value):
    name_columns = [col for search_item in search_list for col in input_data.columns if search_item in col]
    df_name_subset = input_data.filter(items=name_columns)
    df_name_subset = df_name_subset.loc[1::]
    df = df_name_subset.fillna('Test')
    dict2 = df.to_dict('list')
    for key, values in dict2.items():
        date_row_value = 2
        for value in values:
            if not value.isalpha() and value != 'Test':
                sheet_name.append(sheet)
                row_id.append(date_row_value)
                column_name.append(key)
                column_value.append(value)
                reason.append('Value should only be characters..')
            # else:
            #     sheet_name.append(sheet)
            #     row_id.append(date_row_value)
            #     column_name.append(key)
            #     reason.append('Name is null..')
            date_row_value += 1


def get_required_columns(input_data):
    columns = input_data.columns
    required_row = input_data.loc[0]
    required_columns = (dict(zip(columns, required_row)))
    resp = [key for key, value in required_columns.items() if value == 'Required' and 'Date' not in key]
    return input_data[resp]


def convert_and_format_date(input_data, columns):
    # Convert column to datetime format
    for column in columns:
        # input_data[column] = pd.to_datetime(input_data[column], errors='coerce').dt.normalize()
        input_data[column] = pd.to_datetime(input_data[column], errors='coerce')  # .dt.normalize()


def validate_date_columns(input_data, sheet, sheet_name, row_id, column_name, reason, column_value):
    date_cols = [col for col in input_data.columns if 'Date' in col]
    df_subset = input_data.filter(items=date_cols)

    df_subset = df_subset.loc[1::]
    df_subset = df_subset.apply(lambda x: x.str.strip() if x.dtype == "str" else x)

    df = df_subset.fillna(0)
    # convert_and_format_date(df, date_cols)
    dict2 = df.to_dict('list')

    for key, values in dict2.items():
        date_row_value = 2
        for value in values:
            if value != 0 and not isinstance(value, pd._libs.tslibs.nattype.NaTType) and value !="":
                try:
                    resp = datetime.datetime.strptime(value, '%Y-%m-%d')
                except:
                    # if value != datetime.strftime(value, "%Y-%b-%d %HH:%MM:%SS"):
                    sheet_name.append(sheet)
                    row_id.append(date_row_value)
                    column_name.append(key)
                    column_value.append(value)
                    reason.append('Date is not in YYYY-MM-DD format, (Eg. 2024-01-01 for 2024-Jan-01)..')
            else:
                continue
            date_row_value += 1


def validate_null_values(input_data, required_data, sheet, sheet_name, row_id, column_name, reason, column_value):
    required_data.drop(0, inplace=True)
    if len(required_data) > 0:
        row_count = 2
        for idx, r in required_data.iterrows():
            values = list(r[r.isnull()].index)
            # if len(values):

            for value in values:
                sheet_name.append(sheet)
                row_id.append(row_count)
                column_name.append(value)
                column_value.append("")
                reason.append('Values are null')
            row_count += 1
    else:
        for key in required_data.keys():
            sheet_name.append(sheet)
            row_id.append(0)
            column_name.append(key)
            column_value.append("")
            reason.append('No Records available')


def validate_intersection_mapping(input_file, input_data_cross_validation, sheet_name, row_id, column_name, reason, column_value):
    for key, dict_value in input_data_cross_validation.items():
        validation_id = key
        master_column_list = list(dict_value.keys())[0] if len(dict_value.keys()) else []
        split_list = master_column_list.split('_')
        master_sheet = split_list[0]
        master_sheet_column = split_list[1]
        validation_sheet = list(dict_value.values())[0] if len(dict_value.values()) else []
        master_data = pd.read_excel(input_file, sheet_name=master_sheet, skiprows=[1, 2], dtype=object)
        master_data['index_col'] = master_data.index
        filtered_master_data = master_data.filter([master_sheet_column, "index_col"])
        master_data_list = list(filtered_master_data[master_sheet_column])

        for sheet in list(validation_sheet):
            data_list = pd.read_excel(input_file, sheet_name=sheet, skiprows=[1, 2], dtype=object)
            data_list.columns = data_list.columns.str.strip()
            for data in data_list[validation_id]:
                if data not in master_data_list:
                    matching_indices = data_list[data_list[validation_id] == data].index
                    
                    if not matching_indices.empty:
                        row_number = matching_indices[0] + 1
                        sheet_name.append(sheet)
                        row_id.append(row_number)
                        column_name.append(validation_id)
                        column_value.append(data)
                        reason.append(
                            str(validation_id) + ' Not available in the master data (' + str(master_sheet) + ' sheet)')

def get_cross_validation_input_data(cross_validation_input_file):
    data_dict = {}

    input_data = pd.read_excel(cross_validation_input_file, dtype=object)
    for idx, data in input_data.iterrows():
        master_data_dict = {}

        key = data['Master_Sheet']
        master_column_name = data['Master_Sheet_Column']
        key = key + "_" + master_column_name
        master_data_dict[key] = eval(data['Validation_Sheets'])

        data_dict[data['Validate_Column']] = master_data_dict

    return data_dict


def validate_the_dates_with_in_range(input_file, sheets, sheet_name, row_id, column_name, reason, column_value):
    position_data = pd.DataFrame()
    job_req = pd.DataFrame()
    sheet = ''
    for sheet in sheets:
        input_data = pd.read_excel(input_file, sheet_name=sheet, skiprows=[1, 2], dtype=object)
        if sheet == 'Positions':
            position_data = input_data
        if sheet == 'Job-Requisitions':
            job_req = input_data
    position_data.drop_duplicates("Position ID", keep='first', inplace=True)
    job_req.drop_duplicates("Position ID", keep='first', inplace=True)

    start_row = 2
    for idx, data in position_data.iterrows():
        mult_conditions = job_req[(job_req['Recruiting Start Date'] < data['Availability Date']) & (job_req['Position '
                                                                                                            'ID'] ==
                                                                                                    data['Position '
                                                                                                         'ID'])]
        if len(mult_conditions) == 1:
            sheet_name.append(sheet)
            row_id.append(start_row)
            column_name.append("column_value")
            column_value.append(mult_conditions['Availability Date'])
            reason.append("Position ID " + str(data['Position ID']) + 'Recruiting Start Date column values is not '
                                                                      'valid')

            start_row += 1


def get_pre_validation_report(cross_validation_input_file, input_file, validation_report_file_name, load_list):
    sheet_name, row_id, column_name, column_value, reason = [], [], [], [], []

    #cross_validation_input_file = base_path + 'Cross_Validation_Input_Data.xlsx'

    # read Excel sheets
    xls = pd.ExcelFile(input_file)
    if len(load_list) == 0:
        all_sheets = xls.sheet_names
    else:
        all_sheets = load_list

    # all_sheets = ['Supervisory-Org', 'Hire-Continent-Worker', 'Prehire', 'Hire-Employee', 'Positions',
    #               'Job-Requisitions', 'Terminate-Employee']
    # all_sheets = ['Supervisory-Org']
    #
    # all_sheets = ['Hire-CWR']
    
    input_data_cross_validation = get_cross_validation_input_data(cross_validation_input_file)
    if input_data_cross_validation:
        validate_intersection_mapping(input_file, input_data_cross_validation, sheet_name, row_id, column_name, reason, column_value)

    if 'Positions' in load_list and 'Job-Requisitions' in load_list:
        date_range_validation_sheets = ['Positions', 'Job-Requisitions']
        #validate_the_dates_with_in_range(input_file, date_range_validation_sheets, sheet_name, row_id, column_name,
        #                                 reason, column_value)

    for sheet in all_sheets:
        # read Excel sheets
        print(">>>>>>>>>", sheet)
        if sheet == "Absence Input":
            input_data = pd.read_excel(input_file, sheet_name=sheet, skiprows=[0, 1, 3, 4], dtype=object)
        else:
            input_data = pd.read_excel(input_file, sheet_name=sheet, skiprows=[1], dtype=object)

        required_columns = get_required_columns(input_data)

        # sheet = 'Hire-CWR' # change_job

        validate_null_values(input_data, required_columns, sheet, sheet_name, row_id, column_name, reason, column_value)

        validate_date_columns(input_data, sheet, sheet_name, row_id, column_name, reason, column_value)

        search_list = ['Last Name', 'First Name']

        #validate_isalpha(input_data, search_list, sheet, sheet_name, row_id, column_name, reason, column_value)

        # master_data = pd.read_excel('mapping_file.xlsx')

        # column = 'Applicant ID'
        # validate_mapping_fields_by_sheet_with_column(input_data, sheet, column, master_data, sheet_name, row_id,
        #                                              column_name, reason)

    final_out_dict = {}
    final_out_dict['Sheet Name'] = sheet_name
    final_out_dict['Row Number'] = row_id
    final_out_dict['Column Name'] = column_name
    final_out_dict['Column Value'] = column_value
    final_out_dict['Reason'] = reason
    output_df = pd.DataFrame(final_out_dict)
    output_df.drop_duplicates(inplace=True)
    if os.path.isfile(validation_report_file_name):
        os.remove(validation_report_file_name)
    output_df.to_excel(validation_report_file_name, index=False)
    

def protect_pre_validation_file(filepath, secret_data):
    from spire.xls import Workbook, ExcelVersion

    # Create a Workbook object
    workbook = Workbook()
    # Load an Excel file
    workbook.LoadFromFile(filepath)

    # Protect the Excel file with a document-open password
    workbook.Protect(secret_data)

    # Save the result file
    workbook.SaveToFile(filepath, ExcelVersion.Version2013)
    workbook.Dispose()

