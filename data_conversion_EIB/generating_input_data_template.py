import pandas as pd
import os, io
import csv
from utils.pgp_encryption_and_decryption import get_pgp_decryption

def remove_prefix(text, prefix):
    return text[text.startswith(prefix) and len(prefix):]


def csv_to_excel_converter(sheet_name, writer, decrypted_data):
    try:
        data_string = str(decrypted_data, 'utf-8')
    except:
        data_string = str(decrypted_data, 'latin1')
   
    df = pd.read_csv(io.StringIO(data_string), dtype=str, delimiter="~", quotechar='"')
    #print(df.keys())
    #df.to_excel("test.xlsx")

     # Clear data from row 4 onwards in the Excel sheet
    sheet = writer.sheets.get(sheet_name)
    if sheet:
        for row in sheet.iter_rows(min_row=4):
            for cell in row:
                cell.value = None
    print('count', len(df))    
    df.to_excel(writer, sheet_name=sheet_name, startrow=3, index=False, header=False)
    #print('writing to {} completed'.format(sheet_name))


def generate_data_template(input_file, s3_bucket, csv_s3_file_path, pgp_key_file, secret_data, sheet_name_list):
    with pd.ExcelWriter(input_file, mode="a", engine="openpyxl", if_sheet_exists='overlay') as writer:
        for obj in s3_bucket.objects.filter(Prefix=csv_s3_file_path):
            if obj.key.endswith('csv'):
                csv_file = obj.key
                base = os.path.basename(csv_file)
                sheet_name = os.path.splitext(base)[0]
                if sheet_name in sheet_name_list:
                    #print("I am running {} file".format(csv_file))
                    encrypted_data = obj.get()['Body'].read()
                    decrypted_data = get_pgp_decryption(pgp_key_file, encrypted_data, secret_data)
                    try:
                        data_string = str(decrypted_data, 'utf-8')
                    except:
                        data_string = str(decrypted_data, 'latin1')
                    
                    # df = pd.read_csv(data_string, dtype=str, delimiter="~", quotechar='"')
                    if sheet_name in ['Cost-Center', 'Manage-Goals', "Job-Requisitions"]:
                        df = pd.read_csv(io.StringIO(data_string), encoding="utf-8", dtype=str, delimiter="~", quotechar='|')
                    else:
                        df = pd.read_csv(io.StringIO(data_string), encoding="utf-8", dtype=str, delimiter="~", quotechar='"')

                    print('count', len(df))    
                    df.to_excel(writer, sheet_name=sheet_name, startrow=3, index=False, header=False)

    print("Generated the Input Data Template from CSV files")


def generate_data_template_v1(csv_s3_file_path, pgp_key_file, secret_data, sheet_name_list, return_all_csv_df_list):
    #with pd.ExcelWriter(input_file, mode="a", engine="openpyxl", if_sheet_exists='overlay') as writer:
    from utils.get_s3_client import get_s3_bucket
    import boto3
    bucket_name = 'ncgt-nca-onboarding-prod'
    s3_bucket = get_s3_bucket()
    #s3_client = boto3.Session()

    #s3 = s3_client.resource('s3')

    #s3_bucket = s3.Bucket(bucket_name)
    for obj in s3_bucket.objects.filter(Prefix=csv_s3_file_path):
        if obj.key.endswith('csv'):
            csv_file = obj.key
            base = os.path.basename(csv_file)
            sheet_name = os.path.splitext(base)[0]
            
            if sheet_name in sheet_name_list:
                #print("I am running {} file".format(csv_file))
                encrypted_data = obj.get()['Body'].read()
                decrypted_data = get_pgp_decryption(pgp_key_file, encrypted_data, secret_data)
                try:
                    data_string = str(decrypted_data, 'utf-8')
                except:
                    data_string = str(decrypted_data, 'latin1')
                
                quotechar = '"'
                # df = pd.read_csv(data_string, dtype=str, delimiter="~", quotechar='"')
                if sheet_name in ['Cost-Center', 'Manage-Goals', "Job-Requisitions"]:
                    quotechar = '|'
                elif sheet_name == 'Job-History':
                    quotechar = '^'

                df = pd.read_csv(io.StringIO(data_string), encoding="utf-8", dtype=str, delimiter="~", quotechar=quotechar)
                # elif sheet_name in ["Job-History"]:
                #     df = pd.read_csv(io.StringIO(data_string), encoding="utf-8", dtype=str, delimiter="~", quotechar='^')
                # else:
                #     df = pd.read_csv(io.StringIO(data_string), encoding="utf-8", dtype=str, delimiter="~", quotechar='"')

                print(' sheet_name ' + sheet_name + ' count>>', len(df)) 
                temp_dict = {}
                temp_dict[sheet_name] = df  
                return_all_csv_df_list.append(temp_dict) 
                
    return return_all_csv_df_list


import multiprocessing
from utils.get_s3_client import get_s3_bucket

def split_list(a_list):
    half = len(a_list)//2
    return a_list[:half], a_list[half:]

def generate_data_template_with_parallel_processing(input_file, s3_bucket, csv_s3_file_path, pgp_key_file, secret_data, sheet_name_list, future_hire_worker_ids):
    
    import multiprocessing as mp
    manager = multiprocessing.Manager()
    return_all_csv_df_list = manager.list()
    
    if len(sheet_name_list) > 1:
        p1_sheet_list, p2_sheet_list = split_list(sheet_name_list)
        print('>>>>>>>>>>>>>>>>>>', p1_sheet_list, p2_sheet_list)

        # Create a pool of processes
        p1 = multiprocessing.Process(target=generate_data_template_v1, args=(csv_s3_file_path, pgp_key_file, secret_data, p1_sheet_list, return_all_csv_df_list, ))
        p2 = multiprocessing.Process(target=generate_data_template_v1, args=(csv_s3_file_path, pgp_key_file, secret_data, p2_sheet_list, return_all_csv_df_list, ))

        # starting process 1
        p1.start()
        
        # starting process 2
        p2.start()

        # wait until process 1 is finished
        p1.join()
        # wait until process 2 is finished
        p2.join()

        # both processes finished
        print("Both processes finished Done!")
    else:
        generate_data_template_v1(csv_s3_file_path, pgp_key_file, secret_data, sheet_name_list, return_all_csv_df_list)
    print("Total processed loads csv count", len(return_all_csv_df_list))
    with pd.ExcelWriter(input_file, mode="a", engine="openpyxl", if_sheet_exists='overlay') as writer:
        for csv_data_df in return_all_csv_df_list:
            
            for csv_data_key, value_df in csv_data_df.items():
                # Filtering the DataFrame based on the future hire load list
                if len(future_hire_worker_ids):
                    value_df = value_df[value_df['Legacy Worker ID'].isin(future_hire_worker_ids)].reset_index(drop=True)
                    if len(value_df) == 0:
                        print("Provided Future Hire Worker ID's not available in the Input Data...")
               #print(">>>>>>>>>>>>>>", value_df['Applicant ID'])
                value_df.to_excel(writer, sheet_name=csv_data_key, startrow=3, index=False, header=False)
    print("All the process completed and Generated the data template file.")

