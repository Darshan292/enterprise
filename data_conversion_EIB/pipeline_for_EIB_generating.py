import os, json
from generating_input_data_template import generate_data_template, generate_data_template_with_parallel_processing
import shutil
from utils.get_s3_client import get_s3_bucket, upload_s3_bucket
from utils.pgp_encryption_and_decryption import get_pgp_decryption, get_pgp_encryption
from utils.kms_encryption_and_decryption import encrypt_file_with_kms
from pre_validation import get_pre_validation_report, protect_pre_validation_file
import pandas as pd
from Converting_Data_Create_EIB_Final import creating_eib_files, get_eib_file_name, \
    creating_eib_files_with_parallel_processing
# from launch_EIB_integrations import launch_eib_to_workday
from utils.get_credentials import get_secret

if __name__ == '__main__':
    
    base_path = r"C:\\Source_Code\\"
    
    input_file_template = base_path + 'Data Template - Employee + Contingent Worker_Final.xlsx'
    master_data_temp_input_dir = base_path + 'master_data_temp\\'
    input_file = master_data_temp_input_dir + 'Data Template - Employee + Contingent Worker_Final.xlsx'
    
    #### Update all Loads names here
    """ all_loads = ['Supervisory Org', 'Prehire', 
                     'Change-Job', 'End Contingent Worker Contracts', 'Terminate Employee', 'Assign Work Schedule',
                     'Supplier', 'One Time Time Time Payments', 'Compensation', 'Probation Info',
                     'Performance Reviews', 'Manage Goals', 'Add Workday Account',
                     'Put Candidate', 'Role Based Assignments', 'Edit Position Restrictions Additional Data',
                     'NCA Additional Data Position', 'Worker Additional Data', 'International Assignments',
                     "Cost Center Hierarchy","Cost Center","Collective Agreement", "Job Classification","Custom Organizations"
                     ,"Job Family", "Job Family Group", "Put Supervisory Assignment Restrictions", "Location", "Job Category", 
                     "Job Profile", "Comp Grade and Grade Profile", "Create Position", "Job Requisitions","Edit Job Requisitions","Job Requisition Additional Data",
                      "Prehire", 'Hire Employee','Hire CWR', "Employee Compensation", "Update Workday Account","Put Candidate","Add Workday Account",
                      "Change Personal Information", "Overlapping Employee Compensation", "User Based Assignments", "Assign Notice Period", 
                      "Time Off Events", "Absence Input", "Overlapping Hire Employee","Leave Of Absence Events", "National ID", "Emergency Contact",
                      'Job History', 'Licenses', 'Education', 'Other Ids', 'Visas', 'Skills', 'Flexible Work Arrangements', 'Worker Collective Agreement',
                      "Work Contact Change","Future Prehire", "End CWR Contract","Personal Contact Change","Service Dates", "Job History Company","Pronoun Public Preference", "Skills Reference Data", "Future Hires"] """
    
    # Provide the which loads need to run
    load_list = ['Supervisory Org', 'Job Profile', "Custom Organizations", 'Cost Center', 'Company', 'Supplier',
                 'Job Classification', 'Prehire', "Create Position", "Hire Employee"]
    load_list = ['Supervisory Org', 'Job Profile', "Custom Organizations", 'Cost Center', 'Company', 'Supplier',
                 'Job Classification', "Create Position", "Job Requisitions", "Edit Job Requisitions",
                 "Job Requisition Additional Data",
                 "Prehire", 'Hire Employee', 'Hire CWR', "Employee Compensation", "Update Workday Account", "Put Candidate"]
    load_list = ["Create Position", "Job Requisitions", "Job Requisition Roles", "Edit Job Requisitions", "Job Requisition Additional Data",
                 "Prehire", 'Hire Employee', 'Hire CWR', "Employee Compensation", "Update Workday Account", "Put Candidate"]
    load_list = ['Role Based Assignments', "National ID", "Change Personal Information", "Payment Elections", "Emergency Contact",
                 'Flexible Work Arrangements', "Future Comp Change", "Future Prehire", "Future Hires","Future Hire CWR","Future Termination",'Pronoun Public Preference','Probation Info',"Assign Notice Period",'Assign Work Schedule','Worker Additional Data',"User Based Assignments",
                 "Skills Reference Data"]
    # Batch 1 Load List
    load_list = ["Job Profile","Job Family", "Job Family Group"]

    # Batch 4 loads list
    #load_list = ["Licenses", "Passports", "Visas", "Performance Reviews", "Manage Goals", "Job History", "Education", "Skills", "Other Ids", "Worker Collective Agreement", "Personal Contact Change", "Work Contact Change", "Service Dates", "Absence Input", "Time Off Events"]
    #load_list = ["Licenses", "Passports", "Visas", "Skills", "Other Ids", "Worker Collective Agreement", "Personal Contact Change", "Work Contact Change"]
    
    load_list = ["Personal Contact Change"]

    future_hire_worker_ids = []
    # Declare the data template method params as below
    running_env = "UAT"
    running_load_flag = "Future"
    pgp_key_file = r'C:\\Source_Code\\encoded_latest_pgp_private_integration_key.txt'
    #s3_env_key = ''

    job_history_company_file = base_path + 'Job History - Companies.xlsx'
    
    if running_load_flag == "Future":
        future_hire_worker_ids = ["171571", "717296", "720071", "720083", "720086", "712995", "720087", "720058", "720068", "720107", "720108"]

    if running_env == 'DEV':
        s3_env_key = 'p0_build/'
        pgp_public_key_file = r'C:\\Source_Code\\encoded_wd-s3-pgp-key-pair.txt'
        mapping_file = base_path + 'Combined_Mapping_DEV.xlsx'
    
    elif running_env == 'QA':
        s3_env_key = 'p1_build/'
        pgp_public_key_file = r'C:\\Source_Code\\encoded_local_public_key_for_EIB_Generation_non_DEV.txt'
        mapping_file = base_path + 'Combined_Mapping_QA.xlsx'
    
    elif running_env == 'UAT':
        s3_env_key = 'p2_build/'
        pgp_public_key_file = r'C:\\Source_Code\\encoded_local_public_key_for_EIB_Generation_non_DEV.txt'
        mapping_file = base_path + 'Combined_Mapping_UAT.xlsx'
    
    if os.path.exists(master_data_temp_input_dir):
        shutil.rmtree(master_data_temp_input_dir)
    
    if not os.path.exists(master_data_temp_input_dir):
        os.makedirs(master_data_temp_input_dir)
        temp_eib_path = master_data_temp_input_dir + "EIB Template"
        shutil.copytree(base_path + "EIB Template", temp_eib_path)
    
    shutil.copy2(input_file_template, master_data_temp_input_dir)
    shutil.copy2(mapping_file, master_data_temp_input_dir)
    shutil.copy2(job_history_company_file, master_data_temp_input_dir)
    
    # load the eibfile names and sheet names for the load
    eib_files, sheet_name_list = get_eib_file_name(load_list, base_path)
    print(sheet_name_list)
    
    # Declare the data template method params as below
    # pgp_key_file = r'C:\\Source_Code\\latest_pgp_private_integration_key.txt'
    # s3_csv_s3_file_path = 'p0_build/source-data/'
    s3_csv_s3_file_path = str(s3_env_key) + 'source-data/'
    
    s3_bucket = get_s3_bucket()
    
    # fetch secret from secret vault
    secret = json.loads(get_secret("nca-wd-python-paraphrase"))
    secret_data = secret["paraphrase"]
    
    # Calling data template generation method
    #generate_data_template(input_file, s3_bucket, s3_csv_s3_file_path, pgp_key_file, secret_data, sheet_name_list)
    generate_data_template_with_parallel_processing(input_file, s3_bucket, s3_csv_s3_file_path, pgp_key_file, secret_data, sheet_name_list, future_hire_worker_ids)
    print("Generated the latest data template from CSV files")
    
    # Declare the KMS encryption method params as below
    encrypted_filename = master_data_temp_input_dir + 'Encrypted Data Template - Employee + Contingent Worker.xlsx'
    
    # fetch secret from secret vault
    secret = json.loads(get_secret("nca-kms-key-phrase"))
    cmk_id = secret["kms_key"]
    
    # Calling KMS encryption method
    encrypt_file_with_kms(input_file, encrypted_filename, cmk_id)
    print("KMS Encryption is completed for input data template")
    
    # Declare the S3 destination file path
    # s3_destination_file_path = 'p0_build/master-template/Data Template - Employee + Contingent Worker.xlsx'
    s3_destination_file_path = str(s3_env_key) + 'master-template/Data Template - Employee + Contingent Worker.xlsx'
    
    # Calling upload s3 bucket method
    upload_s3_bucket(encrypted_filename, s3_destination_file_path)
    print("Uploaded the Encrypted data template into s3")
    
    # Declare the pre validation script input file
    cross_validation_input_file = base_path + 'Cross_Validation_Input_Data.xlsx'
    validation_report_file_name = base_path + 'Pre_Validation_Report.xlsx'
    
    # Calling pre validation script
    get_pre_validation_report(cross_validation_input_file, input_file, validation_report_file_name, sheet_name_list)
    print("Successfully generated the pre validation report")
    
    # Read the pre validation report and check if any issues is there
    validation_report_df = pd.read_excel(validation_report_file_name)
    
    # protect the pre-validation report
    protect_pre_validation_file(validation_report_file_name, secret_data)
    
    # Provide the which loads need to run
    # load_list =["Worker Additional Data"]
    
    if len(validation_report_df) == 0 or len(validation_report_df) != 0:
        print('Calling EIB generate method')
    
        # Calling EIB generate method
        #creating_eib_files(load_list, master_data_temp_input_dir, mapping_file, input_file)
        creating_eib_files_with_parallel_processing(load_list, master_data_temp_input_dir, mapping_file, input_file)
        print("Generated the EIB files")
        
        for eib_file in eib_files:
            head, tail = os.path.split(eib_file)
            # pgp_public_key_file = r'C:\\Source_Code\\encoded_wd-s3-pgp-key-pair.txt'
            input_eib_file = master_data_temp_input_dir + "EIB Template\\" + tail
            output_eib_file = master_data_temp_input_dir + "EIB Template\\Encrypted_" + tail
    
            get_pgp_encryption(pgp_public_key_file, input_eib_file, output_eib_file, secret_data)
            print("Completed the EIB file PGP Encryption..")
    
            # Declare the S3 destination file path
            # s3_destination_file_path = 'p0_build/final-eib-files/'+ tail + ".pgp"
            s3_destination_file_path = str(s3_env_key) + 'final-eib-files/' + tail + ".pgp"
    
            # Calling upload s3 bucket method
            upload_s3_bucket(output_eib_file, s3_destination_file_path)
    
            print("Uploaded the EIB file Encrypted data into s3")
    else:
        print("There is data issue. Please check it validation report..")
    
    import psutil
    
    for proc in psutil.process_iter():
        # check whether the process name matches
        if proc.name() == input_file:
            print("Killed>>>>>>>>>>>>>>")
            proc.kill()
    if os.path.exists(master_data_temp_input_dir):
        #shutil.rmtree(master_data_temp_input_dir)
        print("Removed the temp directory...")
    
    # integration_system_id = ""
    # url = ''
    
    # Get EIB file name based on the loads
    # eib_files = get_eib_file_name(load_list)
    
    # launch the final EIB file to S3
    # launch_eib_to_workday(eib_files, integration_system_id, url)
