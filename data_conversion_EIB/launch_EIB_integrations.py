import pandas as pd
import requests
import xml.etree.ElementTree as ET
import base64
import os

# integration_system_id = INTEGRATION_SYSTEM__AUDITED_-6-804
def launch_eib_integration(attachment_filename, file_content, integration_system_id, url):
    # # Replace the comment in the XML payload with the employee data

    xml_body = """<wd:Launch_EIB_Request wd:version="v41.0">
            <wd:Integration_System_Reference
                wd:Descriptor="">
                <wd:ID wd:type="Integration_System_ID">{integration_system_id}
                </wd:ID>
            </wd:Integration_System_Reference>
            <wd:Service_Component_Data>
                <wd:Data_Source>true</wd:Data_Source>
                <wd:Field_Override_Data>
                    <wd:Field_Name>Integration Attachment</wd:Field_Name>
                    <wd:Attachment_Data
                        wd:Content_Type="application/xlsx"
                        wd:Filename="{attachment_filename}" wd:Encoding="base64"
                        wd:Compressed="true">
                        <wd:File_Content>{file_content}</wd:File_Content>
                    </wd:Attachment_Data>
                </wd:Field_Override_Data>
            </wd:Service_Component_Data>
        </wd:Launch_EIB_Request>""".format(
        integration_system_id=integration_system_id,
        attachment_filename=attachment_filename,
        file_content=file_content
    )

    xml_payload = """
    <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:wd="urn:com.workday/bsvc">
        <soapenv:Header>
        <wsse:Security soapenv:mustUnderstand="1"
        xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd"
        xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
        <wsse:UsernameToken wsu:Id="UsernameToken-33E628C2ECE52DDBC815879825702681">
            <wsse:Username>{username}</wsse:Username>
            <wsse:Password
                Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText"
                >{password}</wsse:Password>
            <wsse:Nonce
                EncodingType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-soap-message-security-1.0#Base64Binary"
                >hZOa5nxZOyGcvR84ztBXMw==</wsse:Nonce>
        </wsse:UsernameToken>
        </wsse:Security>
        </soapenv:Header>
        <soapenv:Body>
            <-- Body goes here -->
        </soapenv:Body>
    </soapenv:Envelope>
    """.format(
        username='username',
        password='password'
    )

    xml_payload = xml_payload.replace("<-- Body goes here -->", xml_body)

    print(xml_payload)

    # #Make the SOAP API call
    response = requests.post(url=url,
                             data=xml_payload)

    # # Print the response status code and content
    if response.status_code != requests.codes.ok:
        raise Exception('Request failed with status code:', response.status_code, response.content)
    print(response.status_code)
    print(response.content)


def launch_eib_to_workday(eib_files, integration_systemid, host):
    for file in eib_files:
        eib_data = open(file, 'rb').read()
        # Get the file name from the filepath
        path, input_file_name = os.path.split(file)
        # Converting Binary format into Base64 encoded format
        base64_encoded_excel_data = base64.b64encode(eib_data).decode('UTF-8')
        # Launching an integration in Workday
        launch_eib_integration(input_file_name, base64_encoded_excel_data, integration_systemid, host)
        print("Completed")


base_path = r"C:\\Users\\sekara\\OneDrive\\news_crop_v2\\pythonProject\\EIB Template\\"
file_name = 'Add_Update_Organization_v41.2.xlsx'
input_file = base_path + file_name
integration_system_id = "INTEGRATION_SYSTEM__AUDITED_-6-804"
url = ''
# Reading the input Excel file in binary format
data = open(input_file, 'rb').read()
# Converting Binary format into Base64 encoded format
base64_encoded_excel = base64.b64encode(data).decode('UTF-8')
#Launching an integration in Workday
launch_eib_integration(file_name, base64_encoded_excel, integration_system_id, url)
print("Completed")

