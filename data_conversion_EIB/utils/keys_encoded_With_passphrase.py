import cryptocode, json
from get_credentials import get_secret

def encode_with_passphrase(input_file, output_file, passphrase):
    # Read the Excel file in binary mode
    with open(input_file, 'r') as file:
        key_binary_data = file.read()

    encoded_key = cryptocode.encrypt(str(key_binary_data), passphrase)

    with open(output_file, 'w') as key_file:
        key_file.write(str(encoded_key))

    return encoded_key


base_path = r"C:\\Source_Code\\"

# Provide the input and output file
input_file = base_path + 'latest_pgp_public_integration_key_QA.txt'
output_file = base_path + 'encoded_local_public_key_for_EIB_Generation_non_DEV.txt'
#fetch secret from secret vault
secret = json.loads(get_secret("nca-wd-python-paraphrase"))
passphrase = secret["paraphrase"]

resp = encode_with_passphrase(input_file, output_file, passphrase)
print("Encoded completed..")






