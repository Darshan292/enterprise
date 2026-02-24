import boto3, json
from botocore.exceptions import ClientError
from io import StringIO, BytesIO
import base64
from cryptography.fernet import Fernet
import pandas as pd
import os, io
from glob import glob
import base64

region_name = 'us-east-1'  # e.g., 'us-east-1'
NUM_BYTES_FOR_LEN = 4


def create_data_key(cmk_id, key_spec="AES_256"):
    """Generate a data key to use when encrypting and decrypting data"""

    # Create data key
    kms_client = boto3.client('kms', region_name=region_name)

    response = kms_client.generate_data_key(KeyId=cmk_id, KeySpec=key_spec)

    # Return the encrypted and plaintext data key
    return response["CiphertextBlob"], base64.b64encode(response["Plaintext"])


def decrypt_data_key(data_key_encrypted):
    """Decrypt an encrypted data key"""

    # Decrypt the data key
    kms_client = boto3.client('kms', region_name='us-east-1')
    response = kms_client.decrypt(CiphertextBlob=data_key_encrypted)

    # Return plaintext base64-encoded binary data key
    return base64.b64encode((response["Plaintext"]))


def decrypt_file_with_kms(file_contents, flag):
    """Decrypt a file encrypted by encrypt_file()"""

    # The first NUM_BYTES_FOR_LEN tells us the length of the encrypted data key
    # Bytes after that represent the encrypted file data
    data_key_encrypted_len = int.from_bytes(file_contents[:NUM_BYTES_FOR_LEN],
                                            byteorder="big") \
                             + NUM_BYTES_FOR_LEN
    data_key_encrypted = file_contents[NUM_BYTES_FOR_LEN:data_key_encrypted_len]

    # Decrypt the data key before using it
    data_key_plaintext = decrypt_data_key(data_key_encrypted)
    if data_key_plaintext is None:
        return False

    # Decrypt the rest of the file
    f = Fernet(data_key_plaintext)
    file_contents_decrypted = f.decrypt(file_contents[data_key_encrypted_len:])
    if flag == 'CSV':
        try:
            df = pd.read_csv(StringIO(file_contents_decrypted.decode('utf-8')))
        except UnicodeDecodeError:
            df = pd.read_csv(StringIO(file_contents_decrypted.decode('latin-1')))

    else:
        df = pd.read_excel(io.BytesIO(file_contents_decrypted), sheet_name=None)

    print('Decryption completed')

    return df


def encrypt_file_with_kms(input_filename, encrypted_filename, cmk_id):
    """Encrypt JSON data using an AWS KMS CMK"""

    with open(input_filename, "rb") as file:
        file_contents = file.read()

    data_key_encrypted, data_key_plaintext = create_data_key(cmk_id)
    if data_key_encrypted is None:
        return

    # Encrypt the data
    f = Fernet(data_key_plaintext)
    file_contents_encrypted = f.encrypt(file_contents)

    base = os.path.basename(input_filename)
    nca_file_name = os.path.splitdrive(base)[1]

    if os.path.isfile(encrypted_filename):
        os.remove(encrypted_filename)

    ## Write the encrypted data key and encrypted file contents together
    with open(encrypted_filename, 'wb') as file_encrypted:
        file_encrypted.write(len(data_key_encrypted).to_bytes(NUM_BYTES_FOR_LEN,
                                                              byteorder='big'))
        file_encrypted.write(data_key_encrypted)
        file_encrypted.write(file_contents_encrypted)

    print('Encryption completed...', nca_file_name)
