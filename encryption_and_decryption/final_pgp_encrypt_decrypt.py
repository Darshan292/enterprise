from pgpy import PGPUID, PGPKey, PGPMessage
import pgpy
from pgpy.constants import (
    PubKeyAlgorithm,
    KeyFlags,
    HashAlgorithm,
    SymmetricKeyAlgorithm,
    CompressionAlgorithm,
)
import pandas as pd
import boto3
import base64
import cryptocode
from botocore.exceptions import ClientError


aws_access_key = ''
aws_secret_key = ''
region_name = 'us-east-1'
bucket_name = ''

global base_path


def get_pgp_key(public_key_filename, private_key_filename, secret_data):
    # 1. Recipient sets up user, and generates a key for that user
    uid = PGPUID.new("Aravindan Sekar", comment="Honest Abe", email="Aravindan.sekar@newscrop.com")
    key = PGPKey.new(PubKeyAlgorithm.RSAEncryptOrSign, 4096)
    key.add_uid(
        uid,
        usage={KeyFlags.Sign, KeyFlags.EncryptCommunications, KeyFlags.EncryptStorage},
        hashes=[HashAlgorithm.SHA256, HashAlgorithm.SHA384, HashAlgorithm.SHA512, HashAlgorithm.SHA224],
        ciphers=[SymmetricKeyAlgorithm.AES256, SymmetricKeyAlgorithm.AES192, SymmetricKeyAlgorithm.AES128],
        compression=[
            CompressionAlgorithm.ZLIB,
            CompressionAlgorithm.BZ2,
            CompressionAlgorithm.ZIP,
            CompressionAlgorithm.Uncompressed,
        ],
    )


    # Save the public key to a file
    with open(public_key_filename, 'w') as f:
        f.write(str(key.pubkey))

    encoded_key = cryptocode.encrypt(str(key), secret_data)
    #key.protect("passphrase", SymmetricKeyAlgorithm.AES256, HashAlgorithm.SHA256)
    with open(private_key_filename, 'w') as key_file:
        key_file.write(str(encoded_key))
    # Typically, recipient then saves the key information to a file on their server

    return key


def get_pgp_encryption(key_file, input_file, output_file):
    key, _ = pgpy.PGPKey.from_file(key_file)
    key._require_usage_flags = False
    pubkey, _ = PGPKey.from_blob(str(key.pubkey))

    # Read the Excel file in binary mode
    with open(input_file, 'rb') as file:
        excel_binary_data = file.read()

    f_t_e = pgpy.PGPMessage.new(excel_binary_data)

    encrypted_data = key.encrypt(f_t_e)

    with open(output_file, 'w') as encrypted_file:
        encrypted_file.write(str(encrypted_data))

    return encrypted_data


def get_pgp_decryption(key_file, input_file, s3_client, bucket_name):
    key, _ = pgpy.PGPKey.from_file(key_file)
    key._require_usage_flags = False
    response = s3_client.get_object(Bucket=bucket_name, Key=input_file)
    encrypted_content = response['Body'].read()
    cipher_msg = PGPMessage.from_blob(encrypted_content)
    decrypted = key.decrypt(cipher_msg)
    decrypted_data = bytes(decrypted._message.contents) if isinstance(decrypted._message.contents,
                                                                      bytearray) else decrypted._message.contents
    print(f"decrypted: [{decrypted_data}]")
    return decrypted_data



def main():
    global base_path
    base_path = r"C:\\Users\\sekara\\OneDrive\\news_crop_v2\\pythonProject\\EIB Template\\"

    input_file = base_path + 'Add_Update_Organization_v41.2.xlsx'


    public_key_filename = ''
    private_key_filename = ''
   
    # This method is one time run to get the key
    pub_key = get_pgp_key(public_key_filename, private_key_filename)
    print("Got the pgp key..")

    encrypted_file = ''
    #encrypted_data = get_pgp_encryption(local_public_key_file_name, input_file_template, encrypted_file)
    # print("Encryption Completed..")


    local_private_key_file_name = ''
    s3_file_key = ''
    #decrypted_data = get_pgp_decryption(local_private_key_file_name, s3_file_key, s3_client, bucket_name)
    print("Completed....")


if __name__ == "__main__":
    main()
