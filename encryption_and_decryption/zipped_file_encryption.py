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
from zipfile import ZipFile
import zipfile

aws_access_key = ''
aws_secret_key = ''
region_name = ''  # e.g., 'us-east-1'
bucket_name = ''

global base_path

def compress_xlsx_to_zip(zip_file_name, eib_file_name):
    zipf = ZipFile(zip_file_name, "w", zipfile.ZIP_DEFLATED)
    zipf.write(eib_file_name)
    zipf.close()
    print(" compress_xlsx_to_zip Done")

def get_pgp_key(filename):
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

    with open(filename, 'wb') as key_file:
        key_file.write(bytes(key))
    # Typically, recipient then saves the key information to a file on their server

    return key


def get_pgp_encryption(key_file, zip_file_name, eib_input_file, encrypted_file_name):
    key, _ = pgpy.PGPKey.from_file(key_file)
    key._require_usage_flags = False
    pubkey, _ = PGPKey.from_blob(str(key.pubkey))

    archive = zipfile.ZipFile(zip_file_name, 'r')
    eib_data = archive.read(eib_input_file)

    f_t_e = pgpy.PGPMessage.new(str(eib_data))
    #encrypted_data = pubkey.encrypt(f_t_e)
    # If we are using workday key need to enable this line and commented out the above line
    encrypted_data = key.encrypt(f_t_e)

    with open(encrypted_file_name, 'wb') as encrypted_file:
        encrypted_file.write(bytes(encrypted_data))

    return encrypted_data


def get_pgp_decryption(key_file, input_file, s3_client, bucket_name):
    key, _ = pgpy.PGPKey.from_file(key_file)
    key._require_usage_flags = False
    pubkey, _ = PGPKey.from_blob(str(key.pubkey))
    response = s3_client.get_object(Bucket=bucket_name, Key=input_file)
    encrypted_content = response['Body'].read()
    cipher_msg = PGPMessage.from_blob(encrypted_content)
    decrypted = key.decrypt(cipher_msg)
    decrypted_data = bytes(decrypted._message.contents) if isinstance(decrypted._message.contents,
                                                                      bytearray) else decrypted._message.contents
    return decrypted_data


def upload_s3(input_file, s3_client, bucket_name, key):
    s3_client.upload_file(Filename=input_file, Bucket=bucket_name, Key=key)
    # 'p0_build/Add_Supervisory_Organization_EIB_v41.2.xlsx')
    print("Successfully file uploaded into S3")


def main():
    global base_path
    base_path = r"C:\\Users\\2000086748\\OneDrive - Hexaware Technologies\\GTAPIWorkspace\\news_corp\\"

    input_file = 'Add_Supervisory_Organization_EIB_v41.2.xlsx'

    zip_file_name = 'Zipped_Supervisory_Organization_EIB.zip'

    encrypted_file_name = 'Zipped_Encrypted_Supervisory_Organization_EIB.zip'

    s3_client = boto3.client('s3', region_name=region_name)

    public_key_file_name = ""
    latest_key = ''
    local_public_key_file_name = ''
    local_public_key_file_name = latest_key

    # This method is one time run to get the key
    # pub_key = get_pgp_key(local_public_key_file_name)
    # print("Got the pgp key..")

    compress_xlsx_to_zip(zip_file_name, input_file)
    encrypted_data = get_pgp_encryption(local_public_key_file_name, zip_file_name, input_file, encrypted_file_name)

    print("Encryption Completed..")
    #s3_file_path = "p0_build/" + encrypted_file_name
    s3_file_path = encrypted_file_name

    upload_s3(encrypted_file_name, s3_client, bucket_name, s3_file_path)

    print("Completed....")


if __name__ == "__main__":
    main()

