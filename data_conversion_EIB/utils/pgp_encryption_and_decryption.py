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


region_name = 'us-east-1'  # e.g., 'us-east-1'
bucket_name = 'poc-nca-onboarding'

global base_path


def get_pgp_key(public_key_filename, private_key_filename):
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

    with open(private_key_filename, 'w') as key_file:
        key_file.write(str(key))
    # Typically, recipient then saves the key information to a file on their server

    return key


def get_pgp_encryption(public_key, input_file, output_file, secret_data):
    with open(public_key, "r") as f:
        encoded_key = f.read()
    
    decoded_key = cryptocode.decrypt(str(encoded_key), secret_data)
    key, _ = pgpy.PGPKey.from_blob(decoded_key)
    key._require_usage_flags = False
    key = key.pubkey

    # Read the Excel file in binary mode
    with open(input_file, 'rb') as file:
        excel_binary_data = file.read()

    f_t_e = pgpy.PGPMessage.new(excel_binary_data)

    encrypted_data = key.encrypt(f_t_e)

    with open(output_file, 'w') as encrypted_file:
        encrypted_file.write(str(encrypted_data))

    return encrypted_data



def get_pgp_decryption(key_file, encrypted_content, secret_data):
    with open(key_file, "r") as f:
        encoded_key = f.read()
    
    decoded_key = cryptocode.decrypt(str(encoded_key), secret_data)
    
    key, _ = pgpy.PGPKey.from_blob(decoded_key)
    key._require_usage_flags = False
    cipher_msg = PGPMessage.from_blob(encrypted_content)
    decrypted = key.decrypt(cipher_msg)
    decrypted_data = bytes(decrypted._message.contents) if isinstance(decrypted._message.contents,
                                                                      bytearray) else decrypted._message.contents
    
    
    return decrypted_data