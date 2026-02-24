import boto3


def get_s3_bucket():

    region_name = 'us-east-1'  # e.g., 'us-east-1'
    bucket_name = 'ncgt-nca-onboarding-prod'
    session = boto3.Session()

    s3 = session.resource('s3')

    s3_bucket = s3.Bucket(bucket_name)

    return s3_bucket


def upload_s3_bucket(input_file, destination_file_path):
    s3_bucket = get_s3_bucket()
    resp = s3_bucket.upload_file(input_file, destination_file_path)
    return resp
