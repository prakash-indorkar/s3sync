#!/usr/bin/python3
import os
import sys
import boto3
from botocore.client import ClientError
import click

print("\n\n -----------------------------------------------------")
print(" | s3sync - Upload deze directory naar een s3 bucket | ")
print(" -----------------------------------------------------\n\n")

# Get current  wd
workingdirectory = os.getcwd()

# Ingore list
ignorelist = ['.git','.env',__file__]

# Set help texts
endpoint_help = 'S3 endpoint url (bijv. http://127.0.0.1:9000)'
bucket_help = 'S3 bucket (directory) om in te uploaden (of aan te maken)'
accesskey_help = 'S3 Access key'
secretkey_help = 'S3 Secret key'

# Sync function decorated with 
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])
@click.command(context_settings=CONTEXT_SETTINGS,help="Upload a local directory to custom s3 endpoint and bucket")
@click.option('--directory', '-d', default=workingdirectory, help='Directory die geupload moet worden, default: Huidige directory.')
@click.option('--endpoint', '-e', help=endpoint_help)
@click.option('--bucket', '-b', help=bucket_help)
@click.option('--accesskey', '-a', help=accesskey_help)
@click.option('--secretkey', '-s', help=secretkey_help)
def sync(directory, endpoint, bucket, accesskey, secretkey):

    # Check options and ask if not set 
    if not endpoint:
        endpoint = click.prompt(endpoint_help)

    if not bucket:
        bucket = click.prompt(bucket_help)

    if not accesskey:
        accesskey = click.prompt(accesskey_help)

    if not secretkey:
        secretkey = click.prompt(secretkey_help)

    # Init the s3 client
    client = boto3.client('s3', verify=False, aws_access_key_id=accesskey, aws_secret_access_key=secretkey, endpoint_url=endpoint)

    print('Verbinding gemaakt met %s\n\n' % (endpoint))

    # Check bucket exists
    print('Checken of "%s" bucket bestaat...' % (bucket))
    try:
        client.head_bucket(Bucket=bucket)
        print("Check!\n")
    except ClientError:
        # The bucket does not exist, create it
        print('Maak bucket "%s" aan...' % (bucket))
        client.create_bucket(Bucket=bucket)
        print('Bucket "%s" aangemaakt!\n' % (bucket))

    # enumerate local files recursively
    for root, dirs, files in os.walk(directory, topdown=True):

        # Exclude ignore dirs
        dirs[:] = [d for d in dirs if d not in ignorelist]

        for filename in files:

            # Check against ignore
            if filename in ignorelist:
                print(' - "%s" in ingore lijst, overslaan...' % (filename))
                continue

            # construct the full local path
            local_path = os.path.join(root, filename)

            # construct the full S3 path
            relative_path = os.path.relpath(local_path, directory)
            s3_path = os.path.join(relative_path)

            # Get file properties if it exists
            s3file = False
            try:
                s3file = client.head_object(Bucket=bucket, Key=s3_path)
            except:
                s3file = False

            # File present on s3?
            if s3file:
                # Compare size, continue if same
                if os.path.getsize(local_path) == s3file['ContentLength']:
                    print(" - \"%s\" bestaat al. Overslaan..." % s3_path)
                    continue
                else:
                    # Throw execption to trigger upload
                    print(" - \"%s\" bestaat al maar met andere omvang:" % s3_path)
                    #raise Exception('File sizes differ') 

            # File not present or different size
            print("   Uploaden %s..." % s3_path)
            client.upload_file(local_path, bucket, s3_path)

# Main
if __name__ == '__main__':
    sync()