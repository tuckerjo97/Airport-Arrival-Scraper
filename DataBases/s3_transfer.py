from datetime import datetime
import boto3
import os
import json
import traceback
import pandas as pd


def s3_transfer(bucket_name, file_name, direction, file_type, file_to_upload=None):

    """
    This function uploads or downloads files from AWS s3 buckets depending on
    user inputs

    Params:

    bucket_name (str),
    file_name (str),
    file_to_upload (default: None type, accepts any file ending)
    direction (str), only accepts "upload" or "download"

    """

    s3_resource = boto3.resource('s3')
    # bucket = s3_resource.Bucket(name=str(bucket_name))
    # bucket_object = bucket.Object(
    #     bucket_name=str(bucket_name),
    #     key=str(file_name))

    if str(direction).lower() == 'upload':

        try:

            s3_resource.Bucket(name=str(bucket_name)).upload_file(
                Filename=str(file_to_upload),
                Key=str(file_name + '.' + file_type))

        except:

            print("\nupload error occured at s3 bucket block with error \
            message:\n")
            traceback.print_exc(limit=1)

    elif str(direction).lower() == 'download':

        try:
            print(os.getcwd() + '/' + file_name + '.' + file_type)
            print(file_name + '.' + file_type)
            s3_resource.Object(
                str(bucket_name),
                str(file_name + '.' + file_type)).download_file(
                    file_name + '.' + file_type)

            if file_type.lower() == 'csv':

                df = pd.DataFrame(os.getcwd() + '/' + file_name + '.' + file_type)
                delete_file(file_name, file_type)
                return df

            elif file_type.lower() == 'json':

                with open(os.getcwd() + '/' + file_name + '.' + file_type) as f:
                    dictionary = json.load(f)
                    delete_file(file_name, file_type)
                    return dictionary

        except:

            print("\ndownload error occured at s3 bucket block with error \
            message:\n")
            traceback.print_exc(limit=1)

    else:

        print("\ndirection entered can only be upload or download! you entered\
        : {0}\n".format(str(direction)))

def delete_file(file_name, file_type):

    """
    This function is the opposite of the create_file() function. It takes
    two inputs and deletes a specified file from the current working directory
    """

    if os.path.exists(str(file_name) + '.' + file_type):
        os.remove(str(file_name) + '.' + file_type)
    else:
        print("The file does not exist")
