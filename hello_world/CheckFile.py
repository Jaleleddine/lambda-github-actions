
"""
Decription :

This lambda function is designed to check various properties of a CSV file, including its encoding, separator, format, and whether or not it is empty. 
The function takes a file object as input and returns a dictionary containing the results of the checks.

Author : KASSI Amine
Date V1 : 23/03/2023
Date V2 : 24/03/2023
"""

# Import libraries that we need
import csv
import boto3
import chardet
from functools import lru_cache
from io import BytesIO
import io
import pandas as pd

# Create a new client object for the Amazon S3 service using the boto3
# s3 = boto3.client('s3')
s3 = boto3.resource('s3')

# Cache the function's result
# @lru_cache(maxsize=5) 
def check_files(bucket_name, key):
    file = s3.Object(bucket_name, key).get()['Body']
    content = file.read()
    if len(content.strip()) == 0:
        return True, None, None
    else:
        encoding = chardet.detect(content)['encoding']
        dialect = csv.Sniffer().sniff(content[0:20000].decode(encoding))
        return False, encoding.lower(), dialect.delimiter

        
def lambda_handler(event, context):
    # Get the bucket and file name from the S3 event
    bucket_name = event['bucket_name']
    file_name = event['file_name']

    
    # #Check the file not empty
    # file_empty = is_file_empty(bucket_name, file_name)
    
    # # Check the file encoding
    # file_encoding = get_charset(bucket_name, file_name).lower()

    # # Check the file separator
    # response = s3.get_object(Bucket=bucket_name, Key=file_name)
    # content = response['Body'].read()
    # dialect = csv.Sniffer().sniff(content.decode(file_encoding))
    # file_separator = dialect.delimiter
    
    # # Check the file fomat
    if file_name.endswith('.csv'):
        is_csv = True
    else:
        is_csv = False
        
    file_empty, file_encoding, file_separator = check_files(bucket_name, file_name)
    # print( file_separator+" "+file_encoding)
    
 
 
 
    
    # get_file_comment that takes in four parameters: file_empty, file_encoding, file_separator, and is_csv. 
    # The function returns a comment that describes any issues with the file based on these parameters. 
    
    def get_file_comment(file_empty, file_encoding, file_separator, is_csv):
        error_messages = {
            "file_empty": "Le fichier est vide",
            "is_csv": "Le fichier n'est pas sous format csv",
            "file_separator": "Le séparateur du fichier ne correspond pas au ';'",
            "file_encoding": "L'encodage du fichier n'est pas UTF-8"
        }
    
        if file_empty:
            comment = error_messages["file_empty"]
            if not is_csv:
                comment += " et il n'est pas sous format csv"
            if (file_separator != ";") or (file_separator != '\t'):
                comment += " et le séparateur ne correspond pas au ';'"
            if not file_encoding.startswith("utf-"):
                comment += " et l'encodage n'est pas UTF-8"
        else:
            if not is_csv:
                if (file_separator != ";") or (file_separator != '\t'):
                    if file_encoding != "utf-8":
                        comment = error_messages["is_csv"] + " et le séparateur ne correspond pas au ';' et l'encodage n'est pas UTF-8"
                    else:
                        comment = error_messages["is_csv"] + " et le séparateur ne correspond pas au ';'"
                elif file_encoding != "utf-8":
                    comment = error_messages["is_csv"] + " et l'encodage n'est pas UTF-8"
                else:
                    comment = error_messages["is_csv"]
            elif (file_separator != ";") or (file_separator != '\t'):
                if file_encoding != "utf-8":
                    comment = error_messages["file_separator"] + " et l'encodage n'est pas UTF-8"
                else:
                    comment = error_messages["file_separator"]
            elif file_encoding != "utf-8":
                comment = error_messages["file_encoding"]
            else:
                comment = ""
    
        return comment
        
    # Comment for files that are uploaded fortnightly    
    if file_name != "Classeur1.csv":
        commentaire = get_file_comment(file_empty, file_encoding, file_separator, is_csv)
    else:
        commentaire = "Le chargement du fichier est bi-mensuel"
    
    # If all the conditions are verified return Success otherwise return FAILURE
    result = {}
    if (file_empty == False) and (file_encoding.startswith("utf-")) and ((file_separator == ";") or (file_separator == '\t')) and (is_csv == True):
        print('File is perfect')
        result = {
            "Status" : "SUCCESS",
            'Comment': commentaire
        }
        return result
    else:
        result = {
            "Status" : "FAILURE",
            'Comment': commentaire
        }
        return result