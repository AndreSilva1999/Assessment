import boto3
import pandas as pd
import logging


def aws_v3_bucket(file: str,aws_acess_id: str,key: str,region: str):
    """_summary_
    Search for your aws bucket and upload the wanted file to it 
    Args:
        file (str): Your file
        aws_acess_id (str): Your aws id
        key (str): password
        region (str): region
    """
    #Creating low level client
    try:
        open_client= boto3.client("s3",aws_acess_id,key,region_name=region)
        #creating high level client
        res=  boto3.resource("s3",aws_acess_id,key,region_name=region)
        #Featch bucket names
        featch= open_client.list_buckets()
        output= [buckets["Name"] for buckets in featch["Buckets"]]
        for bucket in output:
            logging.warning(f"Bucket Name:{bucket}")
        #Create bucket
        name= input("Select wanted bucket")
        res.meta.client.upload_file(file,name,"Mynewfile")
    except:
        logging.warning("Fail to upload file, verify existing buckets")

    


