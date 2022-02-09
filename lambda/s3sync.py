from __future__ import print_function
import json
import boto3
import botocore
import logging
import os


manifest_bucket = os.environ['manifest_bucket_name']

ds_client = boto3.client('datasync')
s3_client = boto3.client('s3')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    
    # Init manifest, check if the manifest object exist. If it doesn't exist, create a 
    # new object.

    try:
        manifest = s3_client.get_object(Bucket = manifest_bucket, Key = 'manifest')
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "NoSuchKey":
            s3_client.put_object(Bucket=manifest_bucket, Key='manifest', Body='')
            manifest = s3_client.get_object(Bucket = manifest_bucket, Key = 'manifest')
            logger.info('Created new manifest object in manifest_bucket')
    
    manifest_len = len(manifest['Body'].read().decode("utf-8"))
    data = manifest['Body'].read().decode("utf-8")[1:]

    # Checking the length of the manifest file. DataSync filter string length is 
    # limited to 409,600 characters. If the length exceeds 300,000 char's, execute 
    # a task. 
    # If the trigger is EventBridge (10min schedule and event['triggeredBy'] == 'eventbridge'),
    # execute the task if the manifest isn't empty.
    if manifest_len < 100000:
        try:
            if (event['triggeredBy'] == 'eventbridge'):
                run_ds_task()                
        except:
            manifest = s3_client.get_object(
                Bucket = manifest_bucket, 
                Key = 'manifest'
                )
            add_to_manifest(event, manifest)
            logger.info('Successfully added new keys from events to manifest')
    else:
        run_ds_task()
        logger.info('Filter size limit reached (300000 Characters). Successfully run datasync task')
        
def add_to_manifest(event, manifest):
    # Gets the object key for each "Created Object" event and adds it to the 
    # manifest file
    current_filter_str = str(manifest['Body'].read().decode("utf-8"))
    try:
        for record in event['Records']:
            payload_str = record["body"]
            payload = json.loads(payload_str)
            record_key = payload['detail']['object']['key']
            current_filter_str += ('|/' + str(record_key))
        s3_client.put_object(Bucket=manifest_bucket, Key='manifest', Body=current_filter_str)
        logger.info('add_to_manifest: added new keys to manifest')
    except:
        logger.info('add_to_manifest: No records to add to manifest')
    
def run_ds_task():
    logger.info('Calling ds_task from EventBridge')
    # Run the DataSync task with the include filter from the manifest object.
    manifest = s3_client.get_object(Bucket = manifest_bucket, Key = 'manifest')
    data = manifest['Body'].read().decode("utf-8")[1:]
    response = ds_client.start_task_execution(
        TaskArn=ds_task_arn,
        OverrideOptions={'VerifyMode': 'ONLY_FILES_TRANSFERRED'},
        Includes=[
            {
                'FilterType': 'SIMPLE_PATTERN',
                'Value': str(data)
            },
        ]
    )
    logger.info('run_ds_task: Successfully started task execution - {}'.format(response))
    # Delete the content in the manifest, clear the current manifest list for 
    # the next set of objects
    s3_client.put_object(Bucket=manifest_bucket, Key='manifest', Body='')
    logger.info('run_ds_task: Cleared manifest after DataSybc task')
    return response