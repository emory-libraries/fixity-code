import json
import boto3
import random
import time
import string
from datetime import datetime

def lambda_handler(event, context):
    
    tasks=event['tasks'][0]
    statemachine='STATE-MACHINE-ARN'
    bucket=tasks["s3BucketArn"].split(":::")[1]

    input={}
    input['Bucket']=bucket
    input['Key']=tasks["s3Key"]
    input['Algorithm']="SHA1"
    input['ComparedWith']=tasks["s3Key"]
    json_input=json.dumps(input)
    
    randomnumber=''.join([random.choice(string.ascii_letters + string.digits ) for n in range(20)])
    starttime=datetime.now().isoformat()
    stepfunctionclient=boto3.client('stepfunctions')
    execname=str(input['Bucket']+input['Key']+randomnumber)[:79]

    response=stepfunctionclient.start_execution(
        stateMachineArn = statemachine,
        name            = execname,
        input           = json_input
        
    )
    eArn=response['executionArn']
    retry=True
    retries = 1
    state = 'RUNNING'
    while (retry):
         
        time.sleep(2**retries *5 )
        
        executionresponse = stepfunctionclient.describe_execution(
            executionArn=eArn,
        )
        if ('stopDate' in executionresponse):
            retry = False
        else:
            retries +=1 
   
    endtime = datetime.now().isoformat()    
    state=executionresponse['status']
    if (state == "SUCCEEDED"):
        resultCode = "Succeeded"
        resultString = " Start:"+starttime + "," + "End:"+endtime
        
    else:
        resultCode = "PermanentFailure"
        resultString =" Start:"+starttime + "," +" End:"+endtime
        
 
    return {
      "invocationSchemaVersion": "1.0",
      "treatMissingKeysAs" : "PermanentFailure",
      "invocationId" : event["invocationId"],
       "results": [
        {
           "taskId": tasks["taskId"],
           "resultCode": resultCode,
           "resultString": resultString
        }
        ]
    }
