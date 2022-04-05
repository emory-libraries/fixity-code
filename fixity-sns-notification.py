import json
import boto3
import csv
import os


def lambda_handler(event, context):

   # os.remove("/tmp/*.csv")
    data=event['Records'][0]
    client = boto3.client('sns')
    s3client= boto3.client('s3')
    prefix = "https://s3.console.aws.amazon.com/s3/object/"
    bucket = data['s3']['bucket']['name']
    object = data['s3']['object']['key']

    #rewrite file
    response=s3client.get_object(Bucket=bucket,Key=object)
    oldfile = response['Body'].read().decode('utf-8').splitlines()
    oldfilereader= csv.reader(oldfile,delimiter=',')

    #open new file
   
    csvname=object.split('/')[4]
    newcsvname="fixity-report-"+csvname
    newfile=open('/tmp/'+newcsvname,'w')
    newcsvwriter=csv.writer(newfile,delimiter=',')
    newcsvwriter.writerow(['event_sha1','event_bucket','event_start','event_end','initiating_user','outcome','software_version'])
    
    for row in oldfilereader:
        bucketname=row[0]
        event_type="Fixity Check"
        user = "AWS Serverless Fixity"
        outcome = row[3]
        fileset_id=' '
        if outcome == 'succeeded':
            fixitymsg=row[1] 
            outcome="Success"
            start = (row[6].split(',')[0]).split('Start:')[1]
            end = row[6].split(',')[1].split('End:')[1]
        else:
            outcome = "Failure"
            fixitymsg=row[1] 
            if row[6][0:6]=='Lambda':
                start='lambda error'
                end = 'lambda error'
            else:
                timestamp=row[6].strip('PermanentFailure: FAIL')
                start = timestamp.split(',')[0].split('Start:')[1]
                end =timestamp.split(',')[1].split('End:')[1]
        newcsvwriter.writerow([fixitymsg, bucketname, start, end, user, outcome, 'Serverless Fixity v1.0', ' '])
    
    newkeyname='/'.join(object.split('/')[:4])+"/"+newcsvname
    newfile.close()

    response = s3client.put_object(
        Body=open(('/tmp/'+newcsvname),'rb'),
        Bucket=bucket,
        Key=newkeyname

    )
  
    link=prefix+bucket+"/"+newkeyname
    message = "{new fixity check generated: located in cor-devops-binaries}" 
    response = client.publish(
        TargetArn = 'SNS-ARN',
        Message=json.dumps({'default': json.dumps(message),
                            'email': 'log into the console to view the link: '+link}),
        Subject = 'Fixity report generated',
        MessageStructure = 'json'
    )
    os.remove('/tmp/'+newcsvname)
    return {
        'statusCode': 200,
    }

