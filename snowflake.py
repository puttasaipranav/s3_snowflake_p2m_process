from datetime import date as de
from datetime import timedelta as td
from datetime import datetime as dt
import datetime 
from datetime import datetime as dr
import boto3
import os

s3 = boto3.client('s3')
sns = boto3.client('sns')

def lambda_handler(event,context):
    keys = []
    date = []
    data = []
    b = {}
    c = []
    m_data = []
    file_name = []
    count = []
    source_bucket = 'kwa-us-west-2-dev-data-receive'
    target_bucket = 'kwa-us-west-2-dev-data-curate'
    source_prefix = 'Power2Motivate/'
    target_prefix = 'Power2Motivate/'
    

    def get_files():

        r = s3.list_objects_v2(Bucket='kwa-us-west-2-dev-data-receive', Prefix = 'Power2Motivate/')
        
        data.clear()

        for i in r['Contents']:
            keys.append(i['Key'])
            date.append(str(i['LastModified']))
        data.extend(list(a) for a in zip(keys,date))        

        for i in data:
            j = i[0].split("/")[-1],i[1]
            m_data.append(j)
        m_data.pop(0)
    get_files()
    for item in m_data:
        k = item[1].split(" ")[0]
        if k in b:
            b[k].append(item[0])
        else:
            b[k] = [item[0]]
    def dates(): 
        
        today = de.today()
        for i in range(0,7):
            dt =(today - td(days = i))
            c.append(dt.strftime('%Y-%m-%d'))
    dates()

    def files():
        for i in c:
            if i in b:
                file_name.append(b[i])
                
    files()

    file_name = [item for sublist in file_name for item in sublist]
    if len(file_name) ==0:
        sns.publish(TopicArn = 'arn:aws:sns:us-west-2:753184167782:Power2Motivate_mailing_service_dev',Message = 'No files are received this week. Please check and confirm', Subject = 'S3 copy')
        print('No files in this week')
    elif len(file_name) <= 3:
        for key in file_name:
            copy_source = {'Bucket':source_bucket, 'Key': source_prefix+key}
            try:
                s3.copy_object(Bucket = target_bucket, Key = target_prefix+key, CopySource = copy_source)
                print(key,'is copied')
            except Exception as err:
                print ("Error -"+str(err))
    else:
        sns.publish(TopicArn = 'arn:aws:sns:us-west-2:753184167782:Power2Motivate_mailing_service_dev',Message = 'More than 3 files have been detected. Please check and upload required file to S3', Subject = 'S3 copy')
        print('Mail sent')