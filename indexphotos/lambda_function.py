import json
import boto3
import urllib3

def lambda_handler(event, context):
    
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    
    # i.
    rekognition = boto3.client('rekognition')
    s3_object = {
                'S3Object':
                    {
                    'Bucket':bucket_name,
                    'Name':object_key
                    }
                }
    response = rekognition.detect_labels(Image=s3_object, MaxLabels=5)
    rekognition_labels = [labels['Name'].lower() for labels in response['Labels']]
    
    # ii.
    s3 = boto3.resource('s3')
    object = s3.Object(bucket_name,object_key)
    
    print (event)
    
    custom_labels = []
    if 'customlabels' in object.metadata:
        labels_meta = object.metadata['customlabels'].split(',') 
        custom_labels = [label.lower() for label in labels_meta]
    
    # iii.
    all_labels = list(set(rekognition_labels + custom_labels))
    plurals = [label + "s" for label in all_labels]
    json_object = {
        "objectKey": object_key,
        "bucket": bucket_name,
        "createdTimestamp": event['Records'][0]['eventTime'],
        "labels": all_labels + plurals,
    }
    
    print (json_object)
    
    opensearch_url = "https://search-photos-h532eg2iutkfmvgo3pyhkxvsn4.us-east-1.es.amazonaws.com/photos/_doc/"
    user = "user"
    password = "Password0!"
    
    http = urllib3.PoolManager()
    headers = urllib3.make_headers(basic_auth=f'{user}:{password}')
    headers['Content-Type'] = 'application/json'
    response = http.request('POST', opensearch_url, headers=headers, body=json.dumps(json_object))
    
    print (response.data.decode("utf-8"))
   
    
    
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Uploaded!!!!!!')
    }
