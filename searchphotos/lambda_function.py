import json
import boto3
import urllib3


def lambda_handler(event, context):
    # TODO implement
    query = event['queryStringParameters']['q']
    
    client = boto3.client('lex-runtime')
    response = client.post_text(botName='searchPhotosBot',
                                botAlias='prod',
                                userId='user',
                                inputText=query)
                                
    keywords = list(response['slots'].values())
    print (keywords)
    
    urls = []
    for keyword in keywords:
        if keyword is None:
            continue
        params = {
            'q': keyword,
            'size': '100',
            'pretty': 'true',
        }
        opensearch_url = "https://search-photos-h532eg2iutkfmvgo3pyhkxvsn4.us-east-1.es.amazonaws.com/photos/_search/"
        user = "user"
        password = "Password0!"
        http = urllib3.PoolManager()
        headers = urllib3.make_headers(basic_auth=f'{user}:{password}')
        headers['Content-Type'] = 'application/json'
        
        response = http.request('GET', opensearch_url, headers=headers, fields=params)
        hits = json.loads(response.data)['hits']['hits']
        photos = [hit['_source'] for hit in hits]
        
        # objectURL: https://ccbda2photostorage.s3.amazonaws.com/26406_10150179482585271_321873835270_12318659_1114438_n.jpg
        object_urls = [f'https://{photo["bucket"]}.s3.amazonaws.com/{photo["objectKey"]}' for photo in photos]
        urls += object_urls
    
    urls = list(set(urls))[:]
    print (urls)
    
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': json.dumps(urls)
    }
