import json
import boto3
import random
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
import requests
from requests_aws4auth import AWS4Auth


# Direct assignments
ES_INDEX_NAME = "restaurants"
SQS_QUEUE_URL = '<Removed from this place due to security reasons>'
DYNAMODB_TABLE_NAME = 'yelp-restaurants'
region = "us-east-1"
service = "es"
session = boto3.Session()
creds = session.get_credentials()
awsauth = AWS4Auth(creds.access_key, creds.secret_key, region, service, session_token=creds.token)
ES_HOST_URL = '<Removed from this place due to security reasons>'
ES_AUTH = awsauth

logger.info(creds.access_key)
logger.info(creds.secret_key)
logger.info(region)
logger.info(service)
logger.info(creds.token)

def fetch_message_from_sqs():
    sqs = boto3.client('sqs')
    response = sqs.receive_message(
        QueueUrl=SQS_QUEUE_URL,
        MaxNumberOfMessages=1,
    )
    logger.info("Message from SQS----::::")
    logger.info(response)
    # if response.get('Messages'):
    try:
        
        message = response['Messages'][0]
        logger.info(message['Body'])
        message_body = json.loads(message['Body'])
        receipt_handle = message['ReceiptHandle']
        logger.info('RETRIEVED FROM QUEUE')
        logger.info(message_body)
        sqs.delete_message(
            QueueUrl=SQS_QUEUE_URL,
            ReceiptHandle=receipt_handle
        )
        return {"msgtype": 200, "body": message_body}
    except Exception as e:
        return {"msgtype": 404, "body": e}

    

def fetch_id_from_es(cuisine):
    logger.info("************* we are inside the es************")
    url = '%s/%s/_search' % (ES_HOST_URL, ES_INDEX_NAME)
    
    # Adjust the query to include a random score
    query = {
        'size': 5,
        'query': {
            'function_score': {
                'query': {
                    'multi_match': {
                        'query': cuisine,
                        'fields': ['cuisine']
                    }
                },
                'functions': [
                    {
                        "random_score": {"seed": random.randint(0, 10000)}
                    }
                ],
                "boost_mode": "replace"
            }
        }
    }
    headers = {"Content-Type": "application/json"}
    logger.info(cuisine)
    logger.info(url)
    response = requests.get(url, auth=ES_AUTH, headers=headers, data=json.dumps(query)).json()
    logger.info(response)
    records = response['hits']['hits']
    return records



def fetch_restaurant_details_from_dynamodb(id):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    response = table.get_item(Key={'id': id})
    return response



def create_content(restaurant_details, message):
    ### We create a content to be sent via email in this function
    
    logger.info('THE MESSAGE BODY')
    logger.info(message['body'])

    # Extracting main reservation details
    contents = {
        'email': message["body"]["email"]['value']['interpretedValue'],
        'cuisine': message["body"]["cuisine"]['value']['interpretedValue'],
        'location': message["body"]["location"]['value']['interpretedValue'],
        'count': message["body"]["count"]['value']['interpretedValue'],
        'day': message["body"]["date"]['value']['interpretedValue'],
        'time': message["body"]["time"]['value']['interpretedValue'],
        'restaurants': []
    }

    # Extracting restaurant details
    for restaurant in restaurant_details:
        content = {
            'name': restaurant['Item']['name'],
            'address': restaurant['Item']['address'],
            'contact': restaurant['Item']['contact'],
            'latitude': restaurant['Item']['coordinates']['latitude'],
            'longitude': restaurant['Item']['coordinates']['longitude'],
        }
        contents['restaurants'].append(content)

    # Formatting the email
    email_intro = f"Hi! Here are your {contents['cuisine']} restaurant suggestions for {contents['count']} at {contents['time']} on {contents['day']} in the {contents['location']} region:<br><br>"
    
    restaurant_list = "<br>".join(
        f"{index+1}. {restaurant['name']}, located at {restaurant['address'][0]}." +
        f"<br>Contact: {restaurant['contact']}" +
        f"<br>Location: <a href='https://www.google.com/maps/search/?api=1&query={restaurant['latitude']},{restaurant['longitude']}'>Google Maps</a><br>"
        for index, restaurant in enumerate(contents['restaurants'])
    )
    
    email_body = email_intro + restaurant_list + "<br>Enjoy your meal!"

    return email_body



def lambda_handler(event, context):
    message = fetch_message_from_sqs()
    logger.info(message)

    
    if message['msgtype'] == 200:
        records = fetch_id_from_es(message['body']['cuisine']['value']['interpretedValue'])
        restaurant_details = []
        for record in records:
            restaurant_details.append(fetch_restaurant_details_from_dynamodb(record["_source"]["id"]))
        contents = create_content(restaurant_details, message)
        toEmailAddress = message["body"]["email"]['value']['interpretedValue']
        
        logger.info('----------------Look here for the email-----------------')
        logger.info(contents)
        logger.info("--------------------------------------------------------")
        
        client = boto3.client('ses')
        response = client.send_email(
            Source='ms15138@nyu.edu',
            Destination={
                'ToAddresses': [
                    toEmailAddress
                ],
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': 'UTF-8',
                        'Data': contents,
                    },
                    'Text': {
                        'Charset': 'UTF-8',
                        'Data': contents,
                    },
                },
                'Subject': {
                    'Charset': 'UTF-8',
                    'Data': 'Your restaurant recommendations!',
                },
            },
            ReplyToAddresses=[
            ],
        )
        
    else:
        message = {'message': "Not Found"}
    return {
        'statusCode': 200,
        'body': message
    }