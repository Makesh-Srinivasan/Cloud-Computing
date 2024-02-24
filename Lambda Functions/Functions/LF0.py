import json
import boto3
import uuid
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

client = boto3.client('lexv2-runtime', region_name='us-east-1')
sqs = boto3.client('sqs')

def lambda_handler(event, context):
        # Process a standard request coming from API Gateway or another source
        logger.info("Received event: %s", json.dumps(event))
        session_id = event['headers'].get('Session-Id', str(uuid.uuid4()))
        logger.info("Session ID: %s", session_id)
        
        try:
            body = json.loads(event.get('body', '{}'))
            lastUserMessage = body.get('messages')[0]['unstructured']['text']
        except (json.JSONDecodeError, IndexError, TypeError) as e:
            return {'statusCode': 400, 'body': json.dumps("Invalid request format")}
        
        
        session_id = event['headers'].get('Session-Id')
        if not session_id:
            session_id = '123e4567-e89b-12d3-a456-426614174000' #str(uuid.uuid4())

        response = client.recognize_text(
            botId="GONP1SGVTR",
            botAliasId="U0NMONUYDY",
            localeId="en_US",
            sessionId=session_id,
            text=lastUserMessage
        )
        return calling_lex_response(response)



def calling_lex_response(response):
    logger.info("Received Lex response: %s", json.dumps(response))
    botMessage = []
    for i in response.get('messages', [{}]):
        botMessage.append(i.get('content', "I'm not sure how to respond to that"))
    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
        'body': json.dumps({'messages': [{'type': 'unstructured', 'unstructured':{'text':i}} for i in botMessage]})
    }