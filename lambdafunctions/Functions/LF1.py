import json
import boto3
import uuid
import logging
from datetime import datetime, date

logger = logging.getLogger()
logger.setLevel(logging.INFO)
client = boto3.client('lexv2-runtime', region_name='us-east-1')
sqs = boto3.client('sqs')
queue_url = 'https://sqs.us-east-1.amazonaws.com/211125520239/testqueue.fifo'
valid_locations = ["new york", "ny", "manhattan", "nyc", "new york city", "brooklyn", "queens", "staten", "staten island", "bronx"]
valid_cuisines = ["indian", "chinese", "japanese"]



def calling_validation(event):
    slots = event['sessionState']['intent']['slots']
    date_value = None 
    
    try:
        location_slot, cuisine_slot, date_slot, time_slot, count_slot, email_slot = slots.get('location'), slots.get('cuisine'),slots.get('date'),slots.get('time'), slots.get('count'),slots.get('email')
        
        if location_slot is not None and 'interpretedValue' in location_slot['value']:
            value = location_slot.get('value', {}).get('interpretedValue').lower()
            if value not in valid_locations:
                slot_to_elicit = 'location'
                message_content = f"The location '{value}' is not valid. We only serve NYC {slot_to_elicit}s. Enter a different location"
                raise ValueError('')
        
        if cuisine_slot is not None and 'interpretedValue' in cuisine_slot['value']:
            value = cuisine_slot.get('value', {}).get('interpretedValue').lower()
            if value not in valid_cuisines:
                slot_to_elicit = 'cuisine'
                message_content = f"The cuisine '{value}' is not valid. We only serve Indian, Chinese and Japanese {slot_to_elicit}s. Pick one from these"
                raise ValueError('') 
                
        if date_slot is not None and 'interpretedValue' in date_slot['value']:
            date_value = date_slot.get('value', {}).get('interpretedValue').lower()
            if datetime.strptime(date_value, '%Y-%m-%d').date() < date.today():
                slot_to_elicit = 'date'
                message_content = "The date must be valid and can take only today or later. Try again"
                raise ValueError('')

        if time_slot is not None and 'interpretedValue' in time_slot['value'] and date_value:
            datetime_value = datetime.strptime(f"{date_value} {time_slot.get('value', {}).get('interpretedValue')}", '%Y-%m-%d %H:%M')
            if datetime_value < datetime.now():
                slot_to_elicit = 'time'
                message_content = "Please choose a time in the future."
                raise ValueError('')
                
        if count_slot is not None and 'interpretedValue' in count_slot['value']:
            value = count_slot.get('value', {}).get('interpretedValue').lower()
            if int(value)<1 or int(value)>15:
                slot_to_elicit = 'count'
                message_content = f"Too many friends. Most tables have less than single-digit occupancy and NYC is not very spacious. Try Again (Bring only 1-15 people)"
                raise ValueError('')   

    except Exception as e:
        return elicit_slot(
                    intent_name=event['sessionState']['intent']['name'],
                    slots=slots,
                    slot_to_elicit=slot_to_elicit,
                    message_content=message_content
                )
    return {
        'sessionState': {
            'dialogAction': {
                'type': 'Delegate'
            },
            'intent': event['sessionState']['intent']
        }
    }
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def calling_fulfillment(event):
    print(json.dumps(event, indent=4))
    slots = event['sessionState']['intent']['slots']

    message_body = json.dumps({
        slot_name: slot for slot_name, slot in slots.items() if slot
    })
    logger.info(f"Sending message body to SQS: {message_body}")
    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=message_body,
        MessageGroupId='UserPreference' 
    )

    print(f"Message sent to SQS with Message ID: {response['MessageId']}")
    return {
        'sessionState': {
            'dialogAction': {
                'type': 'Close',
                'fulfillmentState': 'Fulfilled'
            },
            'intent': {
                'name': event['sessionState']['intent']['name'],
                'state': 'Fulfilled'
            }
        },
        'messages': [{
            'contentType': 'PlainText',
            'content': 'Thank you, we are processing your request. Mail will be sent shortly'
        }]
    }


def calling_standard_request(event):
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
        session_id = '123e4567-e89b-12d3-a456-426614174000'

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
    botMessage = response.get('messages', [{}])[0].get('content', "I'm not sure how to respond to that.")
    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
        'body': json.dumps({'messages': [{'type': 'unstructured', 'unstructured': {'text': botMessage}}]})
    }



def elicit_slot(intent_name, slots, slot_to_elicit, message_content):
    return {
        'sessionState': {
            'dialogAction': {
                'type': 'ElicitSlot',
                'slotToElicit': slot_to_elicit,
            },
            'intent': {
                'name': intent_name,
                'slots': slots,
                'state': 'InProgress'
            }
        },
        'messages': [{
            'contentType': 'PlainText',
            'content': message_content
        }]
    }



def lambda_handler(event, context):
    print("Received event: " + json.dumps(event))
    if event.get('invocationSource') == 'DialogCodeHook':
        print('Invocation', event['sessionState']['intent']['slots'])
        return calling_validation(event)
    elif event.get('invocationSource') == 'FulfillmentCodeHook':
        return calling_fulfillment(event)
    else:
        return calling_standard_request(event)
