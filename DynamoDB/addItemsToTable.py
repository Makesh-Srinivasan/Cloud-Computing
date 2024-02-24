import boto3
import time
import yaml

TABLE_NAME = "yelp-restaurants"
CUISINES = ['Indian', 'Chinese', 'Japanese']
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(TABLE_NAME)

for filename in CUISINES:
    filepath = "%s%s.json" % ("./", filename)
    data = yaml.safe_load(open(filepath))
    count = 1
    for record in data:
        ITEM = {
            "id": str(record["id"]),
            "name": record["name"],
            "address": record["location"]["display_address"],
            "contact": str(record["phone"]),

            "review_count": str(record["review_count"]),
            "transactions": record["transactions"],
            "zipcode": str(record["location"]["zip_code"]),
            "rating": str(record["rating"]),
            "coordinates": {"latitude": str(record["coordinates"]["latitude"]),"longitude": str(record["coordinates"]["longitude"])},
            "insertAtTimestamp": str(time.time())
        }
        table.put_item(TableName=TABLE_NAME, Item=ITEM)
        count += 1
