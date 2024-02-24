from requests_aws4auth import AWS4Auth
from opensearchpy import OpenSearch, RequestsHttpConnection
import boto3
import yaml


CUISINES = ['Indian', 'Chinese', 'Japanese']
index_name = "restaurants"
region = "us-east-1"
service = "es"
creds = boto3.Session().get_credentials()
awsauth = AWS4Auth(creds.access_key, creds.secret_key, region, service, session_token=creds.token)
host = "search-hw1-restaurants-hpzhdzl6lw4sk7dhbh7hsynwua.us-east-1.es.amazonaws.com"
client = OpenSearch(hosts=[{'host': host, 'port': 443}],http_auth=awsauth,use_ssl=True,verify_certs=True,connection_class=RequestsHttpConnection)



def create_index():
    index_body = {
        'settings': {
            'index': {}
        },
        'mappings': {
            'properties': {
                'Restaurant' :{
                    'properties':{
                        'id': {'type': 'keyword'},
                        'cuisine': {'type': 'text'}
                    }
                }
            }
        }
    }
    response = client.indices.create(index_name, body=index_body)
    print(response)


def load_into_index():
    id = 1
    for filename in CUISINES:
        data = yaml.safe_load(open("%s%s.json" % ("./", filename)))
        for record in data:
            document = {"id": record["id"], "cuisine": filename}
            resp = client.index(
                index='restaurants',
                body=document,
                id=id,
                refresh=True
            )
            id += 1
            print(resp)


if __name__ == "__main__":
    create_index()
    load_into_index()