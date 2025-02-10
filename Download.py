import boto3
import datetime
import json
import mysql.connector
import requests

from botocore.exceptions import ClientError
from dataclasses import dataclass
from typing import List
from urllib.parse import urlparse


BUCKET_NAME = 'f3pugetsound-slack-pictures'
REGION = 'us-west-2'
SECRET_NAME = 'paxminer'


def unquote(s: str):
    return s.replace('"', '').replace("'", "")


def get_secret(session):
    secrets_manager_client = session.client(
        service_name='secretsmanager',
        region_name=REGION
    )
    try:
        get_secret_value_response = secrets_manager_client.get_secret_value(SecretId=SECRET_NAME)
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e
    return get_secret_value_response['SecretString']


@dataclass
class Link:
    ao: str
    date: datetime.date
    url: str

    # example link: https://slackblast-images.s3.amazonaws.com/F06PEBZ6LA2.jpg
    def s3_key(self) -> str:
        date_str = self.date.strftime('%Y-%m-%d')
        file_str = self.url.split('/')[-1]
        return f'{self.ao}/{date_str}-{file_str}'


def get_links(cur) -> List[Link]:
    query = '''
with raw_files as (select bd_date as date, ao_id, json->"$.files[0]" as f from beatdowns),
     simple_files as (select * from raw_files where f IS NOT NULL)
select simple_files.date, aos.ao, aos.region, simple_files.f from simple_files
left join aos on aos.channel_id = simple_files.ao_id
where region = 'Seattle'
order by date desc
'''
    links = []
    cur.execute(query)
    for (date, ao, _, link) in cur.fetchall():
        links.append(Link(ao=ao, date=date, url=unquote(link)))
    return links


def sync_files_to_s3(session, links):
    s3_client = session.client(
        service_name='s3',
        region_name=REGION
    )
    for link in links:
        filename = urlparse(link.url).path.split('/')[-1]
        s3_key = link.s3_key()
        try:
            s3_client.head_object(Bucket=BUCKET_NAME, Key=s3_key)
            print(f"File {filename} already exists in S3. Skipping.")
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                # The file does not exist in S3, so we need to download and upload it
                print(f"File {filename} not found in S3. Downloading and uploading to {s3_key}.")
                response = requests.get(link.url)
                if response.status_code == 200:
                    s3_client.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=response.content)
                    print(f"File {filename} successfully uploaded to S3.")
                else:
                    print(f"Failed to download {filename} from URL. Status code: {response.status_code}")
            else:
                # If there was an error that wasn't a 404, raise it
                raise


def doit():
    session = boto3.session.Session(profile_name='f3-admin')
    raw_secret = get_secret(session)
    print("raw_secret: " + raw_secret)
    secret = json.loads(raw_secret)
    print("secret: " + str(secret))
    cnx = mysql.connector.connect(
        host=secret['host'],
        port=int(secret['port']),
        user=secret['username'],
        password=secret['password'],
        database=secret['dbname'],
        )
    cur = cnx.cursor()
    links = get_links(cur)
    cur.close()
    cnx.close()
    sync_files_to_s3(session, links)


if __name__ == '__main__':
    doit()