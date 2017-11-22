from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials
import argparse
import sys


credentials = GoogleCredentials.get_application_default()
service = build('dataflow', 'v1b3', credentials=credentials)

# for setting up chron job:
# http://zablo.net/blog/post/python-apache-beam-google-dataflow-cron
# Should be able to port this as a task.

# Set the following variables to your values.

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--template', type=str, required=True, help='the template_id to use')
parser.add_argument('--date', type=str, required=True, help='YYYYMMDD of date to fetch area for.')
args = parser.parse_args()

if len(parser.date) != 8:
    sys.exit("Date must be in format YYYYMMDD")

GCSPATH="gs://floracast-datamining/templates/fetch_protected_areas/%s" % parser.template
PROJECT="floracast-firestore"

BODY = {
    "jobName": "fetch-protected-areas-%s" % parser.date,
    "parameters": {
        "date" : parser.date,
        "data_location": "gs://floracast-datamining/protected_areas",
    },
    "environment": {
        "tempLocation": "gs://floracast-datamining/temp",
        # "stagingLocation": "gs://floracast-datamining/staging",
        "zone": "us-central1-f"
    }
}

request = service.projects().templates().launch(projectId=PROJECT, gcsPath=GCSPATH, body=BODY)
response = request.execute()