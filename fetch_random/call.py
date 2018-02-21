from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials
import argparse

credentials = GoogleCredentials.get_application_default()
service = build('dataflow', 'v1b3', credentials=credentials)

# for setting up chron job:
# http://zablo.net/blog/post/python-apache-beam-google-dataflow-cron
# Should be able to port this as a task.

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--template', type=str, required=True, help='the template_id to use')
parser.add_argument('--count', type=str, required=True, help='Number of random areas to fetch.')
args = parser.parse_args()

GCSPATH="gs://floracast-datamining/templates/fetch_random_areas/%s" % args.template
PROJECT="floracast-firestore"

BODY = {
    "jobName": "fetch-random-areas",
    "parameters": {
        "random_area_count" : args.count,
        "output_location": "gs://floracast-datamining/random_areas",
    },
    "environment": {
        "tempLocation": "gs://floracast-datamining/temp",
        # "stagingLocation": "gs://floracast-datamining/staging",
        "zone": "us-central1-f"
    }
}

request = service.projects().templates().launch(projectId=PROJECT, gcsPath=GCSPATH, body=BODY)
response = request.execute()