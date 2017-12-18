from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials
import argparse
import sys
from fetch_shared import utils, gcs
from datetime import datetime

credentials = GoogleCredentials.get_application_default()
service = build('dataflow', 'v1b3', credentials=credentials)
PROJECT = utils.default_project()
# for setting up chron job:
# http://zablo.net/blog/post/python-apache-beam-google-dataflow-cron
# Should be able to port this as a task.

# Set the following variables to your values.

parser = argparse.ArgumentParser()
parser.add_argument('--template', type=str, required=False, help='The template_id in GCS to use')
parser.add_argument('--bucket', type=str, required=True, help='GCS bucket')
parser.add_argument('--taxa', type=str, required=True, help='Date (YYYYMMDD) on which to fetch areas')
args = parser.parse_args()

if args.template is None:
    TEMPLATE = gcs.fetch_latest(PROJECT, args.bucket, "templates/fetch_occurrences")
else:
    TEMPLATE = args.template

print("template", TEMPLATE)

if len(args.taxa) == 0:
    sys.exit("Must inlcude at least one taxa")

BODY = {
    "jobName": "fetch_occurrences-%s" % args.taxa,
    "parameters": {
        "taxa" : args.taxa,
        "data_location": "gs://%s/occurrences/%s/%s/" % (args.bucket, args.taxa, datetime.now().strftime("%s")),
    },
    "environment": {
        "tempLocation": "gs://%s/temp" % args.bucket,
        # "stagingLocation": "gs://floracast-datamining/staging",
        "zone": "us-central1-f"
    }
}

request = service.projects().templates().launch(projectId=PROJECT, gcsPath=TEMPLATE, body=BODY)
response = request.execute()
print(response)