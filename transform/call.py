from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials
import argparse
from fetch_shared import gcs, utils

credentials = GoogleCredentials.get_application_default()
service = build('dataflow', 'v1b3', credentials=credentials)
project = utils.default_project()

# for setting up chron job:
# http://zablo.net/blog/post/python-apache-beam-google-dataflow-cron
# Should be able to port this as a task.

# All you need is a taxon and bucket.
# Occurrence and random fetches should have already been called.

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--bucket', type=str, required=True, help='the bucket to use')
parser.add_argument('--template', type=str, required=False, help='the template_id to use')
parser.add_argument('--taxon', type=str, required=True, help='the taxon for which to transform occurrence data')
args = parser.parse_args()

if args.template == "":
    template = gcs.fetch_latest(project, args.bucket, "templates/transform")
else:
    template = args.template

occurrences = gcs.fetch_latest(project, args.bucket, "occurrences/"+args.taxon)
random = gcs.fetch_latest(project, args.bucket, "random")

BODY = {
    "jobName": "transform",
    "parameters": {
        "occurrence_location" : occurrences,
        "random_location": random,
        "output_location": "gs://%s/transformed" % args.bucket,
    },
    "environment": {
        "tempLocation": "gs://%s/temp",
        # "stagingLocation": "gs://floracast-datamining/staging",
        "zone": "us-central1-f"
    }
}

request = service.projects().templates().launch(projectId=project, gcsPath=template, body=BODY)
response = request.execute()