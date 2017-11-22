from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials

credentials = GoogleCredentials.get_application_default()
service = build('dataflow', 'v1b3', credentials=credentials)

# for setting up chron job:
# http://zablo.net/blog/post/python-apache-beam-google-dataflow-cron
# Should be able to port this as a task.

# Set the following variables to your values.
DATE="20170102"
TEMPLATE_ID="1511317193"

GCSPATH="gs://floracast-datamining/templates/fetch_protected_areas/%s" % TEMPLATE_ID
PROJECT="floracast-firestore"

BODY = {
    "jobName": "fetch-protected-areas-%s" % DATE,
    "parameters": {
        "date" : DATE,
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