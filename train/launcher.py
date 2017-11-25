import argparse
from fetch_shared import utils, gcs
from datetime import datetime


parser = argparse.ArgumentParser()
parser.add_argument('--bucket', type=str, required=True)
parser.add_argument('--taxon', type=str, required=True)
args = parser.parse_args()

_project = utils.default_project()

transformed = gcs.fetch_latest(_project, args.bucket, "transformed/%s" % args.taxon)

model = "gs://%s/models/%s/%s" % (args.bucket, args.taxon, datetime.now().strftime("%s"))

print("./train.sh %s %s" % (transformed, model))