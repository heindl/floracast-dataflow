from predict import download_gcs_directory_to_temp
from google.cloud import storage
import exceptions

client = storage.client.Client(project="floracast-firestore")
# try:
bucket = client.get_bucket("floracast-datamining")
# except exceptions.NotFound:
#     print('Sorry, that bucket does not exist!')
#     exit()

model_path = "gs://floracast-datamining/transformed/58682/1511615426"

print(download_gcs_directory_to_temp(bucket, model_path))