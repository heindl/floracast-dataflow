from google.cloud import storage, exceptions
from os import path


def WriteTFRecords(filepath, records):
    self.response.write('Creating file %s\n' % filename)

    write_retry_params = gcs.RetryParams(backoff_factor=1.1)
    gcs_file = gcs.open(filename,
                        'w',
                        content_type='text/plain',
                        options={'x-goog-meta-foo': 'foo',
                                 'x-goog-meta-bar': 'bar'},
                        retry_params=write_retry_params)
    gcs_file.write('abcde\n')
    gcs_file.write('f'*1024*4 + '\n')
    gcs_file.close()
    self.tmp_filenames_to_clean_up.append(filename)

def fetch_latest(project, bucket_name, parent_path):

    if parent_path.startswith("/"):
        parent_path = parent_path[1:]

    if parent_path.endswith("/"):
        parent_path = parent_path[:-1]

    client = storage.client.Client(project=project)
    try:
        bucket = client.get_bucket(bucket_name)
    except exceptions.NotFound:
        print('Sorry, that bucket does not exist!')
        return
    names = []
    # Note that paging should be done behind the scenes.
    # https://stackoverflow.com/questions/43147339/how-does-paging-work-in-the-list-blobs-function-in-google-cloud-storage-python-c
    for blob in bucket.list_blobs(prefix=parent_path, fields="items/name"):
        # print("name", blob.name)
        if blob.name == parent_path:
            continue
        name = blob.name[len(parent_path)+1:]
        s = name.split("/")
        names.append(path.join(parent_path, s[0]))

    names = sorted(names)

    if len(names) == 0:
        raise ValueError('No latest version found within folder on gcs with parent: %s', parent_path)

    return path.join("gs://", bucket_name, names[len(names)-1]).encode('utf-8')

# result = fetch_latest("firebase-firestore", "floracast-datamining", "occurrences/58682")
# print("result", result)