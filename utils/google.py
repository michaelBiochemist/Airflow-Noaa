#!/usr/bin/env python

from airflow.io.path import ObjectStoragePath
from airflow.models.variable import Variable

bucket_name = Variable.get("Google Bucket")
object_storage_path = f"gs://Google@{bucket_name}/"
gcs_path = ObjectStoragePath(object_storage_path)


def check_folders(endpoint_list):
    for endpoint in endpoint_list:
        folder = gcs_path / endpoint
        folder.mkdir(exist_ok=True)


def write_file(filename, content):
    filey = gcs_path / filename
    filey.write_text(content)
