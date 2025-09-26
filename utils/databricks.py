#!/usr/bin/env python
import time
import uuid

import requests
from airflow.models.variable import Variable
from requests.exceptions import HTTPError

# import base64
# from airflow import AirflowException

base_path = "/Volumes/workspace/default/noaa/"
base_url = "{databricks_instance_url}/api/2.0"
databricks_email = Variable.get("Databricks Email")
databricks_instance_url = Variable.get("DataBricks Instance URL")
databricks_token = Variable.get("DataBricks Token")
job_ids = {"load_bronze_tables": 28401716075112}
jobs_url = "{databricks_instance_url}/api/2.2"

headers = {
    "Authorization": f"Bearer {databricks_token}",
    "Content-Type": "application/json",
}


def upload_noaa_file(sub_path, filename, filedat):
    contents = bytes(filedat, "UTF-8")
    r = handle(
        requests.put(
            f"{base_url}/fs/files{base_path}{sub_path}/{filename}",
            headers=headers,
            data=contents,
        )
    )
    print(f"Status Code for {sub_path}/{filename}: {r.status_code}, {r.content}")


def handle(response, category=""):
    if response.status_code not in (200, 204):
        raise HTTPError(
            f"Error with request {category}, with a status code of {response.status_code}.\nContent is:\n{response.content}"
        )
    else:
        return response


def get_directory(directory):
    r = requests.get(
        f"{base_url}/fs/directories{base_path}/{directory}", headers=headers
    )
    if "application/json" in r.headers["content-type"]:
        j = r.json()
        if r.status_code == 200:
            return j
    elif "error_code" in j.keys():
        if j["error_code"] == "NOT_FOUND":
            return None
        else:
            raise RuntimeError(
                f"Request failed with status code of {r.status_code} and content-type {r.headers['content-type']}. Contents:\n{r.content}"
            )
    else:
        raise RuntimeError(
            f"Request failed with status code of {r.status_code} and content-type {r.headers['content-type']}. Contents:\n{r.content}"
        )


def make_directory(directory):
    r = requests.put(
        f"{base_url}/fs/directories{base_path}/{directory}", headers=headers
    )
    return r


def clear_file(file_path):
    r = requests.delete(f"{base_url}/fs/files{file_path}", headers=headers)
    if r.status_code == 404:
        print(f'Warning: deleted file path "{file_path}" not found.')


def clear_directory(directory, keep_base=True):
    D = get_directory(directory)
    if D is None:
        print(f"Directory {directory} does not exist. Creating directory...")
        return make_directory(directory).content
    elif D == {}:
        print(f"Directory {directory} is already empty.")
        return 0
    for dir_entry in D["contents"]:
        if dir_entry["is_directory"]:
            clear_directory(dir_entry["path"], keep_base=False)
        else:
            clear_file(dir_entry["path"])
            print(f"clearing file {dir_entry['path']}")

    if not keep_base:
        if base_path in directory:
            r = requests.delete(
                f"{base_url}/fs/directories{directory}", headers=headers
            )
        else:
            r = requests.delete(
                f"{base_url}/fs/directories{base_path}/{directory}", headers=headers
            )
        print(f"status code: {r.status_code}\tfor directory: {directory}")
        print(r.content)


def trigger_job(job_id, job_params=None, itoken=str(uuid.uuid4())):
    # params = {"useday": "Tuesday (Edit Here)"}
    content = {
        "idempotency_token": itoken,
        "job_id": job_id,
        "job_parameters": job_params,
    }
    return handle(
        requests.post(f"{jobs_url}/jobs/run-now", headers=headers, json=content)
    ).json()


def list_jobs():
    return requests.get(f"{jobs_url}/jobs/list", headers=headers)


def get_job_run(run_id):
    return handle(
        requests.get(f"{jobs_url}/jobs/runs/get?run_id={run_id}", headers=headers)
    )


def wait_until_finished(run_id):
    while True:
        r = handle(get_job_run(run_id)).json()
        if r["status"]["state"] not in ("QUEUED", "PENDING", "RUNNING", "WAITING"):
            return r
        time.sleep(30)


# job_run['state']['life_cycle_state']
job_noaapi = {
    "access_control_list": [
        {"user_name": databricks_email, "permission_level": "IS_OWNER"}
    ],
    "email_notifications": {
        "on_start": [databricks_email],
        "on_failure": [databricks_email],
        "on_success": [databricks_email],
    },
    "environments": [
        {
            "environment_key": "NOAAEnv2",
            "spec": {"environment_version": "3", "dependencies": ["noaapi"]},
        }
    ],
    "run_name": "NOAA API TEST",
    "tasks": [
        {
            "description": "Grabs NOAA weather data and stores it -- or just fails because this IP is blacklisted.",
            "environment_key": "NOAAEnv2",
            "task_key": str(uuid.uuid4()),
            "python_wheel_task": {
                "entry_point": "run",
                "parameters": [
                    "alerts",
                    "--target",
                    f"file:/Workspace/Users/{databricks_email}/DataEng Training/NOAA",
                ],
                "package_name": "noaapi",
            },
        }
    ],
}
