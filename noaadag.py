import json
import os
from datetime import datetime, timedelta

from airflow import DAG, AirflowException
from airflow.contrib.operators import gcs_delete_operator
from airflow.decorators import task
from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks_sql import \
    DatabricksSqlOperator
from airflow.utils.task_group import TaskGroup

from utils import databricks, google, noaapi

sql_http_path = Variable.get("DataBricks SQL HTTP Path")
db_connection_id = "Databricks"
sql_server_hostname = Variable.get("DataBricks SQL Server Hostname")
db_jdbc_url = Variable.get("DataBricks JDBC URL")

with DAG(
    dag_id="NOAA",
    start_date=datetime(2021, 1, 1),
    schedule=timedelta(days=1),
    catchup=False,
) as noaa_dag:
    endpoint_list = ["zones", "alerts", "stations"]

    # task1 = PythonOperator(task_id="check_gcloud_folders",python_callable=google.check_folders, op_kwargs={"endpoint_list": endpoint_list })
    def grab_and_load_endpoint(endpoint, ti):
        taskdate = ti.dag_run.start_date.strftime("%Y%m%d-%H%m")
        collection = noaapi.paged_endpoint(endpoint)
        for i, j in enumerate(collection):
            contents = json.dumps(j)
            google.write_file(f"{endpoint}/{endpoint}_{taskdate}_{i}.json", contents)
            databricks.upload_noaa_file(endpoint, f"{taskdate}_{i}.json", contents)

    @task(task_id="download")
    def grab_and_save_endpoint(endpoint, ti):
        taskdate = ti.dag_run.start_date.strftime("%Y%m%d-%H%m")
        collection = noaapi.paged_endpoint(endpoint)
        filename = f"/tmp/{endpoint}_{taskdate}.json"
        with open(filename, "w") as saveme:
            json.dump(collection, saveme)
        return filename

    @task(task_id="clear_databricks")
    def clear_databricks(endpoint):
        print(f"endpoint is {endpoint}")
        databricks.clear_directory(endpoint)

    @task(task_id="upload_google")
    def load_to_google(filename, ti):
        taskdate = ti.dag_run.start_date.strftime("%Y%m%d-%H%m")
        with open(filename) as send:
            google.write_file(f"{endpoint}/{endpoint}_{taskdate}.json", send.read())

    @task(task_id="upload_databricks")
    def load_to_databricks(filename, endpoint, ti):
        taskdate = ti.dag_run.start_date.strftime("%Y%m%d-%H%m")
        with open(filename) as F:
            fstr = F.read()
            print(len(fstr))
            collection = json.loads(fstr)
            print(f"Collection length for file {filename} is: {len(collection)}")
            for i, j in enumerate(collection):
                contents = json.dumps(j)
                databricks.upload_noaa_file(endpoint, f"{taskdate}_{i}.json", contents)

    @task(task_id="delete_temporary_storage")
    def delete_temporary_storage():
        print(os.listdir("/tmp"))
        for filename in filter(lambda x: x[-4:] == "json", os.listdir("/tmp")):
            os.remove(f"/tmp/{filename}")

    @task(task_id="load_bronze_tables")
    def load_bronze_tables(ti, custom_params={}, custom_hash=""):
        taskday = ti.dag_run.start_date.strftime("%Y%m%d")
        taskdate = ti.dag_run.start_date.strftime("%Y%m%d-%H%m")
        custom_params["useday"] = taskday
        job_run = databricks.trigger_job(
            databricks.job_ids["load_bronze_tables"],
            job_params=custom_params,
            itoken=hash(f"{ti.task_id}-{ti.dag_id}-{taskdate}{custom_hash}"),
        )
        final_run_details = databricks.wait_until_finished(job_run["run_id"])
        if final_run_details["status"]["termination_details"]["code"] != "SUCCESS":
            raise AirflowException(
                f"Exception in databricks task. Job run state is as follows:\n{final_run_details['status']}"
            )

    @task(task_id="get_station_observations")
    def get_station_observations(ti, ds):
        print(os.listdir("/tmp/"))
        taskdate = ti.dag_run.start_date.strftime("%Y%m%d-%H%m")
        input_path = f"/tmp/florida-stations-{ds}.json"

        with open(input_path) as F:
            station_list = json.load(F)
        print("Station list first row:")
        print(station_list[0])
        observation_list = noaapi.get_observations_distributed(station_list)
        observations = [
            x.pop("observations") for x in observation_list if "observations" in x
        ]
        # observations = list(filter(None, map(lambda x:x.pop('observations', None),observation_list)))
        print(observation_list[0:5])
        with open(f"/tmp/observations-{taskdate}", "w") as F:
            json.dump(observations, F)
        with open(f"/tmp/observation_counts-{taskdate}", "w") as G:
            json.dump(observation_list, G)
        return taskdate

    @task(task_id="load_observations_to_databricks")
    def load_observations(ti):
        taskdate = ti.dag_run.start_date.strftime("%Y%m%d-%H%m")
        collection = json.loads()

    @task(task_id="load_observation_counts_to_databricks")
    def load_observation_counts(ti):
        taskdate = ti.dag_run.start_date.strftime("%Y%m%d-%H%m")
        with open(f"/tmp/observation_counts-{taskdate}", "r") as G:
            databricks.upload_noaa_file(
                "observation_counts", f"{taskdate}.json", G.read()
            )

    with TaskGroup(group_id="Build_Silverlijk_Sql") as LIJK:
        check = DatabricksSqlOperator(
            task_id="check_schema",
            databricks_conn_id=db_connection_id,
            sql="Select current_schema()",
        )
        points = DatabricksSqlOperator(
            task_id="build_and_load_points_table",
            databricks_conn_id=db_connection_id,
            sql=[
                "drop table if exists silverlijk.points",
                """
CREATE TABLE silverlijk.points (
  point_key string primary key,
  geometry_id string, 
  point_id int,
  source_table string,
  x double,
  y double
)
             """,
                """
INSERT INTO silverlijk.points(point_key, geometry_id, point_id, x,y, source_table) 
with temp_stations as (Select explode(features) as record from bronzey.stations),
  temp_geometry as ( select record.properties.stationIdentifier as id, 
  get(record.geometry.coordinates,0) as x, get(record.geometry.coordinates, 1) as y
  from temp_stations )
  SELECT concat(id,'-',string(monotonically_increasing_id()),'-','stations') as point_key,
  id, monotonically_increasing_id() as point_id, x, y, 'stations' as source_table from temp_geometry
             """,
                """
INSERT INTO silverlijk.points(point_key, geometry_id, point_id, x,y, source_table) 
with temp_alerts as (Select explode(features) as record from bronzey.alerts),
  temp_geometry as ( select record.properties.id as id, explode(record.geometry.coordinates) as coordinates  from temp_alerts where record.geometry.type='Polygon'), 
  temp_point as (
    select id as id, explode(coordinates) as coordinates from temp_geometry
  ) 
  select concat(id,'-',string(monotonically_increasing_id()),'-','alerts') as point_key,
  id, monotonically_increasing_id() as point_id, get(coordinates,0) as x, get(coordinates,1) y, 'alerts' as source_table
  From temp_point
  """,
            ],
        )
        alerts = DatabricksSqlOperator(
            task_id="silver_alerts",
            databricks_conn_id=db_connection_id,
            sql="utils/sql/build_silverlijk_alerts.sql",
        )
        stations = DatabricksSqlOperator(
            task_id="silver_stations",
            databricks_conn_id=db_connection_id,
            sql="utils/sql/build_silverlijk_stations.sql",
        )
        zones = DatabricksSqlOperator(
            task_id="silver_zones",
            databricks_conn_id=db_connection_id,
            sql="utils/sql/build_silverlijk_zones.sql",
        )

    with TaskGroup(group_id="Grab_and_Upload_Data") as TG0:
        for endpoint in endpoint_list:
            with TaskGroup(group_id=endpoint) as TG:
                clear_google = gcs_delete_operator.GoogleCloudStorageDeleteOperator(
                    bucket_name=google.bucket_name,
                    prefix=endpoint,
                    task_id="clear_google",
                    gcp_conn_id="Google",
                )

                delete_databricks = clear_databricks(endpoint)
                filename = grab_and_save_endpoint(endpoint)
                load_google = load_to_google(filename)
                load_google << clear_google
                load_databricks = load_to_databricks(filename, endpoint)
                load_databricks << delete_databricks

    with TaskGroup(group_id="Florida_Stations") as TGF:
        get_florida_stations = DatabricksSqlOperator(
            task_id="get_florida_stations",
            databricks_conn_id=db_connection_id,
            sql="utils/sql/get_florida_stations.sql",
            output_path="/tmp/florida-stations-{{ ds }}.json",
            output_format="json",
        )
        observations_date = get_station_observations()
        observations_date << get_florida_stations
        clear_counts = clear_databricks("observation_counts")
        clear_observations = clear_databricks("observations")
        db_upload = load_to_databricks(
            f"/tmp/observations-{observations_date}", "observations"
        )
        upload_counts = load_observation_counts()
        bronze_observations = load_bronze_tables(
            custom_params={"directories": "observations"}, custom_hash="observations"
        )
        bronze_observation_counts = load_bronze_tables(
            custom_params={"directories": "observation_counts"},
            custom_hash="observation_counts",
        )

        silverlijk_observations = DatabricksSqlOperator(
            task_id="silverlijk_observations",
            databricks_conn_id=db_connection_id,
            sql="utils/sql/append_silverlijk_observations.sql",
        )
        silverlijk_observation_counts = DatabricksSqlOperator(
            task_id="silverlijk_observation_counts",
            databricks_conn_id=db_connection_id,
            sql="utils/sql/update_silverlijk_station_observation_counts.sql",
        )
        db_upload << clear_observations
        db_upload << observations_date
        upload_counts << clear_counts
        upload_counts << observations_date
        bronze_observations << db_upload
        bronze_observation_counts << upload_counts
        silverlijk_observations << bronze_observations
        silverlijk_observation_counts << bronze_observation_counts
    delete_all = delete_temporary_storage()
    load_bronze = load_bronze_tables()
    # delete_all << TG0
    TG0 << delete_all
    load_bronze << TG0
    LIJK << load_bronze
    TGF << LIJK
