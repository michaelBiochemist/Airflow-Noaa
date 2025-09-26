#!/usr/bin/env python

from airflow.models.variable import Variable
sql_http_path = Variable.get("DataBricks SQL HTTP Path")
connection_id='Databricks'
sql_server_hostname = Variable.get("DataBricks SQL Server Hostname")
db_jdbc_url = Variable.get("DataBricks JDBC URL")

from airflow.providers.databricks.operators.databricks_sql import
    DatabricksSqlOperator

create = DatabricksSqlOperator(
        databricks_conn_id=connection_id,
        sql_endpoint_name=sql_http_path,
        sql=["drop table if exists silverlijk.points",
             """"
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
  """
             )
