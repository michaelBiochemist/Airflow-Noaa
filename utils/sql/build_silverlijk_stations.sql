Drop table if exists silverlijk.stations;

Create table Silverlijk.stations AS
with temp_record as (SELECT explode(features) as record from bronzey.stations)
SELECT record.geometry
,record.properties.StationIdentifier as ID
, record.properties.name as name
, split(record.properties.forecast, '/')[5] as forecast
, split(record.properties.county, '/')[5] as county
, record.properties.elevation.unitCode elevationUnit
, record.properties.elevation.value elevationValue
 FROM temp_record
;
SELECT count(ID) From silverlijk.stations;
