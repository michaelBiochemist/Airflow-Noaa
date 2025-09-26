Drop table if exists silverlijk.zones;

Create table Silverlijk.zones AS
with temp_record as (SELECT explode(features) as record from bronzey.zones)
SELECT 
  record.properties.id as ID,
  record.properties.name as name,
  record.properties.type,
  record.properties.awipsLocationIdentifier,
  record.properties.state
  , record.properties.forecastOffice
  , record.properties.forecastOffices
  , record.properties.gridIdentifier
  , record.properties.radarStation
  , record.properties.observationStations
  , record.geometry
  , record.properties.cwa

from temp_record;

SELECT count(1) From silverlijk.zones;
