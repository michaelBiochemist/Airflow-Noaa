Drop table if exists silverlijk.alerts;

Create table Silverlijk.alerts AS
with temp_record as (SELECT explode(features) as record from bronzey.alerts)
SELECT record.properties.id as ID,
record.properties.affectedZones, record.properties.category,
record.properties.areaDesc, record.properties.certainty, record.properties.code,
record.properties.description, record.properties.event, 
record.properties.onset,
record.properties.effective,
record.properties.ends,
record.properties.geocode,
record.properties.instruction,
record.properties.replacedAt,
record.properties.replacedBy,
record.properties.scope,
record.properties.response,
record.properties.sender,
record.properties.senderName,
record.properties.sent,
record.properties.severity,
record.properties.urgency,
record.properties.status
, record.geometry, record.properties.parameters
 FROM temp_record

;
SELECT count(ID) From silverlijk.alerts;
