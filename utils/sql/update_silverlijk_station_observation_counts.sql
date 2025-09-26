MERGE INTO silverlijk.station_observation_count AS soc 
    USING bronzey.observation_counts AS oc 
    ON soc.station_id = oc.station_id 
    WHEN MATCHED THEN UPDATE 
        SET soc.observation_count = oc.observation_count;

INSERT INTO silverlijk.station_observation_count 
SELECT oc.station_id, oc.observation_count 
FROM bronzey.observation_counts oc 
LEFT ANTI JOIN silverlijk.station_observation_count soc 
    ON oc.station_id = soc.station_id;
