select s.id
from silverlijk.stations s 
join silverlijk.zones c on s.county=c.id
left join silverlijk.station_observation_count soc on s.id=soc.station_id
where c.State = 'FL'
AND (soc.observation_count is NULL OR soc.observation_count != 0)
