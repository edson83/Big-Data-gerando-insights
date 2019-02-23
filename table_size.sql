SELECT table_name  FROM information_schema.tables WHERE table_schema = 'playpen_analyst' order by table_name;

SELECT schema_name, 
       pg_size_pretty(sum(table_size)::bigint),
       (sum(table_size) / pg_database_size(current_database())) * 100
FROM (
  SELECT pg_catalog.pg_namespace.nspname as schema_name,
         pg_relation_size(pg_catalog.pg_class.oid) as table_size
  FROM   pg_catalog.pg_class
     JOIN pg_catalog.pg_namespace ON relnamespace = pg_catalog.pg_namespace.oid
) t
GROUP BY schema_name
ORDER BY schema_name


select table_schema, table_name, pg_relation_size(table_schema||'.'||table_name)
from information_schema.tables where table_schema = 'playpen_analyst'
order by pg_relation_size desc;

drop table playpen_analyst.packetdata;
select max(date_dt), min(date_dt) from cip.dwell;
select count (1) from playpen_analyst.packetdata;



drop table  playpen_analyst.ec_farma_define_home_work_1;
drop table  playpen_analyst.ec_farma_define_home_work_2;
drop table  playpen_analyst.ec_farma_define_home_work_3;
drop table  playpen_analyst.ec_farma_define_home_work_4;
drop table  playpen_analyst.ec_farma_define_home_work_5;
drop table  playpen_analyst.ec_farma_define_home_work_6;
drop table  playpen_analyst.ec_farma_define_home_work_7;
drop table  playpen_analyst.ec_farma_define_home_work_8;
drop table  playpen_analyst.ec_farma_define_home_work_9;
drop table  playpen_analyst.ec_farma_distribuicao_home;
drop table  playpen_analyst.ec_farma_distribuicao_work;
drop table  playpen_analyst.ec_farma_intersecta_shp_location;
drop table  playpen_analyst.ec_farma_pois;
drop table  playpen_analyst.ec_farma_pois_v2;
drop table  playpen_analyst.ec_farma_pois_v3;
drop table  playpen_analyst.ec_farma_shp;
drop table  playpen_analyst.ec_farma_shp_02;
drop table  playpen_analyst.ec_farma_ulm01_dwell_location;
drop table  playpen_analyst.ec_farma_ulm04_loc_visits;
drop table  playpen_analyst.ec_matrix_road;
drop table  playpen_analyst.ec_matrix_road2;
drop table  playpen_analyst.ec_stats_dwell_distinct_location;
drop table  playpen_analyst.ec_stats_dwell_distinct_user_by_date;
drop table  playpen_analyst.ec_stats_dwell_range_date;
drop table  playpen_analyst.ec_stats_location;
drop table  playpen_analyst.ec_zones;
drop table  playpen_analyst.jb_ec_matrix_road;
drop table  playpen_analyst.jb_lookup_start_hr_period;
drop table  playpen_analyst.jb_od_ec_weekday;
drop table  playpen_analyst.jb_od_ec_weekday_2;
drop table  playpen_analyst.jb_od_ec_weekday_2_sub;
drop table  playpen_analyst.jb_od_ec_weekday_3;
drop table  playpen_analyst.jb_od_ec_weekday_3_sub;
drop table  playpen_analyst.jb_od_ec_weekday_4;
drop table  playpen_analyst.jb_od_ec_weekday_4_sub;


