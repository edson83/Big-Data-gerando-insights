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

