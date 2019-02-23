---------------------------------------Análise Shapefile Cliente-----------------------------------------------------
--------------------------------------------------------------------------------------------------------------

select * from playpen_analyst.puebla_zone_new_4326 limit 10;
select * from playpen_analyst5.national_municipal where nom_mun in ('Puebla');

drop table if exists passo_1;
create temp table passo_1 as select 
        B.nom_mun,
	A.geom
from playpen_analyst.puebla_zone_new_4326  A,
     playpen_analyst5.national_municipal B
where st_contains (B.geom,st_centroid(A.geom));


drop table if exists playpen_analyst.puebla_zone_new_4326_dados;
create table playpen_analyst.puebla_zone_new_4326_dados  as select
	row_number() over() as zone_id,
	nom_mun,
	sum(pob1) as pop,
	round((st_area(A.geom::geography)* 0.000001)::numeric,2) as sq_km,
	st_buffer(A.geom,0.0) as geom
from passo_1 A,
     playpen_analyst.national_ageb_urb B
where st_contains(A.geom,st_centroid(B.geom)) 
group by nom_mun,sq_km,A.geom;


select * from playpen_analyst.puebla_zone_new_4326_dados order by sq_km asc;
select * from playpen_analyst.puebla_zone_new_4326_dados order by pop asc;
create table playpen_analyst.geom_faltante as select distinct geom from passo_1 where geom not in (select distinct geom from playpen_analyst.puebla_zone_new_4326_dados);


     




     