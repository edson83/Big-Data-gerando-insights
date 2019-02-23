

-----------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------NATURA---------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------


--Step 1 -Verifica base das zonas de análise (gerada no query zoneamento)
------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------

select count(1) from playpen_analyst.nat_ii_voronoi; --4328
select * from playpen_analyst.nat_ii_voronoi;

-- Adiciona informação de população e área do shapefile. Corrige problemas de geometria que acusam log de erro de topology exeption

--Filtra censo só para municipios de interesse
drop table if exists playpen_analyst.census_map_dedup_nat;
create table playpen_analyst.census_map_dedup_nat as select 
	A.*
from playpen_analyst3.census_map_dedup A inner join playpen_analyst.nat_II_passo_2 B on A.cd_geocodm = B.codigo_ibge;


drop table if exists playpen_analyst.nat_ii_voronoi_v2;
create table playpen_analyst.nat_ii_voronoi_v2 as select
	sum(population) as pop,
	st_area(A.geom::geography)* 0.000001 as sq_km,
	st_buffer(A.geom,0.0) as geom 
from playpen_analyst.nat_ii_voronoi  A,
     playpen_analyst3.census_map_dedup_nat B
where st_contains(A.geom,st_centroid(B.geom)) is true
group by sq_km,A.geom;


drop table if exists playpen_analyst.nat_ii_voronoi_v3;
create table playpen_analyst.nat_ii_voronoi_v3 as select
	1::numeric as pop,
	st_area(A.geom::geography)* 0.000001 as sq_km,
	st_buffer(A.geom,0.0) as geom 
from playpen_analyst.nat_ii_voronoi  A
where A.geom not in (select distinct geom from playpen_analyst.nat_ii_voronoi_v2);


drop table if exists playpen_analyst.nat_ii_voronoi_v4;
create table playpen_analyst.nat_ii_voronoi_v4 as select 
	* 
from playpen_analyst.nat_ii_voronoi_v2 
union select 
	* 
from playpen_analyst.nat_ii_voronoi_v3;


select count(distinct geom) from playpen_analyst.nat_ii_voronoi_v4; --4328
select * from playpen_analyst.nat_ii_voronoi_v4 order by pop;


drop table if exists playpen_analyst.nat_ii_voronoi_v5;
create table playpen_analyst.nat_ii_voronoi_v5 as select
        row_number () over () as id,
	*
from (select
        A.pop as populacao,
        A.sq_km as area,
        B.nome as cidade,
	B.uf,
	B.regiao,	
	row_number() over(partition by A.geom order by (ST_Area(ST_Intersection(A.geom,B.geom))/ST_Area(A.geom)) desc) as rank,
	A.geom
from playpen_analyst.nat_ii_voronoi_v4 A,
     playpen_analyst.nat_II_passo_2 B

      )C
where rank = 1;

select count(distinct geom) from playpen_analyst.nat_ii_voronoi_v5;
select id, populacao,area,cidade,uf,regiao,rank from playpen_analyst.nat_ii_voronoi_v5;

select * from playpen_analyst.census_map_dedup_nat limit 100;

--Verifica se toda população das regioes esta contemplada
select regiao,sum (populacao) from playpen_analyst.nat_ii_voronoi_v5 group by regiao order by regiao;

--Compara com a populacao da tabela de municipios:
--RM Belo Horizonte        5.414.701  --  5.389.181
--RM Brasilia              3.724.181  --  3.703.380
--RM Rio de Janeiro        11.835.708 --  11.712.037
--RM Curitiba              3.223.836  --  3.206.164 
--Interior RJ              2.719.505  --  2.692.254
--Interior SP              13.425.404 --  13.333.217



--Step 2 -Cria tabela com os locations que de fato são utilizados para dwells
------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------

--2. create table of locations that are actually used by dwells
--(1 min)
drop table if exists playpen_analyst.br_ulm01_dwell_location_oct16;
create table playpen_analyst.br_ulm01_dwell_location_oct16 as 
        SELECT DISTINCT location_id
        from            cip.dwell d
        where           location_id <> 0
        and             date_dt between '20161001' and '20161031';
        
alter table     playpen_analyst.br_ulm01_dwell_location_oct16 add primary key (location_id);

--2.1 add columns to the table. Done as separate step to reduce processing time.
alter table playpen_analyst.br_ulm01_dwell_location_oct16
       add column centroid_geo_wgs84 geometry,
       add column location_geo_wgs84 geometry,
       add column centroid_lat_wgs84 numeric,
       add column centroid_lon_wgs84 numeric,
       add column length_x_m int8;

--2.2 copy the geometry field from cip.location to speed query time later on      
update  playpen_analyst.br_ulm01_dwell_location_oct16
set     centroid_geo_wgs84 = l.centroid_geo_wgs84, 
        location_geo_wgs84 = l.location_geo_wgs84,
        centroid_lon_wgs84 = l.centroid_lon_wgs84, 
        centroid_lat_wgs84 = l.centroid_lat_wgs84,
        length_x_m         = l.length_x_m
from    cip.location l
where   l.location_id = br_ulm01_dwell_location_oct16.location_id;

CREATE INDEX ulm01_location_geo_gix_oct16 ON playpen_analyst.br_ulm01_dwell_location_oct16 USING GIST (location_geo_wgs84);
CREATE INDEX ulm01_centroid_geo_gix_oct16 ON playpen_analyst.br_ulm01_dwell_location_oct16 USING GIST (centroid_geo_wgs84);

select count(1) from playpen_analyst.br_ulm01_dwell_location_oct16; --31227

--Step 3 - Cria depara entre os grids do Smart steps e zonas de análise 
--Cálculo de fatores separados para levar em consideração as razões de área e densidade populacional
--------------------------------------------------------------------------------------

select * from playpen_analyst.nat_II_passo_9 limit 100;

--Calcula razão de área entre os grids do smart steps e as zonas de análise
drop table if exists playpen_analyst.nat_II_passo_9;
create table playpen_analyst.nat_II_passo_9 as Select distinct
        id,
	location_id,
	ST_Area(ST_Intersection(geom,location_geo_wgs84))/ST_Area(location_geo_wgs84) as perc_area, 
	ST_intersection(geom,location_geo_wgs84) as geom
from playpen_analyst.nat_ii_voronoi_v3 A, 
     playpen_analyst.br_ulm01_dwell_location_oct16 B
where ST_Intersects(geom, location_geo_wgs84);

--Verifica se os grids estão sendo integralmente distribuídos no quesito área (alguns poucos podem não estar devido a regiões de fronteira)
select sum(perc_area) as total from playpen_analyst.nat_II_passo_9 group by location_id order by total;

--Verifica população residente nas áreas de intersecção dos grids smart steps com as zonas de análise (necessário para o cálculo do fator de densidade populacional)
drop table if exists playpen_analyst.nat_II_passo_10;
create table playpen_analyst.nat_II_passo_10 as Select distinct
        id,
	location_id,
	B.geom,
	sum((ST_Area(ST_Intersection(A.geom,B.geom))/ST_Area(A.geom))* population) as population	
from playpen_analyst.census_map_dedup_nat A, 
     playpen_analyst.nat_II_passo_9 B
where ST_Intersects(A.geom,st_buffer(B.geom, 0.0)) 
group by id, location_id,B.geom;



--Calcula fator combinado olhando área e densidade populacional
--Arbitrei 66% do peso para a densidade populacional e 33% para a área

drop table if exists playpen_analyst.nat_II_passo_11;
create table playpen_analyst.nat_II_passo_11 as Select distinct
        A.id,
	A.location_id,
	A.perc_area, 
	(B.population/C.total_population) as perc_population,
        (perc_area) + (2*(B.population/C.total_population)) as perc_total
from playpen_analyst.nat_II_passo_9 A 
left join playpen_analyst.nat_II_passo_10 B on A.id = B.id and  A.location_id = B.location_id
left join (select location_id, sum(population) as total_population from playpen_analyst.nat_II_passo_10 group by location_id) C on A.location_id = C.location_id;


drop table if exists playpen_analyst.nat_II_passo_12;
create table playpen_analyst.nat_II_passo_12 as Select distinct
        A.id,
	A.location_id,
	A.perc_total/B.perc_location as perc
from playpen_analyst.nat_II_passo_11 A 
left join (select location_id, sum(perc_total) as perc_location from playpen_analyst.nat_II_passo_11 group by location_id) B  on A.location_id = B.location_id;


select location_id, sum(perc) as total  from playpen_analyst.nat_II_passo_12 group by location_id order by total;
select count(distinct id) from playpen_analyst.nat_II_passo_12;
--4307


--Step 3 - Marca local de casa e trabalho de toda a base 
------------------------------------------------------------

select * from playpen_analyst.br_ulm04_loc_visits_oct16 limit 100;

drop table if exists playpen_analyst.br_ulm04_loc_visits_oct16;
create table playpen_analyst.br_ulm04_loc_visits_oct16 AS 
select  d.user_id,
        d.location_id,
        count(1)::int2 visits,
--home: weekdays before 7am or after 9pm and weekends before 9am or after 10pm
        (
        SUM (CASE WHEN EXTRACT(isodow from d.start_dt) <=5 and (extract(hour from d.start_dt) <=6 or extract(hour from d.start_dt) >=21) THEN 1 ELSE 0 END)+
        SUM (CASE WHEN EXTRACT(isodow from d.start_dt) >=6 and (extract(hour from d.start_dt) <=8 or extract(hour from d.start_dt) >=22) THEN 1 ELSE 0 END)
        )::int2 nighttime_visits,
        (
        COUNT (DISTINCT CASE WHEN EXTRACT(isodow from d.start_dt) <=5 and (extract(hour from d.start_dt) <=6 or extract(hour from d.start_dt) >= 21) THEN d.date_dt ELSE NULL END)+
        COUNT (DISTINCT CASE WHEN EXTRACT(isodow from d.start_dt) >=6 and (extract(hour from d.start_dt) <=8 or extract(hour from d.start_dt) >= 22) THEN d.date_dt ELSE NULL END)
        )::int2 nighttime_days,
--work: weekdays between 9am and 4pm
        SUM (CASE WHEN (EXTRACT(isodow from d.start_dt) <=5 AND EXTRACT (hour from d.start_dt) between 9 and 15) THEN 1 ELSE 0 END)::int2 daytime_visits,
        COUNT (DISTINCT CASE WHEN (EXTRACT(isodow from d.start_dt) <=5 AND EXTRACT (hour from d.start_dt) between 9 and 15) THEN d.date_dt ELSE NULL END)::int2 daytime_days,
        0::int2 other_visits  --placeholder for next step
FROM    cip.dwell d

WHERE           d.location_id <> 0  and d.date_dt between '20161001' and '20161031'
GROUP BY        d.user_id, d.location_id;

select count(distinct user_id) from playpen_analyst.br_ulm04_loc_visits_oct16;

--4.1   set count of 'other' visits
--(2.5 min)
update  playpen_analyst.br_ulm04_loc_visits_oct16
set     other_visits = visits-nighttime_visits-daytime_visits;

create index    ulm04lv_user_id_loc_id_oct16 on playpen_analyst.AR_ulm04_loc_visits(user_id, location_id);
create index    ulm04lv_location_id_oct16 on playpen_analyst.AR_ulm04_loc_visits(location_id);


drop table if exists playpen_analyst.nat_II_passo_13;
create table playpen_analyst.nat_II_passo_13 AS select 
	*,
	ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY nighttime_days desc) RANK_home,
	ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY daytime_days desc) RANK_work
from playpen_analyst.br_ulm04_loc_visits_oct16;


drop table if exists playpen_analyst.nat_II_passo_14;
create table playpen_analyst.nat_II_passo_14 as select 
    A.user_id,
    A.location_home,
    A.daytime_days,
    B.location_work,
    B.nighttime_days
from (select user_id,location_id as location_home, daytime_days from playpen_analyst.nat_II_passo_13 where RANK_home=1) A
left join (select user_id,location_id as location_work, nighttime_days from playpen_analyst.nat_II_passo_13 where RANK_work=1) B on A.user_id = B.user_id;

select count(1) from playpen_analyst.nat_II_passo_14; --18.714.026
select count(distinct user_id) from playpen_analyst.nat_II_passo_14; --18.714.026
select * from playpen_analyst.nat_II_passo_14 where location_home is null;



--Step 4 - Adiciona informação de CRM a base do home/work 
---------------------------------------------------------

create table playpen_analyst.nat_II_passo_15 as select * from playpen_analyst.natura_passo_15;

drop table if exists playpen_analyst.nat_II_passo_15;
create table playpen_analyst.nat_II_passo_15 as select distinct 
    A.*,
    gender as genero,    
    case    when age :: numeric > 0 and  age ::    numeric < 1520 then 'ATE 20'         
            when age ::  numeric > 1520 and age :: numeric < 2529 then 'DE 20 a 30'        
            when age ::  numeric > 2529 and age :: numeric < 3540 then 'DE 30 a 40'          
            when age ::  numeric > 3540 and age :: numeric < 4550 then 'DE 40 a 50'        
            when age ::  numeric > 4550 then 'Mais 50'                                               
            else 'u' end as faixa_etaria,
     socio_economics_ob:: numeric as renda,
     ds_cluster
from playpen_analyst.nat_II_passo_14 A left join cip.crm_dynamic B on A.user_id = B.user_id
                                       left join playpen_analyst.crm_resumo1610 C on B.user_imsi_hash = C.hash;

select distinct age from cip.crm_dynamic;
                                       
--Retira duplicidade da base de CRM

drop table if exists playpen_analyst.nat_II_passo_16;
create table playpen_analyst.nat_II_passo_16 as select *
from (select *, row_number()over (partition by user_id) as rank from playpen_analyst.nat_II_passo_15)A
where rank = 1;

select count(1) from playpen_analyst.nat_II_passo_16;--18.714.026
select count(distinct user_id) from playpen_analyst.nat_II_passo_16;--18.714.026


--Calcula informação de renda de cada location_id para complementar a base de CRM acima
--Calculo realizado somente para os location_id das cidades de analise 
--Locations de fora das cidades de análise reberão o valor médio nacional

drop table if exists playpen_analyst.census_map_dedup_nat_rend;
create table playpen_analyst.census_map_dedup_nat_rend as select 
	A.*
from playpen_analyst3.censo_completo A inner join playpen_analyst.nat_II_passo_2 B on A.cd_geocodm = B.codigo_ibge;


drop table if exists playpen_analyst.nat_II_passo_17;
create table playpen_analyst.nat_II_passo_17 as Select distinct
	location_id,
	(sum(renda_med_resp * population) /sum (population)) as renda_med_resp	
from  playpen_analyst.br_ulm01_dwell_location_oct16 A, 
      playpen_analyst.census_map_dedup_nat_rend B
where ST_contains(A.location_geo_wgs84 ,st_centroid(B.geometry)) 
group by location_id;

select * from playpen_analyst.nat_II_passo_17 order by renda_med_resp;

--1194.57 é a renda média dos responsáveis dos domicílios no Brasil
select (sum(renda_med_resp * population) /sum (population))as renda_med_resp from playpen_analyst3.censo_completo;


--Completa passo_6 com a melhor informação de renda

drop table if exists playpen_analyst.nat_II_passo_18;
create table playpen_analyst.nat_II_passo_18 as Select distinct
	user_id,
	location_home,
	daytime_days,
	location_work,
	nighttime_days,
	genero,
	faixa_etaria,
	case when renda > 1 then renda 
	     when renda < 1 and renda_med_resp > 0 then renda_med_resp
	     when renda < 1 and renda_med_resp is null then 1194.57
	     else 0 end as renda,
	ds_cluster
from  playpen_analyst.nat_II_passo_16 A left join playpen_analyst.nat_II_passo_17 B on A.location_home = B.location_id;


drop table if exists playpen_analyst.nat_II_passo_19;
create table playpen_analyst.nat_II_passo_19 as Select distinct
       A.id,
       A.location_id,
       regiao
from (select id,location_id,perc_area, row_number()over(partition by location_id order by perc_area desc) as rank from playpen_analyst.nat_II_passo_11) A
left join playpen_analyst.nat_ii_voronoi_v5 B on A.id = B.id
where A.rank =1;

select count(distinct location_id) from playpen_analyst.nat_II_passo_19;

drop table if exists playpen_analyst.nat_II_passo_20;
create table playpen_analyst.nat_II_passo_20 as Select distinct
	user_id,
	location_home,
	genero,
	faixa_etaria,
	renda,
	ds_cluster,
	regiao,
	cume_dist() over (partition by regiao order by renda desc) as ntil
from playpen_analyst.nat_II_passo_18 A left join playpen_analyst.nat_II_passo_19 B on A.location_home = B.location_id
where genero not in ('u') and faixa_etaria not in ('u') and ds_cluster is not null;

select count(distinct user_id) from playpen_analyst.nat_II_passo_20;


drop table if exists playpen_analyst.nat_II_passo_21;
create table playpen_analyst.nat_II_passo_21 as Select 
*,
case    when regiao in ('RM Rio de Janeiro') and ntil < 0.035 then 'A'
	when regiao in ('RM Rio de Janeiro') and ntil < 0.094 then 'B1'
	when regiao in ('RM Rio de Janeiro') and ntil < 0.269 then 'B2'
	when regiao in ('RM Rio de Janeiro') and ntil < 0.501 then 'C1'
	when regiao in ('RM Rio de Janeiro') and ntil < 0.767 then 'C2'
	when regiao in ('RM Rio de Janeiro') and ntil < 1.01  then 'D-E'

	when regiao in ('RM Belo Horizonte') and ntil < 0.035 then 'A'
	when regiao in ('RM Belo Horizonte') and ntil < 0.092 then 'B1'
	when regiao in ('RM Belo Horizonte') and ntil < 0.276 then 'B2'
	when regiao in ('RM Belo Horizonte') and ntil < 0.516 then 'C1'
	when regiao in ('RM Belo Horizonte') and ntil < 0.791 then 'C2'
	when regiao in ('RM Belo Horizonte') and ntil < 1.01  then 'D-E'

	when regiao in ('RM Brasilia') and ntil < 0.099 then 'A'
	when regiao in ('RM Brasilia') and ntil < 0.195 then 'B1'
	when regiao in ('RM Brasilia') and ntil < 0.415 then 'B2'
	when regiao in ('RM Brasilia') and ntil < 0.635 then 'C1'
	when regiao in ('RM Brasilia') and ntil < 0.852 then 'C2'
	when regiao in ('RM Brasilia') and ntil < 1.01  then 'D-E'

	when regiao in ('RM Curitiba') and ntil < 0.054 then 'A'
	when regiao in ('RM Curitiba') and ntil < 0.136 then 'B1'
	when regiao in ('RM Curitiba') and ntil < 0.379 then 'B2'
	when regiao in ('RM Curitiba') and ntil < 0.655 then 'C1'
	when regiao in ('RM Curitiba') and ntil < 0.883 then 'C2'
	when regiao in ('RM Curitiba') and ntil < 1.01  then 'D-E'

	when regiao in ('Interior RJ') and ntil < 0.036 then 'A'
	when regiao in ('Interior RJ') and ntil < 0.098 then 'B1'
	when regiao in ('Interior RJ') and ntil < 0.308 then 'B2'
	when regiao in ('Interior RJ') and ntil < 0.561 then 'C1'
	when regiao in ('Interior RJ') and ntil < 0.815 then 'C2'
	when regiao in ('Interior RJ') and ntil < 1.01  then 'D-E'

	when regiao in ('Interior SP') and ntil < 0.036 then 'A'
	when regiao in ('Interior SP') and ntil < 0.098 then 'B1'
	when regiao in ('Interior SP') and ntil < 0.308 then 'B2'
	when regiao in ('Interior SP') and ntil < 0.561 then 'C1'
	when regiao in ('Interior SP') and ntil < 0.815 then 'C2'
	when regiao in ('Interior SP') and ntil < 1.01  then 'D-E'

        when regiao is null and ntil < 0.029 then 'A'
	when regiao is null and ntil < 0.079 then 'B1'
	when regiao is null and ntil < 0.252 then 'B2'
	when regiao is null and ntil < 0.508 then 'C1'
	when regiao is null and ntil < 0.764 then 'C2'
	when regiao is null and ntil < 1.01  then 'D-E'

else 'Erro' end as classe
from playpen_analyst.nat_II_passo_20;

	
--Verifica distibuição de classes por regiao

select classe, regiao, count(distinct user_id) as qtd from playpen_analyst.nat_II_passo_21 group by classe,regiao;
select * from playpen_analyst.nat_II_passo_21 limit 1000;


--Adiciona informação de potencial de mercado ao CRM

drop table if exists playpen_analyst.nat_II_pot_cons_ajs;
CREATE TABLE playpen_analyst.nat_II_pot_cons_ajs
(
CODIGO      character varying,
LOCALIDADE  character varying,	
UF          character varying,	
MUNICIPIO   character varying,	
A           numeric(20,10),
B1          numeric(20,10),
B2          numeric(20,10),
C1          numeric(20,10),
C2          numeric(20,10),
D_E         numeric(20,10)

)  
WITH (
  OIDS=FALSE
);
ALTER TABLE playpen_analyst.nat_II_pot_cons_ajs
  OWNER TO standard_analyst_br5;


--Importa dados para a tabela criada acima


--cria shapefile dos subdistritos
drop table if exists playpen_analyst.nat_II_passo_22;
create table playpen_analyst.nat_II_passo_22 as Select
	cd_geocods,
	st_union(geom) as geom_ds
from playpen_analyst.census_map_dedup_nat
group by cd_geocods;

--cria shapefile dos distritos
drop table if exists playpen_analyst.nat_II_passo_23;
create table playpen_analyst.nat_II_passo_23 as Select
	cd_geocodd,
	st_union(geom) as geom_dd
from playpen_analyst.census_map_dedup_nat
group by cd_geocodd;


--cria shapefile dos bairros
drop table if exists playpen_analyst.nat_II_passo_24;
create table playpen_analyst.nat_II_passo_24 as Select

(substring(cd_geocodb,1,8)||substring(cd_geocodb,11,2)) as cd_geocodb,
geom_b	

from   (select
	cd_geocodb,
	st_union(geom) as geom_b
from playpen_analyst.census_map_dedup_nat
group by cd_geocodb
       ) A;


select * from playpen_analyst.nat_II_passo_24 limit 100;


--seleciona location_id das zonas de interesse
drop table if exists playpen_analyst.nat_II_passo_25;
create table playpen_analyst.nat_II_passo_25 as Select
	*
from playpen_analyst.br_ulm01_dwell_location_oct16 where location_id in (select distinct location_id from playpen_analyst.nat_II_passo_19);

select count(distinct location_id) from playpen_analyst.nat_II_passo_25;--7.428
select count(distinct location_id) from playpen_analyst.nat_II_passo_12;--7.428

drop table if exists playpen_analyst.nat_II_passo_26;
create table playpen_analyst.nat_II_passo_26 as Select
	location_id,
	cd_geocods
	
from (select

	location_id,
	cd_geocods,
	row_number () over(partition by location_id order by (ST_Area(ST_Intersection(A.location_geo_wgs84,B.geom_ds))/ST_Area(A.location_geo_wgs84)) desc) as rank
from playpen_analyst.nat_II_passo_25 A,
     playpen_analyst.nat_II_passo_22 B
where st_intersects(B.geom_ds,A.location_geo_wgs84)
      )A
where rank = 1;
select count(distinct location_id) from playpen_analyst.nat_II_passo_26; --7.425


drop table if exists playpen_analyst.nat_II_passo_27;
create table playpen_analyst.nat_II_passo_27 as Select
	location_id,
	cd_geocodd

from (select

	location_id,
	cd_geocodd,
	row_number () over(partition by location_id order by (ST_Area(ST_Intersection(A.location_geo_wgs84,B.geom_dd))/ST_Area(A.location_geo_wgs84)) desc) as rank
from playpen_analyst.nat_II_passo_25 A,
     playpen_analyst.nat_II_passo_23 B
where st_intersects(B.geom_dd,A.location_geo_wgs84)
      )A
where rank = 1;
select count(distinct location_id) from playpen_analyst.nat_II_passo_27; --7.425



drop table if exists playpen_analyst.nat_II_passo_28;
create table playpen_analyst.nat_II_passo_28 as Select
	location_id,
	cd_geocodb

from (select

	location_id,
	cd_geocodb,
	row_number () over(partition by location_id order by (ST_Area(ST_Intersection(A.location_geo_wgs84,B.geom_b))/ST_Area(A.location_geo_wgs84)) desc) as rank
from playpen_analyst.nat_II_passo_25 A,
     playpen_analyst.nat_II_passo_24 B
where st_intersects(B.geom_b,A.location_geo_wgs84)
      )A
where rank = 1;
select count(distinct location_id) from playpen_analyst.nat_II_passo_28; --7.425



drop table if exists playpen_analyst.nat_II_passo_29_ajs;
create table playpen_analyst.nat_II_passo_29_ajs as Select

x.*,
case    when IPC is null and classe = 'A'   then 5086.335533 
	when IPC is null and classe = 'B1'  then 3551.80183
	when IPC is null and classe = 'B2'  then 2090.177917
	when IPC is null and classe = 'C1'  then 1448.360443
	when IPC is null and classe = 'C2'  then 986.6388688
	when IPC is null and classe = 'D-E' then 647.9907138

else IPC end as IPC_ajs

from (select
	A.*,
	B.cd_geocods,
	C.cd_geocodd,
	D.cd_geocodb,
	coalesce(G.localidade,E.localidade,F.localidade) as localidade,
	coalesce(G.municipio,E.municipio,F.municipio) as municipio,
	coalesce(G.a,E.a,F.a) as a,
	coalesce(G.b1,E.b1,F.b1) as b1,
	coalesce(G.b2,E.b2,F.b2) as b2,
	coalesce(G.c1,E.c1,F.c1) as c1,
	coalesce(G.c2,E.c2,F.c2) as c2,
	coalesce(G.d_e,E.d_e,F.d_e) as d_e,
	case when classe = 'A'   then coalesce(G.a,E.a,F.a)
             when classe = 'B1'  then coalesce(G.b1,E.b1,F.b1)
             when classe = 'B2'  then coalesce(G.b2,E.b2,F.b2)
             when classe = 'C1'  then coalesce(G.c1,E.c1,F.c1)
             when classe = 'C2'  then coalesce(G.c2,E.c2,F.c2)
             when classe = 'D-E' then coalesce(G.d_e,E.d_e,F.d_e)
             else null end as IPC
	

from playpen_analyst.nat_II_passo_21 A left join playpen_analyst.nat_II_passo_26 B on A.location_home = B.location_id
                                       left join playpen_analyst.nat_II_passo_27 C on A.location_home = C.location_id
                                       left join playpen_analyst.nat_II_passo_28 D on A.location_home = D.location_id
                                       left join playpen_analyst.nat_II_pot_cons_ajs E on B.cd_geocods = E.codigo
                                       left join playpen_analyst.nat_II_pot_cons_ajs F on C.cd_geocodd = F.codigo
                                       left join playpen_analyst.nat_II_pot_cons_ajs G on D.cd_geocodb = G.codigo

       ) x;


select *
from (select distinct
		municipio,
		localidade,
		regiao,
		a 
	from playpen_analyst.nat_II_passo_29_ajs 
        where a > 0 
      )A
order by municipio;

--Avalia qualidade do dado de potencial

--Escopo

--RM Curitiba
--RM Rio de Janeiro
--RM Brasilia
--Interior SP
--Interior RJ
--RM Belo Horizonte

select 
municipio,count(distinct a)
from playpen_analyst.nat_II_pot_cons_ajs 
where municipio in ('BELO HORIZONTE','RIO DE JANEIRO','CAMPINAS','BRASILIA','CURITIBA')
group by municipio;

--BELO HORIZONTE 159
--BRASILIA 19
--CAMPINAS 5
--CURITIBA 71
--RIO DE JANEIRO 143

select 
regiao,
count(distinct a)
from playpen_analyst.nat_II_passo_29_ajs 
group by regiao;

--Interior RJ 69
--Interior SP 245
--RM Belo Horizonte 97
--RM Brasilia 21
--RM Curitiba 89
--RM Rio de Janeiro 224


--Verifica distribuição de idade e gênero do censo

--cria tabela do censo e sobe os dados
drop table if exists playpen_analyst.censo_idade_genero;
CREATE TABLE playpen_analyst.censo_idade_genero
(

Cod_setor  character varying,
Cod_municipio character varying,
ate_20_m numeric(20,10),
de_20_a_29_m numeric(20,10),
de_30_a_39_m numeric(20,10),
de_40_a_49_m numeric(20,10),
de_50_a_59_m numeric(20,10),
mais_60_m numeric(20,10),
ate_20_f numeric(20,10),
de_20_a_29_f numeric(20,10),
de_30_a_39_f numeric(20,10),
de_40_a_49_f numeric(20,10),
de_50_a_59_f numeric(20,10),
mais_60_f numeric(20,10)


)  
WITH (
  OIDS=FALSE
);
   ALTER TABLE playpen_analyst.censo_idade_genero
   OWNER TO standard_analyst_br5;


drop table if exists playpen_analyst.nat_II_passo_30;
create table playpen_analyst.nat_II_passo_30 as Select
	A.*,
        case when regiao is not null then regiao else 'outros' end as regiao
from playpen_analyst.censo_idade_genero A left join playpen_analyst.nat_II_passo_2 B on A.cod_municipio::numeric = B.codigo_ibge::numeric;
 

drop table if exists playpen_analyst.nat_II_passo_31;
create table playpen_analyst.nat_II_passo_31 as Select
        regiao,
	sum(ate_20_m + ate_20_f) as ate_20, 
	sum(de_20_a_29_m + de_20_a_29_f) as de_20_a_29,
	sum(de_30_a_39_m + de_30_a_39_f) as de_30_a_39,
	sum(de_40_a_49_m + de_40_a_49_f) as de_40_a_49,
	sum(de_50_a_59_m + de_50_a_59_f + mais_60_m + mais_60_f) as mais_50,
	sum(ate_20_m + de_20_a_29_m + de_30_a_39_m + de_40_a_49_m + de_50_a_59_m + mais_60_m) as masc,
	sum(ate_20_f + de_20_a_29_f + de_30_a_39_f + de_40_a_49_f + de_50_a_59_f + mais_60_f) as fem
from playpen_analyst.nat_II_passo_30
group by regiao;

select * from playpen_analyst.nat_II_passo_31 order by regiao;

-- Verifica distribuição de idade e gênero do CRM vivo

drop table if exists playpen_analyst.nat_II_passo_32_ajs;
create table playpen_analyst.nat_II_passo_32_ajs as Select
	genero,
	faixa_etaria,
	regiao,
	count(distinct user_id) as qtd
from playpen_analyst.nat_II_passo_29_ajs
group by genero,faixa_etaria,regiao;


select * from playpen_analyst.nat_II_passo_33 limit 100;


drop table if exists playpen_analyst.nat_II_passo_33_ajs;
create table playpen_analyst.nat_II_passo_33_ajs as Select

*,

case when regiao in ('Interior RJ') and faixa_etaria in ('ATE 20')     then  16.99103931
     when regiao in ('Interior RJ') and faixa_etaria in ('DE 20 a 30') then  1.91467077
     when regiao in ('Interior RJ') and faixa_etaria in ('DE 30 a 40') then  0.471475648
     when regiao in ('Interior RJ') and faixa_etaria in ('DE 40 a 50') then  0.526266085
     when regiao in ('Interior RJ') and faixa_etaria in ('Mais 50')    then  0.789086313

     when regiao in ('Interior SP') and faixa_etaria in ('ATE 20')     then  20.22356535
     when regiao in ('Interior SP') and faixa_etaria in ('DE 20 a 30') then  2.111472198
     when regiao in ('Interior SP') and faixa_etaria in ('DE 30 a 40') then  0.487978026
     when regiao in ('Interior SP') and faixa_etaria in ('DE 40 a 50') then  0.493652226
     when regiao in ('Interior SP') and faixa_etaria in ('Mais 50')    then  0.812871166

     when regiao in ('RM Belo Horizonte') and faixa_etaria in ('ATE 20')     then  16.28785676
     when regiao in ('RM Belo Horizonte') and faixa_etaria in ('DE 20 a 30') then  2.172868493
     when regiao in ('RM Belo Horizonte') and faixa_etaria in ('DE 30 a 40') then  0.512295641
     when regiao in ('RM Belo Horizonte') and faixa_etaria in ('DE 40 a 50') then  0.498815097
     when regiao in ('RM Belo Horizonte') and faixa_etaria in ('Mais 50')    then  0.714798303

     when regiao in ('RM Brasilia') and faixa_etaria in ('ATE 20')     then  19.9916753
     when regiao in ('RM Brasilia') and faixa_etaria in ('DE 20 a 30') then  2.391127436
     when regiao in ('RM Brasilia') and faixa_etaria in ('DE 30 a 40') then  0.529373477
     when regiao in ('RM Brasilia') and faixa_etaria in ('DE 40 a 50') then  0.466927034
     when regiao in ('RM Brasilia') and faixa_etaria in ('Mais 50')    then  0.53979354

     when regiao in ('RM Curitiba') and faixa_etaria in ('ATE 20')     then  21.93074484
     when regiao in ('RM Curitiba') and faixa_etaria in ('DE 20 a 30') then  2.36868456
     when regiao in ('RM Curitiba') and faixa_etaria in ('DE 30 a 40') then  0.488034129
     when regiao in ('RM Curitiba') and faixa_etaria in ('DE 40 a 50') then  0.498072581
     when regiao in ('RM Curitiba') and faixa_etaria in ('Mais 50')    then  0.693694467

     when regiao in ('RM Rio de Janeiro') and faixa_etaria in ('ATE 20')     then  28.98130357
     when regiao in ('RM Rio de Janeiro') and faixa_etaria in ('DE 20 a 30') then  2.680163701
     when regiao in ('RM Rio de Janeiro') and faixa_etaria in ('DE 30 a 40') then  0.577335301
     when regiao in ('RM Rio de Janeiro') and faixa_etaria in ('DE 40 a 50') then  0.530784599
     when regiao in ('RM Rio de Janeiro') and faixa_etaria in ('Mais 50')    then  0.634589016

     when regiao is null and faixa_etaria in ('ATE 20')     then  18.17905609
     when regiao is null and faixa_etaria in ('DE 20 a 30') then  1.945370358
     when regiao is null and faixa_etaria in ('DE 30 a 40') then  0.42887981
     when regiao is null and faixa_etaria in ('DE 40 a 50') then  0.468276944
     when regiao is null and faixa_etaria in ('Mais 50')    then  0.772737918

end as peso_idade,


case when regiao in ('Interior RJ') and genero in ('m') then  0.941006408
     when regiao in ('Interior RJ') and genero in ('f') then  1.062479662
     
     when regiao in ('Interior SP') and genero in ('m') then  0.893327053
     when regiao in ('Interior SP') and genero in ('f') then  1.127959297
     
     when regiao in ('RM Belo Horizonte') and genero in ('m') then  0.897901308
     when regiao in ('RM Belo Horizonte') and genero in ('f') then  1.117836146
     
     when regiao in ('RM Brasilia') and genero in ('m') then  0.856083292
     when regiao in ('RM Brasilia') and genero in ('f') then  1.187876065
     
     when regiao in ('RM Curitiba') and genero in ('m') then  0.855285493
     when regiao in ('RM Curitiba') and genero in ('f') then  1.191113145
    
     when regiao in ('RM Rio de Janeiro') and genero in ('m') then  0.926024584
     when regiao in ('RM Rio de Janeiro') and genero in ('f') then  1.077332238
     
     when regiao is null and genero in ('m') then  0.910776952
     when regiao is null and genero in ('f') then  1.104598828
     
end as peso_genero

from playpen_analyst.nat_II_passo_29_ajs;


-- Testa se a aplicação dos pesos de CRM geraram os efeitos desejados

drop table if exists playpen_analyst.nat_II_passo_33_teste_CRM ;
create table playpen_analyst.nat_II_passo_33_teste_CRM as select
	genero,
	faixa_etaria,
	classe,
	regiao,
	count(distinct user_id) as qtd,
	sum(peso_idade) as peso_idade,
	sum(peso_genero) as peso_genero,
	sum(peso_idade*peso_genero) as peso_idade_genero
from playpen_analyst.nat_II_passo_33_ajs
group by genero,faixa_etaria,classe,regiao;

select * from playpen_analyst.nat_II_passo_33_teste_CRM where peso_idade_genero is null;


--Step 1 -Dwells
------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------
drop table if exists playpen_analyst.nat_II_passo_34;
create table playpen_analyst.nat_II_passo_34 as select 
        user_id,
        location_id,          
        date_dt,
        case when (EXTRACT (hour from start_dt))in (6,7,8,9,10,11) then 'manha'
             when (EXTRACT (hour from start_dt))in (12,13,14,15,16,17) then 'tarde'
             when (EXTRACT (hour from start_dt))in (18,19,20,21,22,23) then 'noite'
             else 'Erro' end as periodo,
        CASE WHEN EXTRACT(isodow from start_dt) <=5 then 0 else 1 end as FDS
        
from cip.dwell 

where date_dt between '20161001' and '20161031' and 
      location_id in (select distinct location_id from playpen_analyst.nat_II_passo_19) and
      EXTRACT (hour from start_dt) in (6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23);   



drop table if exists playpen_analyst.nat_II_passo_35_ajs;
create table playpen_analyst.nat_II_passo_35_ajs as select 
	A.*,
	B.location_home, 
	C.id as id_home,
	D.populacao as pop
from playpen_analyst.nat_II_passo_34 A inner join playpen_analyst.nat_II_passo_29_ajs B on A.user_id = B.user_id
                                       left join playpen_analyst.nat_II_passo_19 C on B.location_home = C.location_id
                                       left join playpen_analyst.nat_ii_voronoi_v5 D on C.id = D.id;


select * from playpen_analyst.nat_II_passo_35_ajs limit 100;

drop table if exists playpen_analyst.nat_II_passo_36_ajs;
create table playpen_analyst.nat_II_passo_36_ajs as select 
	id_home,
	date_dt,
	periodo,
	FDS,
	pop,
	sum(peso_idade*peso_genero) as qtd
from playpen_analyst.nat_II_passo_35_ajs A left join playpen_analyst.nat_II_passo_33_ajs B on A.user_id = B.user_id
group by id_home,date_dt,periodo,FDS,pop;


drop table if exists playpen_analyst.nat_II_passo_37_ajs;
create table playpen_analyst.nat_II_passo_37_ajs as select 
	id_home,
	date_dt,
	periodo,
	FDS,
	pop,
	qtd,	
	pop/qtd as peso
from playpen_analyst.nat_II_passo_36_ajs;

select * from playpen_analyst.nat_II_passo_37_ajs where peso is null; --1176

drop table if exists playpen_analyst.nat_II_passo_38_ajs;
create table playpen_analyst.nat_II_passo_38_ajs as select 
	A.id_home,
	A.date_dt,
	A.periodo,
	A.FDS,	
	A.qtd,
	coalesce(peso,peso2) as peso
from playpen_analyst.nat_II_passo_37_ajs  A  left join (select date_dt,
				                         periodo,
				                         FDS, 
				                         avg(peso)as peso2
				                         from playpen_analyst.nat_II_passo_37_ajs where peso > 0
				                         group by date_dt, periodo,FDS) B

 on A.date_dt = B.date_dt and A.periodo = B.periodo and A.FDS = B.FDS;

select * from playpen_analyst.nat_II_passo_38_ajs where peso is null; 

--Verifica parte do processo de extrapolacao
--Ok

select 
	id_home, 
	populacao,
	date_dt,
	periodo,
	FDS,
	sum(peso*qtd) as pop_extr 
from  playpen_analyst.nat_II_passo_38_ajs A left join playpen_analyst.nat_ii_voronoi_v5 B on A.id_home = B.id 
group by id_home,populacao,date_dt,periodo,FDS;


drop table if exists playpen_analyst.nat_II_passo_39_ajs;
create table playpen_analyst.nat_II_passo_39_ajs as select 

	A.user_id,
        A.location_id,
        D.id as id,         
        A.date_dt,
        case when A.FDS = 1 then 'Fim de semana' else 'Util' end as tipo_dia,
        A.periodo, 
        A.id_home,
        F.id as id_work,
        C.genero,
        C.faixa_etaria,
        C.classe,
        C.ds_cluster,
        C.ipc_ajs as ipc,
        peso as peso_puro,
        (peso*peso_genero*peso_idade) as peso
        

        from playpen_analyst.nat_II_passo_35_ajs A left join playpen_analyst.nat_II_passo_38_ajs B on A.id_home = B.id_home and A.date_dt = B.date_dt and A.periodo = B.periodo
                                                   left join playpen_analyst.nat_II_passo_33_ajs C on A.user_id = C.user_id
                                                   left join playpen_analyst.nat_II_passo_19 D on A.location_id = D.location_id
                                                   left join playpen_analyst.nat_II_passo_18 E on A.user_id = E.user_id
                                                   left join playpen_analyst.nat_II_passo_19 F on E.location_work = F.location_id;


--Verifica processo de extrapolação com os pesos de CRM juntos.
                                               
drop table if exists playpen_analyst.nat_II_passo_39_teste;
create table playpen_analyst.nat_II_passo_39_teste as select  
	id_home, 
	populacao,
	date_dt,
	periodo,
	tipo_dia,
	count(1) as qtd,
	sum(peso_puro) as peso_puro,
	sum(peso) as pop_extr 
from  playpen_analyst.nat_II_passo_39_ajs A left join playpen_analyst.nat_ii_voronoi_v5 B on A.id_home = B.id 
group by id_home,populacao,date_dt,periodo,tipo_dia;

select count(1) from playpen_analyst.nat_II_passo_39 where peso_puro is null; --5.699.588
select count(1) from playpen_analyst.nat_II_passo_39 where peso is null; ----5.699.588


drop table if exists playpen_analyst.nat_II_passo_40_ajs;
create table playpen_analyst.nat_II_passo_40_ajs as select 

        user_id,
        location_id,        
        date_dt,
        tipo_dia,
        periodo, 
        case when id = id_home then 'Residente'
             when id = id_work then 'Trabalho/Estudo'
             else 'visitante' end as contexto,
        genero,
        faixa_etaria,
        classe,
        ds_cluster,
        ipc,
        peso as peso_no_cap,
        case when peso < 250 then peso else 250 end as peso

from playpen_analyst.nat_II_passo_39_ajs;

select sum(peso) from playpen_analyst.nat_II_passo_40_ajs; --3.930.399.547
select sum(peso_no_cap) from playpen_analyst.nat_II_passo_40_ajs; --3.431.516.356
select * from playpen_analyst.nat_II_passo_40_ajs where peso is null; --0
select count(1) from playpen_analyst.nat_II_passo_40_ajs;


drop table if exists playpen_analyst.apagar1;
create table playpen_analyst.apagar1 as select 

	genero,
	faixa_etaria,
	classe,
	count(distinct user_id) as qtd,
	sum (peso_no_cap) as peso_no_cap,
	sum(peso) as peso
	
from playpen_analyst.nat_II_passo_40_ajs
group by genero,faixa_etaria,classe;

select * from playpen_analyst.apagar1;


drop table if exists playpen_analyst.nat_II_passo_41_ajs;
create table playpen_analyst.nat_II_passo_41_ajs as select 
	id as zona,
	tipo_dia,
	periodo,
	contexto,
	genero,
	faixa_etaria,
	classe,
	ds_cluster,
	sum(perc*peso_no_cap) as peso,	
	sum(perc*peso_no_cap*ipc) as IPC
from playpen_analyst.nat_II_passo_40_ajs A left join playpen_analyst.nat_II_passo_12 B on A.location_id = B.location_id
group by id,tipo_dia,periodo,contexto,genero,faixa_etaria,classe,ds_cluster;


select sum(peso) from playpen_analyst.nat_II_passo_41_ajs; --3.429.408.623
select * from playpen_analyst.nat_II_passo_41_ajs where peso is null;

drop table if exists playpen_analyst.nat_II_passo_42_ajs;
create table playpen_analyst.nat_II_passo_42_ajs as select 

	zona,
	area,
        cidade,
        uf,
        regiao,   
        tipo_dia,
	periodo,
        contexto,
	genero,
	faixa_etaria,
	classe,
	ds_cluster as cluster_comportamento,
        case when tipo_dia in ('Fim de semana') then ceiling (peso/10) 
             when tipo_dia in ('Util')          then ceiling (peso/21)         
             end as pessoas,
        case when tipo_dia in ('Fim de semana') then ceiling (IPC/10) 
             when tipo_dia in ('Util')          then ceiling (IPC/21)         
             end as IPC_anual   
        
from playpen_analyst.nat_II_passo_41_ajs A left join playpen_analyst.nat_ii_voronoi_v5 B on A.zona = B.id;

select * from playpen_analyst.nat_II_passo_42_ajs limit 100; --3.918.976.642
select * from playpen_analyst.nat_II_passo_42_ajs where pessoas is null; 

---Similaridade de fluxo

drop table if exists playpen_analyst.nat_II_passo_43_ajs;
create table playpen_analyst.nat_II_passo_43_ajs as select 
	A.id,
	B.regiao as regiao_analise,
	A.id_home,
	C.regiao as regiao_home,
	sum(peso) as volume
from playpen_analyst.nat_II_passo_39_ajs A left join playpen_analyst.nat_ii_voronoi_v5 B on A.id = B.id
                                           left join playpen_analyst.nat_ii_voronoi_v5 C on A.id_home = C.id
where tipo_dia in ('Util')
group by A.id,regiao_analise,id_home,regiao_home;

select count(1) from playpen_analyst.nat_II_passo_43_ajs;

--Normaliza o potencial de consumo e populacao por cidade

drop table if exists playpen_analyst.nat_II_pot_cons_cidade_ajs;
CREATE TABLE playpen_analyst.nat_II_pot_cons_cidade_ajs
(
CODIGO      character varying,
LOCALIDADE  character varying,	
UF          character varying,	
TOTAL       numeric(20,10)

)  
WITH (
  OIDS=FALSE
);
ALTER TABLE playpen_analyst.nat_II_pot_cons_cidade_ajs
  OWNER TO standard_analyst_br5;


drop table if exists playpen_analyst.natura_munic_id_ajs_v2;
create table playpen_analyst.natura_munic_id_ajs_v2 as select
*
from (select
	A.id,
	B.codigo_ibg,
	B.population,
        row_number() over(partition by A.id order by (ST_Area(ST_Intersection(A.geom,B.geom))/ST_Area(A.geom)) desc) as rank
from playpen_analyst.nat_ii_voronoi_v5 A,
     playpen_analyst.mapa_municipios B
     where codigo_ibg in (select distinct codigo_ibge from playpen_analyst.nat_II_passo_2)
    ) intermed
where rank = 1;


drop table if exists playpen_analyst.nat_II_passo_44_ajs;
create table playpen_analyst.nat_II_passo_44_ajs as select 
*,
total/IPC_anual as fator_ipc,
population::numeric/pessoas as fator_pessoas

from (select
        cidade,
        A.uf,
        regiao,   
        tipo_dia,
	periodo,
        total,        
        sum(IPC_anual) as IPC_anual,
        sum(pessoas) as pessoas,
        population
from playpen_analyst.nat_II_passo_42_ajs A left join playpen_analyst.natura_munic_id_ajs_v2 B on A.zona = B.id
                                           left join playpen_analyst.nat_II_pot_cons_cidade_ajs C on B.codigo_ibg = C.codigo 
group by cidade,A.uf,regiao,tipo_dia,periodo,total,population
      )intermed;


drop table if exists playpen_analyst.nat_II_passo_45_ajs;
create table playpen_analyst.nat_II_passo_45_ajs as select 
        A.zona,
        A.area,
        A.cidade,
        A.uf,
        A.regiao,   
        A.tipo_dia,
	A.periodo,
        A.contexto,
	A.genero,
	A.faixa_etaria,
	A.classe,
	A.cluster_comportamento,
        ceiling(A.pessoas  * fator_pessoas) as pessoas,
        ceiling(A.IPC_anual * fator_ipc) as IPC_anual   

from playpen_analyst.nat_II_passo_42_ajs A inner join  playpen_analyst.nat_II_passo_44_ajs B on 
A.cidade = B.cidade and A.uf = B.uf and A.regiao = B.regiao and A.tipo_dia = B.tipo_dia and A.periodo = B.periodo
where A.pessoas > 0;

--Testes
--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------

--Analisa existencia de missings
select * from playpen_analyst.nat_II_passo_45_ajs where zona is null;
select * from playpen_analyst.nat_II_passo_45_ajs where area is null;
select * from playpen_analyst.nat_II_passo_45_ajs where cidade is null;
select * from playpen_analyst.nat_II_passo_45_ajs where uf is null;
select * from playpen_analyst.nat_II_passo_45_ajs where regiao is null;
select * from playpen_analyst.nat_II_passo_45_ajs where tipo_dia is null;
select * from playpen_analyst.nat_II_passo_45_ajs where periodo is null;
select * from playpen_analyst.nat_II_passo_45_ajs where contexto is null;
select * from playpen_analyst.nat_II_passo_45_ajs where genero is null;
select * from playpen_analyst.nat_II_passo_45_ajs where faixa_etaria is null;
select * from playpen_analyst.nat_II_passo_45_ajs where classe is null;
select * from playpen_analyst.nat_II_passo_45_ajs where cluster_comportamento is null;
select * from playpen_analyst.nat_II_passo_45_ajs where pessoas is null;
select * from playpen_analyst.nat_II_passo_45_ajs where IPC_anual is null;


--Teste 1: Todas as zonas presentes

select distinct zona from playpen_analyst.nat_II_passo_45_ajs order by zona; --4307, suposto ter 4328

drop table if exists playpen_analyst.nat_II_zonas_faltantes_ajs;
create table playpen_analyst.nat_II_zonas_faltantes_ajs as select distinct 
	* 
from playpen_analyst.nat_ii_voronoi_v5 
where id not in (select distinct zona from playpen_analyst.nat_II_passo_45_ajs);
select count(1) from playpen_analyst.nat_II_zonas_faltantes_ajs; 


--Teste 2: População por cidade, por período e por tipo de dia. Potencial por cidade, por período e por tipo de dia.

drop table if exists playpen_analyst.natura_teste_2;
create table playpen_analyst.natura_teste_2 as select

	regiao,
	cidade,
	A.uf,
	periodo,
	tipo_dia,
	sum(pessoas) as pessoas,
	C.population as pop_censo,
	sum(IPC_anual) as IPC_anual,
	total as IPC_pof

from playpen_analyst.nat_II_passo_45_ajs A left join playpen_analyst.nat_II_passo_1 B on A.cidade = B.nome and A.uf = B.uf
                                           left join playpen_analyst.natura_munic_id_ajs_v2 C on A.zona = C.id
                                           left join playpen_analyst.nat_II_pot_cons_cidade_ajs D on C.codigo_ibg = D.codigo 
group by regiao,cidade,C.population,A.uf,periodo,tipo_dia,total;

select * from playpen_analyst.natura_teste_2;

	
--Teste 3: Distribuição dos perfis por região e correlacão dos potenciais com os perfis

select 
        regiao,
	genero,
	faixa_etaria,
	classe,
	cluster_comportamento,
	sum(pessoas) as pessoas,
	sum(IPC_anual) as ipc

from playpen_analyst.nat_II_passo_45_ajs A left join playpen_analyst.nat_II_passo_1 B on A.cidade = B.nome
group by regiao,genero,faixa_etaria,classe,cluster_comportamento;


--Teste 4: Contexto por tipo de dia, periodo, variabilidade por area

select
	zona,
	tipo_dia,
	periodo,
	contexto,
	sum(pessoas) as pessoas

from playpen_analyst.nat_II_passo_45_ajs
group by zona,tipo_dia,periodo,contexto;


--Deixa no formato de entrega---------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------

drop table if exists playpen_analyst.nat_II_passo_45_demog;
create table playpen_analyst.nat_II_passo_45_demog as select 
        A.zona,
        A.area,
        A.cidade,
        A.uf,
        A.regiao,   
        A.tipo_dia,
	A.periodo,
        A.contexto,
	A.genero,
	A.faixa_etaria,
	A.classe,
        sum(A.pessoas) as pessoas,
        sum(A.IPC_anual) as IPC_anual    
from playpen_analyst.nat_II_passo_45_ajs A
group by 1,2,3,4,5,6,7,8,9,10,11;

select * from playpen_analyst.nat_II_passo_45_demog where regiao in ('RM Rio de Janeiro','Interior RJ') and pessoas>0 and IPC_anual>0 and tipo_dia in ('Util'); 
select * from playpen_analyst.nat_II_passo_45_demog where regiao in ('RM Rio de Janeiro','Interior RJ') and pessoas>0 and IPC_anual>0 and tipo_dia in ('Fim de semana'); 

select sum(pessoas),sum(ipc_anual) from playpen_analyst.nat_II_passo_45_demog where tipo_dia = 'Util' and periodo ='manha' and classe in ('B2','C1','C2');
select * from playpen_analyst.nat_II_passo_45_demog limit 100;

drop table if exists playpen_analyst.nat_II_passo_45_cluster;
create table playpen_analyst.nat_II_passo_45_cluster as select 
        A.zona,
        A.area,
        A.cidade,
        A.uf,
        A.regiao,   
        A.tipo_dia,
	A.periodo,
        A.contexto,
	A.cluster_comportamento,
        sum(A.pessoas) as pessoas,
        sum(A.IPC_anual) as IPC_anual    
from playpen_analyst.nat_II_passo_45_ajs A
group by 1,2,3,4,5,6,7,8,9;


select * from playpen_analyst.nat_II_passo_45_cluster where regiao in ('RM Rio de Janeiro','Interior RJ') and pessoas>0 and IPC_anual>0; 


--Compara com a populacao da tabela de municipios:
--RM Belo Horizonte        5.414.701  --  5.389.181
--RM Brasilia              3.724.181  --  3.703.380
--RM Rio de Janeiro        11.835.708 --  11.712.037
--RM Curitiba              3.223.836  --  3.206.164 
--Interior RJ              2.719.505  --  2.692.254
--Interior SP              13.425.404 --  13.333.217

select * from playpen_analyst.nat_II_passo_43_ajs where volume is not null and regiao_analise in ('Interior SP') and regiao_home in ('Interior SP');
select * from playpen_analyst.nat_II_passo_43_ajs where volume is not null and regiao_analise in ('RM Rio de Janeiro','Interior RJ') and regiao_home in ('RM Rio de Janeiro','Interior RJ');
select * from playpen_analyst.nat_II_passo_43_ajs where volume is not null and regiao_analise in ('RM Belo Horizonte','RM Brasilia','RM Curitiba') and regiao_home in ('RM Belo Horizonte','RM Brasilia','RM Curitiba');


select distinct classe from playpen_analyst.nat_II_passo_45_demog;

--Criação do Shapefile com informação

drop table if exists playpen_analyst.nat_II_passo_shapefile;
create table playpen_analyst.nat_II_passo_shapefile as select 
        zona,
        area,
        cidade,
        uf,
        regiao,   
case when A.tipo_dia in ('Util') and periodo in ('manha') then pessoas else 0 end as pp_mn_nf,
case when A.tipo_dia in ('Util') and periodo in ('tarde') then pessoas else 0 end as pp_tr_nf,
case when A.tipo_dia in ('Util') and periodo in ('noite') then pessoas else 0 end as pp_nt_nf,
case when A.tipo_dia in ('Util') and periodo in ('manha') then IPC_anual else 0 end as pt_mn_nf,
case when A.tipo_dia in ('Util') and periodo in ('tarde') then IPC_anual else 0 end as pt_tr_nf,
case when A.tipo_dia in ('Util') and periodo in ('noite') then IPC_anual else 0 end as pt_nt_nf,
case when A.tipo_dia in ('Fim de semana') and periodo in ('manha') then pessoas else 0 end as pp_mn_fd,
case when A.tipo_dia in ('Fim de semana') and periodo in ('tarde') then pessoas else 0 end as pp_tr_fd,
case when A.tipo_dia in ('Fim de semana') and periodo in ('noite') then pessoas else 0 end as pp_nt_fd,        
case when A.tipo_dia in ('Fim de semana') and periodo in ('manha') then IPC_anual else 0 end as pt_mn_fd,       
case when A.tipo_dia in ('Fim de semana') and periodo in ('tarde') then IPC_anual else 0 end as pt_tr_fd,        
case when A.tipo_dia in ('Fim de semana') and periodo in ('noite') then IPC_anual else 0 end as pt_nt_fd,
      
case when A.tipo_dia in ('Util') and periodo in ('manha') and classe in ('A') then pessoas else 0 end as pp_mn_nf_A,
case when A.tipo_dia in ('Util') and periodo in ('tarde')and classe in ('A') then pessoas else 0 end as pp_tr_nf_A,
case when A.tipo_dia in ('Util') and periodo in ('noite')and classe in ('A') then pessoas else 0 end as pp_nt_nf_A,
case when A.tipo_dia in ('Util') and periodo in ('manha')and classe in ('A') then IPC_anual else 0 end as pt_mn_nf_A,
case when A.tipo_dia in ('Util') and periodo in ('tarde')and classe in ('A') then IPC_anual else 0 end as pt_tr_nf_A,
case when A.tipo_dia in ('Util') and periodo in ('noite')and classe in ('A') then IPC_anual else 0 end as pt_nt_nf_A,
case when A.tipo_dia in ('Fim de semana') and periodo in ('manha')and classe in ('A') then pessoas else 0 end as pp_mn_fd_A,
case when A.tipo_dia in ('Fim de semana') and periodo in ('tarde')and classe in ('A') then pessoas else 0 end as pp_tr_fd_A,
case when A.tipo_dia in ('Fim de semana') and periodo in ('noite')and classe in ('A') then pessoas else 0 end as pp_nt_fd_A,        
case when A.tipo_dia in ('Fim de semana') and periodo in ('manha')and classe in ('A') then IPC_anual else 0 end as pt_mn_fd_A,       
case when A.tipo_dia in ('Fim de semana') and periodo in ('tarde')and classe in ('A') then IPC_anual else 0 end as pt_tr_fd_A,        
case when A.tipo_dia in ('Fim de semana') and periodo in ('noite')and classe in ('A') then IPC_anual else 0 end as pt_nt_fd_A,  

case when A.tipo_dia in ('Util') and periodo in ('manha') and classe in ('B1') then pessoas else 0 end as pp_mn_nf_B1,
case when A.tipo_dia in ('Util') and periodo in ('tarde')and classe in ('B1') then pessoas else 0 end as pp_tr_nf_B1,
case when A.tipo_dia in ('Util') and periodo in ('noite')and classe in ('B1') then pessoas else 0 end as pp_nt_nf_B1,
case when A.tipo_dia in ('Util') and periodo in ('manha')and classe in ('B1') then IPC_anual else 0 end as pt_mn_nf_B1,
case when A.tipo_dia in ('Util') and periodo in ('tarde')and classe in ('B1') then IPC_anual else 0 end as pt_tr_nf_B1,
case when A.tipo_dia in ('Util') and periodo in ('noite')and classe in ('B1') then IPC_anual else 0 end as pt_nt_nf_B1,
case when A.tipo_dia in ('Fim de semana') and periodo in ('manha')and classe in ('B1') then pessoas else 0 end as pp_mn_fd_B1,
case when A.tipo_dia in ('Fim de semana') and periodo in ('tarde')and classe in ('B1') then pessoas else 0 end as pp_tr_fd_B1,
case when A.tipo_dia in ('Fim de semana') and periodo in ('noite')and classe in ('B1') then pessoas else 0 end as pp_nt_fd_B1,        
case when A.tipo_dia in ('Fim de semana') and periodo in ('manha')and classe in ('B1') then IPC_anual else 0 end as pt_mn_fd_B1,       
case when A.tipo_dia in ('Fim de semana') and periodo in ('tarde')and classe in ('B1') then IPC_anual else 0 end as pt_tr_fd_B1,        
case when A.tipo_dia in ('Fim de semana') and periodo in ('noite')and classe in ('B1') then IPC_anual else 0 end as pt_nt_fd_B1, 


case when A.tipo_dia in ('Util') and periodo in ('manha') and classe in ('B2') then pessoas else 0 end as pp_mn_nf_B2,
case when A.tipo_dia in ('Util') and periodo in ('tarde')and classe in ('B2') then pessoas else 0 end as pp_tr_nf_B2,
case when A.tipo_dia in ('Util') and periodo in ('noite')and classe in ('B2') then pessoas else 0 end as pp_nt_nf_B2,
case when A.tipo_dia in ('Util') and periodo in ('manha')and classe in ('B2') then IPC_anual else 0 end as pt_mn_nf_B2,
case when A.tipo_dia in ('Util') and periodo in ('tarde')and classe in ('B2') then IPC_anual else 0 end as pt_tr_nf_B2,
case when A.tipo_dia in ('Util') and periodo in ('noite')and classe in ('B2') then IPC_anual else 0 end as pt_nt_nf_B2,
case when A.tipo_dia in ('Fim de semana') and periodo in ('manha')and classe in ('B2') then pessoas else 0 end as pp_mn_fd_B2,
case when A.tipo_dia in ('Fim de semana') and periodo in ('tarde')and classe in ('B2') then pessoas else 0 end as pp_tr_fd_B2,
case when A.tipo_dia in ('Fim de semana') and periodo in ('noite')and classe in ('B2') then pessoas else 0 end as pp_nt_fd_B2,        
case when A.tipo_dia in ('Fim de semana') and periodo in ('manha')and classe in ('B2') then IPC_anual else 0 end as pt_mn_fd_B2,       
case when A.tipo_dia in ('Fim de semana') and periodo in ('tarde')and classe in ('B2') then IPC_anual else 0 end as pt_tr_fd_B2,        
case when A.tipo_dia in ('Fim de semana') and periodo in ('noite')and classe in ('B2') then IPC_anual else 0 end as pt_nt_fd_B2,      
        
        
case when A.tipo_dia in ('Util') and periodo in ('manha') and classe in ('C1') then pessoas else 0 end as pp_mn_nf_C1,
case when A.tipo_dia in ('Util') and periodo in ('tarde')and classe in ('C1') then pessoas else 0 end as pp_tr_nf_C1,
case when A.tipo_dia in ('Util') and periodo in ('noite')and classe in ('C1') then pessoas else 0 end as pp_nt_nf_C1,
case when A.tipo_dia in ('Util') and periodo in ('manha')and classe in ('C1') then IPC_anual else 0 end as pt_mn_nf_C1,
case when A.tipo_dia in ('Util') and periodo in ('tarde')and classe in ('C1') then IPC_anual else 0 end as pt_tr_nf_C1,
case when A.tipo_dia in ('Util') and periodo in ('noite')and classe in ('C1') then IPC_anual else 0 end as pt_nt_nf_C1,
case when A.tipo_dia in ('Fim de semana') and periodo in ('manha')and classe in ('C1') then pessoas else 0 end as pp_mn_fd_C1,
case when A.tipo_dia in ('Fim de semana') and periodo in ('tarde')and classe in ('C1') then pessoas else 0 end as pp_tr_fd_C1,
case when A.tipo_dia in ('Fim de semana') and periodo in ('noite')and classe in ('C1') then pessoas else 0 end as pp_nt_fd_C1,        
case when A.tipo_dia in ('Fim de semana') and periodo in ('manha')and classe in ('C1') then IPC_anual else 0 end as pt_mn_fd_C1,       
case when A.tipo_dia in ('Fim de semana') and periodo in ('tarde')and classe in ('C1') then IPC_anual else 0 end as pt_tr_fd_C1,        
case when A.tipo_dia in ('Fim de semana') and periodo in ('noite')and classe in ('C1') then IPC_anual else 0 end as pt_nt_fd_C1,            

case when A.tipo_dia in ('Util') and periodo in ('manha') and classe in ('C2') then pessoas else 0 end as pp_mn_nf_C2,
case when A.tipo_dia in ('Util') and periodo in ('tarde')and classe in ('C2') then pessoas else 0 end as pp_tr_nf_C2,
case when A.tipo_dia in ('Util') and periodo in ('noite')and classe in ('C2') then pessoas else 0 end as pp_nt_nf_C2,
case when A.tipo_dia in ('Util') and periodo in ('manha')and classe in ('C2') then IPC_anual else 0 end as pt_mn_nf_C2,
case when A.tipo_dia in ('Util') and periodo in ('tarde')and classe in ('C2') then IPC_anual else 0 end as pt_tr_nf_C2,
case when A.tipo_dia in ('Util') and periodo in ('noite')and classe in ('C2') then IPC_anual else 0 end as pt_nt_nf_C2,
case when A.tipo_dia in ('Fim de semana') and periodo in ('manha')and classe in ('C2') then pessoas else 0 end as pp_mn_fd_C2,
case when A.tipo_dia in ('Fim de semana') and periodo in ('tarde')and classe in ('C2') then pessoas else 0 end as pp_tr_fd_C2,
case when A.tipo_dia in ('Fim de semana') and periodo in ('noite')and classe in ('C2') then pessoas else 0 end as pp_nt_fd_C2,        
case when A.tipo_dia in ('Fim de semana') and periodo in ('manha')and classe in ('C2') then IPC_anual else 0 end as pt_mn_fd_C2,       
case when A.tipo_dia in ('Fim de semana') and periodo in ('tarde')and classe in ('C2') then IPC_anual else 0 end as pt_tr_fd_C2,        
case when A.tipo_dia in ('Fim de semana') and periodo in ('noite')and classe in ('C2') then IPC_anual else 0 end as pt_nt_fd_C2,            

case when A.tipo_dia in ('Util') and periodo in ('manha') and classe in ('D-E') then pessoas else 0 end as pp_mn_nf_DE,
case when A.tipo_dia in ('Util') and periodo in ('tarde')and classe in ('D-E') then pessoas else 0 end as pp_tr_nf_DE,
case when A.tipo_dia in ('Util') and periodo in ('noite')and classe in ('D-E') then pessoas else 0 end as pp_nt_nf_DE,
case when A.tipo_dia in ('Util') and periodo in ('manha')and classe in ('D-E') then IPC_anual else 0 end as pt_mn_nf_DE,
case when A.tipo_dia in ('Util') and periodo in ('tarde')and classe in ('D-E') then IPC_anual else 0 end as pt_tr_nf_DE,
case when A.tipo_dia in ('Util') and periodo in ('noite')and classe in ('D-E') then IPC_anual else 0 end as pt_nt_nf_DE,
case when A.tipo_dia in ('Fim de semana') and periodo in ('manha')and classe in ('D-E') then pessoas else 0 end as pp_mn_fd_DE,
case when A.tipo_dia in ('Fim de semana') and periodo in ('tarde')and classe in ('D-E') then pessoas else 0 end as pp_tr_fd_DE,
case when A.tipo_dia in ('Fim de semana') and periodo in ('noite')and classe in ('D-E') then pessoas else 0 end as pp_nt_fd_DE,        
case when A.tipo_dia in ('Fim de semana') and periodo in ('manha')and classe in ('D-E') then IPC_anual else 0 end as pt_mn_fd_DE,       
case when A.tipo_dia in ('Fim de semana') and periodo in ('tarde')and classe in ('D-E') then IPC_anual else 0 end as pt_tr_fd_DE,        
case when A.tipo_dia in ('Fim de semana') and periodo in ('noite')and classe in ('D-E') then IPC_anual else 0 end as pt_nt_fd_DE            

from playpen_analyst.nat_II_passo_45_demog A
where pessoas>0 and IPC_anual>0;


drop table if exists playpen_analyst.nat_II_passo_shapefile_2;
create table playpen_analyst.nat_II_passo_shapefile_2 as select 
B.id as zona,
B.area,
B.cidade,
B.uf,
B.regiao,   
coalesce(sum( pp_mn_nf),0) as  pp_mn_nf,
coalesce(sum( pp_tr_nf),0) as  pp_tr_nf,
coalesce(sum( pp_nt_nf),0) as  pp_nt_nf,
coalesce(sum( pt_mn_nf),0) as  pt_mn_nf,
coalesce(sum( pt_tr_nf),0) as  pt_tr_nf,
coalesce(sum( pt_nt_nf),0) as  pt_nt_nf,
coalesce(sum( pp_mn_fd),0) as  pp_mn_fd,
coalesce(sum( pp_tr_fd),0) as  pp_tr_fd,
coalesce(sum( pp_nt_fd),0) as  pp_nt_fd,
coalesce(sum( pt_mn_fd),0) as  pt_mn_fd,
coalesce(sum( pt_tr_fd),0) as  pt_tr_fd,
coalesce(sum( pt_nt_fd),0) as  pt_nt_fd,
coalesce(sum( pp_mn_nf_A),0) as  pp_mn_nf_A,
coalesce(sum( pp_tr_nf_A),0) as  pp_tr_nf_A,
coalesce(sum( pp_nt_nf_A),0) as  pp_nt_nf_A,
coalesce(sum( pt_mn_nf_A),0) as  pt_mn_nf_A,
coalesce(sum( pt_tr_nf_A),0) as  pt_tr_nf_A,
coalesce(sum( pt_nt_nf_A),0) as  pt_nt_nf_A,
coalesce(sum( pp_mn_fd_A),0) as  pp_mn_fd_A,
coalesce(sum( pp_tr_fd_A),0) as  pp_tr_fd_A,
coalesce(sum( pp_nt_fd_A),0) as  pp_nt_fd_A,
coalesce(sum( pt_mn_fd_A),0) as  pt_mn_fd_A,
coalesce(sum( pt_tr_fd_A),0) as  pt_tr_fd_A,
coalesce(sum( pt_nt_fd_A),0) as  pt_nt_fd_A,
coalesce(sum( pp_mn_nf_B1),0) as  pp_mn_nf_B1,
coalesce(sum( pp_tr_nf_B1),0) as  pp_tr_nf_B1,
coalesce(sum( pp_nt_nf_B1),0) as  pp_nt_nf_B1,
coalesce(sum( pt_mn_nf_B1),0) as  pt_mn_nf_B1,
coalesce(sum( pt_tr_nf_B1),0) as  pt_tr_nf_B1,
coalesce(sum( pt_nt_nf_B1),0) as  pt_nt_nf_B1,
coalesce(sum( pp_mn_fd_B1),0) as  pp_mn_fd_B1,
coalesce(sum( pp_tr_fd_B1),0) as  pp_tr_fd_B1,
coalesce(sum( pp_nt_fd_B1),0) as  pp_nt_fd_B1,
coalesce(sum( pt_mn_fd_B1),0) as  pt_mn_fd_B1,
coalesce(sum( pt_tr_fd_B1),0) as  pt_tr_fd_B1,
coalesce(sum( pt_nt_fd_B1),0) as  pt_nt_fd_B1,
coalesce(sum( pp_mn_nf_B2),0) as  pp_mn_nf_B2,
coalesce(sum( pp_tr_nf_B2),0) as  pp_tr_nf_B2,
coalesce(sum( pp_nt_nf_B2),0) as  pp_nt_nf_B2,
coalesce(sum( pt_mn_nf_B2),0) as  pt_mn_nf_B2,
coalesce(sum( pt_tr_nf_B2),0) as  pt_tr_nf_B2,
coalesce(sum( pt_nt_nf_B2),0) as  pt_nt_nf_B2,
coalesce(sum( pp_mn_fd_B2),0) as  pp_mn_fd_B2,
coalesce(sum( pp_tr_fd_B2),0) as  pp_tr_fd_B2,
coalesce(sum( pp_nt_fd_B2),0) as  pp_nt_fd_B2,
coalesce(sum( pt_mn_fd_B2),0) as  pt_mn_fd_B2,
coalesce(sum( pt_tr_fd_B2),0) as  pt_tr_fd_B2,
coalesce(sum( pt_nt_fd_B2),0) as  pt_nt_fd_B2,
coalesce(sum( pp_mn_nf_C1),0) as  pp_mn_nf_C1,
coalesce(sum( pp_tr_nf_C1),0) as  pp_tr_nf_C1,
coalesce(sum( pp_nt_nf_C1),0) as  pp_nt_nf_C1,
coalesce(sum( pt_mn_nf_C1),0) as  pt_mn_nf_C1,
coalesce(sum( pt_tr_nf_C1),0) as  pt_tr_nf_C1,
coalesce(sum( pt_nt_nf_C1),0)as  pt_nt_nf_C1,
coalesce(sum( pp_mn_fd_C1),0) as  pp_mn_fd_C1,
coalesce(sum( pp_tr_fd_C1),0) as  pp_tr_fd_C1,
coalesce(sum( pp_nt_fd_C1),0) as  pp_nt_fd_C1,
coalesce(sum( pt_mn_fd_C1),0) as  pt_mn_fd_C1,
coalesce(sum( pt_tr_fd_C1),0) as  pt_tr_fd_C1,
coalesce(sum( pt_nt_fd_C1),0) as  pt_nt_fd_C1,
coalesce(sum( pp_mn_nf_C2),0) as  pp_mn_nf_C2,
coalesce(sum( pp_tr_nf_C2),0) as  pp_tr_nf_C2,
coalesce(sum( pp_nt_nf_C2),0) as  pp_nt_nf_C2,
coalesce(sum( pt_mn_nf_C2),0) as  pt_mn_nf_C2,
coalesce(sum( pt_tr_nf_C2),0) as  pt_tr_nf_C2,
coalesce(sum( pt_nt_nf_C2),0) as  pt_nt_nf_C2,
coalesce(sum( pp_mn_fd_C2),0) as  pp_mn_fd_C2,
coalesce(sum( pp_tr_fd_C2),0) as  pp_tr_fd_C2,
coalesce(sum( pp_nt_fd_C2),0) as  pp_nt_fd_C2,
coalesce(sum( pt_mn_fd_C2),0) as  pt_mn_fd_C2,
coalesce(sum( pt_tr_fd_C2),0) as  pt_tr_fd_C2,
coalesce(sum( pt_nt_fd_C2),0) as  pt_nt_fd_C2,
coalesce(sum( pp_mn_nf_DE),0) as  pp_mn_nf_DE,
coalesce(sum( pp_tr_nf_DE),0) as  pp_tr_nf_DE,
coalesce(sum( pp_nt_nf_DE),0) as  pp_nt_nf_DE,
coalesce(sum( pt_mn_nf_DE),0) as  pt_mn_nf_DE,
coalesce(sum( pt_tr_nf_DE),0) as  pt_tr_nf_DE,
coalesce(sum( pt_nt_nf_DE),0) as  pt_nt_nf_DE,
coalesce(sum( pp_mn_fd_DE),0) as  pp_mn_fd_DE,
coalesce(sum( pp_tr_fd_DE),0) as  pp_tr_fd_DE,
coalesce(sum( pp_nt_fd_DE),0) as  pp_nt_fd_DE,
coalesce(sum( pt_mn_fd_DE),0) as  pt_mn_fd_DE,
coalesce(sum( pt_tr_fd_DE),0) as  pt_tr_fd_DE,
coalesce(sum( pt_nt_fd_DE),0) as  pt_nt_fd_DE,
geom
           
from playpen_analyst.nat_II_passo_shapefile A full join playpen_analyst.nat_ii_voronoi_v5 B on A.zona = B.id
group by B.id, B.area, B.cidade, B.uf, B.regiao, B.geom;



select sum(pp_mn_nf),sum(pt_mn_nf) from playpen_analyst.nat_II_passo_shapefile_2;



-----------------Ajustes complementares após a entrega --------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------


drop table if exists playpen_analyst.nat_II_passo_shapefile_3;
create table playpen_analyst.nat_II_passo_shapefile_3 as select 

((0.5*pt_mn_nf_a)+ (0.5*pt_tr_nf_a)+ (0.5*pt_mn_nf_b1)+ (0.5*pt_tr_nf_b1)) as pt_AB1_nf,
((0.5*pp_mn_nf_a)+ (0.5*pp_tr_nf_a)+ (0.5*pp_mn_nf_b1)+ (0.5*pp_tr_nf_b1)) as pp_AB1_nf,
((0.5*pt_mn_nf_b2)+ (0.5*pt_tr_nf_b2)+ (0.5*pt_mn_nf_c1)+ (0.5*pt_tr_nf_c1) + (0.5*pt_mn_nf_c2)+ (0.5*pt_tr_nf_c2)) as pt_B2C1C2_nf,
((0.5*pp_mn_nf_b2)+ (0.5*pp_tr_nf_b2)+ (0.5*pp_mn_nf_c1)+ (0.5*pp_tr_nf_c1) + (0.5*pp_mn_nf_c2)+ (0.5*pp_tr_nf_c2)) as pp_B2C1C2_nf,
((0.5*pt_mn_nf_de)+ (0.5*pt_tr_nf_de)) as pt_DE_nf,
((0.5*pp_mn_nf_de)+ (0.5*pp_tr_nf_de)) as pp_DE_nf,
	*
from playpen_analyst.nat_II_passo_shapefile_2;


select * from playpen_analyst.nat_II_passo_shapefile_3 limit 100;


---Tabela auxiliar para consulta dos CEP´s.

drop table if exists playpen_analyst.nat_II_passo_rank_cep;
create table playpen_analyst.nat_II_passo_rank_cep as select
	uf,
	regiao,
	cidade,
	zona,
	round(pt_B2C1C2_nf::numeric,0) as potencial_B2C1C2,
	round(pp_B2C1C2_nf::numeric,0) as pessoas_B2C1C2,
	round(pt_B2C1C2_nf::numeric,0)/(case when round(pp_B2C1C2_nf::numeric,0)>0 then round(pp_B2C1C2_nf::numeric,0) else 1 end) :: numeric pot_por_pess,
	row_number() over(partition by cidade order by pt_B2C1C2_nf desc) as rank,
	geom
from playpen_analyst.nat_II_passo_shapefile_3;


drop table if exists playpen_analyst.nat_II_passo_rank_cep_v2;
create table playpen_analyst.nat_II_passo_rank_cep_v2 as select
	A.uf,
	A.regiao,
	A.cidade,
	A.zona,
	A.rank,
	A.potencial_B2C1C2,
	A.pessoas_B2C1C2,
	round(pot_por_pess::numeric,0) as pot_por_pess,
	zonas_cidade as num_zonas_cidade,
	round(avg_potencial_B2C1C2::numeric,0)as avg_potencial_B2C1C2 ,
	round(avg_pessoas_B2C1C2::numeric,0)as avg_pessoas_B2C1C2,
	round(avg_pot_por_pess_B2C1C2::numeric,0) as avg_pot_por_pess_B2C1C2,
	A.geom	
from playpen_analyst.nat_II_passo_rank_cep A
left join (select max(rank)as zonas_cidade, 
		  avg(potencial_B2C1C2) as avg_potencial_B2C1C2,
                  avg(pessoas_B2C1C2) as avg_pessoas_B2C1C2, 
                  avg(pot_por_pess) as avg_pot_por_pess_B2C1C2,
                  cidade
           from playpen_analyst.nat_II_passo_rank_cep
           cidade
           group by cidade
          )B on A.cidade = B.cidade;



drop table if exists playpen_analyst.nat_II_passo_rank_cep_v3;
create table playpen_analyst.nat_II_passo_rank_cep_v3 as select
        B.cep,
	A.uf,
	A.regiao,
	A.cidade,
	A.zona,
	A.rank,
	A.potencial_B2C1C2,
	A.pessoas_B2C1C2,
	A.pot_por_pess,
	A.num_zonas_cidade,
	A.avg_potencial_B2C1C2 ,
	A.avg_pessoas_B2C1C2,
	A.avg_pot_por_pess_B2C1C2
	
from playpen_analyst.nat_II_passo_rank_cep_v2 A,
     playpen_analyst.cep B

where st_contains(A.geom,B.geom) and cep>0;

select * from playpen_analyst.nat_II_passo_rank_cep limit 1000;
select * from playpen_analyst.nat_II_passo_rank_cep_v2 limit 1000;
select * from playpen_analyst.nat_II_passo_rank_cep_v3;


-----------------Ajustes complementares a entrega para a fase 1--------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------

select * from playpen_analyst.natura_shp_v03_cep limit 100;
select * from playpen_analyst.nat_II_passo_shapefile_3_f1 limit 100;



drop table if exists playpen_analyst.nat_II_passo_shapefile_3_f1;
create table playpen_analyst.nat_II_passo_shapefile_3_f1 as select 
	'SP'::text as UF,
	'Regiao Metrop. SP'::text as regiao,
	munic_name as cidade,
	zona,
	natura_fas as pt_B2C1C2_nf,
	natura_f_1 as pp_B2C1C2_nf,
	A.geom	
from playpen_analyst.natura_shp_v03_cep A,
     playpen_analyst.natura_municipios B
where st_contains (B.geom,st_centroid(A.geom));


---Tabela auxiliar para consulta dos CEP´s.

drop table if exists playpen_analyst.nat_II_passo_rank_cep_f1;
create table playpen_analyst.nat_II_passo_rank_cep_f1 as select
	uf,
	regiao,
	cidade,
	zona,
	round(pt_B2C1C2_nf::numeric,0) as potencial_B2C1C2,
	round(pp_B2C1C2_nf::numeric,0) as pessoas_B2C1C2,
	round(pt_B2C1C2_nf::numeric,0)/(case when round(pp_B2C1C2_nf::numeric,0)>0 then round(pp_B2C1C2_nf::numeric,0) else 1 end) :: numeric pot_por_pess,
	row_number() over(partition by cidade order by pt_B2C1C2_nf desc) as rank,
	geom
from playpen_analyst.nat_II_passo_shapefile_3_f1;


drop table if exists playpen_analyst.nat_II_passo_rank_cep_f1_v2;
create table playpen_analyst.nat_II_passo_rank_cep_f1_v2 as select
	A.uf,
	A.regiao,
	A.cidade,
	A.zona,
	A.rank,
	A.potencial_B2C1C2,
	A.pessoas_B2C1C2,
	round(pot_por_pess::numeric,0) as pot_por_pess,
	zonas_cidade as num_zonas_cidade,
	round(avg_potencial_B2C1C2::numeric,0)as avg_potencial_B2C1C2 ,
	round(avg_pessoas_B2C1C2::numeric,0)as avg_pessoas_B2C1C2,
	round(avg_pot_por_pess_B2C1C2::numeric,0) as avg_pot_por_pess_B2C1C2,
	A.geom	
from playpen_analyst.nat_II_passo_rank_cep_f1 A
left join (select max(rank)as zonas_cidade, 
		  avg(potencial_B2C1C2) as avg_potencial_B2C1C2,
                  avg(pessoas_B2C1C2) as avg_pessoas_B2C1C2, 
                  avg(pot_por_pess) as avg_pot_por_pess_B2C1C2,
                  cidade
           from playpen_analyst.nat_II_passo_rank_cep_f1
           cidade
           group by cidade
          )B on A.cidade = B.cidade;



drop table if exists playpen_analyst.nat_II_passo_rank_cep_f1_v3;
create table playpen_analyst.nat_II_passo_rank_cep_f1_v3 as select
        B.cep,
	A.uf,
	A.regiao,
	A.cidade,
	A.zona,
	A.rank,
	A.potencial_B2C1C2,
	A.pessoas_B2C1C2,
	A.pot_por_pess,
	A.num_zonas_cidade,
	A.avg_potencial_B2C1C2 ,
	A.avg_pessoas_B2C1C2,
	A.avg_pot_por_pess_B2C1C2
	
from playpen_analyst.nat_II_passo_rank_cep_f1_v2 A,
     playpen_analyst.cep B

where st_contains(A.geom,B.geom) and cep>0;

select * from playpen_analyst.nat_II_passo_rank_cep_f1 limit 1000;
select * from playpen_analyst.nat_II_passo_rank_cep_f1_v2 limit 1000;
select * from playpen_analyst.nat_II_passo_rank_cep_f1_v3 order by pot_por_pess;
select * from playpen_analyst.nat_II_passo_rank_cep_v3 order by pot_por_pess desc limit 20000;


  