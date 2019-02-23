--Neuquen

--CLEANED UP CODE for creating neuquen, using:

--uses 'inferred' POI, weighted average of the overlapping locations
--extended home dwell hours for weekends
--work dwells 9am-4pm
--min home threshold of 3 days, work 3 days

--0. get valid user population - those seen at least 10 days in the month.

drop table if exists playpen_analyst.AR_ulm00_valid_users;
create table playpen_analyst.AR_ulm00_valid_users as
        select  user_id, count(distinct date_dt) count_days
        from    cip.dwell
        where   date_dt between '20150406' and '20150503'
        group by user_id
        having  count(distinct date_dt) >=10;
        
alter table playpen_analyst.AR_ulm00_valid_users add primary key (user_id);

--1. create table of locations that are actually used by dwells
--(1 min)
drop table if exists playpen_analyst.AR_ulm01_dwell_location;
create table playpen_analyst.AR_ulm01_dwell_location as 
        SELECT DISTINCT location_id
        from            cip.dwell d
        where           location_id <> 0
        and             date_dt between '20150406' and '20150503';
        
alter table     playpen_analyst.AR_ulm01_dwell_location add primary key (location_id);

--1.1 add columns to the table. Done as separate step to reduce processing time.
alter table playpen_analyst.AR_ulm01_dwell_location
       add column centroid_geo_wgs84 geometry,
       add column location_geo_wgs84 geometry,
       add column centroid_lat_wgs84 numeric,
       add column centroid_lon_wgs84 numeric,
       add column length_x_m int8;

--1.2 copy the geometry field from cip.location to speed query time later on      
update  playpen_analyst.AR_ulm01_dwell_location 
set     centroid_geo_wgs84 = l.centroid_geo_wgs84, 
        location_geo_wgs84 = l.location_geo_wgs84,
        centroid_lon_wgs84 = l.centroid_lon_wgs84, 
        centroid_lat_wgs84 = l.centroid_lat_wgs84,
        length_x_m         = l.length_x_m
from    cip.location l
where   l.location_id = AR_ulm01_dwell_location.location_id;

CREATE INDEX ulm01_location_geo_gix ON playpen_analyst.AR_ulm01_dwell_location USING GIST (location_geo_wgs84);
CREATE INDEX ulm01_centroid_geo_gix ON playpen_analyst.AR_ulm01_dwell_location USING GIST (centroid_geo_wgs84);


--ULM process

--2.    Creates a buffer around location grids to enlarge locations that are considered the same POI.

--For Votuporanga and neuquen, no buffer was used and only overlapping/touching locations were grouped together
--drop table if exists    playpen_analyst.ulm02_loc_buffers;
--create table playpen_analyst.ulm02_loc_buffers as 
--        select  l.location_id, ST_Buffer(l.location_geo_wgs84::geography,length_x_m/4) buffer_geo
--        from    cip.location l  
--        join    playpen_analyst.AR_ulm01_dwell_location udl
--        on 	l.location_id = udl.location_id;

--3.    Create overlaps table. Normally based on the buffers in step 2, but for neuquen, we not using buffers, but only those 
--      this version uses intersections without buffers (simpler and faster)

drop table if exists    playpen_analyst.AR_ulm03_loc_overlaps;
create table            playpen_analyst.AR_ulm03_loc_overlaps as
        SELECT          l1.location_id loc1, l2.location_id loc2
        from            playpen_analyst.AR_ulm01_dwell_location l1
        cross join      playpen_analyst.AR_ulm01_dwell_location l2
        where           ST_Intersects(l1.location_geo_wgs84,l2.location_geo_wgs84) = true;

--3.1 create primary key and indexes
--(<1 sec)
alter table playpen_analyst.AR_ulm03_loc_overlaps add primary key(loc1,loc2);


--create index bigger_olo_loc2_idx2 on playpen_analyst.AR_ulm03_loc_overlaps(loc2);

--4.    create loc_visits table 
--      Groups together users and the locations they visit
--      and categorises the times into 
--              nighttime_visits (number of times they have night-time visits, weekdays before 7am & after 9pm and weekends before 9am and after 10pm, when people are normally at home);
--              nighttime_days - unique days using same criteria
--              daytime_visits - weekdays between 9 and 4
--              daytime_days - unique days using same criteria
--              other_visits - non work or home

--(2 minutes, 14m rows for 1 week)
--(10 minutes for 1 month)
drop table if exists playpen_analyst.AR_ulm04_loc_visits;
create table playpen_analyst.AR_ulm04_loc_visits AS 
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
JOIN    playpen_analyst.AR_ulm00_valid_users vu
ON      d.user_id = vu.user_id
AND     d.location_id <> 0  --some invalid locations exist
WHERE   d.date_dt between '20150406' and '20150503'
GROUP BY        d.user_id, d.location_id;

--4.1   set count of 'other' visits
--(2.5 min)
update  playpen_analyst.AR_ulm04_loc_visits
set     other_visits = visits-nighttime_visits-daytime_visits;


--############## CHANGE TO CODE###############################################################################
--alter table     playpen_analyst.AR_ulm04_loc_visits add primary key(user_id, location_id);
create index    ulm04lv_user_id_loc_id on playpen_analyst.AR_ulm04_loc_visits(user_id, location_id);
create index    ulm04lv_location_id on playpen_analyst.AR_ulm04_loc_visits(location_id);
--create index    ulm04lv_user_id_poi_rank on playpen_analyst.AR_ulm04_loc_visits(user_id, poi_ranking);


--############################################################################################################
--4.2   assign poi_ranking
--this groups all related locations (i.e. those touching each other) as one poi
--compensates for different locations/cells being contacted when in the same location.

alter table     playpen_analyst.AR_ulm04_loc_visits add column poi_ranking smallint;


DO $$
DECLARE newcount int :=0;
        oldcount int :=0;
        thepoirank int :=0;
BEGIN
--	oldcount := 0
	newcount := (select count(*) from playpen_analyst.AR_ulm04_loc_visits where poi_ranking is null);
	thepoirank:= (SELECT coalesce(max(poi_ranking),0) from playpen_analyst.AR_ulm04_loc_visits) + 1;
	WHILE 		oldcount <> newcount LOOP --changed from newcount=0 because it would go into an endless loop if it can't assign any more due to data issues.
                UPDATE		playpen_analyst.AR_ulm04_loc_visits
                SET		poi_ranking =  	thepoirank
                FROM		(SELECT 	user_id, location_id, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY visits desc, location_id) RANK
                                FROM		playpen_analyst.AR_ulm04_loc_visits
                                WHERE		poi_ranking is null
                                AND             location_id <> 0) as visit_rank
                JOIN		playpen_analyst.AR_ulm03_loc_overlaps
                ON		visit_rank.location_id = AR_ulm03_loc_overlaps.loc1
                WHERE		rank = 1
                AND		visit_rank.user_id = AR_ulm04_loc_visits.user_id
                AND		AR_ulm03_loc_overlaps.loc2 = AR_ulm04_loc_visits.location_id
                AND		AR_ulm04_loc_visits.poi_ranking is null;
                oldcount := newcount;
                newcount := (select count(*) from playpen_analyst.AR_ulm04_loc_visits where poi_ranking is null);
                thepoirank := thepoirank + 1;
	END LOOP;
END $$


select min(poi_ranking) from playpen_analyst.AR_ulm04_loc_visits;
--###add index###
create index ulm04_user_id_poi_ranking on playpen_analyst.AR_ulm04_loc_visits(user_id, poi_ranking);
--###############

--*********************************************************************************************************************************************************************************************
--5.    Create POI table with 'inferred' pois
--*********************************************************************************************************************************************************************************************
--Create POI table, grouping together locations with the same poi_ranking, i.e. overlapping locations. These grouped locations are assigned as follows:
--     Home: if daytime dwells on 3 or more days
--     Work: if nighttime dwells on 3 or more days
--     Other: all others.
--If a location meets both home and work criteria, home 'wins' as it is assumed that user is at home during the day"
--Inferred POI Location is a weighted average of both the centroid location and radius.
--NB: For nationwide mxazil, to simplify and speed up the code, the most frequently visited location square will be used as the POI.

--1 min
DROP TABLE IF EXISTS playpen_analyst.AR_ulm05_pois_v3;
CREATE TABLE    playpen_analyst.AR_ulm05_pois_v3 as
SELECT          user_id, 
                poi_ranking, 
                sum(visits)::smallint visits, 
                sum(nighttime_days)::smallint nighttime_days, 
                sum(daytime_days)::smallint daytime_days, 
                sum(other_visits)::smallint other_visits,
                CASE WHEN sum(nighttime_days) >= 3 THEN 'home'
 --                    WHEN sum(daytime_days) >= 3 and sum(nighttime_days) <= 2 THEN 'work'
                     WHEN sum(daytime_days) >= 3  THEN 'work'
                     ELSE 'other'                 
                END category,
                'POINT('|| sum(centroid_lon_wgs84*visits)/sum(visits)||' '||sum(centroid_lat_wgs84*visits)/sum(visits)||')' wkt,
                sum(length_x_m*visits/2)/sum(visits) radius
--Antonio suggested an alternative method of pinpointing the centroid. Issue is that if a small and large location overlap, 
--the centroid will be 'dragged' closer to the larger location, which, in theory, will locate it closer to the less precise location.
--this code aimed to weight the smaller location higher, but I couldn't get it working correctly given deadlines. This should be revisited if 
--it is decided to keep the logic of 'inferred' POIs
--               'POINT('|| sum(centroid_lon_wgs84*visits*(1/length_x_m))/sum(visits*(1/length_x_m))||' '||sum(centroid_lat_wgs84*visits*(1/length_x_m))/sum(visits*(1/length_x_m))||')' wkt_antonio
--               sum(length_x_m*visits/2)/sum(visits) radius_antonio
FROM            playpen_analyst.AR_ulm04_loc_visits lv
JOIN            playpen_analyst.AR_ulm01_dwell_location l
ON              l.location_id = lv.location_id
GROUP BY        user_id, poi_ranking;



--(2 min)
alter table playpen_analyst.AR_ulm05_pois_v3 add primary key (user_id, poi_ranking);
create index ulm05_pois_cat_idx on playpen_analyst.AR_ulm05_pois_v3 (category);
create index ulm05_pois_ranking_idx on playpen_analyst.AR_ulm05_pois_v3 (poi_ranking);



--5.1   For people with home but no work, reassign 2nd/3rd etc homes to work if at least as many dwells in day as night.
--10 sec
drop table if exists update_poi;
create temp table update_poi as (
        select  user_id, poi_ranking
        from    playpen_analyst.AR_ulm05_pois_v3 p
        join    (
                select home.user_id from
                        (select distinct user_id from playpen_analyst.AR_ulm05_pois_v3 where category='home') home
                left join
                        (select distinct user_id from playpen_analyst.AR_ulm05_pois_v3 where category='work') work
                using(user_id)
                where work.user_id is null
                ) a
        using (user_id)
        where category='home'
        and   poi_ranking > 1
        and   daytime_days >= nighttime_days
        );

alter table update_poi add primary key (user_id, poi_ranking);

--(4 sec)
update  playpen_analyst.AR_ulm05_pois_v3
set     category = 'work'
from    update_poi u
where   AR_ulm05_pois_v3.user_id = u.user_id
and     AR_ulm05_pois_v3.poi_ranking = u.poi_ranking;


--5.2   create table that contains unique new poi locations, based on wkt and radius
--(2 min)
drop table if exists    playpen_analyst.AR_ulm05a_pois_unique_loc;
create table            playpen_analyst.AR_ulm05a_pois_unique_loc as
        select          wkt, radius, 
                        st_GeomFromText(wkt,4326) asgeom, 
                        st_setsrid(st_expand(st_buffer(st_GeomFromText(wkt,4326)::geography, radius)::geometry, 0),4326) poi_square
        from            playpen_analyst.AR_ulm05_pois_v3
        group by        wkt, radius;

--(22 sec)


alter table playpen_analyst.AR_ulm05a_pois_unique_loc add primary key (wkt, radius);
CREATE INDEX ulm05a_poiassquare_test ON playpen_analyst.AR_ulm05a_pois_unique_loc USING GIST (poi_square);


--5.3 adapts client´s zone system

drop table if exists playpen_analyst.zones_neuquen_2;
create table playpen_analyst.zones_neuquen_2 as select 
	id as zone_id,
	geom 
from playpen_analyst.zones_neuquen;


drop table if exists playpen_analyst.zones_neuquen_3;
create table playpen_analyst.zones_neuquen_3 as select
	zone_id,
	B.totalpobl as population,
	st_area(A.geom::geography)* 0.000001 as sq_km,
	A.geom
from playpen_analyst.zones_neuquen_2 A,
     playpen_analyst.neuquen_con_datos_4326 B
where st_contains(A.geom,st_centroid(B.geom));


drop table if exists playpen_analyst.zones_neuquen_4;
create table playpen_analyst.zones_neuquen_4 as select
	zone_id,
	sum(population) as pop,
	sq_km,
	st_buffer(geom,0.0) as geom
from playpen_analyst.zones_neuquen_3
group by zone_id,sq_km,geom;


--6. 'spread' pois with zones intersecting them. This is done based on:
--      1. Proportion of zone intersecting the POI. For larger zones, this will almost always be 1.0 (100%)
--      2. Proportion of the population density of the zones intersecting the POI. This was used instead of population for neuquen because the zones had such huge disparities
--         in density (i.e. small dense zones next to huge empty ones). In future, can use population
--      3. For neuquen, prop in 2 was weighted 3x, 1 weighted 1x, so favours densely populated area, even if more of location was on unpopulated place.

--(This step performed in 2 queries - one to find intersection and areas, one to get proportion of population to the total
--(40 min)


drop table if exists playpen_analyst.AR_ulm06_poi_to_zone_match_pop_dens;
create table playpen_analyst.AR_ulm06_poi_to_zone_match_pop_dens_v3 as 
        select  p.wkt,
                p.radius, 
                zone_id, 
                pop,
                st_area(st_intersection(geom, p.poi_square)) area_intersection,
                st_area(p.poi_square) area_poi_square,
                st_area(st_intersection(geom, p.poi_square))/st_area(p.poi_square)::numeric prop_by_area,
                pop/loc_pop.pop_of_zones_touching_loc prop_by_pop,
                (pop/sq_km)/loc_pop.pop_dens_of_zones_touching_loc prop_by_pop_dens
        from    playpen_analyst.AR_ulm05a_pois_unique_loc p
        join    playpen_analyst.zones_neuquen_4 z
        on      st_intersects(z.geom, p.poi_square)=true
        left join    (  --total population density of zones touching location
                select  wkt, radius, sum(pop) pop_of_zones_touching_loc, sum(pop/sq_km) pop_dens_of_zones_touching_loc
                from    playpen_analyst.zones_neuquen_4 z
                join    playpen_analyst.AR_ulm05a_pois_unique_loc p
                on      st_intersects(geom, p.poi_square)=true
                group by wkt, radius 
                ) loc_pop
         using (wkt, radius);

  
--6.1   adjust the prop_by_area for those locations only partially covered by zone squares, normally over water
--      (may have to make exceptions for some zones that are adjacent to land and shouldn't all be attributed to the zone in question)
--(11 sec)
update  playpen_analyst.AR_ulm06_poi_to_zone_match_pop_dens_v3
set     prop_by_area = prop_by_area / new_denominator
from    (
        select  wkt, radius, sum(prop_by_area) new_denominator 
        from    playpen_analyst.AR_ulm06_poi_to_zone_match_pop_dens_v3
        group by wkt, radius
        having  round(sum(prop_by_area)::numeric,2)<>1 
        ) a
where   a.wkt = AR_ulm06_poi_to_zone_match_pop_dens_v3.wkt
and     a.radius = AR_ulm06_poi_to_zone_match_pop_dens_v3.radius;


--6.2   Calculate the final proportion attributable to each intersecting zone, by setting the weighted average between the following proportions:
--              1. proportion of the area of each zone covering the location grid square  (weighted 1x)
--              2. proportion of the population density of all zones intersecting the grid square. (weighted 3x)

--      For example, if location 1 is intersected by zones A, B, and C
--      A covers 60% of location 1, B covers 30%, and C covers 10%
--      A has population density of 1000/sq km, B=3000, and C=6000. so A has 10% of pop dens, B 30%, and C 60%
--      if equally weighted, A would receive 35% of pois, B 30%, and C 35%
--      however this gives too much to large unpopulated zones. so used a 3 to 1 proportion of pop dens to area
--      would give a result of A - 22.5%, B - 30%, C - 47.5%
-- (7 sec)

alter table     playpen_analyst.AR_ulm06_poi_to_zone_match_pop_dens_v3 add column proportion numeric;
update          playpen_analyst.AR_ulm06_poi_to_zone_match_pop_dens_v3
set             proportion = (prop_by_pop_dens*3+prop_by_area)/4;

alter table     playpen_analyst.AR_ulm06_poi_to_zone_match_pop_dens_v3 add primary key(wkt,radius,zone_id);


--7.    Create user weight
--7.1   Part 1 - create user weight by zone
--      using only the 'top' home location (since users can have >1 home)
--7 sec

drop table if exists playpen_analyst.AR_ulm07b_weight_by_zone;
create table    playpen_analyst.AR_ulm07b_weight_by_zone as (
        select  z.zone_id, z.pop, sum(lz.proportion) user_count, 
                z.pop/sum(lz.proportion)::numeric expansion_factor,
                sum(lz.proportion)/z.pop::numeric poi_to_pop
        from    
                (
                select  user_id, poi_ranking, wkt, radius, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY nighttime_days desc) nighttime_rank
                from    playpen_analyst.AR_ulm05_pois_v3
                where category='home'
                ) uw
        join    playpen_analyst.AR_ulm06_poi_to_zone_match_pop_dens_v3 lz
        on      uw.wkt = lz.wkt
        and     uw.radius = lz.radius
        right join    playpen_analyst.zones_neuquen_4 z
        on      lz.zone_id = z.zone_id
        where uw.nighttime_rank=1
--        and     z.zone_id not in(1,2,4,5,7,9,10,13) --skipping these for neuquen as they're outliers and full sample not included.
--        and     fully_in_sample = true --skipping these for neuquen as they're outliers and full sample not included.
        group by z.zone_id, z.pop
);

 alter table playpen_analyst.AR_ulm07b_weight_by_zone  add primary key (zone_id);

select * from playpen_analyst.AR_ulm07b_weight_by_zone;
select sum(pop) from playpen_analyst.AR_ulm07b_weight_by_zone;

--7.2   supplemental for neuquen (or any partial mxazil database) - add in average weighting for zones not fully in sample
/*insert into  playpen_analyst.AR_ulm07b_weight_by_zone (
        select  z.zone_id, z.pop, 
                sum_user_count          user_count, 
                sum_pop/sum_user_count  expansion_factor,
                sum_user_count/sum_pop  poi_to_pop
        from    (
                select  sum(user_count) sum_user_count,
                        sum(z.pop)        sum_pop
                from    playpen_analyst.AR_ulm07b_weight_by_zone wbz
                join    playpen_analyst.zones_neuquen_4 z
                using   (zone_id)
                where z.fully_in_sample = true
                ) totals
        cross join
                playpen_analyst.zones_neuquen_4 z
        where z.fully_in_sample <> true
        );*/

--7.3   assign weights to users, based on their primary (most visited) home location
--(6 sec)

drop table if exists playpen_analyst.AR_ulm07_user_weight;
create table playpen_analyst.AR_ulm07_user_weight as
select user_id, sum(expansion_factor*proportion) user_weight
from    (
        select  user_id, wkt, radius, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY nighttime_days desc) nighttime_rank
        from    playpen_analyst.AR_ulm05_pois_v3
        where category='home'
        ) users_location

join    playpen_analyst.AR_ulm06_poi_to_zone_match_pop_dens_v3 lz
using   (wkt,radius)
join    playpen_analyst.AR_ulm07b_weight_by_zone zones 
on      lz.zone_id = zones.zone_id
where   users_location.nighttime_rank = 1
group by user_id;


--=======================================================================================================================
--8.    Identify journeys
--8.1   Pull together all journey information into one master table

DROP TABLE IF EXISTS		playpen_analyst.AR_ulm08_ref_journeys_simples_01;  
CREATE TABLE			playpen_analyst.AR_ulm08_ref_journeys_simples_01 (date_dt date, journey_id int, user_id bigint, user_weight float, start_poi_code int, end_poi_code int, start_location_id int8, end_location_id int8, start_zone int, end_zone int, split_loc_weight float, distance_m float, purpose_raw text, purpose text, purpose_work_others text, day_cat text, mode_ind int, start_dt timestamp, social_class text, affluence text);

DO $$
DECLARE
  vdate date:='2016-02-01';

BEGIN
WHILE vdate <= '2016-02-01'

LOOP
	INSERT INTO		playpen_analyst.AR_ulm08_ref_journeys_simples_01
	SELECT			j.date_dt,
                                j.journey_id,
                                j.user_id, 
                                uw.user_weight,
                                lv1.poi_ranking as start_poi_code,
                                lv2.poi_ranking as end_poi_code,
                                lv1.location_id as start_location_id,   
                                lv2.location_id as end_location_id,
                                lz1.zone_id as start_zone,
                                lz2.zone_id as end_zone,
                                (lz1.proportion * lz2.proportion) as split_loc_weight,
--				ROUND(st_distance(dl1.centroid_geo_wgs84::geography,dl2.centroid_geo_wgs84::geography)) as distance,
                                j.distance_m,
                                poi1.category||poi2.category AS purpose_raw,
                                CASE poi1.category||poi2.category
                                        WHEN 'homework' THEN 'OB_HBW' 
                                        WHEN 'workhome' THEN 'RT_HBW' 
                                        WHEN 'homeother' THEN 'OB_HBO' 
                                        WHEN 'otherhome' THEN 'RT_HBO'  
                                        WHEN 'homehome' THEN CASE WHEN random()>0.5 THEN 'OB_HBO' ELSE 'RT_HBO' END
                                        ELSE 'NHB' 
                                END as purpose,	
                                CASE poi1.category||poi2.category
                                        WHEN 'homework' THEN 'HBW' 
                                        WHEN 'workhome' THEN 'HBW' 
                                        ELSE 'other' 
                                END as purpose_work_others,	
                                CASE WHEN extract(isodow from date_dt) <=5 
                                        THEN 'weekday'
                                        ELSE 'weekend'
                                END as day_cat,
                                null as mode_ind,
                                j.start_dt
                                --crm.social_class
                                
        FROM 			playpen_analyst.AR_ulm07_user_weight uw
       -- LEFT JOIN               playpen_analyst.br_crm_2 crm --left joining until complete set is provided
       -- ON                      uw.user_id = crm.user_id
	JOIN			cip.journey j
	ON				j.user_id = uw.user_id
	JOIN			playpen_analyst.AR_ulm04_loc_visits_v2  lv1
	ON				j.start_location_id = lv1.location_id
	AND				j.user_id = lv1.user_id
	JOIN			playpen_analyst.AR_ulm04_loc_visits_v2  lv2
	ON				j.end_location_id = lv2.location_id
	AND				j.user_id = lv2.user_id
	JOIN			playpen_analyst.AR_ulm05_pois_v3 poi1
	ON				lv1.user_id = poi1.user_id
        AND				lv1.poi_ranking = poi1.poi_ranking
	JOIN			playpen_analyst.AR_ulm05_pois_v3 poi2
	ON				lv2.user_id = poi2.user_id
	AND				lv2.poi_ranking = poi2.poi_ranking
	JOIN			playpen_analyst.AR_ulm06_poi_to_zone_match_pop_dens_v3 lz1
	ON				poi1.wkt = lz1.wkt
	AND                             poi1.radius = lz1.radius
	JOIN			playpen_analyst.AR_ulm06_poi_to_zone_match_pop_dens_v3 lz2
	ON				poi2.wkt = lz2.wkt
	AND                             poi2.radius = lz2.radius
	WHERE 			j.date_dt = vdate ;
--	WHERE 			j.date_dt = '20150518' ;
        RAISE NOTICE '% Processed', vdate;             
        vdate := vdate + 1;
END LOOP;
END  
$$; 

-- Faz a uniao de todas as tabelas que foram geradas separadamente.

drop table if exists playpen_analyst.AR_ulm08_ref_journeys_simples;

create table playpen_analyst.AR_ulm08_ref_journeys_simples as select * from 

	playpen_analyst.AR_ulm08_ref_journeys_simples_01 union select *   from 
	playpen_analyst.AR_ulm08_ref_journeys_simples_02 union select  *  from 
	playpen_analyst.AR_ulm08_ref_journeys_simples_03 union select  *  from 
	playpen_analyst.AR_ulm08_ref_journeys_simples_04 union select  *  from 
	playpen_analyst.AR_ulm08_ref_journeys_simples_05 union select  *  from 
	playpen_analyst.AR_ulm08_ref_journeys_simples_06 union select  *  from 
	playpen_analyst.AR_ulm08_ref_journeys_simples_07 union select  *  from 
	playpen_analyst.AR_ulm08_ref_journeys_simples_08 union select  *  from 
	playpen_analyst.AR_ulm08_ref_journeys_simples_09 union select  *  from 
	playpen_analyst.AR_ulm08_ref_journeys_simples_10 union select  *  from 
	playpen_analyst.AR_ulm08_ref_journeys_simples_11 union select  *  from 
	playpen_analyst.AR_ulm08_ref_journeys_simples_12 union select  *  from 
	playpen_analyst.AR_ulm08_ref_journeys_simples_13 union select  *  from 
	playpen_analyst.AR_ulm08_ref_journeys_simples_14 union select  *  from 
	playpen_analyst.AR_ulm08_ref_journeys_simples_15 union select  *  from 
	playpen_analyst.AR_ulm08_ref_journeys_simples_16 union select  *  from 
	playpen_analyst.AR_ulm08_ref_journeys_simples_17 union select  *  from 
	playpen_analyst.AR_ulm08_ref_journeys_simples_18 union select  *  from 
	playpen_analyst.AR_ulm08_ref_journeys_simples_19 union select  *  from 
	playpen_analyst.AR_ulm08_ref_journeys_simples_20 union select  *  from 
	playpen_analyst.AR_ulm08_ref_journeys_simples_21 union select  *  from 
	playpen_analyst.AR_ulm08_ref_journeys_simples_22 union select  *  from 
	playpen_analyst.AR_ulm08_ref_journeys_simples_23 union select  *  from 
	playpen_analyst.AR_ulm08_ref_journeys_simples_24 union select  *  from 
	playpen_analyst.AR_ulm08_ref_journeys_simples_25 union select  *  from 
	playpen_analyst.AR_ulm08_ref_journeys_simples_26 union select  *  from 
	playpen_analyst.AR_ulm08_ref_journeys_simples_27 union select  *  from
	playpen_analyst.AR_ulm08_ref_journeys_simples_28;

ALTER TABLE playpen_analyst.AR_ulm08_ref_journeys_simples DROP COLUMN social_class, DROP COLUMN affluence;


-- Ajusta CRM em query separada

--Adiciona CRM

drop table if exists playpen_analyst.AR_ulm08_ref_journeys_simples_v2;
create table playpen_analyst.AR_ulm08_ref_journeys_simples_v2 as select 
	A.*,
	edad,
        sexo,
        peso
from playpen_analyst.AR_ulm08_ref_journeys_simples  A left join playpen_analyst.crm_ar_201504_user_id_2 B on A.user_id = B.user_id;


--8.2   Alter table with scaling factor fields

ALTER TABLE	playpen_analyst.AR_ulm08_ref_journeys_simples_v2 ADD COLUMN scale float;
ALTER TABLE	playpen_analyst.AR_ulm08_ref_journeys_simples_v2 ADD COLUMN scale_1000 float;

ALTER TABLE	playpen_analyst.AR_ulm08_ref_journeys_simples_v2 ADD COLUMN journey_weight_6 float;
ALTER TABLE	playpen_analyst.AR_ulm08_ref_journeys_simples_v2 ADD COLUMN journey_weight_6_1000 float;
ALTER TABLE	playpen_analyst.AR_ulm08_ref_journeys_simples_v2 ADD COLUMN journey_weight_9 float;
ALTER TABLE	playpen_analyst.AR_ulm08_ref_journeys_simples_v2 ADD COLUMN journey_weight_9_1000 float;
ALTER TABLE	playpen_analyst.AR_ulm08_ref_journeys_simples_v2 ADD COLUMN journey_weight_0 float;
ALTER TABLE	playpen_analyst.AR_ulm08_ref_journeys_simples_v2 ADD COLUMN journey_weight_0_1000 float;



--8.2   Simpler scaling - assume 1000 as radius of all via points, since we don't have that information for mx

-- (6 min)
UPDATE		playpen_analyst.AR_ulm08_ref_journeys_simples_v2
SET		scale_1000 = distance_m/1000,
                journey_weight_0_1000 = CASE WHEN distance_m/1000 = 0 or distance_m/1000 > 10 THEN peso*split_loc_weight ELSE (peso * split_loc_weight) * (1+0/exp(distance_m/1000)) END,
		journey_weight_6_1000 = CASE WHEN distance_m/1000 = 0 or distance_m/1000 > 10 THEN peso*split_loc_weight ELSE (peso * split_loc_weight) * (1+6/exp(distance_m/1000)) END,
                journey_weight_9_1000 = CASE WHEN distance_m/1000 = 0 or distance_m/1000 > 10 THEN peso*split_loc_weight ELSE (peso * split_loc_weight) * (1+9/exp(distance_m/1000)) END;


-- (4 min)
create index ulm08_crm_v3 on playpen_analyst.AR_ulm08_ref_journeys_simples_v2 (date_dt, journey_id, user_id);
create index ulm08_ref_journey_startend5_crm_v3 on playpen_analyst.AR_ulm08_ref_journeys_simples_v2(start_zone, end_zone);
create index ulm08_ref_journey_end5_crm_v3 on playpen_analyst.AR_ulm08_ref_journeys_simples_v2(end_zone);



--9. Produce matrices
--Table lookup_start_hr_period has 3 columns: start_hr, day_cat, period. Allows flexibility to designate which period for each hour of the day.
--neuquen - 1.5 min


DROP TABLE IF EXISTS playpen_analyst.AR_ulm08_ref_journeys_simples_v4;
CREATE TABLE         playpen_analyst.AR_ulm08_ref_journeys_simples_v4 AS

--9.1   create matrices using social class file provided by Luiz
--      FIRST SET OF MATRICES BY 5 'standard' purposes

SELECT  start_zone, end_zone, rj.day_cat, period, rj.purpose, edad,sexo,
--        SUM(journey_weight_9*split_loc_weight)/(CASE per.day_cat WHEN 'weekday' THEN 20 WHEN 'weekend' THEN 10 END) as daily_journeys,
          SUM(journey_weight_6_1000)/(CASE rj.day_cat WHEN 'weekday' THEN 20 WHEN 'weekend' THEN 8 END) as journeys
--        SUM(final_journey_weight)/(CASE rj.day_cat WHEN 'weekday' THEN 20 WHEN 'weekend' THEN 10 END) as journeys
	FROM   playpen_analyst.AR_ulm08_ref_journeys_simples_v2 rj

	JOIN   playpen_analyst.lookup_start_hr_period per
		ON extract(hour from rj.start_dt) = per.start_hr::numeric
		AND rj.day_cat = per.day_cat
	GROUP BY start_zone, end_zone, rj.day_cat, period, rj.purpose,edad,sexo
        ORDER BY start_zone, end_zone, rj.day_cat, period, rj.purpose,edad,sexo;


--9.4 Stochastic rounding to nearest 1 journey. This is acceptable since we're providing results as average weekday/weekend, so dividing by 20 and 10 respectively.

DROP TABLE IF EXISTS playpen_analyst.AR_ulm08_ref_journeys_simples_v5;
CREATE TABLE         playpen_analyst.AR_ulm08_ref_journeys_simples_v5 AS select * from playpen_analyst.AR_ulm08_ref_journeys_simples_v4;

UPDATE  playpen_analyst.AR_ulm08_ref_journeys_simples_v5
SET     journeys = CASE WHEN (RANDOM()) > (journeys - FLOOR(journeys)) THEN (FLOOR(journeys)) ELSE (CEILING(journeys)) END;


DROP TABLE IF EXISTS playpen_analyst.AR_ulm08_ref_journeys_simples_v6;
CREATE TABLE         playpen_analyst.AR_ulm08_ref_journeys_simples_v6 AS select * from playpen_analyst.AR_ulm08_ref_journeys_simples_v5
where journeys > 0;


--Ajusta para ter exatamente o formato de entrega

DROP TABLE IF EXISTS playpen_analyst.AR_ulm08_ref_journeys_simples_v7;
CREATE TABLE playpen_analyst.AR_ulm08_ref_journeys_simples_v7 AS select
	start_zone,
	end_zone,
	period,
	edad,
	sexo,
	sum(journeys) as journeys
from playpen_analyst.AR_ulm08_ref_journeys_simples_v6
where day_cat = 'weekday'
group by start_zone,end_zone,period,edad,sexo;



select sum(journeys) from  playpen_analyst.AR_ulm08_ref_journeys_simples_v7;--267759
select sum(pop) from playpen_analyst.zones_neuquen_4;--238108

select * from playpen_analyst.AR_ulm08_ref_journeys_simples_v7 order by journeys;




 

