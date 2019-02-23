CREATE TABLE playpen_analyst.br_vot_regioes_ibge(
	"Meso" character varying,
	"Micro" character varying,
	"Municipio" character varying,
	"Distrito" character varying
)WITH (OIDS=FALSE);

--Rodar pelo pgsql
\copy playpen_analyst.crm_social_class from c:/Users/R345386/Desktop/crm_social_class.csv  with delimiter as ';' CSV HEADER quote as '"'


Zona,"ZonaAgreg","RM"

drop table if exists playpen_analyst.br_ooh_agreg_posvenda1;
CREATE TABLE playpen_analyst.br_ooh_agreg_posvenda1(
	"zona_OOH" numeric(8,0),
	"ID_agreg" numeric(8,0),
	"RM" character varying
)WITH (OIDS=FALSE);




drop table playpen_analyst.br_clientid_zipcode;

-- problema de permissão -> rodar pelo pgsql com \ na frente
copy cluster.amostra from '/home/tdi/Área de Trabalho/cluster_Amostra_V2.csv'  with delimiter as ',' CSV HEADER quote as '"';

COPY br_zipcode_census
TO '/home/tdi/Área de Trabalho/CSV- Desktop/br_zipcode_census.csv'
WITH DELIMITER ';'
CSV HEADER


create table playpen_analyst.br_location_test as select * from cip.location where length_x_m = 250;


\COPY playpen_analyst.br_location_test
TO '/Users/R345386/Desktop/CIP/Databases/br_location_test.csv'
WITH DELIMITER ';'
CSV HEADER


drop table if exists playpen_analyst.br_cgi_lat_long_v2;
CREATE TABLE playpen_analyst.br_cgi_lat_long_v2(
	"cd_cgi" character varying,
	"ds_lngt" character varying,
	"ds_lttd" character varying
	
)WITH (OIDS=FALSE);

--Rodar pelo pgsql
\copy playpen_analyst.br_cgi_lat_long_v2 from 'F:/upload smart steps/cgi_lat_long_v2.csv'  with delimiter as ',' CSV HEADER quote as '"'


CREATE TABLE playpen_analyst.vot_renda(
	"user_id" character varying,
	"user_id_crm" character varying,
	"gender" character varying,
	"age" character varying,
	"socio_economics" character varying,
	"socio_economics_ob" character varying
)WITH (OIDS=FALSE);

--Rodar pelo pgsql
\copy playpen_analyst.vot_renda from d:/users_id_vot_crm.csv  with delimiter as ';' CSV HEADER quote as '"'


CREATE TABLE PLAYPEN_ANALYST.co_valle_cauca_idade(
	"Codigo" character varying,
	"0_a_4" numeric(8,0),
	"5_a_9" numeric(8,0),
	"10_a_14" numeric(8,0),
	"15_a_19" numeric(8,0),
	"20_a_24" numeric(8,0),
	"25_a_29" numeric(8,0),
	"30_a_34" numeric(8,0),
	"35_a_39" numeric(8,0),
	"40_a_44" numeric(8,0),
	"45_a_49" numeric(8,0),
	"50_a_54" numeric(8,0),
	"55_a_59" numeric(8,0),
	"60_a_64" numeric(8,0),
	"65_a_69" numeric(8,0),
	"70_a_74" numeric(8,0),
	"75_a_79" numeric(8,0),
	"80_o_mas" numeric(8,0),
	"A1_T" numeric(8,0)
)WITH (OIDS=FALSE);



DROP TABLE IF EXISTS playpen_analyst.lookup_start_hr_period;
CREATE TABLE playpen_analyst.lookup_start_hr_period(	
	"daycat" character varying,
	"hour" numeric(8,0),
	"period"  character varying
)WITH (OIDS=FALSE);

\copy playpen_analyst.br_vot_regioes_ibge from d:/time_bands.csv  with delimiter as ';' CSV HEADER quote as '"'


select * from playpen_analyst.lookup_start_hr_period;

drop table if exists playpen_analyst.CRM_jul_2017_parte5;
CREATE TABLE playpen_analyst.CRM_jul_2017_parte5(
        "Idade" numeric(8,0),
        "ID_sexo" character varying,
        "Renda" numeric(8,0),
        "ds_cluster" character varying,
        "hash" character varying,
	"row_number" numeric(8,0)
		
)WITH (OIDS=FALSE);

--Rodar pelo pgsql
\copy playpen_analyst.vot_renda from d:/users_id_vot_crm.csv  with delimiter as ';' CSV HEADER quote as '"'


drop table if exists playpen_analyst.CRM5_link;
CREATE TABLE playpen_analyst.CRM5_link(       
        "user_id" character varying,
        "hash" character varying		
)WITH (OIDS=FALSE);
