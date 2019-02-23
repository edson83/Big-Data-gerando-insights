CREATE TABLE playpen_analyst.br_vot_regioes_ibge(
	"Meso" character varying,
	"Micro" character varying,
	"Municipio" character varying,
	"Distrito" character varying
)WITH (OIDS=FALSE);

--Rodar pelo pgsql
\copy playpen_analyst.crm_social_class from c:/Users/R345386/Desktop/crm_social_class.csv  with delimiter as ';' CSV HEADER quote as '"'


-- problema de permissão -> rodar pelo pgsql com \ na frente
copy cluster.amostra from '/home/tdi/Área de Trabalho/cluster_Amostra_V2.csv'  with delimiter as ',' CSV HEADER quote as '"';

COPY br_zipcode_census
TO '/home/tdi/Área de Trabalho/CSV- Desktop/br_zipcode_census.csv'
WITH DELIMITER ';'
CSV HEADER




