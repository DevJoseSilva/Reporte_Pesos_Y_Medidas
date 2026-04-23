
drop temporary table if exists tmp_mlm;
CREATE TEMPORARY TABLE tmp_mlm (
mlm VARCHAR(100)
)
;
LOAD DATA LOCAL INFILE '{path_file}'
INTO TABLE tmp_mlm
LINES TERMINATED BY '\r\n'
(mlm)
;

ALTER TABLE tmp_mlm ADD INDEX idx_mlm (mlm); 


drop temporary table if exists tp_mlm_publicados;

create temporary table tp_mlm_publicados(
	arc_id_en_canal varchar(100),
	arc_art_id int,
	ctc_nombre varchar(250)
) ;

insert INTO tp_mlm_publicados (arc_id_en_canal, arc_art_id, ctc_nombre)
SELECT apcc.arc_id_en_canal, 
	apcc.arc_art_id, 
	cc.ctc_nombre
FROM tmp_mlm tp1
INNER JOIN articulos_publicados_cuentas_canales apcc
    ON tp1.mlm = apcc.arc_id_en_canal AND apcc.arc_eliminado IS NULL
LEFT JOIN cuentas_canales cc
    ON cc.ctc_id = apcc.arc_ctc_id AND cc.ctc_eliminado IS NULL
;
ALTER TABLE tp_mlm_publicados ADD INDEX idx_art (arc_art_id);

drop temporary table if exists tp_sku_distinto;
create temporary table tp_sku_distinto(
	tp_art_id int,
	tp_sku varchar(250)
);
insert into tp_sku_distinto
select DISTINCT arc_art_id,
	arc_art_id
from tp_mlm_publicados
;
create index idx_sku on tp_sku_distinto(tp_art_id, tp_sku);


DROP TEMPORARY TABLE IF EXISTS tp_skus;
create temporary table tp_skus(
	apv_art_id int,
	apv_sku varchar(250),
	prv_nombre varchar(150),
	mar_nombre varchar(250),
	tpa_id int null
);
insert into tp_skus (apv_art_id, apv_sku, prv_nombre, mar_nombre, tpa_id)
SELECT 
    ap.apv_art_id,
    ap.apv_sku,
    p.prv_nombre,
    m.mar_nombre,
    a.art_tpa_id 
FROM articulos_proveedores ap
INNER JOIN tp_sku_distinto tp1
    ON ap.apv_art_id = tp1.tp_sku
   AND ap.apv_principal = 1
   AND ap.apv_eliminado IS NULL
INNER JOIN proveedores p ON p.prv_id = ap.apv_prv_id AND p.prv_eliminado IS NULL
INNER JOIN articulos a   ON a.art_id  = tp1.tp_art_id AND a.art_eliminado IS NULL
INNER JOIN marcas m      ON a.art_mar_id = m.mar_id    AND m.mar_eliminado IS NULL
;
ALTER TABLE tp_skus ADD INDEX idx_sku (apv_art_id, apv_sku, tpa_id);



SELECT
    arc_id_en_canal AS id_en_canal,
    ctc_nombre      AS cuenta,
    prv_nombre      AS proveedor,
    mar_nombre      AS marca,
    apv_sku         AS sku_gme,
    CONCAT(
        'https://www.mercadolibre.com.mx/publicaciones/listado?page=1&search=',
        arc_id_en_canal,
        '&sort=DEFAULT'
    ) AS permalink
FROM tp_mlm_publicados tp1
LEFT JOIN tp_skus tp2 ON tp1.arc_art_id = tp2.apv_art_id
;

select tp1.apv_sku as sku_padre ,ap.apv_sku as sku_hijo from tp_skus tp1
inner join ensambles e 
	ON tp1.apv_art_id = e.ens_art_padre
    AND e.ens_eliminado IS NULL
JOIN articulos_proveedores ap 
                ON e.ens_art_hijo = ap.apv_art_id
                AND ap.apv_principal = 1
                AND ap.apv_eliminado IS NULL
where tp1.tpa_id = 2

