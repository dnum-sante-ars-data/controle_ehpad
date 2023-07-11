WITH finess_clean as 
(SELECT
	IIF(LENGTH(tf_with.finess )= 8, '0'|| tf_with.finess, tf_with.finess) as finess,
	tf_with.categ_lib,
	tf_with.rs,
	IIF(LENGTH(tf_with.ej_finess )= 8, '0'|| tf_with.ej_finess, tf_with.ej_finess) as ej_finess,
	tf_with.ej_rs,
	tf_with.statut_jur_lib,
	IIF(tf_with.adresse_num_voie IS NULL, '',SUBSTRING(CAST(tf_with.adresse_num_voie as TEXT),1,LENGTH(CAST(tf_with.adresse_num_voie as TEXT))-2) || ' ')  || 
		IIF(tf_with.adresse_comp_voie IS NULL, '',tf_with.adresse_comp_voie || ' ')  || 
		IIF(tf_with.adresse_type_voie IS NULL, '',tf_with.adresse_type_voie || ' ') || 
		IIF(tf_with.adresse_nom_voie IS NULL, '',tf_with.adresse_nom_voie || ' ') || 
		IIF(tf_with.adresse_lieuditbp IS NULL, '',tf_with.adresse_lieuditbp || ' ') || 
		IIF(tf_with.adresse_lib_routage IS NULL, '',tf_with.adresse_lib_routage) as adresse,
	IIF(LENGTH(tf_with.ej_finess )= 8, '0'|| tf_with.ej_finess, tf_with.ej_finess) as ej_finess,
	CAST(adresse_code_postal AS INTEGER) as adresse_code_postal,
	tf_with.com_code,
	c.typecom 
FROM 
	"t-finess" tf_with
	LEFT JOIN commune_2022 c on c.com = tf_with.com_code AND  c.dep is not null
	LEFT JOIN departement_2022 d on d.dep = c.dep
	LEFT JOIN region_2022  r on d.reg = r.reg 
WHERE
	tf_with.categ_code IN (159,
							160,
							162,
							165,
							166,
							172,
							175,
							176,
							177,
							178,
							180,
							182,
							183,
							184,
							185,
							186,
							188,
							189,
							190,
							191,
							192,
							193,
							194,
							195,
							196,
							197,
							198,
							199,
							2,
							200,
							202,
							205,
							207,
							208,
							209,
							212,
							213,
							216,
							221,
							236,
							237,
							238,
							241,
							246,
							247,
							249,
							250,
							251,
							252,
							253,
							255,
							262,
							265,
							286,
							295,
							343,
							344,
							354,
							368,
							370,
							375,
							376,
							377,
							378,
							379,
							381,
							382,
							386,
							390,
							393,
							394,
							395,
							396,
							397,
							402,
							411,
							418,
							427,
							434,
							437,
							440,
							441,
							445,
							446,
							448,
							449,
							450,
							453,
							460,
							461,
							462,
							463,
							464,
							500,
							501,
							502,
							606,
							607,
							608,
							609,
							614,
							633)
	)
SELECT *
FROM finess_clean
WHERE typecom isnull

SELECT *  
FROM commune_2022 c
	LEFT JOIN commune_2022 c_a_d on c.com = c_a_d.comparent and c.typecom IN ('COM')

	
SELECT *
FROM commune_2022 c 
WHERE c.com in (1059)











