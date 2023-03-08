-- nombre de réclamation
WITH table_recla as (
SELECT 
	se.ndeg_finessrpps as finess,
	COUNT(*) as nb_recla
FROM sirec_export se
WHERE 
	se.ndeg_finessrpps  IS NOT NULL
	AND se.Signalement != 'Oui'
	AND se.date_de_la_demande_du_requerant  >= '2018-01-01' 
	AND se.date_de_la_demande_du_requerant < '2023-01-01'
	AND se.domaine_fonctionnel  = 'Médico-Social - Personnes âgées'
GROUP BY 1
),
-- Motig IGAS
igas as (
SELECT 
	se.ndeg_finessrpps as finess, 
	SUM(IIF(se.motifs_igas like '%Hôtellerie-locaux-restauration%',1,0)) as "Hôtellerie-locaux-restauration",
	SUM(IIF(se.motifs_igas like '%Problème d?organisation ou de fonctionnement de l?établissement ou du service%',1,0)) as "Problème d?organisation ou de fonctionnement de l?établissement ou du service",
	SUM(IIF(se.motifs_igas like '%Problème de qualité des soins médicaux%',1,0)) as "Problème de qualité des soins médicaux",
	SUM(IIF(se.motifs_igas like '%Problème de qualité des soins paramédicaux%',1,0)) as "Problème de qualité des soins paramédicaux",
	SUM(IIF(se.motifs_igas like '%Recherche d?établissement ou d?un professionnel%',1,0)) as "Recherche d?établissement ou d?un professionnel",
	SUM(IIF(se.motifs_igas like '%Mise en cause attitude des professionnels%',1,0)) as "Mise en cause attitude des professionnels",
	SUM(IIF(se.motifs_igas like '%Informations et droits des usagers%',1,0)) as "Informations et droits des usagers",
	SUM(IIF(se.motifs_igas like '%Facturation et honoraires%',1,0)) as "Facturation et honoraires",
	SUM(IIF(se.motifs_igas like '%Santé-environnementale%',1,0)) as "Santé-environnementale",
	SUM(IIF(se.motifs_igas like '%Activités d?esthétique réglementées%',1,0)) as "Activités d?esthétique réglementées",
	SUM(IIF(se.motifs_igas like '%A renseigner%',1,0)) as "A renseigner",
	SUM(IIF(se.motifs_igas like '%COVID-19%',1,0)) as "COVID-19"
FROM sirec_export se
WHERE 
	se.signalement = 'Non'
	AND se.ndeg_finessrpps  IS NOT NULL
	AND se.date_de_la_demande_du_requerant  >= '2018-01-01' 
	AND se.date_de_la_demande_du_requerant < '2023-01-01'
	AND se.domaine_fonctionnel  = 'Médico-Social - Personnes âgées'
GROUP BY 1
),
-- info signalement
sign as (
SELECT 
	finess,
	COUNT(*) as nb_signa
FROM 
(SELECT 
	CASE 
		WHEN substring(eb.declarant_organismendeg_finess,-9) == substring(CAST(eb.survenue_du_cas_en_collectivitendeg_finess as text),1,9)
			THEN substring(eb.declarant_organismendeg_finess,-9)
		WHEN eb.survenue_du_cas_en_collectivitendeg_finess IS NULL
			THEN substring(eb.declarant_organismendeg_finess,-9)
		ELSE 
			substring(CAST(eb.survenue_du_cas_en_collectivitendeg_finess as text),1,9)
	END as finess, *
FROM "020820221058_Extraction_BFC" eb  
WHERE 
	eb.reclamation != 'Oui'
	AND eb.date_de_reception >= '2018-01-01' 
	AND eb.date_de_reception < '2023-01-01'
	AND eb.declarant_type_etablissement_si_esems like "%Etablissement d'hébergement pour personnes âgées dépendantes%") as sub_table
GROUP BY 1
)
-- Pour checker les "MOTIF IGAS"
SELECT
	tf.finess,
	s.nb_signa,
	tr.nb_recla,
	i."Hôtellerie-locaux-restauration",
	i."Problème d?organisation ou de fonctionnement de l?établissement ou du service",
	i."Problème de qualité des soins médicaux",
	i."Problème de qualité des soins paramédicaux",
	i."Recherche d?établissement ou d?un professionnel",
	i."Mise en cause attitude des professionnels",
	i."Informations et droits des usagers",
	i."Facturation et honoraires",
	i."Santé-environnementale",
	i."Activités d?esthétique réglementées",
	i."A renseigner",
	i."COVID-19"
FROM 
	"t-finess" tf 
	LEFT JOIN table_recla tr on tf.finess = tr.finess
	LEFT JOIN igas i on i.finess = tf.finess
	LEFT JOIN sign s on s.finess = tf.finess