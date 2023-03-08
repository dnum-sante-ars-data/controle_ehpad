WITH tfiness_clean as (
SELECT
	IIF(LENGTH(tf_with.finess = 8), '0'|| tf_with.finess, tf_with.finess) as finess,
	tf_with.categ_lib,
	tf_with.rs,
	tf_with.ej_finess,
	tf_with.ej_rs,
	tf_with.statut_jur_lib,
	tf_with.adresse_num_voie || tf_with.adresse_comp_voie || tf_with.adresse_type_voie || tf_with.adresse_nom_voie || tf_with.adresse_lieuditbp || tf_with.adresse_lib_routage as adresse,
	tf_with.adresse_code_postal,
	tf_with.com_code
FROM "t-finess" tf_with
WHERE
	tf_with.categ_code IN (182,183,186,192,194,195,196,209,246,255,354,370,377,379,381,390,395,396,437,445,500,501,502)
),
-- nombre de réclamation
table_recla as (
SELECT 
	IIF(LENGTH(se.ndeg_finessrpps = 8), '0'|| se.ndeg_finessrpps, se.ndeg_finessrpps) as finess,
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
	IIF(LENGTH(se.ndeg_finessrpps = 8), '0'|| se.ndeg_finessrpps, se.ndeg_finessrpps) as finess, 
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
table_signalement as (
SELECT
	declarant_organismendeg_finess,
	survenue_du_cas_en_collectivitendeg_finess,
	date_de_reception,
	reclamation,
	declarant_type_etablissement_si_esems
FROM "020820221058_Extraction_BFC"
UNION
SELECT
	declarant_organismendeg_finess,
	survenue_du_cas_en_collectivitendeg_finess,
	date_de_reception,
	reclamation,
	declarant_type_etablissement_si_esems
FROM "101020221034_Extraction_Occ"
UNION
SELECT
	declarant_organismendeg_finess,
	survenue_du_cas_en_collectivitendeg_finess,
	date_de_reception,
	reclamation,
	declarant_type_etablissement_si_esems
FROM "130720220948_Extraction_ARA"
),
-- info signalement
sign as (
SELECT 
	finess,
	COUNT(*) as nb_signa
FROM 
(SELECT 
	CASE 
		WHEN substring(tb.declarant_organismendeg_finess,-9) == substring(CAST(tb.survenue_du_cas_en_collectivitendeg_finess as text),1,9)
			THEN substring(tb.declarant_organismendeg_finess,-9)
		WHEN tb.survenue_du_cas_en_collectivitendeg_finess IS NULL
			THEN substring(tb.declarant_organismendeg_finess,-9)
		ELSE 
			substring(CAST(tb.survenue_du_cas_en_collectivitendeg_finess as text),1,9)
	END as finess, *
FROM table_signalement tb  
WHERE 
	tb.reclamation != 'Oui'
	AND tb.date_de_reception >= '2018-01-01' 
	AND tb.date_de_reception < '2023-01-01'
	AND tb.declarant_type_etablissement_si_esems like "%Etablissement d'hébergement pour personnes âgées dépendantes%") as sub_table
GROUP BY 1
),
-- Pour checker les "MOTIF IGAS"
recla_signalement as(
SELECT
	tfc.finess,
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
	tfiness_clean tfc 
	LEFT JOIN table_recla tr on tr.finess = tfc.finess
	LEFT JOIN igas i on i.finess = tfc.finess
	LEFT JOIN sign s on s.finess = tfc.finess
)
SELECT 
	r.reg as REG,
	d.dep as DEP,
	d.libelle as LIBELLE,
	tf.finess,
	tf.categ_lib,
	tf.rs,
	tf.ej_finess,
	tf.ej_rs,
	tf.statut_jur_lib,
	tf.adresse,
	tf.adresse_code_postal,
	c.libelle as LIBELLE_COMMUNE,
	com_code,
	cta.capacite_autorisee_totale_,
	ce.total_heberg_comp_inter_places_autorisees,
	ce.total_accueil_de_jour_places_autorisees,
	ce.total_accueil_de_nuit_places_autorisees,
	o1.taux_occ_2020,
	o2.taux_occ_2021,
	o3.taux_occ_2022,
	o3.nb_lits_occ_2022,
	etra.nombre_total_de_chambres_installees_au_3112,
	o3.taux_occ_trimestre3,
	h.prixhebpermcs,
	gp.gmp,
	gp.pmp,
	etra.personnes_gir_1,
	etra.personnes_gir_2,
	etra.personnes_gir_3,
	eira.taux_plus_10_medics_cip13,
	eira.taux_atu,
	eira.taux_hospit_mco,
	eira.taux_hospit_had,
	ec.charges_dexploitation,
	ep.groupe_i__produits_de_la_tarification,  
	ep2.unnamed_1 as produit_compte_70,
	ep3.produits_dexploitation,
	"Saisie des indicateurs du TDB MS (campagne 2022)",
	d2.taux_dabsenteisme_hors_formation_en_,
	etra2.taux_dabsenteisme_hors_formation_en_,
	etra.taux_dabsenteisme_hors_formation_en_,
	"Rotation moyenne du personnel sur la période 2019-2021",
	d2.taux_detp_vacants_en_,
	etra2.taux_detp_vacants,
	etra.taux_detp_vacants,
	etra.dont_taux_detp_vacants_concernant_la_fonction_soins,
	etra.dont_taux_detp_vacants_concernant_la_fonction_socio_educative, 
	d3.taux_de_prestations_externes_sur_les_prestations_directes_en_,
	etra2.taux_de_prestations_externes_sur_les_prestations_directes,
	etra.taux_de_prestations_externes_sur_les_prestations_directes,
	"Taux moyen de prestations externes sur les prestations directes",
	d3.nombre_de_personnes_accompagnees_dans_leffectif_au_3112,
	etra2.nombre_de_personnes_accompagnees_dans_leffectif_au_3112,
	etra.nombre_de_personnes_accompagnees_dans_leffectif_au_3112,
	"Nombre moyen d'ETP par usager sur la période 2018-2020",
	"etra.etpsoin",
	etra.etp_directionencadrement, 
	etra."-_dont_nombre_detp_reels_de_personnel_medical_dencadrement",
	etra."-_dont_autre_directionencadrement",
	etra.etp_administration_gestion,
	etra.etp_services_generaux,
	etra.etp_restauration,
	etra."etp_socio-educatif",
	etra."-_dont_nombre_detp_reels_daide_medico-psychologique",
	etra."-_dont_nombre_detp_reels_danimateur",
	etra."-_dont_nombre_detp_reels_de_moniteur_educateur_au_3112",
	etra."-_dont_nombre_detp_reels_deducateur_specialise_au_3112",
	etra."-_dont_nombre_detp_reels_dassistant_social_au_3112",
	etra.etp_paramedical,
	etra."-_dont_nombre_detp_reels_dinfirmier",
	etra."-_dont_nombre_detp_reels_daide_medico-psychologique1",
	etra."-_dont_nombre_detp_reels_daide_soignant",
	etra."-_dont_nombre_detp_reels_de_kinesitherapeute",
	etra."-_dont_nombre_detp_reels_de_psychomotricien",
	etra."-_dont_nombre_detp_reels_dergotherapeute",
	etra."-_dont_nombre_detp_reels_dorthophoniste",
	etra."-_dont_autre_paramedical",
	etra.etp_psychologue,
	etra.etp_ash,
	etra.etp_medical,
	etra."-_dont_nombre_detp_reels_de_medecin_coordonnateur",
	etra."-_dont_autre_medical",
	etra.etp_personnel_education_nationale,
	etra.etp_autres_fonctions,
	"total faire somme etp sauf dont",
	rs.nb_recla as "Nombre de réclamations sur la période 2018-2022",
	"Rapport réclamations / capacité",
	rs."Hôtellerie-locaux-restauration" as "Recla IGAS : Hôtellerie-locaux-restauration",
	rs."Problème d?organisation ou de fonctionnement de l?établissement ou du service" as "Recla IGAS : Problème d’organisation ou de fonctionnement de l’établissement ou du service",
	rs."Problème de qualité des soins médicaux" as "Recla IGAS : Problème de qualité des soins médicaux",
	rs."Problème de qualité des soins paramédicaux" as "Recla IGAS : Problème de qualité des soins paramédicaux",
	rs."Recherche d?établissement ou d?un professionnel" as "Recla IGAS : Recherche d’établissement ou d’un professionnel",
	rs."Mise en cause attitude des professionnels" as "Recla IGAS : Mise en cause attitude des professionnels",
	rs."Informations et droits des usagers" as "Recla IGAS : Informations et droits des usagers",
	rs."Facturation et honoraires" as "Recla IGAS : Facturation et honoraires",
	rs."Santé-environnementale" as "Recla IGAS : Santé-environnementale",
	rs."Activités d?esthétique réglementées" as "Recla IGAS : Activités d’esthétique réglementées",	
	rs.nb_signa as "Nombre d'EI sur la période 36mois",
	"Nombre d'EIG sur la période 2020-2023",
	"Nombre d'EIAS sur la période 2020-2022",
	"Somme EI + EIGS + EIAS sur la période 2020-2022",
	"nb EI/EIG : Acte de prévention",
	"nb EI/EIG : Autre prise en charge",
	"nb EI/EIG : Chute",
	"nb EI/EIG : Disparition inquiétante et fugues (Hors SDRE/SDJ/SDT)",
	"nb EI/EIG : Dispositif médical",
	"nb EI/EIG : Fausse route",
	"nb EI/EIG : Infection associée aux soins (IAS) hors ES",
	"nb EI/EIG : Infection associée aux soins en EMS et ambulatoire (IAS hors ES)",
	"nb EI/EIG : Parcours/Coopération interprofessionnelle",
	"nb EI/EIG : Prise en charge chirurgicale",
	"nb EI/EIG : Prise en charge diagnostique",
	"nb EI/EIG : Prise en charge en urgence",
	"nb EI/EIG : Prise en charge médicamenteuse",
	"nb EI/EIG : Prise en charge des cancers",
	"nb EI/EIG : Prise en charge psychiatrique",
	"nb EI/EIG : Suicide",
	"nb EI/EIG : Tentative de suicide",
	"ICE 2022 (réalisé)",
	"Inspection SUR SITE 2022 - Déjà réalisée",
	"Controle SUR PIECE 2022 - Déjà réalisé",
	"Inspection / contrôle Programmé 2023"
FROM 
	tfiness_clean tf 
	LEFT JOIN commune_2022 c on c.com = tf.com_code 
	LEFT JOIN departement_2022 d on d.dep = c.dep 
	LEFT JOIN region_2022  r on d.reg = r.reg
	LEFT JOIN capacites_ehpad ce on ce."et-ndegfiness"  = tf.finess 
	LEFT JOIN capacite_totale_auto cta on IIF(LENGTH(cta.numero_finess_et_ = 8), '0'|| cta.numero_finess_et_, cta.numero_finess_et_) = tf.finess
	LEFT JOIN occupation_2019_2020 o1 on o1.finess_19 = tf.finess 
	LEFT JOIN occupation_2021 o2  on o2.finess = tf.finess 
	LEFT JOIN occupation_2022 o3  on IIF(LENGTH(o3.finess = 8), '0'|| o3.finess, o3.finess) = tf.finess 
	LEFT JOIN "export-tdbesms-2021-region-agg" etra on IIF(LENGTH(etra.finess_geographique = 8), '0'|| etra.finess_geographique, etra.finess_geographique) = tf.finess 
	LEFT JOIN hebergement h on IIF(LENGTH(h.finesset = 8), '0'|| h.finesset, h.finesset) = tf.finess 
	LEFT JOIN gmp_pmp gp on IIF(LENGTH(gp.finess_et = 8), '0'|| gp.finess_et, gp.finess_et) = tf.finess
	LEFT JOIN EHPAD_Indicateurs_2021_REG_agg eira on eira.et_finess = tf.finess
	LEFT JOIN errd_charges ec on SUBSTRING(ec."structure_-_finess_-_raison_sociale",1,9) = tf.finess  
	LEFT JOIN errd_produitstarif ep on SUBSTRING(ep."structure_-_finess_-_raison_sociale",1,9) = tf.finess
	LEFT JOIN errd_produits70 ep2 on SUBSTRING(ep2."structure_-_finess_-_raison_sociale",1,9) = tf.finess
	LEFT JOIN errd_produitsencaiss ep3 on SUBSTRING(ep3."structure_-_finess_-_raison_sociale",1,9) = tf.finess
	LEFT JOIN diamant_2019 d2 on SUBSTRING(d2.finess,1,9) = tf.finess
	LEFT JOIN "export-tdbesms-2020-region_agg" etra2 on IIF(LENGTH(etra2.finess_geographique = 8), '0'|| etra2.finess_geographique, etra2.finess_geographique) = tf.finess
	LEFT JOIN diamantç2019_2 d3 on SUBSTRING(d3.finess,1,9) = tf.finess
	LEFT JOIN recla_signalement rs on rs.finess = tf.finess 
--test capacité totale 
	
	
--SELECT *
--FROM capacite_totale_auto cta
--WHERE cta.numero_finess_et_ = 100001239
 