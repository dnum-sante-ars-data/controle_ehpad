
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
-- occupations 2022
occupation_2022_clean AS (
SELECT 
	"index",
	IIF(LENGTH(finess = 8), '0'|| finess, finess) AS finess,
	taux_occ_trimestre3,
	nb_lits_autorises_installes,
	nb_lits_occ_2022,
	taux_occ_2022,
	rowid
FROM occupation_2022
),
-- hébergement
hebergement_clean AS (
SELECT 
	"index",
	IIF(LENGTH(finesset = 8), '0'|| finesset, finesset) AS finesset,
	prixhebpermcs
FROM hebergement
),
-- ANAP
tdb_esms_2021_clean AS (
SELECT 
	IIF(LENGTH(finess_geographique = 8), '0'|| finess_geographique, finess_geographique) AS finess_geographique,
	personnes_gir_1,
	personnes_gir_2,
	personnes_gir_3,
	etp_directionencadrement,
	"-_dont_nombre_detp_reels_de_personnel_medical_dencadrement",
	"-_dont_autre_directionencadrement",
	etp_administration_gestion,
	etp_services_generaux,
	etp_restauration,
	"etp_socio-educatif",
	"-_dont_nombre_detp_reels_daide_medico-psychologique",
	"-_dont_nombre_detp_reels_danimateur",
	"-_dont_nombre_detp_reels_de_moniteur_educateur_au_3112",
	"-_dont_nombre_detp_reels_deducateur_specialise_au_3112",
	"-_dont_nombre_detp_reels_dassistant_social_au_3112",
	"-_dont_autre_socio-educatif",
	etp_paramedical,
	"-_dont_nombre_detp_reels_dinfirmier",
	"-_dont_nombre_detp_reels_daide_medico-psychologique1",
	"-_dont_nombre_detp_reels_daide_soignant",
	"-_dont_nombre_detp_reels_de_kinesitherapeute",
	"-_dont_nombre_detp_reels_de_psychomotricien",
	"-_dont_nombre_detp_reels_dergotherapeute",
	"-_dont_nombre_detp_reels_dorthophoniste",
	"-_dont_autre_paramedical",
	etp_psychologue,
	etp_ash,
	etp_medical,
	"-_dont_nombre_detp_reels_de_medecin_coordonnateur",
	"-_dont_autre_medical",
	etp_personnel_education_nationale,
	etp_autres_fonctions
FROM "export-tdbesms-2021-region-agg"
)
-- test nb lignes starts here part 1/2
--SELECT 
--COUNT(*)
--FROM (
-- test nb lignes stops here part 1/2, go below for the 2nd part
	SELECT
--	identfication de l'établissement
	r.reg AS "Region",
	d.dep AS "Code dép",
	d.libelle AS "Département",
	tf.finess AS "FINESS géographique",
	tf.categ_lib AS "Catégorie",
	tf.rs AS "Raison sociale ET",
	tf.ej_finess AS "FINESS juridique",
	tf.ej_rs AS "Raison sociale EJ",
	tf.statut_jur_lib AS "Statut juridique",
	tf.adresse AS "Adresse",
	tf.adresse_code_postal AS "Code postal",
	c.libelle AS "Commune",
	tf.com_code AS "Code commune INSEE",
--	caractéristiques ESMS
	o1.taux_occ_2020 AS "Taux d'occupation 2020",
	o2.taux_occ_2021 AS "Taux d'occupation 2021",
	o3.taux_occ_2022 AS "Taux d'occupation 2022",
	o3.taux_occ_trimestre3 AS "Taux occupation au 31/12/2022",
	h.prixhebpermcs AS "Prix de journée hébergement (EHPAD uniquement)",
	etra.personnes_gir_1 AS "Part de résidents GIR 1 (31/12/202X)",
	etra.personnes_gir_2 AS "Part de résidents GIR 2 (31/12/202X)",
	etra.personnes_gir_3 AS "Part de résidents GIR 3 (31/12/202X)",
	etra.etp_directionencadrement AS "Direction / Encadrement",
	etra."-_dont_nombre_detp_reels_de_personnel_medical_dencadrement" AS "dont personnel médical d'encadrement",
	etra."-_dont_autre_directionencadrement" AS "dont autre Direction / Encadrement",
	etra.etp_administration_gestion AS "Administration / Gestion",
	etra.etp_services_generaux AS "Services généraux",
	etra.etp_restauration AS "Restauration",
	etra."etp_socio-educatif" AS "Socio-éducatif",
	etra."-_dont_nombre_detp_reels_daide_medico-psychologique" AS "dont AMP",
	etra."-_dont_nombre_detp_reels_danimateur" AS "dont animateur",
	etra."-_dont_nombre_detp_reels_de_moniteur_educateur_au_3112" AS "dont moniteur éducateur",
	etra."-_dont_nombre_detp_reels_deducateur_specialise_au_3112" AS "dont éducateur spécialisé",
	etra."-_dont_nombre_detp_reels_dassistant_social_au_3112" AS "dont assistant(e) social(e)",
	etra."-_dont_autre_socio-educatif" AS "dont autre socio-éducatif",
	etra.etp_paramedical AS "Paramédical",
	etra."-_dont_nombre_detp_reels_dinfirmier" AS "dont infirmier",
	etra."-_dont_nombre_detp_reels_daide_medico-psychologique1" AS "dont AMP",
	etra."-_dont_nombre_detp_reels_daide_soignant" AS "dont aide-soignant(e) ",
	etra."-_dont_nombre_detp_reels_de_kinesitherapeute" AS "dont kinésithérapeute",
	etra."-_dont_nombre_detp_reels_de_psychomotricien" AS "dont psychomotricien(ne)",
	etra."-_dont_nombre_detp_reels_dergotherapeute" AS "dont ergothérapeute",
	etra."-_dont_nombre_detp_reels_dorthophoniste" AS "dont orthophoniste",
	etra."-_dont_autre_paramedical" AS "dont autre paramédical",
	etra.etp_psychologue AS "Psychologue",
	etra.etp_ash AS "ASH",
	etra.etp_medical AS "Médical",
	etra."-_dont_autre_medical" AS "dont autre médical",
	etra.etp_personnel_education_nationale AS "Personnel éducation nationale",
	etra.etp_autres_fonctions AS "Autres fonctions",
--  signalements de SIVSS
	--signalements
	"" AS "Nombre d'EI sur la période 36mois",
	"" AS "nb EI/EIG : Acte de prévention",
	"" AS "nb EI/EIG : Autre prise en charge",
	"" AS "nb EI/EIG : Chute",
	"" AS "nb EI/EIG : Disparition inquiétante et fugues (Hors SDRE/SDJ/SDT)",
	"" AS "nb EI/EIG : Dispositif médical",
	"" AS "nb EI/EIG : Fausse route",
	"" AS "nb EI/EIG : Infection associée aux soins (IAS) hors ES",
	"" AS "nb EI/EIG : Infection associée aux soins en EMS et ambulatoire (IAS hors ES)",
	"" AS "nb EI/EIG : Parcours/Coopération interprofessionnelle",
	"" AS "nb EI/EIG : Prise en charge chirurgicale",
	"" AS "nb EI/EIG : Prise en charge diagnostique",
	"" AS "nb EI/EIG : Prise en charge en urgence",
	"" AS "nb EI/EIG : Prise en charge médicamenteuse",
	"" AS "nb EI/EIG : Prise en charge des cancers",
	"" AS "nb EI/EIG : Prise en charge psychiatrique",
	"" AS "nb EI/EIG : Suicide",
	"" AS "nb EI/EIG : Tentative de suicide"
	FROM 
	    --identification
		tfiness_clean tf 
		LEFT JOIN commune_2022 c on c.com = tf.com_code 
		LEFT JOIN departement_2022 d on d.dep = c.dep 
		LEFT JOIN region_2022  r on d.reg = r.reg
		--caractéristiques ESMS
		LEFT JOIN occupation_2019_2020 o1 on o1.finess_19 = tf.finess 
		LEFT JOIN occupation_2021 o2  on o2.finess = tf.finess 
		LEFT JOIN occupation_2022_clean o3  on o3.finess = tf.finess 
		LEFT JOIN hebergement_clean h on h.finesset = tf.finess 
		LEFT JOIN tdb_esms_2021_clean etra on etra.finess_geographique = tf.finess
		--signalements
-- test nb lignes starts here part2/2
--		) identification
-- test nb lignes stops here part2/2
