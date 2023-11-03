# -*- coding: utf-8 -*-

import json
from os import listdir
import pandas as pd
from modules.init_db.init_db import _connDb
from utils import utils
import json


def _executeTransform(region):
    #Appeler les requetes sql
    dbname = utils.read_settings('settings/settings_demo.json',"db","name")
    """conn = _connDb(dbname)
    conn.create_function("NULLTOZERO", 1, _nullToZero)
    conn.create_function("MOY3", 3, _moy3)
    if region == 32:
        print('Exécution requête ciblage HDF')
        df_ciblage = pd.read_sql_query( _requeteCiblageHDF(region), conn)
        print('Exécution requête controle HDF')
        df_controle = pd.read_sql_query( _requeteControleHDF(region), conn)
    else : 
        print('Exécution requête ciblage')
        df_ciblage = pd.read_sql_query( _requeteCiblage(region), conn)
        print('Exécution requête controle')
        df_controle = pd.read_sql_query( _requeteControle(region), conn)
    return df_ciblage, df_controle"""
    dbname = utils.read_settings('settings/settings_demo.json',"db","name")
    conn = _connDb(dbname)
    cursor = connection.cursor()
    parametres = {
    "region" :region, 
    "param_N": "2023",
    "param_N_1": "2022",
    "param_N_2": "2021",
    "param_N_3": "2020",
    "param_N_4": "2019"
	}
    with open('requete.json',"r") as fichier:
         data=json.load(fichier)
        
    if region == 32:
        print('Exécution requête ciblage HDF')
        df_ciblage=cursor.execute(query, parametres)
        print('Exécution requête controle HDF')
        query=data["requete_finale"][0]['requete_controle']
        df_controle=cursor.execute(query, parametres) 
    else : 
        print('Exécution requête ciblage')
        query=data["requete_finale"][0]['requete_ciblage']
        df_ciblage=cursor.execute(query, parametres)
        print('Exécution requête controle')
        query=data["requete_finale"][0]['requete_controle']
        df_controle=cursor.execute(query, parametres) 
    return df_ciblage, df_controle


def _inittable():
    dbname=utils.read_settings('settings/settings_demo.json',"db","name")
    conn = _connDb(dbname)
    cursor = conn.cursor()
    parametres = {
    "param_N": "2023",
    "param_N_1": "2022",
    "param_N_2": "2021",
    "param_N_3": "2020",
    "param_N_4": "2019",
    "param_fin_mois":"2023-09-30",
    "param_debut_mois":"2023-10-01",
    "param_debut_mois_N_3":"2020-10-01"
        }
    with open('modules/transform/requete.json',"r") as fichier:
        data=json.load(fichier)


    for req in data["table_intermediaire"]:
        for table,query in req.items():
            query_final=query
            for cle, valeur in parametres.items():
                cle_placeholder = "{{" + cle + "}}"
                query_final = query_final.replace(cle_placeholder, valeur)
            cursor.execute(query_final, parametres)
            conn.commit()
            print(table, "a été ajouté")
    return

  
       
        
    
# Definiton des functions utiles en SQL
def _nullToZero(value):
    if value is None:
        return 0
    else:
        return value
    
def _moy3(value1, value2, value3):
    value_list = [value1,value2,value3]
    res = []
    for val in value_list:
        if val != None :
            res.append(str(val).replace(",", '.'))
    if len(res)== 0:
        return None
    else :
        clean_res = [float(i) for i in res]
        return sum(clean_res)/len(clean_res) #statistics.mean(res)

def _functionCreator():
    dbname = utils.read_settings('settings/settings_demo.json',"db","name")
    conn = _connDb(dbname)
    return

# Requete signalement et réclamation
# Jointure des tables de t-finess + signalement
def _testNomRegion(region):
    dbname = utils.read_settings('settings/settings_demo.json',"db","name")
    conn = _connDb(dbname)
    test  = '''SELECT 'oui'
	FROM region_{n} r 
	WHERE r.ncc = '{}'
    '''.format(region)
    df = pd.read_sql_query(test, conn)
    return df


def _requeteCiblage(region):
    requeteCiblage = '''-- CIBLAGE
WITH tfiness_clean as (
SELECT
	IIF(LENGTH(tf_with.finess )= 8, '0'|| tf_with.finess, tf_with.finess) as finess,
	tf_with.categ_lib,
    tf_with.categ_code,
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
	tf_with.com_code 
FROM "t-finess" tf_with
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
),
-- nombre de réclamation
table_recla as (
SELECT 
	IIF(LENGTH(se.ndeg_finessrpps )= 8, '0'|| se.ndeg_finessrpps, se.ndeg_finessrpps) as finess,
	COUNT(*) as nb_recla
FROM reclamations se
WHERE 
	se.ndeg_finessrpps  IS NOT NULL
	AND (se.Signalement = 'Non' or se.Signalement IS NULL)
GROUP BY 1
),
-- Motig IGAS
igas as (
SELECT 
	IIF(LENGTH(se.ndeg_finessrpps )= 8, '0'|| se.ndeg_finessrpps, se.ndeg_finessrpps) as finess, 
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
FROM reclamations se
WHERE 
	(se.Signalement = 'Non' or se.Signalement IS NULL)
	AND se.ndeg_finessrpps  IS NOT NULL
GROUP BY 1
),
table_signalement AS (
	SELECT 
    declarant_organismendeg_finess, 
    survenue_du_cas_en_collectivitendeg_finess,
    date_de_reception, reclamation, 
    declarant_type_etablissement_si_esems, 
    ceci_est_un_eigs, 
    famille_principale, 
    nature_principale  
    FROM all_sivss
),
-- info signalement
sign as (
SELECT 
	finess,
	COUNT(*) as nb_signa,
	SUM(IIF(famille_principale = 'Evénements/incidents dans un établissement ou organisme' AND ceci_est_un_eigs = 'Non',1,0)) as "Nombre d'EI sur la période 36mois",
	SUM(IIF(ceci_est_un_eigs = 'Oui', 1, 0)) as NB_EIGS,
	SUM(IIF(famille_principale = 'Evénements indésirables/graves associés aux soins' AND ceci_est_un_eigs = 'Non',1,0)) AS NB_EIAS
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
--	AND tb.declarant_type_etablissement_si_esems like "%Etablissement d'hébergement pour personnes âgées dépendantes%"
	) as sub_table
GROUP BY 1
),
-- Pour checker les "MOTIF IGAS"
recla_signalement as(
SELECT
	tfc.finess,
	s.nb_signa,
	tr.nb_recla,
	s."Nombre d'EI sur la période 36mois",
	s.NB_EIGS,
	s.NB_EIAS,
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
),
clean_occupation_n as (
SELECT 
	IIF(LENGTH(o3.finess) = 8, '0'|| o3.finess, o3.finess) as finess, 
	o3.taux_occ_{n},
	o3.nb_lits_occ_{n},
	o3.taux_occ_trimestre3 
FROM occupation_{n} o3 
),
clean_capacite_totale_auto as (
SELECT 
	IIF(LENGTH(cta.etiquettes_de_lignes )= 8, '0'|| cta.etiquettes_de_lignes, cta.etiquettes_de_lignes) as finess,
	cta.somme_de_capacite_autorisee_totale_ 
FROM capacite_totale_auto cta 
),
clean_hebergement as (
SELECT 
	IIF(LENGTH(h.finesset )= 8, '0'|| h.finesset, h.finesset) as finess,
	h.prixhebpermcs 
FROM hebergement h 
),
clean_tdb_{n_2} as (
SELECT 
	IIF(LENGTH(tdb_{n_2}.finess_geographique) = 8, '0'|| tdb_{n_2}.finess_geographique, tdb_{n_2}.finess_geographique) as finess,
	*
FROM "export-tdbesms-{n_2}-region_agg" tdb_{n_2}
), 
clean_tdb_{n_1} as (
SELECT 
	IIF(LENGTH(tdb_{n_1}.finess_geographique )= 8, '0'|| tdb_{n_1}.finess_geographique, tdb_{n_1}.finess_geographique) as finess,
	*
FROM "export-tdbesms-{n_1}-region-agg" tdb_{n_1}
), 
-- La partie suivante sert à créer un table temporaire pour classer les charges et les produits
correspondance as(
SELECT  
	SUBSTRING(cecpp."finess_-_rs_et" ,1,9) as finess,
	cecpp.cadre
FROM 
	choix_errd_ca_pa_ph cecpp
	LEFT JOIN doublons_errd_ca dou on SUBSTRING(dou.finess,1,9) = SUBSTRING(cecpp."finess_-_rs_et",1,9) AND cecpp.cadre != 'ERRD' 
WHERE
	dou.finess IS NULL
),
grouped_errd_charges AS (
SELECT 
	SUBSTRING(ec."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(ec.charges_dexploitation) as sum_charges_dexploitation
FROM errd_charges ec
GROUP BY 1
),
grouped_errd_produitstarif AS (
SELECT 
	SUBSTRING(ep."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(ep.groupe_i__produits_de_la_tarification) as sum_groupe_i__produits_de_la_tarification
FROM errd_produitstarif ep
GROUP BY 1
),
grouped_errd_produits70 AS (
SELECT 
	SUBSTRING(ep2."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(ep2.unnamed_1) as sum_produits70
FROM errd_produits70 ep2
GROUP BY 1
),
grouped_errd_produitsencaiss AS (
SELECT 
	SUBSTRING(ep3."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(ep3.produits_dexploitation) as sum_produits_dexploitation
FROM errd_produitsencaiss ep3
GROUP BY 1
),
grouped_caph_charges AS (
SELECT 
	SUBSTRING(cch."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(cch.charges_dexploitation) as sum_charges_dexploitation
FROM caph_charges cch 
GROUP BY 1
),
grouped_caph_produitstarif AS (
SELECT 
	SUBSTRING(cch2."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(cch2.groupe_i__produits_de_la_tarification) as sum_groupe_i__produits_de_la_tarification
FROM caph_produitstarif cch2
GROUP BY 1
),
grouped_caph_produits70 AS (
SELECT
	SUBSTRING(cch3."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(cch3.unnamed_1) as sum_produits70
FROM caph_produits70 cch3
GROUP BY 1
),
grouped_caph_produitsencaiss AS (
SELECT 
	SUBSTRING(cch4."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(cch4.produits_dexploitation) as sum_produits_dexploitation
FROM caph_produitsencaiss cch4
GROUP BY 1
),
grouped_capa_charges AS (
SELECT 
	SUBSTRING(cc."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(cc.charges_dexploitation) as sum_charges_dexploitation
FROM capa_charges cc 
GROUP BY 1
),
grouped_capa_produitstarif AS (
SELECT 
	SUBSTRING(cpt."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(cpt.produits_de_lexercice) as sum_groupe_i__produits_de_la_tarification
FROM capa_produitstarif cpt 
GROUP BY 1
),
charges_produits as (
SELECT 
	cor.finess,
	CASE 
		WHEN cor.cadre = 'ERRD'
			THEN gec.sum_charges_dexploitation
		WHEN cor.cadre = 'CA PA'
			THEN gc.sum_charges_dexploitation
		WHEN cor.cadre = 'CA PH'
			THEN gcch.sum_charges_dexploitation 
	END as "Total des charges",
	CASE 
		WHEN cor.cadre = 'ERRD'
			THEN gep.sum_groupe_i__produits_de_la_tarification
		WHEN cor.cadre = 'CA PA'
			THEN gcp.sum_groupe_i__produits_de_la_tarification
		WHEN cor.cadre = 'CA PH'
			THEN gcch2.sum_groupe_i__produits_de_la_tarification
	END as "Produits de la tarification",
	CASE 
		WHEN cor.cadre = 'ERRD'
			THEN gep2.sum_produits70
		WHEN cor.cadre = 'CA PA'
			THEN 0
		WHEN cor.cadre = 'CA PH'
			THEN gcch3.sum_produits70
	END as "Produits du compte 70",
	CASE 
		WHEN cor.cadre = 'ERRD'
			THEN gep3.sum_produits_dexploitation
		WHEN cor.cadre = 'CA PA'
			THEN 0
		WHEN cor.cadre = 'CA PH'
			THEN gcch4.sum_produits_dexploitation
	END as "Total des produits (hors c/775, 777, 7781 et 78)"
FROM 
	correspondance cor
	LEFT JOIN grouped_errd_charges gec on gec.finess = cor.finess AND cor.cadre = 'ERRD' 
	LEFT JOIN grouped_errd_produitstarif gep on gep.finess = cor.finess AND cor.cadre = 'ERRD'
	LEFT JOIN grouped_errd_produits70 gep2 on gep2.finess = cor.finess AND cor.cadre = 'ERRD'
	LEFT JOIN grouped_errd_produitsencaiss gep3 on gep3.finess = cor.finess AND cor.cadre = 'ERRD'
	LEFT JOIN grouped_caph_charges gcch on gcch.finess = cor.finess AND cor.cadre = 'CA PH'
	LEFT JOIN grouped_caph_produitstarif gcch2 on gcch2.finess = cor.finess AND cor.cadre = 'CA PH'
	LEFT JOIN grouped_caph_produits70 gcch3 on gcch3.finess = cor.finess AND cor.cadre = 'CA PH'
	LEFT JOIN grouped_caph_produitsencaiss gcch4 on gcch4.finess = cor.finess AND cor.cadre = 'CA PH'
	LEFT JOIN grouped_capa_charges gc on gc.finess = cor.finess AND cor.cadre = 'CA PA'
	LEFT JOIN grouped_capa_produitstarif gcp on gcp.finess = cor.finess AND cor.cadre = 'CA PA'
),
inspections as (
SELECT 
	IIF(LENGTH(code_finess)= 8, '0'|| code_finess, code_finess) as finess,
	count(distinct identifiant_de_la_mission) as "ICE N (réalisé)",
	SUM(IIF(type_de_mission = 'Inspection' OR type_de_mission = 'Inspection Technique' OR type_de_mission = 'Evaluation' OR type_de_mission = 'Contrôle' OR type_de_mission = 'Enquête administrative' OR type_de_mission = 'Visites de conformité' OR type_de_mission = 'Contrôle sur place / Visite de vérification', 1, 0)) as "Inspection SUR SITE N - Déjà réalisée",
	SUM(IIF(type_de_mission = 'Contrôle sur pièces' OR type_de_mission = 'Contrôle sur pièces EHPAD' OR type_de_mission = 'EHPAD Contrôle sur pièces' OR type_de_mission = 'Ctrl_sur_Pièces', 1, 0)) as "Controle SUR PIECE N - Déjà réalisé",
	SUM(IIF(type_de_planification = "Programmé", 1, 0)) as "Inspection / contrôle Programmé 2023"
FROM "ESMS export-mission-4 evts 01 03 23 v2"
GROUP BY code_finess
),
communes as (
SELECT c.com, c.dep, c.ncc  
FROM commune_{n} c 
WHERE c.reg IS NOT NULL
UNION ALL
SELECT c.com, c2.dep, c.ncc
FROM commune_{n} c 
	LEFT JOIN commune_{n} c2 on c.comparent = c2.com AND c2.dep IS NOT NULL
WHERE c.reg IS NULL and c.com != c.comparent
)
SELECT 
--identfication de l'établissement
	r.ncc as Region,
	d.dep as "Code dép",
	d.ncc AS "Département",
	tf.categ_lib as Catégorie,
    tf.finess as "FINESS géographique",
	tf.rs as "Raison sociale ET",
	tf.ej_finess as "FINESS juridique",
	tf.ej_rs as "Raison sociale EJ",
	tf.statut_jur_lib as "Statut juridique",
	tf.adresse as Adresse,
	IIF(LENGTH(tf.adresse_code_postal) = 4, '0'|| tf.adresse_code_postal, tf.adresse_code_postal) AS "Code postal",
	c.NCC AS "Commune",
	IIF(LENGTH(tf.com_code) = 4, '0'|| tf.com_code, tf.com_code) AS "Code commune INSEE",
	CASE
        WHEN tf.categ_code = 500
            THEN CAST(NULLTOZERO(ce.total_heberg_comp_inter_places_autorisees) as INTEGER) + CAST(NULLTOZERO(ce.total_accueil_de_jour_places_autorisees) as INTEGER) + CAST(NULLTOZERO(ce.total_accueil_de_nuit_places_autorisees) as INTEGER)
        ELSE CAST(ccta.somme_de_capacite_autorisee_totale_ as INTEGER)
    END as "Capacité totale autorisée",
	CAST(ce.total_heberg_comp_inter_places_autorisees as INTEGER) as "HP Total auto",
	CAST(ce.total_accueil_de_jour_places_autorisees as INTEGER) as "AJ Total auto",
	CAST(ce.total_accueil_de_nuit_places_autorisees as INTEGER) as "HT total auto",
	co3.nb_lits_occ as "Nombre de résidents au 31/12/N",
	etra.nombre_total_de_chambres_installees_au_3112 as "Nombre de places installées au 31/12/N",
	ROUND(gp.gmp) as GMP,
	ROUND(gp.pmp) as PMP,
	ROUND((eira.taux_plus_10_medics_cip13*100), 2) as "Part des résidents ayant plus de 10 médicaments consommés par mois",
	ROUND((eira.taux_atu*100), 2) as "Taux de recours aux urgences sans hospitalisation des résidents d'EHPAD",
	ROUND((eira.taux_hospit_mco*100), 2) as "Taux de recours à l'hospitalisation MCO des résidents d'EHPAD",
	ROUND((eira.taux_hospit_had*100), 2) as "Taux de recours à l'HAD des résidents d'EHPAD",
	ROUND(chpr."Total des charges") AS "Total des charges",
	ROUND(chpr."Produits de la tarification") AS "Produits de la tarification", 
	ROUND(chpr."Produits du compte 70") AS "Produits du compte 70",
	ROUND(chpr."Total des produits (hors c/775, 777, 7781 et 78)") AS "Total des produits (hors c/775, 777, 7781 et 78)",
	"" as "Saisie des indicateurs du TDB MS (campagne N)",
	CAST(d2.taux_dabsenteisme_hors_formation_en_ as decmail) as "Taux d'absentéisme N-3",
	etra2.taux_dabsenteisme_hors_formation_en_ as "Taux d'absentéisme N-2",
	etra.taux_dabsenteisme_hors_formation_en_ as "Taux d'absentéisme N-1",
    ROUND(MOY3(d2.taux_dabsenteisme_hors_formation_en_ ,etra2.taux_dabsenteisme_hors_formation_en_ , etra.taux_dabsenteisme_hors_formation_en_) ,2) as "Absentéisme moyen sur la période (N-3)-(N-1)",
	CAST(d2.taux_de_rotation_des_personnels_en_ as decimal) as "Taux de rotation du personnel titulaire {n_3}",
	etra2.taux_de_rotation_des_personnels as "Taux de rotation du personnel titulaire {n_2}",
	etra.taux_de_rotation_des_personnels as "Taux de rotation du personnel titulaire {n_1}",
	ROUND(MOY3(d2.taux_de_rotation_des_personnels_en_ , etra2.taux_de_rotation_des_personnels , etra.taux_de_rotation_des_personnels), 2) as "Rotation moyenne du personnel sur la période {n_3}-{n_1}",
	CAST(d2.taux_detp_vacants_en_ as decimal) as "ETP vacants {n_3}",
	etra2.taux_detp_vacants as "ETP vacants {n_2}",
	etra.taux_detp_vacants as "ETP vacants {n_1}",
	etra.dont_taux_detp_vacants_concernant_la_fonction_soins as "dont fonctions soins {n_1}",
	etra.dont_taux_detp_vacants_concernant_la_fonction_socio_educative as "dont fonctions socio-éducatives {n_1}", 
	CAST(REPLACE(d3.taux_de_prestations_externes_sur_les_prestations_directes_en_,',','.')as decimal) as "Taux de prestations externes sur les prestations directes {n_3}",
	etra2.taux_de_prestations_externes_sur_les_prestations_directes as "Taux de prestations externes sur les prestations directes {n_2}", 
	etra.taux_de_prestations_externes_sur_les_prestations_directes as "Taux de prestations externes sur les prestations directes {n_1}",
	ROUND(MOY3(d3.taux_de_prestations_externes_sur_les_prestations_directes_en_ , etra2.taux_de_prestations_externes_sur_les_prestations_directes , etra.taux_de_prestations_externes_sur_les_prestations_directes) ,2) as "Taux moyen de prestations externes sur les prestations directes",
	ROUND(CAST(REPLACE(d3.nombre_de_personnes_accompagnees_dans_leffectif_au_3112,',','.')as decimal)/CAST(REPLACE(d3.nombre_detp_reel_au_3112,',','.')as decimal), 2) as "Nombre total d'ETP par usager en {n_3}",
    ROUND((etra2.etp_directionencadrement + etra2.etp_administration_gestion + etra2.etp_services_generaux + etra2.etp_restauration + etra2."etp_socio-educatif" + etra2.etp_paramedical + etra2.etp_psychologue + etra2.etp_ash + etra2.etp_medical + etra2.etp_personnel_education_nationale + etra2.etp_autres_fonctions)/etra2.nombre_de_personnes_accompagnees_dans_leffectif_au_3112, 2) as "Nombre total d'ETP par usager en {n_2}",
	ROUND((etra.etp_directionencadrement + etra.etp_administration_gestion + etra.etp_services_generaux + etra.etp_restauration + etra."etp_socio-educatif" + etra.etp_paramedical + etra.etp_psychologue + etra.etp_ash + etra.etp_medical + etra.etp_personnel_education_nationale + etra.etp_autres_fonctions)/etra.nombre_de_personnes_accompagnees_dans_leffectif_au_3112, 2) as "Nombre total d'ETP par usager en {n_1}",
	MOY3(ROUND(CAST(REPLACE(d3.nombre_de_personnes_accompagnees_dans_leffectif_au_3112,',','.')as decimal)/CAST(REPLACE(d3.nombre_detp_reel_au_3112,',','.')as decimal), 2) , ROUND((etra2.etp_directionencadrement + etra2.etp_administration_gestion + etra2.etp_services_generaux + etra2.etp_restauration + etra2."etp_socio-educatif" + etra2.etp_paramedical + etra2.etp_psychologue + etra2.etp_ash + etra2.etp_medical + etra2.etp_personnel_education_nationale + etra2.etp_autres_fonctions)/etra2.nombre_de_personnes_accompagnees_dans_leffectif_au_3112, 2) , ROUND((etra.etp_directionencadrement + etra.etp_administration_gestion + etra.etp_services_generaux + etra.etp_restauration + etra."etp_socio-educatif" + etra.etp_paramedical + etra.etp_psychologue + etra.etp_ash + etra.etp_medical + etra.etp_personnel_education_nationale + etra.etp_autres_fonctions)/etra.nombre_de_personnes_accompagnees_dans_leffectif_au_3112, 2))AS "Nombre moyen d'ETP par usager sur la période 2018-{n_2}",
	ROUND((etra.etp_paramedical + etra.etp_medical)/etra.nombre_de_personnes_accompagnees_dans_leffectif_au_3112, 2) as "ETP 'soins' par usager en {n_1}",
	ROUND(etra."-_dont_nombre_detp_reels_de_medecin_coordonnateur", 2) as "dont médecin coordonnateur",
	ROUND(etra.etp_directionencadrement + etra.etp_administration_gestion + etra.etp_services_generaux + etra.etp_restauration + etra."etp_socio-educatif" + etra.etp_paramedical + etra.etp_psychologue + etra.etp_ash + etra.etp_medical + etra.etp_personnel_education_nationale + etra.etp_autres_fonctions, 2) as "Total du nombre d'ETP",
	NULLTOZERO(rs.nb_recla) as "Nombre de réclamations sur la période 2018-{n}",
	NULLTOZERO(ROUND(rs.nb_recla / ccta.somme_de_capacite_autorisee_totale_, 4)*100) as "Rapport réclamations / capacité",
	NULLTOZERO(rs."Hôtellerie-locaux-restauration") as "Recla IGAS : Hôtellerie-locaux-restauration",
	NULLTOZERO(rs."Problème d?organisation ou de fonctionnement de l?établissement ou du service") as "Recla IGAS : Problème d’organisation ou de fonctionnement de l’établissement ou du service",
	NULLTOZERO(rs."Problème de qualité des soins médicaux") as "Recla IGAS : Problème de qualité des soins médicaux",
	NULLTOZERO(rs."Problème de qualité des soins paramédicaux") as "Recla IGAS : Problème de qualité des soins paramédicaux",
	NULLTOZERO(rs."Recherche d?établissement ou d?un professionnel") as "Recla IGAS : Recherche d’établissement ou d’un professionnel",
	NULLTOZERO(rs."Mise en cause attitude des professionnels") as "Recla IGAS : Mise en cause attitude des professionnels",
	NULLTOZERO(rs."Informations et droits des usagers") as "Recla IGAS : Informations et droits des usagers",
	NULLTOZERO(rs."Facturation et honoraires") as "Recla IGAS : Facturation et honoraires",
	NULLTOZERO(rs."Santé-environnementale") as "Recla IGAS : Santé-environnementale",
	NULLTOZERO(rs."Activités d?esthétique réglementées") as "Recla IGAS : Activités d’esthétique réglementées",
	NULLTOZERO(rs.NB_EIGS) as "Nombre d'EIG sur la période {n_2}-2023",
	NULLTOZERO(rs.NB_EIAS) as "Nombre d'EIAS sur la période {n_2}-2023",
	NULLTOZERO(rs."Nombre d'EI sur la période 36mois") + NULLTOZERO(rs.NB_EIGS) + NULLTOZERO(rs.NB_EIAS) as "Somme EI + EIGS + EIAS sur la période {n_2}-2023",
	NULLTOZERO(i."ICE N (réalisé)") as "ICE N (réalisé)",
	NULLTOZERO(i."Inspection SUR SITE N - Déjà réalisée") as "Inspection SUR SITE {n} - Déjà réalisée",
	NULLTOZERO(i."Controle SUR PIECE N - Déjà réalisé") as "Controle SUR PIECE {n} - Déjà réalisé",
	NULLTOZERO(i."Inspection / contrôle Programmé 2023") as "Inspection / contrôle Programmé 2023"
FROM
	tfiness_clean tf 
	LEFT JOIN communes c on c.com = tf.com_code
	LEFT JOIN departement_{n} d on d.dep = c.dep
	LEFT JOIN region_{n}  r on d.reg = r.reg
	LEFT JOIN capacites_ehpad ce on ce."et-ndegfiness" = tf.finess
	LEFT JOIN clean_capacite_totale_auto ccta on ccta.finess = tf.finess
	LEFT JOIN occupation_{n_3}_{n_2} o1 on o1.finess_19 = tf.finess
	LEFT JOIN occupation_{n_1} o2  on o2.finess = tf.finess
	LEFT JOIN clean_occupation_{n} co3  on co3.finess = tf.finess
	LEFT JOIN clean_tdb_{n_1} etra on etra.finess = tf.finess
	LEFT JOIN clean_hebergement c_h on c_h.finess = tf.finess
	LEFT JOIN gmp_pmp gp on IIF(LENGTH(gp.finess_et) = 8, '0'|| gp.finess_et, gp.finess_et) = tf.finess
	LEFT JOIN charges_produits chpr on chpr.finess = tf.finess
	LEFT JOIN EHPAD_Indicateurs_{n_1}_REG_agg eira on eira.et_finess = tf.finess
	LEFT JOIN diamant_{n_3} d2 on SUBSTRING(d2.finess,1,9) = tf.finess
	LEFT JOIN clean_tdb_{n_2} etra2 on etra2.finess = tf.finess
	LEFT JOIN diamantç{n_3}_2 d3 on SUBSTRING(d3.finess,1,9) = tf.finess
	LEFT JOIN recla_signalement rs on rs.finess = tf.finess
	LEFT JOIN inspections i on i.finess = tf.finess
WHERE r.reg = '{}'
ORDER BY tf.finess ASC'''.format(region)
    return requeteCiblage
 
    
def _requeteControle(region):
    requeteControle = '''
-- Controle
WITH tfiness_clean as (
SELECT
	IIF(LENGTH(tf_with.finess )= 8, '0'|| tf_with.finess, tf_with.finess) as finess,
	tf_with.categ_lib,
    tf_with.categ_code,
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
	tf_with.com_code 
FROM "t-finess" tf_with
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
),
-- nombre de réclamation
table_recla as (
SELECT 
	IIF(LENGTH(se.ndeg_finessrpps )= 8, '0'|| se.ndeg_finessrpps, se.ndeg_finessrpps) as finess,
	COUNT(*) as nb_recla
FROM reclamations_mars20_mars2023 se
WHERE 
	se.ndeg_finessrpps  IS NOT NULL
	AND (se.Signalement = 'Non' or se.Signalement IS NULL)
GROUP BY 1
),
-- Motig IGAS
igas as (
SELECT 
	IIF(LENGTH(se.ndeg_finessrpps )= 8, '0'|| se.ndeg_finessrpps, se.ndeg_finessrpps) as finess, 
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
FROM reclamations_mars20_mars2023 se
WHERE 
	(se.Signalement = 'Non' or se.Signalement IS NULL)
	AND se.ndeg_finessrpps  IS NOT NULL
GROUP BY 1
),
table_signalement AS (
	SELECT 
    declarant_organismendeg_finess, 
    survenue_du_cas_en_collectivitendeg_finess,
    date_de_reception, reclamation, 
    declarant_type_etablissement_si_esems, 
    ceci_est_un_eigs, 
    famille_principale, 
    nature_principale  
    FROM all_sivss
),
-- info signalement
sign as (
SELECT 
	finess,
	COUNT(*) as nb_signa,
	SUM(IIF(famille_principale = 'Evénements/incidents dans un établissement ou organisme' AND ceci_est_un_eigs = 'Non',1,0)) as "Nombre d'EI sur la période 36mois",
	SUM(IIF(ceci_est_un_eigs = 'Oui', 1, 0)) as NB_EIGS,
	SUM(IIF(famille_principale = 'Evénements indésirables/graves associés aux soins' AND ceci_est_un_eigs = 'Non',1,0)) AS NB_EIAS,
	SUM(IIF(famille_principale = 'Evénements/incidents dans un établissement ou organisme' AND ceci_est_un_eigs = 'Non',1,0)) + SUM(IIF(ceci_est_un_eigs = 'Oui', 1, 0)) + SUM(IIF(famille_principale = 'Evénements indésirables/graves associés aux soins' AND ceci_est_un_eigs = 'Non',1,0)) AS "Somme EI + EIGS + EIAS sur la période",
	SUM(IIF(famille_principale = 'Evénements/incidents dans un établissement ou organisme' AND ceci_est_un_eigs = 'Non' AND nature_principale = 'Acte de prévention',1,0)) + SUM(IIF(ceci_est_un_eigs = 'Oui' AND nature_principale = 'Acte de prévention', 1, 0)) + SUM(IIF(famille_principale = 'Evénements indésirables/graves associés aux soins' AND ceci_est_un_eigs = 'Non' AND nature_principale = 'Acte de prévention',1,0)) AS 'nb EI/EIG : Acte de prévention',
	SUM(IIF(famille_principale = 'Evénements/incidents dans un établissement ou organisme' AND ceci_est_un_eigs = 'Non' AND nature_principale = 'Autre prise en charge',1,0)) + SUM(IIF(ceci_est_un_eigs = 'Oui' AND nature_principale = 'Autre prise en charge', 1, 0)) + SUM(IIF(famille_principale = 'Evénements indésirables/graves associés aux soins' AND ceci_est_un_eigs = 'Non' AND nature_principale = 'Autre prise en charge',1,0)) AS 'nb EI/EIG : Autre prise en charge',
	SUM(IIF(famille_principale = 'Evénements/incidents dans un établissement ou organisme' AND ceci_est_un_eigs = 'Non' AND nature_principale = 'Chute',1,0)) + SUM(IIF(ceci_est_un_eigs = 'Oui' AND nature_principale = 'Chute', 1, 0)) + SUM(IIF(famille_principale = 'Evénements indésirables/graves associés aux soins' AND ceci_est_un_eigs = 'Non' AND nature_principale = 'Chute',1,0)) AS 'nb EI/EIG : Chute',
	SUM(IIF(famille_principale = 'Evénements/incidents dans un établissement ou organisme' AND ceci_est_un_eigs = 'Non' AND nature_principale = 'Disparition inquiétante et fugues (Hors SDRE/SDJ/SDT)',1,0)) + SUM(IIF(ceci_est_un_eigs = 'Oui' AND nature_principale = 'Disparition inquiétante et fugues (Hors SDRE/SDJ/SDT)', 1, 0)) + SUM(IIF(famille_principale = 'Evénements indésirables/graves associés aux soins' AND ceci_est_un_eigs = 'Non' AND nature_principale = 'Disparition inquiétante et fugues (Hors SDRE/SDJ/SDT)',1,0)) AS 'nb EI/EIG : Disparition inquiétante et fugues (Hors SDRE/SDJ/SDT)',
	SUM(IIF(famille_principale = 'Evénements/incidents dans un établissement ou organisme' AND ceci_est_un_eigs = 'Non' AND nature_principale = 'Dispositif médical',1,0)) + SUM(IIF(ceci_est_un_eigs = 'Oui' AND nature_principale = 'Dispositif médical', 1, 0)) + SUM(IIF(famille_principale = 'Evénements indésirables/graves associés aux soins' AND ceci_est_un_eigs = 'Non' AND nature_principale = 'Dispositif médical',1,0)) AS 'nb EI/EIG : Dispositif médical',
	SUM(IIF(famille_principale = 'Evénements/incidents dans un établissement ou organisme' AND ceci_est_un_eigs = 'Non' AND nature_principale = 'Fausse route',1,0)) + SUM(IIF(ceci_est_un_eigs = 'Oui' AND nature_principale = 'Fausse route', 1, 0)) + SUM(IIF(famille_principale = 'Evénements indésirables/graves associés aux soins' AND ceci_est_un_eigs = 'Non' AND nature_principale = 'Fausse route',1,0)) AS 'nb EI/EIG : Fausse route',
	SUM(IIF(famille_principale = 'Evénements/incidents dans un établissement ou organisme' AND ceci_est_un_eigs = 'Non' AND nature_principale = 'Infection associée aux soins (IAS) hors ES',1,0)) + SUM(IIF(ceci_est_un_eigs = 'Oui' AND nature_principale = 'Infection associée aux soins (IAS) hors ES', 1, 0)) + SUM(IIF(famille_principale = 'Evénements indésirables/graves associés aux soins' AND ceci_est_un_eigs = 'Non' AND nature_principale = 'Infection associée aux soins (IAS) hors ES',1,0)) AS 'nb EI/EIG : Infection associée aux soins (IAS) hors ES',
	SUM(IIF(famille_principale = 'Evénements/incidents dans un établissement ou organisme' AND ceci_est_un_eigs = 'Non' AND nature_principale = 'Infection associée aux soins en EMS et ambulatoire (IAS hors ES)',1,0)) + SUM(IIF(ceci_est_un_eigs = 'Oui' AND nature_principale = 'Infection associée aux soins en EMS et ambulatoire (IAS hors ES)', 1, 0)) + SUM(IIF(famille_principale = 'Evénements indésirables/graves associés aux soins' AND ceci_est_un_eigs = 'Non' AND nature_principale = 'Infection associée aux soins en EMS et ambulatoire (IAS hors ES)',1,0)) AS 'nb EI/EIG : Infection associée aux soins en EMS et ambulatoire (IAS hors ES)',
	SUM(IIF(famille_principale = 'Evénements/incidents dans un établissement ou organisme' AND ceci_est_un_eigs = 'Non' AND nature_principale = 'Parcours/Coopération interprofessionnelle',1,0)) + SUM(IIF(ceci_est_un_eigs = 'Oui' AND nature_principale = 'Parcours/Coopération interprofessionnelle', 1, 0)) + SUM(IIF(famille_principale = 'Evénements indésirables/graves associés aux soins' AND ceci_est_un_eigs = 'Non' AND nature_principale = 'Parcours/Coopération interprofessionnelle',1,0)) AS 'nb EI/EIG : Parcours/Coopération interprofessionnelle',
	SUM(IIF(famille_principale = 'Evénements/incidents dans un établissement ou organisme' AND ceci_est_un_eigs = 'Non' AND nature_principale = 'Prise en charge chirurgicale',1,0)) + SUM(IIF(ceci_est_un_eigs = 'Oui' AND nature_principale = 'Prise en charge chirurgicale', 1, 0)) + SUM(IIF(famille_principale = 'Evénements indésirables/graves associés aux soins' AND ceci_est_un_eigs = 'Non' AND nature_principale = 'Prise en charge chirurgicale',1,0)) AS 'nb EI/EIG : Prise en charge chirurgicale',
	SUM(IIF(famille_principale = 'Evénements/incidents dans un établissement ou organisme' AND ceci_est_un_eigs = 'Non' AND nature_principale = 'Prise en charge diagnostique',1,0)) + SUM(IIF(ceci_est_un_eigs = 'Oui' AND nature_principale = 'Prise en charge diagnostique', 1, 0)) + SUM(IIF(famille_principale = 'Evénements indésirables/graves associés aux soins' AND ceci_est_un_eigs = 'Non' AND nature_principale = 'Prise en charge diagnostique',1,0)) AS 'nb EI/EIG : Prise en charge diagnostique',
	SUM(IIF(famille_principale = 'Evénements/incidents dans un établissement ou organisme' AND ceci_est_un_eigs = 'Non' AND nature_principale = 'Prise en charge en urgence',1,0)) + SUM(IIF(ceci_est_un_eigs = 'Oui' AND nature_principale = 'Prise en charge en urgence', 1, 0)) + SUM(IIF(famille_principale = 'Evénements indésirables/graves associés aux soins' AND ceci_est_un_eigs = 'Non' AND nature_principale = 'Prise en charge en urgence',1,0)) AS 'nb EI/EIG : Prise en charge en urgence',
	SUM(IIF(famille_principale = 'Evénements/incidents dans un établissement ou organisme' AND ceci_est_un_eigs = 'Non' AND nature_principale = 'Prise en charge médicamenteuse',1,0)) + SUM(IIF(ceci_est_un_eigs = 'Oui' AND nature_principale = 'Prise en charge médicamenteuse', 1, 0)) + SUM(IIF(famille_principale = 'Evénements indésirables/graves associés aux soins' AND ceci_est_un_eigs = 'Non' AND nature_principale = 'Prise en charge médicamenteuse',1,0)) AS 'nb EI/EIG : Prise en charge médicamenteuse',
	SUM(IIF(famille_principale = 'Evénements/incidents dans un établissement ou organisme' AND ceci_est_un_eigs = 'Non' AND nature_principale = 'Prise en charge des cancers',1,0)) + SUM(IIF(ceci_est_un_eigs = 'Oui' AND nature_principale = 'Prise en charge des cancers', 1, 0)) + SUM(IIF(famille_principale = 'Evénements indésirables/graves associés aux soins' AND ceci_est_un_eigs = 'Non' AND nature_principale = 'Prise en charge des cancers',1,0)) AS 'nb EI/EIG : Prise en charge des cancers',
	SUM(IIF(famille_principale = 'Evénements/incidents dans un établissement ou organisme' AND ceci_est_un_eigs = 'Non' AND nature_principale = 'Prise en charge psychiatrique',1,0)) + SUM(IIF(ceci_est_un_eigs = 'Oui' AND nature_principale = 'Prise en charge psychiatrique', 1, 0)) + SUM(IIF(famille_principale = 'Evénements indésirables/graves associés aux soins' AND ceci_est_un_eigs = 'Non' AND nature_principale = 'Prise en charge psychiatrique',1,0)) AS 'nb EI/EIG : Prise en charge psychiatrique',
	SUM(IIF(famille_principale = 'Evénements/incidents dans un établissement ou organisme' AND ceci_est_un_eigs = 'Non' AND nature_principale = 'Suicide',1,0)) + SUM(IIF(ceci_est_un_eigs = 'Oui' AND nature_principale = 'Suicide', 1, 0)) + SUM(IIF(famille_principale = 'Evénements indésirables/graves associés aux soins' AND ceci_est_un_eigs = 'Non' AND nature_principale = 'Suicide',1,0)) AS 'nb EI/EIG : Suicide',
	SUM(IIF(famille_principale = 'Evénements/incidents dans un établissement ou organisme' AND ceci_est_un_eigs = 'Non' AND nature_principale = 'Tentative de suicide',1,0)) + SUM(IIF(ceci_est_un_eigs = 'Oui' AND nature_principale = 'Tentative de suicide', 1, 0)) + SUM(IIF(famille_principale = 'Evénements indésirables/graves associés aux soins' AND ceci_est_un_eigs = 'Non' AND nature_principale = 'Tentative de suicide',1,0)) AS 'nb EI/EIG : Tentative de suicide'
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
--	AND tb.declarant_type_etablissement_si_esems like "%Etablissement d'hébergement pour personnes âgées dépendantes%"
	) as sub_table
GROUP BY 1
),
-- Pour checker les "MOTIF IGAS"
recla_signalement as(
SELECT
	tfc.finess,
	s.nb_signa,
	tr.nb_recla,
	s."Nombre d'EI sur la période 36mois",
	s.NB_EIGS,
	s.NB_EIAS,
	s."Somme EI + EIGS + EIAS sur la période",
	s."nb EI/EIG : Acte de prévention",
	s."nb EI/EIG : Autre prise en charge",
	s."nb EI/EIG : Chute",
	s."nb EI/EIG : Disparition inquiétante et fugues (Hors SDRE/SDJ/SDT)",
	s."nb EI/EIG : Dispositif médical",
	s."nb EI/EIG : Fausse route",
	s."nb EI/EIG : Infection associée aux soins (IAS) hors ES",
	s."nb EI/EIG : Infection associée aux soins en EMS et ambulatoire (IAS hors ES)",
	s."nb EI/EIG : Parcours/Coopération interprofessionnelle",
	s."nb EI/EIG : Prise en charge chirurgicale",
	s."nb EI/EIG : Prise en charge diagnostique",
	s."nb EI/EIG : Prise en charge en urgence",
	s."nb EI/EIG : Prise en charge médicamenteuse",
	s."nb EI/EIG : Prise en charge des cancers",
	s."nb EI/EIG : Prise en charge psychiatrique",
	s."nb EI/EIG : Suicide",
	s."nb EI/EIG : Tentative de suicide",
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
),
clean_occupation_N as (
SELECT 
	IIF(LENGTH(o3.finess) = 8, '0'|| o3.finess, o3.finess) as finess, 
	o3.taux_occ_{n},
	o3.nb_lits_autorises_installes,
	o3.nb_lits_occ_{n},
	o3.taux_occ_trimestre3 
FROM occupation_{n} o3 
),
clean_capacite_totale_auto as (
SELECT 
	IIF(LENGTH(cta.etiquettes_de_lignes )= 8, '0'|| cta.etiquettes_de_lignes, cta.etiquettes_de_lignes) as finess,
	cta.somme_de_capacite_autorisee_totale_ 
FROM capacite_totale_auto cta 
),
clean_hebergement as (
SELECT 
	IIF(LENGTH(h.finesset )= 8, '0'|| h.finesset, h.finesset) as finess,
	h.prixhebpermcs 
FROM hebergement h 
),
clean_tdb_{n_2} as (
SELECT 
	IIF(LENGTH(tdb_{n_2}.finess_geographique) = 8, '0'|| tdb_{n_2}.finess_geographique, tdb_{n_2}.finess_geographique) as finess,
	*
FROM "export-tdbesms-{n_2}-region_agg" tdb_{n_2}
), 
clean_tdb_{n_1} as (
SELECT 
	IIF(LENGTH(tdb_{n_1}.finess_geographique )= 8, '0'|| tdb_{n_1}.finess_geographique, tdb_{n_1}.finess_geographique) as finess,
	*
FROM "export-tdbesms-{n_1}-region-agg" tdb_{n_1}
), 
-- La partie suivante sert à créer un table temporaire pour classer les charges et les produits
correspondance as(
SELECT  
	SUBSTRING(cecpp."finess_-_rs_et" ,1,9) as finess,
	cecpp.cadre
FROM 
	choix_errd_ca_pa_ph cecpp
	LEFT JOIN doublons_errd_ca dou on SUBSTRING(dou.finess,1,9) = SUBSTRING(cecpp."finess_-_rs_et",1,9) AND cecpp.cadre != 'ERRD' 
WHERE
	dou.finess IS NULL
),
grouped_errd_charges AS (
SELECT 
	SUBSTRING(ec."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(ec.charges_dexploitation) as sum_charges_dexploitation
FROM errd_charges ec
GROUP BY 1
),
grouped_errd_produitstarif AS (
SELECT 
	SUBSTRING(ep."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(ep.groupe_i__produits_de_la_tarification) as sum_groupe_i__produits_de_la_tarification
FROM errd_produitstarif ep
GROUP BY 1
),
grouped_errd_produits70 AS (
SELECT 
	SUBSTRING(ep2."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(ep2.unnamed_1) as sum_produits70
FROM errd_produits70 ep2
GROUP BY 1
),
grouped_errd_produitsencaiss AS (
SELECT 
	SUBSTRING(ep3."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(ep3.produits_dexploitation) as sum_produits_dexploitation
FROM errd_produitsencaiss ep3
GROUP BY 1
),
grouped_caph_charges AS (
SELECT 
	SUBSTRING(cch."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(cch.charges_dexploitation) as sum_charges_dexploitation
FROM caph_charges cch 
GROUP BY 1
),
grouped_caph_produitstarif AS (
SELECT 
	SUBSTRING(cch2."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(cch2.groupe_i__produits_de_la_tarification) as sum_groupe_i__produits_de_la_tarification
FROM caph_produitstarif cch2
GROUP BY 1
),
grouped_caph_produits70 AS (
SELECT
	SUBSTRING(cch3."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(cch3.unnamed_1) as sum_produits70
FROM caph_produits70 cch3
GROUP BY 1
),
grouped_caph_produitsencaiss AS (
SELECT 
	SUBSTRING(cch4."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(cch4.produits_dexploitation) as sum_produits_dexploitation
FROM caph_produitsencaiss cch4
GROUP BY 1
),
grouped_capa_charges AS (
SELECT 
	SUBSTRING(cc."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(cc.charges_dexploitation) as sum_charges_dexploitation
FROM capa_charges cc 
GROUP BY 1
),
grouped_capa_produitstarif AS (
SELECT 
	SUBSTRING(cpt."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(cpt.produits_de_lexercice) as sum_groupe_i__produits_de_la_tarification
FROM capa_produitstarif cpt 
GROUP BY 1
),
charges_produits as (
SELECT 
	cor.finess,
	CASE 
		WHEN cor.cadre = 'ERRD'
			THEN gec.sum_charges_dexploitation
		WHEN cor.cadre = 'CA PA'
			THEN gc.sum_charges_dexploitation
		WHEN cor.cadre = 'CA PH'
			THEN gcch.sum_charges_dexploitation 
	END as "Total des charges",
	CASE 
		WHEN cor.cadre = 'ERRD'
			THEN gep.sum_groupe_i__produits_de_la_tarification
		WHEN cor.cadre = 'CA PA'
			THEN gcp.sum_groupe_i__produits_de_la_tarification
		WHEN cor.cadre = 'CA PH'
			THEN gcch2.sum_groupe_i__produits_de_la_tarification
	END as "Produits de la tarification",
	CASE 
		WHEN cor.cadre = 'ERRD'
			THEN gep2.sum_produits70
		WHEN cor.cadre = 'CA PA'
			THEN 0
		WHEN cor.cadre = 'CA PH'
			THEN gcch3.sum_produits70
	END as "Produits du compte 70",
	CASE 
		WHEN cor.cadre = 'ERRD'
			THEN gep3.sum_produits_dexploitation
		WHEN cor.cadre = 'CA PA'
			THEN 0
		WHEN cor.cadre = 'CA PH'
			THEN gcch4.sum_produits_dexploitation
	END as "Total des produits (hors c/775, 777, 7781 et 78)"
FROM 
	correspondance cor
	LEFT JOIN grouped_errd_charges gec on gec.finess = cor.finess AND cor.cadre = 'ERRD' 
	LEFT JOIN grouped_errd_produitstarif gep on gep.finess = cor.finess AND cor.cadre = 'ERRD'
	LEFT JOIN grouped_errd_produits70 gep2 on gep2.finess = cor.finess AND cor.cadre = 'ERRD'
	LEFT JOIN grouped_errd_produitsencaiss gep3 on gep3.finess = cor.finess AND cor.cadre = 'ERRD'
	LEFT JOIN grouped_caph_charges gcch on gcch.finess = cor.finess AND cor.cadre = 'CA PH'
	LEFT JOIN grouped_caph_produitstarif gcch2 on gcch2.finess = cor.finess AND cor.cadre = 'CA PH'
	LEFT JOIN grouped_caph_produits70 gcch3 on gcch3.finess = cor.finess AND cor.cadre = 'CA PH'
	LEFT JOIN grouped_caph_produitsencaiss gcch4 on gcch4.finess = cor.finess AND cor.cadre = 'CA PH'
	LEFT JOIN grouped_capa_charges gc on gc.finess = cor.finess AND cor.cadre = 'CA PA'
	LEFT JOIN grouped_capa_produitstarif gcp on gcp.finess = cor.finess AND cor.cadre = 'CA PA'
),
inspections as (
SELECT 
	IIF(LENGTH(code_finess)= 8, '0'|| code_finess, code_finess) as finess,
	count(distinct identifiant_de_la_mission) as "ICE {n} (réalisé)",
	SUM(IIF(type_de_mission = 'Inspection' OR type_de_mission = 'Inspection Technique' OR type_de_mission = 'Evaluation' OR type_de_mission = 'Contrôle' OR type_de_mission = 'Enquête administrative' OR type_de_mission = 'Visites de conformité' OR type_de_mission = 'Contrôle sur place / Visite de vérification', 1, 0)) as "Inspection SUR SITE {n} - Déjà réalisée",
	SUM(IIF(type_de_mission = 'Contrôle sur pièces' OR type_de_mission = 'Contrôle sur pièces EHPAD' OR type_de_mission = 'EHPAD Contrôle sur pièces' OR type_de_mission = 'Ctrl_sur_Pièces', 1, 0)) as "Controle SUR PIECE {n} - Déjà réalisé",
	SUM(IIF(type_de_planification = "Programmé", 1, 0)) as "Inspection / contrôle Programmé 2023"
FROM "ESMS export-mission-4 evts 01 03 23 v2"
GROUP BY code_finess
),
communes as (
SELECT c.com, c.dep, c.ncc  
FROM commune_{n} c 
WHERE c.reg IS NOT NULL
UNION ALL
SELECT c.com, c2.dep, c.ncc
FROM commune_{n} c 
	LEFT JOIN commune_{n} c2 on c.comparent = c2.com AND c2.dep IS NOT NULL
WHERE c.reg IS NULL and c.com != c.comparent
)
SELECT 
--identfication de l'établissement
	r.ncc as Region,
	d.dep as "Code dép",
	d.ncc AS "Département",
	tf.categ_lib as Catégorie,
    tf.finess as "FINESS géographique",
	tf.rs as "Raison sociale ET",
	tf.ej_finess as "FINESS juridique",
	tf.ej_rs as "Raison sociale EJ",
	tf.statut_jur_lib as "Statut juridique",
	tf.adresse as Adresse,
	IIF(LENGTH(tf.adresse_code_postal) = 4, '0'|| tf.adresse_code_postal, tf.adresse_code_postal) AS "Code postal",
	c.NCC AS "Commune",
	IIF(LENGTH(tf.com_code) = 4, '0'|| tf.com_code, tf.com_code) AS "Code commune INSEE",
	--	caractéristiques ESMS
	CASE
        WHEN tf.categ_code = 500
            THEN CAST(NULLTOZERO(ce.total_heberg_comp_inter_places_autorisees) as INTEGER) + CAST(NULLTOZERO(ce.total_accueil_de_jour_places_autorisees) as INTEGER) + CAST(NULLTOZERO(ce.total_accueil_de_nuit_places_autorisees) as INTEGER)
        ELSE CAST(ccta.somme_de_capacite_autorisee_totale_ as INTEGER)
    END as "Capacité totale autorisée",
	CAST(ce.total_heberg_comp_inter_places_autorisees as INTEGER) as "HP Total auto",
	CAST(ce.total_accueil_de_jour_places_autorisees as INTEGER) as "AJ Total auto",
	CAST(ce.total_accueil_de_nuit_places_autorisees as INTEGER) as "HT total auto",
	o1.taux_occ_{n_2} AS "Taux d'occupation {n_2}",
	o2.taux_occ_{n_1} AS "Taux d'occupation {n_1}",
	co3.taux_occ_{n} AS "Taux d'occupation {n}",
	co3.nb_lits_occ_{n} as "Nombre de résidents au 31/12/{n}",
	etra.nombre_total_de_chambres_installees_au_3112 as "Nombre de places installées au 31/12/{n}",
	co3.taux_occ_trimestre3 AS "Taux occupation au 31/12/{n}",
	c_h.prixhebpermcs AS "Prix de journée hébergement (EHPAD uniquement)",
	ROUND(gp.gmp) as GMP,
	ROUND(gp.pmp) as PMP,
	etra.personnes_gir_1 AS "Part de résidents GIR 1 (31/12/{n_1})",
	etra.personnes_gir_2 AS "Part de résidents GIR 2 (31/12/{n_1})",
	etra.personnes_gir_3 AS "Part de résidents GIR 3 (31/12/{n_1})",
	ROUND((eira.taux_plus_10_medics_cip13*100), 2) as "Part des résidents ayant plus de 10 médicaments consommés par mois",
	ROUND((eira.taux_atu*100), 2) as "Taux de recours aux urgences sans hospitalisation des résidents d'EHPAD",
	ROUND((eira.taux_hospit_mco*100), 2) as "Taux de recours à l'hospitalisation MCO des résidents d'EHPAD",
	ROUND((eira.taux_hospit_had*100), 2) as "Taux de recours à l'HAD des résidents d'EHPAD",
	ROUND(chpr."Total des charges") AS "Total des charges",
	ROUND(chpr."Produits de la tarification") AS "Produits de la tarification", 
	ROUND(chpr."Produits du compte 70") AS "Produits du compte 70",
	ROUND(chpr."Total des produits (hors c/775, 777, 7781 et 78)") AS "Total des produits (hors c/775, 777, 7781 et 78)",
	"" as "Saisie des indicateurs du TDB MS (campagne {n})",
	CAST(d2.taux_dabsenteisme_hors_formation_en_ as decmail) as "Taux d'absentéisme {n_3}",
	etra2.taux_dabsenteisme_hors_formation_en_ as "Taux d'absentéisme {n_2}",
	etra.taux_dabsenteisme_hors_formation_en_ as "Taux d'absentéisme {n_1}",
    ROUND(MOY3(d2.taux_dabsenteisme_hors_formation_en_ ,etra2.taux_dabsenteisme_hors_formation_en_ , etra.taux_dabsenteisme_hors_formation_en_) ,2) as "Absentéisme moyen sur la période {n_3}-{n_1}",
	CAST(d2.taux_de_rotation_des_personnels_en_ as decimal) as "Taux de rotation du personnel titulaire {n_3}",
	etra2.taux_de_rotation_des_personnels as "Taux de rotation du personnel titulaire {n_2}",
	etra.taux_de_rotation_des_personnels as "Taux de rotation du personnel titulaire {n_1}",
	ROUND(MOY3(d2.taux_de_rotation_des_personnels_en_ , etra2.taux_de_rotation_des_personnels , etra.taux_de_rotation_des_personnels), 2) as "Rotation moyenne du personnel sur la période {n_3}-{n_1}",
	CAST(d2.taux_detp_vacants_en_ as decimal) as "ETP vacants {n_3}",
	etra2.taux_detp_vacants as "ETP vacants {n_2}",
	etra.taux_detp_vacants as "ETP vacants {n_1}",
	etra.dont_taux_detp_vacants_concernant_la_fonction_soins as "dont fonctions soins {n_1}",
	etra.dont_taux_detp_vacants_concernant_la_fonction_socio_educative as "dont fonctions socio-éducatives {n_1}", 
	CAST(REPLACE(d3.taux_de_prestations_externes_sur_les_prestations_directes_en_,',','.')as decimal) as "Taux de prestations externes sur les prestations directes {n_3}",
	etra2.taux_de_prestations_externes_sur_les_prestations_directes as "Taux de prestations externes sur les prestations directes {n_2}", 
	etra.taux_de_prestations_externes_sur_les_prestations_directes as "Taux de prestations externes sur les prestations directes {n_1}",
	ROUND(MOY3(d3.taux_de_prestations_externes_sur_les_prestations_directes_en_ , etra2.taux_de_prestations_externes_sur_les_prestations_directes , etra.taux_de_prestations_externes_sur_les_prestations_directes) ,2) as "Taux moyen de prestations externes sur les prestations directes",
	ROUND(CAST(REPLACE(d3.nombre_de_personnes_accompagnees_dans_leffectif_au_3112,',','.')as decimal)/CAST(REPLACE(d3.nombre_detp_reel_au_3112,',','.')as decimal), 2) as "Nombre total d'ETP par usager en {n_3}",
    ROUND((etra2.etp_directionencadrement + etra2.etp_administration_gestion + etra2.etp_services_generaux + etra2.etp_restauration + etra2."etp_socio-educatif" + etra2.etp_paramedical + etra2.etp_psychologue + etra2.etp_ash + etra2.etp_medical + etra2.etp_personnel_education_nationale + etra2.etp_autres_fonctions)/etra2.nombre_de_personnes_accompagnees_dans_leffectif_au_3112, 2) as "Nombre total d'ETP par usager en {n_2}",
	ROUND((etra.etp_directionencadrement + etra.etp_administration_gestion + etra.etp_services_generaux + etra.etp_restauration + etra."etp_socio-educatif" + etra.etp_paramedical + etra.etp_psychologue + etra.etp_ash + etra.etp_medical + etra.etp_personnel_education_nationale + etra.etp_autres_fonctions)/etra.nombre_de_personnes_accompagnees_dans_leffectif_au_3112, 2) as "Nombre total d'ETP par usager en {n_1}",
	MOY3(ROUND(CAST(REPLACE(d3.nombre_de_personnes_accompagnees_dans_leffectif_au_3112,',','.')as decimal)/CAST(REPLACE(d3.nombre_detp_reel_au_3112,',','.')as decimal), 2) , ROUND((etra2.etp_directionencadrement + etra2.etp_administration_gestion + etra2.etp_services_generaux + etra2.etp_restauration + etra2."etp_socio-educatif" + etra2.etp_paramedical + etra2.etp_psychologue + etra2.etp_ash + etra2.etp_medical + etra2.etp_personnel_education_nationale + etra2.etp_autres_fonctions)/etra2.nombre_de_personnes_accompagnees_dans_leffectif_au_3112, 2) , ROUND((etra.etp_directionencadrement + etra.etp_administration_gestion + etra.etp_services_generaux + etra.etp_restauration + etra."etp_socio-educatif" + etra.etp_paramedical + etra.etp_psychologue + etra.etp_ash + etra.etp_medical + etra.etp_personnel_education_nationale + etra.etp_autres_fonctions)/etra.nombre_de_personnes_accompagnees_dans_leffectif_au_3112, 2))AS "Nombre moyen d'ETP par usager sur la période 2018-{n_2}",
	ROUND((etra.etp_paramedical + etra.etp_medical)/etra.nombre_de_personnes_accompagnees_dans_leffectif_au_3112, 2) as "ETP 'soins' par usager en {n_1}",
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
	ROUND(etra."-_dont_nombre_detp_reels_de_medecin_coordonnateur", 2) as "dont médecin coordonnateur",
	etra."-_dont_autre_medical" AS "dont autre médical",
	etra.etp_personnel_education_nationale AS "Personnel éducation nationale",
	etra.etp_autres_fonctions AS "Autres fonctions",
	ROUND(etra.etp_directionencadrement + etra.etp_administration_gestion + etra.etp_services_generaux + etra.etp_restauration + etra."etp_socio-educatif" + etra.etp_paramedical + etra.etp_psychologue + etra.etp_ash + etra.etp_medical + etra.etp_personnel_education_nationale + etra.etp_autres_fonctions, 2) as "Total du nombre d'ETP",
	NULLTOZERO(rs.nb_recla) as "Nombre de réclamations sur la période 2018-{n}",
	NULLTOZERO(ROUND(rs.nb_recla / ccta.somme_de_capacite_autorisee_totale_, 4)*100) as "Rapport réclamations / capacité",
	NULLTOZERO(rs."Hôtellerie-locaux-restauration") as "Recla IGAS : Hôtellerie-locaux-restauration",
	NULLTOZERO(rs."Problème d?organisation ou de fonctionnement de l?établissement ou du service") as "Recla IGAS : Problème d’organisation ou de fonctionnement de l’établissement ou du service",
	NULLTOZERO(rs."Problème de qualité des soins médicaux") as "Recla IGAS : Problème de qualité des soins médicaux",
	NULLTOZERO(rs."Problème de qualité des soins paramédicaux") as "Recla IGAS : Problème de qualité des soins paramédicaux",
	NULLTOZERO(rs."Recherche d?établissement ou d?un professionnel") as "Recla IGAS : Recherche d’établissement ou d’un professionnel",
	NULLTOZERO(rs."Mise en cause attitude des professionnels") as "Recla IGAS : Mise en cause attitude des professionnels",
	NULLTOZERO(rs."Informations et droits des usagers") as "Recla IGAS : Informations et droits des usagers",
	NULLTOZERO(rs."Facturation et honoraires") as "Recla IGAS : Facturation et honoraires",
	NULLTOZERO(rs."Santé-environnementale") as "Recla IGAS : Santé-environnementale",
	NULLTOZERO(rs."Activités d?esthétique réglementées") as "Recla IGAS : Activités d’esthétique réglementées",
	NULLTOZERO(rs."Nombre d'EI sur la période 36mois") as "Nombre d'EI sur la période 36mois",
	NULLTOZERO(rs.NB_EIGS) as "Nombre d'EIG sur la période {n_2}-2023",
	NULLTOZERO(rs.NB_EIAS) as "Nombre d'EIAS sur la période {n_2}-2023",
	NULLTOZERO(rs."Nombre d'EI sur la période 36mois" + NULLTOZERO(rs.NB_EIGS) + NULLTOZERO(rs.NB_EIAS)) as "Somme EI + EIGS + EIAS sur la période {n_2}-2023",
	NULLTOZERO(rs."nb EI/EIG : Acte de prévention") as "nb EI/EIG : Acte de prévention",
	NULLTOZERO(rs."nb EI/EIG : Autre prise en charge") as "nb EI/EIG : Autre prise en charge",
	NULLTOZERO(rs."nb EI/EIG : Chute") as "nb EI/EIG : Chute",
	NULLTOZERO(rs."nb EI/EIG : Disparition inquiétante et fugues (Hors SDRE/SDJ/SDT)") as "nb EI/EIG : Disparition inquiétante et fugues (Hors SDRE/SDJ/SDT)",
	NULLTOZERO(rs."nb EI/EIG : Dispositif médical") as "nb EI/EIG : Dispositif médical",
	NULLTOZERO(rs."nb EI/EIG : Fausse route") as "nb EI/EIG : Fausse route",
	NULLTOZERO(rs."nb EI/EIG : Infection associée aux soins (IAS) hors ES") as "nb EI/EIG : Infection associée aux soins (IAS) hors ES",
	NULLTOZERO(rs."nb EI/EIG : Infection associée aux soins en EMS et ambulatoire (IAS hors ES)") as "nb EI/EIG : Infection associée aux soins en EMS et ambulatoire (IAS hors ES)",
	NULLTOZERO(rs."nb EI/EIG : Parcours/Coopération interprofessionnelle") as "nb EI/EIG : Parcours/Coopération interprofessionnelle",
	NULLTOZERO(rs."nb EI/EIG : Prise en charge chirurgicale") as "nb EI/EIG : Prise en charge chirurgicale",
	NULLTOZERO(rs."nb EI/EIG : Prise en charge diagnostique") as "nb EI/EIG : Prise en charge diagnostique",
	NULLTOZERO(rs."nb EI/EIG : Prise en charge en urgence") as "nb EI/EIG : Prise en charge en urgence",
	NULLTOZERO(rs."nb EI/EIG : Prise en charge médicamenteuse") as "nb EI/EIG : Prise en charge médicamenteuse",
	NULLTOZERO(rs."nb EI/EIG : Prise en charge des cancers") as "nb EI/EIG : Prise en charge des cancers",
	NULLTOZERO(rs."nb EI/EIG : Prise en charge psychiatrique") as "nb EI/EIG : Prise en charge psychiatrique",
	NULLTOZERO(rs."nb EI/EIG : Suicide") as "nb EI/EIG : Suicide",
	NULLTOZERO(rs."nb EI/EIG : Tentative de suicide") as "nb EI/EIG : Tentative de suicide",
	NULLTOZERO(i."ICE {n} (réalisé)") as "ICE {n} (réalisé)",
	NULLTOZERO(i."Inspection SUR SITE {n} - Déjà réalisée") as "Inspection SUR SITE {n} - Déjà réalisée",
	NULLTOZERO(i."Controle SUR PIECE {n} - Déjà réalisé") as "Controle SUR PIECE {n} - Déjà réalisé",
	NULLTOZERO(i."Inspection / contrôle Programmé 2023") as "Inspection / contrôle Programmé 2023"
FROM
 --identification
	tfiness_clean tf 
	LEFT JOIN communes c on c.com = tf.com_code
	LEFT JOIN departement_{n} d on d.dep = c.dep
	LEFT JOIN region_{n}  r on d.reg = r.reg
	LEFT JOIN capacites_ehpad ce on ce."et-ndegfiness" = tf.finess
	LEFT JOIN clean_capacite_totale_auto ccta on ccta.finess = tf.finess
	LEFT JOIN occupation_{n_3}_{n_2} o1 on o1.finess_19 = tf.finess
	LEFT JOIN occupation_{n_1} o2  on o2.finess = tf.finess
	LEFT JOIN clean_occupation_{n} co3  on co3.finess = tf.finess
	LEFT JOIN clean_tdb_{n_1} etra on etra.finess = tf.finess
	LEFT JOIN clean_hebergement c_h on c_h.finess = tf.finess
	LEFT JOIN gmp_pmp gp on IIF(LENGTH(gp.finess_et) = 8, '0'|| gp.finess_et, gp.finess_et) = tf.finess
	LEFT JOIN charges_produits chpr on chpr.finess = tf.finess
	LEFT JOIN EHPAD_Indicateurs_{n_1}_REG_agg eira on eira.et_finess = tf.finess
	LEFT JOIN diamant_{n_3} d2 on SUBSTRING(d2.finess,1,9) = tf.finess
	LEFT JOIN clean_tdb_{n_2} etra2 on etra2.finess = tf.finess
	LEFT JOIN diamantç{n_3}_2 d3 on SUBSTRING(d3.finess,1,9) = tf.finess
	LEFT JOIN recla_signalement rs on rs.finess = tf.finess
	LEFT JOIN inspections i on i.finess = tf.finess
WHERE r.reg = '{}'
ORDER BY tf.finess ASC
'''.format(region)
    return requeteControle

def _requeteCiblageHDF(region):
    requeteCiblage = '''-- CIBLAGE
WITH tfiness_clean as (
SELECT
	IIF(LENGTH(tf_with.finess )= 8, '0'|| tf_with.finess, tf_with.finess) as finess,
	tf_with.categ_lib,
    tf_with.categ_code,
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
	tf_with.com_code 
FROM "t-finess" tf_with
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
),
-- nombre de réclamation
table_recla as (
SELECT 
	IIF(LENGTH(se.ndeg_finessrpps )= 8, '0'|| se.ndeg_finessrpps, se.ndeg_finessrpps) as finess,
	COUNT(*) as nb_recla
FROM reclamations_mars20_mars2023 se
WHERE 
	se.ndeg_finessrpps  IS NOT NULL
	AND (se.Signalement = 'Non' or se.Signalement IS NULL)
GROUP BY 1
),
-- Motig IGAS
igas as (
SELECT 
	IIF(LENGTH(se.ndeg_finessrpps )= 8, '0'|| se.ndeg_finessrpps, se.ndeg_finessrpps) as finess, 
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
FROM reclamations_mars20_mars2023 se
WHERE 
	(se.Signalement = 'Non' or se.Signalement IS NULL)
	AND se.ndeg_finessrpps  IS NOT NULL
GROUP BY 1
),

-- info signalement
sign as (
SELECT 
	IIF(LENGTH(se.ndeg_finessrpps )= 8, '0'|| se.ndeg_finessrpps, se.ndeg_finessrpps) as finess,
	COUNT(*) as nb_signa
FROM reclamations_mars20_mars2023 se	
WHERE 
	se.signalement = 'Oui'
	AND se.ndeg_finessrpps  IS NOT NULL
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
),
clean_occupation_{n} as (
SELECT 
	IIF(LENGTH(o3.finess) = 8, '0'|| o3.finess, o3.finess) as finess, 
	o3.taux_occ_{n},
	o3.nb_lits_occ_{n},
	o3.taux_occ_trimestre3 
FROM occupation_{n} o3 
),
clean_capacite_totale_auto as (
SELECT 
	IIF(LENGTH(cta.etiquettes_de_lignes )= 8, '0'|| cta.etiquettes_de_lignes, cta.etiquettes_de_lignes) as finess,
	cta.somme_de_capacite_autorisee_totale_ 
FROM capacite_totale_auto cta 
),
clean_hebergement as (
SELECT 
	IIF(LENGTH(h.finesset )= 8, '0'|| h.finesset, h.finesset) as finess,
	h.prixhebpermcs 
FROM hebergement h 
),
clean_tdb_{n_2} as (
SELECT 
	IIF(LENGTH(tdb_{n_2}.finess_geographique) = 8, '0'|| tdb_{n_2}.finess_geographique, tdb_{n_2}.finess_geographique) as finess,
	*
FROM "export-tdbesms-{n_2}-region_agg" tdb_{n_2}
), 
clean_tdb_{n_1} as (
SELECT 
	IIF(LENGTH(tdb_{n_1}.finess_geographique )= 8, '0'|| tdb_{n_1}.finess_geographique, tdb_{n_1}.finess_geographique) as finess,
	*
FROM "export-tdbesms-{n_1}-region-agg" tdb_{n_1}
), 
-- La partie suivante sert à créer un table temporaire pour classer les charges et les produits
correspondance as(
SELECT  
	SUBSTRING(cecpp."finess_-_rs_et" ,1,9) as finess,
	cecpp.cadre
FROM 
	choix_errd_ca_pa_ph cecpp
	LEFT JOIN doublons_errd_ca dou on SUBSTRING(dou.finess,1,9) = SUBSTRING(cecpp."finess_-_rs_et",1,9) AND cecpp.cadre != 'ERRD' 
WHERE
	dou.finess IS NULL
),
grouped_errd_charges AS (
SELECT 
	SUBSTRING(ec."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(ec.charges_dexploitation) as sum_charges_dexploitation
FROM errd_charges ec
GROUP BY 1
),
grouped_errd_produitstarif AS (
SELECT 
	SUBSTRING(ep."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(ep.groupe_i__produits_de_la_tarification) as sum_groupe_i__produits_de_la_tarification
FROM errd_produitstarif ep
GROUP BY 1
),
grouped_errd_produits70 AS (
SELECT 
	SUBSTRING(ep2."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(ep2.unnamed_1) as sum_produits70
FROM errd_produits70 ep2
GROUP BY 1
),
grouped_errd_produitsencaiss AS (
SELECT 
	SUBSTRING(ep3."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(ep3.produits_dexploitation) as sum_produits_dexploitation
FROM errd_produitsencaiss ep3
GROUP BY 1
),
grouped_caph_charges AS (
SELECT 
	SUBSTRING(cch."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(cch.charges_dexploitation) as sum_charges_dexploitation
FROM caph_charges cch 
GROUP BY 1
),
grouped_caph_produitstarif AS (
SELECT 
	SUBSTRING(cch2."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(cch2.groupe_i__produits_de_la_tarification) as sum_groupe_i__produits_de_la_tarification
FROM caph_produitstarif cch2
GROUP BY 1
),
grouped_caph_produits70 AS (
SELECT
	SUBSTRING(cch3."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(cch3.unnamed_1) as sum_produits70
FROM caph_produits70 cch3
GROUP BY 1
),
grouped_caph_produitsencaiss AS (
SELECT 
	SUBSTRING(cch4."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(cch4.produits_dexploitation) as sum_produits_dexploitation
FROM caph_produitsencaiss cch4
GROUP BY 1
),
grouped_capa_charges AS (
SELECT 
	SUBSTRING(cc."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(cc.charges_dexploitation) as sum_charges_dexploitation
FROM capa_charges cc 
GROUP BY 1
),
grouped_capa_produitstarif AS (
SELECT 
	SUBSTRING(cpt."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(cpt.produits_de_lexercice) as sum_groupe_i__produits_de_la_tarification
FROM capa_produitstarif cpt 
GROUP BY 1
),
charges_produits as (
SELECT 
	cor.finess,
	CASE 
		WHEN cor.cadre = 'ERRD'
			THEN gec.sum_charges_dexploitation
		WHEN cor.cadre = 'CA PA'
			THEN gc.sum_charges_dexploitation
		WHEN cor.cadre = 'CA PH'
			THEN gcch.sum_charges_dexploitation 
	END as "Total des charges",
	CASE 
		WHEN cor.cadre = 'ERRD'
			THEN gep.sum_groupe_i__produits_de_la_tarification
		WHEN cor.cadre = 'CA PA'
			THEN gcp.sum_groupe_i__produits_de_la_tarification
		WHEN cor.cadre = 'CA PH'
			THEN gcch2.sum_groupe_i__produits_de_la_tarification
	END as "Produits de la tarification",
	CASE 
		WHEN cor.cadre = 'ERRD'
			THEN gep2.sum_produits70
		WHEN cor.cadre = 'CA PA'
			THEN 0
		WHEN cor.cadre = 'CA PH'
			THEN gcch3.sum_produits70
	END as "Produits du compte 70",
	CASE 
		WHEN cor.cadre = 'ERRD'
			THEN gep3.sum_produits_dexploitation
		WHEN cor.cadre = 'CA PA'
			THEN 0
		WHEN cor.cadre = 'CA PH'
			THEN gcch4.sum_produits_dexploitation
	END as "Total des produits (hors c/775, 777, 7781 et 78)"
FROM 
	correspondance cor
	LEFT JOIN grouped_errd_charges gec on gec.finess = cor.finess AND cor.cadre = 'ERRD' 
	LEFT JOIN grouped_errd_produitstarif gep on gep.finess = cor.finess AND cor.cadre = 'ERRD'
	LEFT JOIN grouped_errd_produits70 gep2 on gep2.finess = cor.finess AND cor.cadre = 'ERRD'
	LEFT JOIN grouped_errd_produitsencaiss gep3 on gep3.finess = cor.finess AND cor.cadre = 'ERRD'
	LEFT JOIN grouped_caph_charges gcch on gcch.finess = cor.finess AND cor.cadre = 'CA PH'
	LEFT JOIN grouped_caph_produitstarif gcch2 on gcch2.finess = cor.finess AND cor.cadre = 'CA PH'
	LEFT JOIN grouped_caph_produits70 gcch3 on gcch3.finess = cor.finess AND cor.cadre = 'CA PH'
	LEFT JOIN grouped_caph_produitsencaiss gcch4 on gcch4.finess = cor.finess AND cor.cadre = 'CA PH'
	LEFT JOIN grouped_capa_charges gc on gc.finess = cor.finess AND cor.cadre = 'CA PA'
	LEFT JOIN grouped_capa_produitstarif gcp on gcp.finess = cor.finess AND cor.cadre = 'CA PA'
),
inspections as (
SELECT 
	IIF(LENGTH(code_finess)= 8, '0'|| code_finess, code_finess) as finess,
	count(distinct identifiant_de_la_mission) as "ICE {n} (réalisé)",
	SUM(IIF(type_de_mission = 'Inspection' OR type_de_mission = 'Inspection Technique' OR type_de_mission = 'Evaluation' OR type_de_mission = 'Contrôle' OR type_de_mission = 'Enquête administrative' OR type_de_mission = 'Visites de conformité' OR type_de_mission = 'Contrôle sur place / Visite de vérification', 1, 0)) as "Inspection SUR SITE {n} - Déjà réalisée",
	SUM(IIF(type_de_mission = 'Contrôle sur pièces' OR type_de_mission = 'Contrôle sur pièces EHPAD' OR type_de_mission = 'EHPAD Contrôle sur pièces' OR type_de_mission = 'Ctrl_sur_Pièces', 1, 0)) as "Controle SUR PIECE {n} - Déjà réalisé",
	SUM(IIF(type_de_planification = "Programmé", 1, 0)) as "Inspection / contrôle Programmé 2023"
FROM "ESMS export-mission-4 evts 01 03 23 v2"
GROUP BY code_finess
),
communes as (
SELECT c.com, c.dep, c.ncc  
FROM commune_{n} c 
WHERE c.reg IS NOT NULL
UNION ALL
SELECT c.com, c2.dep, c.ncc
FROM commune_{n} c 
	LEFT JOIN commune_{n} c2 on c.comparent = c2.com AND c2.dep IS NOT NULL
WHERE c.reg IS NULL and c.com != c.comparent
)
SELECT 
--identfication de l'établissement
	r.ncc as Region,
	d.dep as "Code dép",
	d.ncc AS "Département",
	tf.categ_lib as Catégorie,
    tf.finess as "FINESS géographique",
	tf.rs as "Raison sociale ET",
	tf.ej_finess as "FINESS juridique",
	tf.ej_rs as "Raison sociale EJ",
	tf.statut_jur_lib as "Statut juridique",
	tf.adresse as Adresse,
	IIF(LENGTH(tf.adresse_code_postal) = 4, '0'|| tf.adresse_code_postal, tf.adresse_code_postal) AS "Code postal",
	c.NCC AS "Commune",
	IIF(LENGTH(tf.com_code) = 4, '0'|| tf.com_code, tf.com_code) AS "Code commune INSEE",
	CASE
        WHEN tf.categ_code = 500
            THEN CAST(NULLTOZERO(ce.total_heberg_comp_inter_places_autorisees) as INTEGER) + CAST(NULLTOZERO(ce.total_accueil_de_jour_places_autorisees) as INTEGER) + CAST(NULLTOZERO(ce.total_accueil_de_nuit_places_autorisees) as INTEGER)
        ELSE CAST(ccta.somme_de_capacite_autorisee_totale_ as INTEGER)
    END as "Capacité totale autorisée",
	CAST(ce.total_heberg_comp_inter_places_autorisees as INTEGER) as "HP Total auto",
	CAST(ce.total_accueil_de_jour_places_autorisees as INTEGER) as "AJ Total auto",
	CAST(ce.total_accueil_de_nuit_places_autorisees as INTEGER) as "HT total auto",
	co3.nb_lits_occ_{n} as "Nombre de résidents au 31/12/{n}",
	etra.nombre_total_de_chambres_installees_au_3112 as "Nombre de places installées au 31/12/{n}",
	ROUND(gp.gmp) as GMP,
	ROUND(gp.pmp) as PMP,
	ROUND((eira.taux_plus_10_medics_cip13*100), 2) as "Part des résidents ayant plus de 10 médicaments consommés par mois",
	ROUND((eira.taux_atu*100), 2) as "Taux de recours aux urgences sans hospitalisation des résidents d'EHPAD",
	ROUND((eira.taux_hospit_mco*100), 2) as "Taux de recours à l'hospitalisation MCO des résidents d'EHPAD",
	ROUND((eira.taux_hospit_had*100), 2) as "Taux de recours à l'HAD des résidents d'EHPAD",
	ROUND(chpr."Total des charges") AS "Total des charges",
	ROUND(chpr."Produits de la tarification") AS "Produits de la tarification", 
	ROUND(chpr."Produits du compte 70") AS "Produits du compte 70",
	ROUND(chpr."Total des produits (hors c/775, 777, 7781 et 78)") AS "Total des produits (hors c/775, 777, 7781 et 78)",
	"" as "Saisie des indicateurs du TDB MS (campagne {n})",
	CAST(d2.taux_dabsenteisme_hors_formation_en_ as decmail) as "Taux d'absentéisme {n_3}",
	etra2.taux_dabsenteisme_hors_formation_en_ as "Taux d'absentéisme {n_2}",
	etra.taux_dabsenteisme_hors_formation_en_ as "Taux d'absentéisme {n_1}",
    ROUND(MOY3(d2.taux_dabsenteisme_hors_formation_en_ ,etra2.taux_dabsenteisme_hors_formation_en_ , etra.taux_dabsenteisme_hors_formation_en_) ,2) as "Absentéisme moyen sur la période {n_3}-{n_1}",
	CAST(d2.taux_de_rotation_des_personnels_en_ as decimal) as "Taux de rotation du personnel titulaire {n_3}",
	etra2.taux_de_rotation_des_personnels as "Taux de rotation du personnel titulaire {n_2}",
	etra.taux_de_rotation_des_personnels as "Taux de rotation du personnel titulaire {n_1}",
	ROUND(MOY3(d2.taux_de_rotation_des_personnels_en_ , etra2.taux_de_rotation_des_personnels , etra.taux_de_rotation_des_personnels), 2) as "Rotation moyenne du personnel sur la période {n_3}-{n_1}",
	CAST(d2.taux_detp_vacants_en_ as decimal) as "ETP vacants {n_3}",
	etra2.taux_detp_vacants as "ETP vacants {n_2}",
	etra.taux_detp_vacants as "ETP vacants {n_1}",
	etra.dont_taux_detp_vacants_concernant_la_fonction_soins as "dont fonctions soins {n_1}",
	etra.dont_taux_detp_vacants_concernant_la_fonction_socio_educative as "dont fonctions socio-éducatives {n_1}", 
	CAST(REPLACE(d3.taux_de_prestations_externes_sur_les_prestations_directes_en_,',','.')as decimal) as "Taux de prestations externes sur les prestations directes {n_3}",
	etra2.taux_de_prestations_externes_sur_les_prestations_directes as "Taux de prestations externes sur les prestations directes {n_2}", 
	etra.taux_de_prestations_externes_sur_les_prestations_directes as "Taux de prestations externes sur les prestations directes {n_1}",
	ROUND(MOY3(d3.taux_de_prestations_externes_sur_les_prestations_directes_en_ , etra2.taux_de_prestations_externes_sur_les_prestations_directes , etra.taux_de_prestations_externes_sur_les_prestations_directes) ,4) as "Taux moyen de prestations externes sur les prestations directes",
	ROUND(CAST(REPLACE(d3.nombre_de_personnes_accompagnees_dans_leffectif_au_3112,',','.')as decimal)/CAST(REPLACE(d3.nombre_detp_reel_au_3112,',','.')as decimal), 2) as "Nombre total d'ETP par usager en {n_3}",
    ROUND((etra2.etp_directionencadrement + etra2.etp_administration_gestion + etra2.etp_services_generaux + etra2.etp_restauration + etra2."etp_socio-educatif" + etra2.etp_paramedical + etra2.etp_psychologue + etra2.etp_ash + etra2.etp_medical + etra2.etp_personnel_education_nationale + etra2.etp_autres_fonctions)/etra2.nombre_de_personnes_accompagnees_dans_leffectif_au_3112, 2) as "Nombre total d'ETP par usager en {n_2}",
	ROUND((etra.etp_directionencadrement + etra.etp_administration_gestion + etra.etp_services_generaux + etra.etp_restauration + etra."etp_socio-educatif" + etra.etp_paramedical + etra.etp_psychologue + etra.etp_ash + etra.etp_medical + etra.etp_personnel_education_nationale + etra.etp_autres_fonctions)/etra.nombre_de_personnes_accompagnees_dans_leffectif_au_3112, 2) as "Nombre total d'ETP par usager en {n_1}",
	MOY3(ROUND(CAST(REPLACE(d3.nombre_de_personnes_accompagnees_dans_leffectif_au_3112,',','.')as decimal)/CAST(REPLACE(d3.nombre_detp_reel_au_3112,',','.')as decimal), 2) , ROUND((etra2.etp_directionencadrement + etra2.etp_administration_gestion + etra2.etp_services_generaux + etra2.etp_restauration + etra2."etp_socio-educatif" + etra2.etp_paramedical + etra2.etp_psychologue + etra2.etp_ash + etra2.etp_medical + etra2.etp_personnel_education_nationale + etra2.etp_autres_fonctions)/etra2.nombre_de_personnes_accompagnees_dans_leffectif_au_3112, 2) , ROUND((etra.etp_directionencadrement + etra.etp_administration_gestion + etra.etp_services_generaux + etra.etp_restauration + etra."etp_socio-educatif" + etra.etp_paramedical + etra.etp_psychologue + etra.etp_ash + etra.etp_medical + etra.etp_personnel_education_nationale + etra.etp_autres_fonctions)/etra.nombre_de_personnes_accompagnees_dans_leffectif_au_3112, 2))AS "Nombre moyen d'ETP par usager sur la période 2018-{n_2}",
	ROUND((etra.etp_paramedical + etra.etp_medical)/etra.nombre_de_personnes_accompagnees_dans_leffectif_au_3112, 2) as "ETP 'soins' par usager en {n_1}",
	ROUND(etra."-_dont_nombre_detp_reels_de_medecin_coordonnateur", 2) as "dont médecin coordonnateur",
	ROUND(etra.etp_directionencadrement + etra.etp_administration_gestion + etra.etp_services_generaux + etra.etp_restauration + etra."etp_socio-educatif" + etra.etp_paramedical + etra.etp_psychologue + etra.etp_ash + etra.etp_medical + etra.etp_personnel_education_nationale + etra.etp_autres_fonctions, 2) as "Total du nombre d'ETP",
	NULLTOZERO(rs.nb_recla) as "Nombre de réclamations sur la période 2018-{n}",
	NULLTOZERO(ROUND(rs.nb_recla / ccta.somme_de_capacite_autorisee_totale_, 4)*100) as "Rapport réclamations / capacité",
	NULLTOZERO(rs."Hôtellerie-locaux-restauration") as "Recla IGAS : Hôtellerie-locaux-restauration",
	NULLTOZERO(rs."Problème d?organisation ou de fonctionnement de l?établissement ou du service") as "Recla IGAS : Problème d’organisation ou de fonctionnement de l’établissement ou du service",
	NULLTOZERO(rs."Problème de qualité des soins médicaux") as "Recla IGAS : Problème de qualité des soins médicaux",
	NULLTOZERO(rs."Problème de qualité des soins paramédicaux") as "Recla IGAS : Problème de qualité des soins paramédicaux",
	NULLTOZERO(rs."Recherche d?établissement ou d?un professionnel") as "Recla IGAS : Recherche d’établissement ou d’un professionnel",
	NULLTOZERO(rs."Mise en cause attitude des professionnels") as "Recla IGAS : Mise en cause attitude des professionnels",
	NULLTOZERO(rs."Informations et droits des usagers") as "Recla IGAS : Informations et droits des usagers",
	NULLTOZERO(rs."Facturation et honoraires") as "Recla IGAS : Facturation et honoraires",
	NULLTOZERO(rs."Santé-environnementale") as "Recla IGAS : Santé-environnementale",
	NULLTOZERO(rs."Activités d?esthétique réglementées") as "Recla IGAS : Activités d’esthétique réglementées",
	NULLTOZERO(rs.nb_signa) as "Nombre de Signalement sur la période {n_2}-2023",
	NULLTOZERO(i."ICE {n} (réalisé)") as "ICE {n} (réalisé)",
	NULLTOZERO(i."Inspection SUR SITE {n} - Déjà réalisée") as "Inspection SUR SITE {n} - Déjà réalisée",
	NULLTOZERO(i."Controle SUR PIECE {n} - Déjà réalisé") as "Controle SUR PIECE {n} - Déjà réalisé",
	NULLTOZERO(i."Inspection / contrôle Programmé 2023") as "Inspection / contrôle Programmé 2023"
FROM
	tfiness_clean tf 
	LEFT JOIN communes c on c.com = tf.com_code
	LEFT JOIN departement_{n} d on d.dep = c.dep
	LEFT JOIN region_{n}  r on d.reg = r.reg
	LEFT JOIN capacites_ehpad ce on ce."et-ndegfiness" = tf.finess
	LEFT JOIN clean_capacite_totale_auto ccta on ccta.finess = tf.finess
	LEFT JOIN occupation_{n_3}_{n_2} o1 on o1.finess_19 = tf.finess
	LEFT JOIN occupation_{n_1} o2  on o2.finess = tf.finess
	LEFT JOIN clean_occupation_{n} co3  on co3.finess = tf.finess
	LEFT JOIN clean_tdb_{n_1} etra on etra.finess = tf.finess
	LEFT JOIN clean_hebergement c_h on c_h.finess = tf.finess
	LEFT JOIN gmp_pmp gp on IIF(LENGTH(gp.finess_et) = 8, '0'|| gp.finess_et, gp.finess_et) = tf.finess
	LEFT JOIN charges_produits chpr on chpr.finess = tf.finess
	LEFT JOIN EHPAD_Indicateurs_{n_1}_REG_agg eira on eira.et_finess = tf.finess
	LEFT JOIN diamant_{n_3} d2 on SUBSTRING(d2.finess,1,9) = tf.finess
	LEFT JOIN clean_tdb_{n_2} etra2 on etra2.finess = tf.finess
	LEFT JOIN diamantç{n_3}_2 d3 on SUBSTRING(d3.finess,1,9) = tf.finess
	LEFT JOIN recla_signalement rs on rs.finess = tf.finess
	LEFT JOIN inspections i on i.finess = tf.finess
WHERE r.reg = {}
ORDER BY tf.finess ASC'''.format(region)
    return requeteCiblage

def _requeteControleHDF(region):
    requeteControle = '''
    -- Controle
WITH tfiness_clean as (
SELECT
	IIF(LENGTH(tf_with.finess )= 8, '0'|| tf_with.finess, tf_with.finess) as finess,
	tf_with.categ_lib,
    tf_with.categ_code,
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
	tf_with.com_code 
FROM "t-finess" tf_with
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
),
-- nombre de réclamation
table_recla as (
SELECT 
	IIF(LENGTH(se.ndeg_finessrpps )= 8, '0'|| se.ndeg_finessrpps, se.ndeg_finessrpps) as finess,
	COUNT(*) as nb_recla
FROM reclamations_mars20_mars2023 se
WHERE 
	se.ndeg_finessrpps  IS NOT NULL
	AND (se.Signalement = 'Non' or se.Signalement IS NULL)
GROUP BY 1
),
-- Motig IGAS
igas as (
SELECT 
	IIF(LENGTH(se.ndeg_finessrpps )= 8, '0'|| se.ndeg_finessrpps, se.ndeg_finessrpps) as finess, 
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
FROM reclamations_mars20_mars2023 se
WHERE 
	(se.Signalement = 'Non' or se.Signalement IS NULL)
	AND se.ndeg_finessrpps  IS NOT NULL
GROUP BY 1
),
-- info signalement
sign as (
SELECT 
	IIF(LENGTH(se.ndeg_finessrpps )= 8, '0'|| se.ndeg_finessrpps, se.ndeg_finessrpps) as finess,
	COUNT(*) as nb_signa
FROM reclamations_mars20_mars2023 se	
WHERE 
	se.signalement = 'Oui'
	AND se.ndeg_finessrpps  IS NOT NULL
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
),
clean_occupation_{n} as (
SELECT 
	IIF(LENGTH(o3.finess) = 8, '0'|| o3.finess, o3.finess) as finess, 
	o3.taux_occ_{n},
	o3.nb_lits_autorises_installes,
	o3.nb_lits_occ_{n},
	o3.taux_occ_trimestre3 
FROM occupation_{n} o3 
),
clean_capacite_totale_auto as (
SELECT 
	IIF(LENGTH(cta.etiquettes_de_lignes )= 8, '0'|| cta.etiquettes_de_lignes, cta.etiquettes_de_lignes) as finess,
	cta.somme_de_capacite_autorisee_totale_ 
FROM capacite_totale_auto cta 
),
clean_hebergement as (
SELECT 
	IIF(LENGTH(h.finesset )= 8, '0'|| h.finesset, h.finesset) as finess,
	h.prixhebpermcs 
FROM hebergement h 
),
clean_tdb_{n_2} as (
SELECT 
	IIF(LENGTH(tdb_{n_2}.finess_geographique) = 8, '0'|| tdb_{n_2}.finess_geographique, tdb_{n_2}.finess_geographique) as finess,
	*
FROM "export-tdbesms-{n_2}-region_agg" tdb_{n_2}
), 
clean_tdb_{n_1} as (
SELECT 
	IIF(LENGTH(tdb_{n_1}.finess_geographique )= 8, '0'|| tdb_{n_1}.finess_geographique, tdb_{n_1}.finess_geographique) as finess,
	*
FROM "export-tdbesms-{n_1}-region-agg" tdb_{n_1}
), 
-- La partie suivante sert à créer un table temporaire pour classer les charges et les produits
correspondance as(
SELECT  
	SUBSTRING(cecpp."finess_-_rs_et" ,1,9) as finess,
	cecpp.cadre
FROM 
	choix_errd_ca_pa_ph cecpp
	LEFT JOIN doublons_errd_ca dou on SUBSTRING(dou.finess,1,9) = SUBSTRING(cecpp."finess_-_rs_et",1,9) AND cecpp.cadre != 'ERRD' 
WHERE
	dou.finess IS NULL
),
grouped_errd_charges AS (
SELECT 
	SUBSTRING(ec."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(ec.charges_dexploitation) as sum_charges_dexploitation
FROM errd_charges ec
GROUP BY 1
),
grouped_errd_produitstarif AS (
SELECT 
	SUBSTRING(ep."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(ep.groupe_i__produits_de_la_tarification) as sum_groupe_i__produits_de_la_tarification
FROM errd_produitstarif ep
GROUP BY 1
),
grouped_errd_produits70 AS (
SELECT 
	SUBSTRING(ep2."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(ep2.unnamed_1) as sum_produits70
FROM errd_produits70 ep2
GROUP BY 1
),
grouped_errd_produitsencaiss AS (
SELECT 
	SUBSTRING(ep3."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(ep3.produits_dexploitation) as sum_produits_dexploitation
FROM errd_produitsencaiss ep3
GROUP BY 1
),
grouped_caph_charges AS (
SELECT 
	SUBSTRING(cch."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(cch.charges_dexploitation) as sum_charges_dexploitation
FROM caph_charges cch 
GROUP BY 1
),
grouped_caph_produitstarif AS (
SELECT 
	SUBSTRING(cch2."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(cch2.groupe_i__produits_de_la_tarification) as sum_groupe_i__produits_de_la_tarification
FROM caph_produitstarif cch2
GROUP BY 1
),
grouped_caph_produits70 AS (
SELECT
	SUBSTRING(cch3."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(cch3.unnamed_1) as sum_produits70
FROM caph_produits70 cch3
GROUP BY 1
),
grouped_caph_produitsencaiss AS (
SELECT 
	SUBSTRING(cch4."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(cch4.produits_dexploitation) as sum_produits_dexploitation
FROM caph_produitsencaiss cch4
GROUP BY 1
),
grouped_capa_charges AS (
SELECT 
	SUBSTRING(cc."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(cc.charges_dexploitation) as sum_charges_dexploitation
FROM capa_charges cc 
GROUP BY 1
),
grouped_capa_produitstarif AS (
SELECT 
	SUBSTRING(cpt."structure_-_finess_-_raison_sociale",1,9) as finess,
	SUM(cpt.produits_de_lexercice) as sum_groupe_i__produits_de_la_tarification
FROM capa_produitstarif cpt 
GROUP BY 1
),
charges_produits as (
SELECT 
	cor.finess,
	CASE 
		WHEN cor.cadre = 'ERRD'
			THEN gec.sum_charges_dexploitation
		WHEN cor.cadre = 'CA PA'
			THEN gc.sum_charges_dexploitation
		WHEN cor.cadre = 'CA PH'
			THEN gcch.sum_charges_dexploitation 
	END as "Total des charges",
	CASE 
		WHEN cor.cadre = 'ERRD'
			THEN gep.sum_groupe_i__produits_de_la_tarification
		WHEN cor.cadre = 'CA PA'
			THEN gcp.sum_groupe_i__produits_de_la_tarification
		WHEN cor.cadre = 'CA PH'
			THEN gcch2.sum_groupe_i__produits_de_la_tarification
	END as "Produits de la tarification",
	CASE 
		WHEN cor.cadre = 'ERRD'
			THEN gep2.sum_produits70
		WHEN cor.cadre = 'CA PA'
			THEN 0
		WHEN cor.cadre = 'CA PH'
			THEN gcch3.sum_produits70
	END as "Produits du compte 70",
	CASE 
		WHEN cor.cadre = 'ERRD'
			THEN gep3.sum_produits_dexploitation
		WHEN cor.cadre = 'CA PA'
			THEN 0
		WHEN cor.cadre = 'CA PH'
			THEN gcch4.sum_produits_dexploitation
	END as "Total des produits (hors c/775, 777, 7781 et 78)"
FROM 
	correspondance cor
	LEFT JOIN grouped_errd_charges gec on gec.finess = cor.finess AND cor.cadre = 'ERRD' 
	LEFT JOIN grouped_errd_produitstarif gep on gep.finess = cor.finess AND cor.cadre = 'ERRD'
	LEFT JOIN grouped_errd_produits70 gep2 on gep2.finess = cor.finess AND cor.cadre = 'ERRD'
	LEFT JOIN grouped_errd_produitsencaiss gep3 on gep3.finess = cor.finess AND cor.cadre = 'ERRD'
	LEFT JOIN grouped_caph_charges gcch on gcch.finess = cor.finess AND cor.cadre = 'CA PH'
	LEFT JOIN grouped_caph_produitstarif gcch2 on gcch2.finess = cor.finess AND cor.cadre = 'CA PH'
	LEFT JOIN grouped_caph_produits70 gcch3 on gcch3.finess = cor.finess AND cor.cadre = 'CA PH'
	LEFT JOIN grouped_caph_produitsencaiss gcch4 on gcch4.finess = cor.finess AND cor.cadre = 'CA PH'
	LEFT JOIN grouped_capa_charges gc on gc.finess = cor.finess AND cor.cadre = 'CA PA'
	LEFT JOIN grouped_capa_produitstarif gcp on gcp.finess = cor.finess AND cor.cadre = 'CA PA'
),
inspections as (
SELECT 
	IIF(LENGTH(code_finess)= 8, '0'|| code_finess, code_finess) as finess,
	count(distinct identifiant_de_la_mission) as "ICE N (réalisé)",
	SUM(IIF(type_de_mission = 'Inspection' OR type_de_mission = 'Inspection Technique' OR type_de_mission = 'Evaluation' OR type_de_mission = 'Contrôle' OR type_de_mission = 'Enquête administrative' OR type_de_mission = 'Visites de conformité' OR type_de_mission = 'Contrôle sur place / Visite de vérification', 1, 0)) as "Inspection SUR SITE {n} - Déjà réalisée",
	SUM(IIF(type_de_mission = 'Contrôle sur pièces' OR type_de_mission = 'Contrôle sur pièces EHPAD' OR type_de_mission = 'EHPAD Contrôle sur pièces' OR type_de_mission = 'Ctrl_sur_Pièces', 1, 0)) as "Controle SUR PIECE {n} - Déjà réalisé",
	SUM(IIF(type_de_planification = "Programmé", 1, 0)) as "Inspection / contrôle Programmé 2023"
FROM "ESMS export-mission-4 evts 01 03 23 v2"
GROUP BY code_finess
),
communes as (
SELECT c.com, c.dep, c.ncc  
FROM commune_{n} c 
WHERE c.reg IS NOT NULL
UNION ALL
SELECT c.com, c2.dep, c.ncc
FROM commune_{n} c 
	LEFT JOIN commune_{n} c2 on c.comparent = c2.com AND c2.dep IS NOT NULL
WHERE c.reg IS NULL and c.com != c.comparent
)
SELECT 
--identfication de l'établissement 
	r.ncc as Region,
	d.dep as "Code dép",
	d.ncc AS "Département",
	tf.categ_lib as Catégorie,
    tf.finess as "FINESS géographique",
	tf.rs as "Raison sociale ET",
	tf.ej_finess as "FINESS juridique",
	tf.ej_rs as "Raison sociale EJ",
	tf.statut_jur_lib as "Statut juridique",
	tf.adresse as Adresse,
	IIF(LENGTH(tf.adresse_code_postal) = 4, '0'|| tf.adresse_code_postal, tf.adresse_code_postal) AS "Code postal",
	c.NCC AS "Commune",
	IIF(LENGTH(tf.com_code) = 4, '0'|| tf.com_code, tf.com_code) AS "Code commune INSEE",
	--	caractéristiques ESMS
	CASE
        WHEN tf.categ_code = 500
            THEN CAST(NULLTOZERO(ce.total_heberg_comp_inter_places_autorisees) as INTEGER) + CAST(NULLTOZERO(ce.total_accueil_de_jour_places_autorisees) as INTEGER) + CAST(NULLTOZERO(ce.total_accueil_de_nuit_places_autorisees) as INTEGER)
        ELSE CAST(ccta.somme_de_capacite_autorisee_totale_ as INTEGER)
    END as "Capacité totale autorisée",
	CAST(ce.total_heberg_comp_inter_places_autorisees as INTEGER) as "HP Total auto",
	CAST(ce.total_accueil_de_jour_places_autorisees as INTEGER) as "AJ Total auto",
	CAST(ce.total_accueil_de_nuit_places_autorisees as INTEGER) as "HT total auto",
	o1.taux_occ_{n_2} AS "Taux d'occupation {n_2}",
	o2.taux_occ_{n_1} AS "Taux d'occupation {n_1}",
	co3.taux_occ_{n} AS "Taux d'occupation {n}",
	co3.nb_lits_occ_{n} as "Nombre de résidents au 31/12/{n}",
	etra.nombre_total_de_chambres_installees_au_3112 as "Nombre de places installées au 31/12/{n}",
	co3.taux_occ_trimestre3 AS "Taux occupation au 31/12/{n}",
	c_h.prixhebpermcs AS "Prix de journée hébergement (EHPAD uniquement)",
	ROUND(gp.gmp) as GMP,
	ROUND(gp.pmp) as PMP,
	etra.personnes_gir_1 AS "Part de résidents GIR 1 (31/12/{n_1})",
	etra.personnes_gir_2 AS "Part de résidents GIR 2 (31/12/{n_1})",
	etra.personnes_gir_3 AS "Part de résidents GIR 3 (31/12/{n_1})",
	ROUND((eira.taux_plus_10_medics_cip13*100), 2) as "Part des résidents ayant plus de 10 médicaments consommés par mois",
	ROUND((eira.taux_atu*100), 2) as "Taux de recours aux urgences sans hospitalisation des résidents d'EHPAD",
	ROUND((eira.taux_hospit_mco*100), 2) as "Taux de recours à l'hospitalisation MCO des résidents d'EHPAD",
	ROUND((eira.taux_hospit_had*100), 2) as "Taux de recours à l'HAD des résidents d'EHPAD",
	ROUND(chpr."Total des charges") AS "Total des charges",
	ROUND(chpr."Produits de la tarification") AS "Produits de la tarification", 
	ROUND(chpr."Produits du compte 70") AS "Produits du compte 70",
	ROUND(chpr."Total des produits (hors c/775, 777, 7781 et 78)") AS "Total des produits (hors c/775, 777, 7781 et 78)",
	"" as "Saisie des indicateurs du TDB MS (campagne {n})",
	CAST(d2.taux_dabsenteisme_hors_formation_en_ as decmail) as "Taux d'absentéisme {n_3}",
	etra2.taux_dabsenteisme_hors_formation_en_ as "Taux d'absentéisme {n_2}",
	etra.taux_dabsenteisme_hors_formation_en_ as "Taux d'absentéisme {n_1}",
    ROUND(MOY3(d2.taux_dabsenteisme_hors_formation_en_ ,etra2.taux_dabsenteisme_hors_formation_en_ , etra.taux_dabsenteisme_hors_formation_en_) ,2) as "Absentéisme moyen sur la période {n_3}-{n_1}",
	CAST(d2.taux_de_rotation_des_personnels_en_ as decimal) as "Taux de rotation du personnel titulaire {n_3}",
	etra2.taux_de_rotation_des_personnels as "Taux de rotation du personnel titulaire {n_2}",
	etra.taux_de_rotation_des_personnels as "Taux de rotation du personnel titulaire {n_1}",
	ROUND(MOY3(d2.taux_de_rotation_des_personnels_en_ , etra2.taux_de_rotation_des_personnels , etra.taux_de_rotation_des_personnels), 2) as "Rotation moyenne du personnel sur la période {n_3}-{n_1}",
	CAST(d2.taux_detp_vacants_en_ as decimal) as "ETP vacants {n_3}",
	etra2.taux_detp_vacants as "ETP vacants {n_2}",
	etra.taux_detp_vacants as "ETP vacants {n_1}",
	etra.dont_taux_detp_vacants_concernant_la_fonction_soins as "dont fonctions soins {n_1}",
	etra.dont_taux_detp_vacants_concernant_la_fonction_socio_educative as "dont fonctions socio-éducatives {n_1}", 
	CAST(REPLACE(d3.taux_de_prestations_externes_sur_les_prestations_directes_en_,',','.')as decimal) as "Taux de prestations externes sur les prestations directes {n_3}",
	etra2.taux_de_prestations_externes_sur_les_prestations_directes as "Taux de prestations externes sur les prestations directes {n_2}", 
	etra.taux_de_prestations_externes_sur_les_prestations_directes as "Taux de prestations externes sur les prestations directes {n_1}",
	ROUND(MOY3(d3.taux_de_prestations_externes_sur_les_prestations_directes_en_ , etra2.taux_de_prestations_externes_sur_les_prestations_directes , etra.taux_de_prestations_externes_sur_les_prestations_directes) ,2) as "Taux moyen de prestations externes sur les prestations directes",
	ROUND(CAST(REPLACE(d3.nombre_de_personnes_accompagnees_dans_leffectif_au_3112,',','.')as decimal)/CAST(REPLACE(d3.nombre_detp_reel_au_3112,',','.')as decimal), 2) as "Nombre total d'ETP par usager en {n_3}",
    ROUND((etra2.etp_directionencadrement + etra2.etp_administration_gestion + etra2.etp_services_generaux + etra2.etp_restauration + etra2."etp_socio-educatif" + etra2.etp_paramedical + etra2.etp_psychologue + etra2.etp_ash + etra2.etp_medical + etra2.etp_personnel_education_nationale + etra2.etp_autres_fonctions)/etra2.nombre_de_personnes_accompagnees_dans_leffectif_au_3112, 2) as "Nombre total d'ETP par usager en {n_2}",
	ROUND((etra.etp_directionencadrement + etra.etp_administration_gestion + etra.etp_services_generaux + etra.etp_restauration + etra."etp_socio-educatif" + etra.etp_paramedical + etra.etp_psychologue + etra.etp_ash + etra.etp_medical + etra.etp_personnel_education_nationale + etra.etp_autres_fonctions)/etra.nombre_de_personnes_accompagnees_dans_leffectif_au_3112, 2) as "Nombre total d'ETP par usager en {n_1}",
	MOY3(ROUND(CAST(REPLACE(d3.nombre_de_personnes_accompagnees_dans_leffectif_au_3112,',','.')as decimal)/CAST(REPLACE(d3.nombre_detp_reel_au_3112,',','.')as decimal), 2) , ROUND((etra2.etp_directionencadrement + etra2.etp_administration_gestion + etra2.etp_services_generaux + etra2.etp_restauration + etra2."etp_socio-educatif" + etra2.etp_paramedical + etra2.etp_psychologue + etra2.etp_ash + etra2.etp_medical + etra2.etp_personnel_education_nationale + etra2.etp_autres_fonctions)/etra2.nombre_de_personnes_accompagnees_dans_leffectif_au_3112, 2) , ROUND((etra.etp_directionencadrement + etra.etp_administration_gestion + etra.etp_services_generaux + etra.etp_restauration + etra."etp_socio-educatif" + etra.etp_paramedical + etra.etp_psychologue + etra.etp_ash + etra.etp_medical + etra.etp_personnel_education_nationale + etra.etp_autres_fonctions)/etra.nombre_de_personnes_accompagnees_dans_leffectif_au_3112, 2))AS "Nombre moyen d'ETP par usager sur la période 2018-{n_2}",
	ROUND((etra.etp_paramedical + etra.etp_medical)/etra.nombre_de_personnes_accompagnees_dans_leffectif_au_3112, 2) as "ETP 'soins' par usager en {n_1}",
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
	ROUND(etra."-_dont_nombre_detp_reels_de_medecin_coordonnateur", 2) as "dont médecin coordonnateur",
	etra."-_dont_autre_medical" AS "dont autre médical",
	etra.etp_personnel_education_nationale AS "Personnel éducation nationale",
	etra.etp_autres_fonctions AS "Autres fonctions",
	ROUND(etra.etp_directionencadrement + etra.etp_administration_gestion + etra.etp_services_generaux + etra.etp_restauration + etra."etp_socio-educatif" + etra.etp_paramedical + etra.etp_psychologue + etra.etp_ash + etra.etp_medical + etra.etp_personnel_education_nationale + etra.etp_autres_fonctions, 2) as "Total du nombre d'ETP",
	NULLTOZERO(rs.nb_recla) as "Nombre de réclamations sur la période 2018-{n}",
	NULLTOZERO(ROUND(rs.nb_recla / ccta.somme_de_capacite_autorisee_totale_, 4)*100) as "Rapport réclamations / capacité",
	NULLTOZERO(rs."Hôtellerie-locaux-restauration") as "Recla IGAS : Hôtellerie-locaux-restauration",
	NULLTOZERO(rs."Problème d?organisation ou de fonctionnement de l?établissement ou du service") as "Recla IGAS : Problème d’organisation ou de fonctionnement de l’établissement ou du service",
	NULLTOZERO(rs."Problème de qualité des soins médicaux") as "Recla IGAS : Problème de qualité des soins médicaux",
	NULLTOZERO(rs."Problème de qualité des soins paramédicaux") as "Recla IGAS : Problème de qualité des soins paramédicaux",
	NULLTOZERO(rs."Recherche d?établissement ou d?un professionnel") as "Recla IGAS : Recherche d’établissement ou d’un professionnel",
	NULLTOZERO(rs."Mise en cause attitude des professionnels") as "Recla IGAS : Mise en cause attitude des professionnels",
	NULLTOZERO(rs."Informations et droits des usagers") as "Recla IGAS : Informations et droits des usagers",
	NULLTOZERO(rs."Facturation et honoraires") as "Recla IGAS : Facturation et honoraires",
	NULLTOZERO(rs."Santé-environnementale") as "Recla IGAS : Santé-environnementale",
	NULLTOZERO(rs."Activités d?esthétique réglementées") as "Recla IGAS : Activités d’esthétique réglementées",
	NULLTOZERO(rs.nb_signa) as "Nombre de Signalement sur la période {n_2}-2023",
--	NULLTOZERO(rs."Nombre d'EI sur la période 36mois") as "Nombre d'EI sur la période 36mois",
--	NULLTOZERO(rs.NB_EIGS) as "Nombre d'EIG sur la période {n_2}-2023",
--	NULLTOZERO(rs.NB_EIAS) as "Nombre d'EIAS sur la période {n_2}-2023",
--	NULLTOZERO(rs."Nombre d'EI sur la période 36mois" + NULLTOZERO(rs.NB_EIGS) + NULLTOZERO(rs.NB_EIAS)) as "Somme EI + EIGS + EIAS sur la période {n_2}-2023",
--	NULLTOZERO(rs."nb EI/EIG : Acte de prévention") as "nb EI/EIG : Acte de prévention",
--	NULLTOZERO(rs."nb EI/EIG : Autre prise en charge") as "nb EI/EIG : Autre prise en charge",
--	NULLTOZERO(rs."nb EI/EIG : Chute") as "nb EI/EIG : Chute",
--	NULLTOZERO(rs."nb EI/EIG : Disparition inquiétante et fugues (Hors SDRE/SDJ/SDT)") as "nb EI/EIG : Disparition inquiétante et fugues (Hors SDRE/SDJ/SDT)",
--	NULLTOZERO(rs."nb EI/EIG : Dispositif médical") as "nb EI/EIG : Dispositif médical",
--	NULLTOZERO(rs."nb EI/EIG : Fausse route") as "nb EI/EIG : Fausse route",
--	NULLTOZERO(rs."nb EI/EIG : Infection associée aux soins (IAS) hors ES") as "nb EI/EIG : Infection associée aux soins (IAS) hors ES",
--	NULLTOZERO(rs."nb EI/EIG : Infection associée aux soins en EMS et ambulatoire (IAS hors ES)") as "nb EI/EIG : Infection associée aux soins en EMS et ambulatoire (IAS hors ES)",
--	NULLTOZERO(rs."nb EI/EIG : Parcours/Coopération interprofessionnelle") as "nb EI/EIG : Parcours/Coopération interprofessionnelle",
--	NULLTOZERO(rs."nb EI/EIG : Prise en charge chirurgicale") as "nb EI/EIG : Prise en charge chirurgicale",
--	NULLTOZERO(rs."nb EI/EIG : Prise en charge diagnostique") as "nb EI/EIG : Prise en charge diagnostique",
--	NULLTOZERO(rs."nb EI/EIG : Prise en charge en urgence") as "nb EI/EIG : Prise en charge en urgence",
--	NULLTOZERO(rs."nb EI/EIG : Prise en charge médicamenteuse") as "nb EI/EIG : Prise en charge médicamenteuse",
--	NULLTOZERO(rs."nb EI/EIG : Prise en charge des cancers") as "nb EI/EIG : Prise en charge des cancers",
--	NULLTOZERO(rs."nb EI/EIG : Prise en charge psychiatrique") as "nb EI/EIG : Prise en charge psychiatrique",
--	NULLTOZERO(rs."nb EI/EIG : Suicide") as "nb EI/EIG : Suicide",
--	NULLTOZERO(rs."nb EI/EIG : Tentative de suicide") as "nb EI/EIG : Tentative de suicide",
	NULLTOZERO(i."ICE {n} (réalisé)") as "ICE {n} (réalisé)",
	NULLTOZERO(i."Inspection SUR SITE {n} - Déjà réalisée") as "Inspection SUR SITE {n} - Déjà réalisée",
	NULLTOZERO(i."Controle SUR PIECE {n} - Déjà réalisé") as "Controle SUR PIECE {n} - Déjà réalisé",
	NULLTOZERO(i."Inspection / contrôle Programmé 2023") as "Inspection / contrôle Programmé 2023"
FROM
 --identification
	tfiness_clean tf 
	LEFT JOIN communes c on c.com = tf.com_code
	LEFT JOIN departement_{n} d on d.dep = c.dep
	LEFT JOIN region_{n}  r on d.reg = r.reg
	LEFT JOIN capacites_ehpad ce on ce."et-ndegfiness" = tf.finess
	LEFT JOIN clean_capacite_totale_auto ccta on ccta.finess = tf.finess
	LEFT JOIN occupation_{n_3}_{n_2} o1 on o1.finess_19 = tf.finess
	LEFT JOIN occupation_{n_1} o2  on o2.finess = tf.finess
	LEFT JOIN clean_occupation_{n} co3  on co3.finess = tf.finess
	LEFT JOIN clean_tdb_{n_1} etra on etra.finess = tf.finess
	LEFT JOIN clean_hebergement c_h on c_h.finess = tf.finess
	LEFT JOIN gmp_pmp gp on IIF(LENGTH(gp.finess_et) = 8, '0'|| gp.finess_et, gp.finess_et) = tf.finess
	LEFT JOIN charges_produits chpr on chpr.finess = tf.finess
	LEFT JOIN EHPAD_Indicateurs_{n_1}_REG_agg eira on eira.et_finess = tf.finess
	LEFT JOIN diamant_{n_3} d2 on SUBSTRING(d2.finess,1,9) = tf.finess
	LEFT JOIN clean_tdb_{n_2} etra2 on etra2.finess = tf.finess
	LEFT JOIN diamantç{n_3}_2 d3 on SUBSTRING(d3.finess,1,9) = tf.finess
	LEFT JOIN recla_signalement rs on rs.finess = tf.finess
	LEFT JOIN inspections i on i.finess = tf.finess
WHERE r.reg = '{}'
ORDER BY tf.finess ASC
    '''.format(region)
    return requeteControle
