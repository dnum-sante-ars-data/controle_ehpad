010781029

SELECT 
etiquettes_de_lignes, somme_de_capacite_autorisee_totale_  
FROM capacite_totale_auto cta 
WHERE etiquettes_de_lignes = '420787442'



--'730009420'


SELECT 
ce."ej-ndegfiness", 
CAST(ce.total_heberg_comp_inter_places_autorisees as INTEGER) as "HP Total auto",
	CAST(ce.total_accueil_de_jour_places_autorisees as INTEGER) as "AJ Total auto",
	CAST(ce.total_accueil_de_nuit_places_autorisees as INTEGER) as "HT total auto",
	*
FROM capacites_ehpad ce 
WHERE "et-ndegfiness" = '420787442'