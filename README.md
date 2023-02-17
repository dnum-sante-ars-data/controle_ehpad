# Structure du projet

main.py
README.md
_ data
    |_ input
        demo.xlsx
    |_ to_csv
        demo.csv
    |_ output
        demo.csv
    |_ database
        controle_ehpad.db
	demo.db
_ modules
    |_ init_db
        init_db.py
    |_ transform
        transform.py
        transform_query.json
    |_ export
        export.py
_ utils
        utils.py
_ settings
        settings.json
        settings_demo.json


# Fonctionnalités du script
... à remplir

# Prérequis
... données dans input : format, règles etc. 
... settings à créer sur base de settings.demo

# Commandes du script
python main.py init_db
python main.py transform
python main.py export
python main.py all
