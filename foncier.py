#!/usr/local/bin/python
# -*- coding: utf-8 -*-
# Python 2.7.X

"""
Présentation et logique du programme
------------------------------------
Le programme accepte 4 arguments (-s, -a, -f, -sql), exécuter 'python foncier.py --help' pour plus de détails
Et fonctionne avec les variables d'environnement du fichier .env (ou .env.sample) situé dans ce même dossier (bien remplir chaque variable)
A partir de là, le but est d'importer dans un premier temps toutes les données nécessaires au bon fonctionnement du script (via les variables tables_dependencies et urlImport),
puis d'exécuter l'ensemble des fichiers SQL du dossier de procédures SQL (argument -sql). 
Le script est découpé en deux, avant et après la qualification des propriétaires. Ceci permettant de sortir du script afin de qualifier manuellement des propriétaires non classés.
Puis de repartir du point de sortie pour terminer l'exécution du script.
"""

import os
import sys
import subprocess
from dotenv import load_dotenv
import pandas as pd
import argparse
import io
from urllib import request
from urllib.error import URLError
import zipfile
load_dotenv()

# Création des arguments
parser = argparse.ArgumentParser(description='MAJ des fichiers fonciers Majic 3')
parser.add_argument("-s", "--schema", required=True, help="Nom du schéma de travail PostgreSQL")
parser.add_argument("-a", "--annee", required=True, help="Année de MAJ")
parser.add_argument("-f", "--sourcefile", required=False, help="Chemin vers les sources de données")
parser.add_argument("-ff", "--sourceschema", required=False, help="Chemin vers le schéma des sources de données")
parser.add_argument("-sql", "--sqlpathfiles", required=True, help="Chemin vers les scripts SQL")
args = parser.parse_args()

# Console : recommandation (travail dans un schéma PostgreSQL vide)
clean_schema = input('Avant de lancer le script, il est préférable que le schéma {0} soit vide, souhaitez-vous poursuivre ? (y/n)'.format(args.schema)).lower().strip() == 'y'

if clean_schema:
    
    # Pour permettre de lancer le script de zéro jusqu'à la qualification manuelle des propriétaires
    # Ou de la qualification manuelle des propriétaires jusqu'à la fin
    start_script = input('y: lancement du script à partir du début jusqu\'à la qualification des propriétaires / n : lancement du script après la qualification manuelle des propriétaires ? (y/n): ').lower().strip() == 'y'
    
    # Chargement des variables d'environnement
    PGBINPATH = os.getenv('PGBINPATH')
    QGISBINPATH = os.getenv('QGISBINPATH')
    PGHOST = os.getenv('PGHOST')
    PGDB = os.getenv('PGDB')
    PGUSER = os.getenv('PGUSER')
    PGPORT = os.getenv('PGPORT')
    PGPASSWORD = os.getenv('PGPASSWORD')
    # PGPASSWORD est paramétré dans C:\Users\froger\AppData\Roaming\postgresql\pgpass.conf
    PGPRODHOST = os.getenv('PGPRODHOST')
    PGPRODDB = os.getenv('PGPRODDB')
    PGPRODUSER = os.getenv('PGPRODUSER')
    PGPRODPORT = os.getenv('PGPRODPORT')
    PGPRODPASSWORD = os.getenv('PGPRODPASSWORD')

    YEAR = args.annee
    PREVIOUSYEAR = int(YEAR) - 1
    SCHEMANAME = args.schema
    PATHSOURCE = args.sourcefile
    SCHEMASOURCE = args.sourceschema
    PATHSQL = args.sqlpathfiles

    # Vérification de la présence des dossiers de procédures SQL
    if not os.path.exists(os.path.join(PATHSQL, '01_import')):
        print("Le dossier 01_import est manquant dans {0}".format(PATHSQL))
        exit()
    if not os.path.exists(os.path.join(PATHSQL, '02_creation_tables_finales')):
        print("Le dossier 02_creation_tables_finales est manquant dans {0}".format(PATHSQL))
        exit()
    if not os.path.exists(os.path.join(PATHSQL, '03_traitements_proprietaire')):
        print("Le dossier 03_traitements_proprietaire est manquant dans {0}".format(PATHSQL))
        exit()
    if not os.path.exists(os.path.join(PATHSQL, '04_tables_stat')):
        print("Le dossier 04_tables_stat est manquant dans {0}".format(PATHSQL))
        exit()
    if not os.path.exists(os.path.join(PATHSQL, '05_adaptation_donnees_mairie')):
        print("Le dossier 05_adaptation_donnees_mairie est manquant dans {0}".format(PATHSQL))
        exit()
        
    # Démarrage
    if start_script:
        os.chdir(PGBINPATH)
        print('Lancement du script')
        print('Création des schémas')
        SQLQUERYSCHEMA = ("CREATE SCHEMA IF NOT EXISTS " + str(SCHEMANAME) + "; CREATE SCHEMA IF NOT EXISTS donnees_bases; DROP TABLE IF EXISTS " + str(SCHEMANAME) + ".chargepropglobal; CREATE TABLE IF NOT EXISTS " + str(SCHEMANAME) + ".chargepropglobal (data character varying); DROP TABLE IF EXISTS " + str(SCHEMANAME) + ".chargpdl; CREATE TABLE IF NOT EXISTS " + str(SCHEMANAME) + ".chargpdl (data character varying); DROP TABLE IF EXISTS " + str(SCHEMANAME) + ".chargenbatglobal; CREATE TABLE IF NOT EXISTS " + str(SCHEMANAME) + ".chargenbatglobal (data character varying); DROP TABLE IF EXISTS " + str(SCHEMANAME) + ".charglot; CREATE TABLE IF NOT EXISTS " + str(SCHEMANAME) + ".charglot (data character varying); DROP TABLE IF EXISTS " + str(SCHEMANAME) + ".chargebatiglobal; CREATE TABLE IF NOT EXISTS " + str(SCHEMANAME) + ".chargebatiglobal (data character varying);")
        try:
            subprocess.check_call(['psql', '-U', PGUSER, '-h', PGHOST, '-p', PGPORT, '-d', PGDB, '-c', '{0}'.format(SQLQUERYSCHEMA)])
            print('Schémas créés')
        except subprocess.CalledProcessError as e:
            print(e.output)

        os.chdir(PGBINPATH)
        # Chaque année, les fichiers réceptionnés et extraits des ZIP ont une nomenclature particulière (exemple : ART.DC21.W22758.PDLL.A2022.N000627)
        # On recrée le nom de chaque fichier à partir de CODESRC, TYPEFILES, CODEFILES
        # On ajoute chaque fichier dans list_import
        if PATHSOURCE:
            CODESRC = 'N001216'
            TYPEFILES = ['PROP', 'PDLL', 'NBAT', 'LLOC', 'BATI']
            CODEFILES = ['754', '755', '756', '757', '758']

            list_import = []
            
            for t in TYPEFILES:
                for c in CODEFILES:
                    if t == 'PROP':
                        list_import.append("\COPY {0}.chargepropglobal FROM '{6}/ART.DC21.W{3}{4}.{5}.{2}' delimiter AS '|';".format(SCHEMANAME, YEAR, CODESRC, int(str(YEAR)[-2:]), c, t, PATHSOURCE))
                    elif t == 'PDLL':
                        list_import.append("\COPY {0}.chargpdl FROM '{6}/ART.DC21.W{3}{4}.{5}.{2}' delimiter AS '|';".format(SCHEMANAME, YEAR, CODESRC, int(str(YEAR)[-2:]), c, t, PATHSOURCE))
                    elif t == 'NBAT':
                        list_import.append("\COPY {0}.chargenbatglobal FROM '{6}/ART.DC21.W{3}{4}.{5}.{2}' delimiter AS '|';".format(SCHEMANAME, YEAR, CODESRC, int(str(YEAR)[-2:]), c, t, PATHSOURCE))
                    elif t == 'LLOC':
                        list_import.append("\COPY {0}.charglot FROM '{6}/ART.DC21.W{3}{4}.{5}.{2}' delimiter AS '|';".format(SCHEMANAME, YEAR, CODESRC, int(str(YEAR)[-2:]), c, t, PATHSOURCE))
                    elif t == 'BATI':
                        list_import.append("\COPY {0}.chargebatiglobal FROM '{6}/ART.DC21.W{3}{4}.{5}.{2}' delimiter AS '|';".format(SCHEMANAME, YEAR, CODESRC, int(str(YEAR)[-2:]), c, t, PATHSOURCE))
            
            # On importe dans PostgreSQL chaque fichier de list_import      
            try:
                for import_query in list_import:
                    print('Exécution de l''import de {0}'.format(import_query))
                    subprocess.check_call(['psql', '-U', PGUSER, '-h', PGHOST, '-p', PGPORT, '-d', PGDB, '-c', '{0}'.format(import_query)])
            except subprocess.CalledProcessError as e:
                print(e.output)

        # Exécution des scripts SQL du dossier 01_import
        print('Exécution des scripts SQL 01_import')
        for sqlfile in os.listdir(os.path.join(PATHSQL, '01_import')):
            pathfile = os.path.join(PATHSQL, '01_import', sqlfile)
            if os.path.isfile(pathfile):
                if (sqlfile.startswith("0") and sqlfile.endswith(".sql")):
                    if SCHEMASOURCE:
                        try:
                            print('Exécution de {0}'.format(pathfile))
                            subprocess.check_call(['psql', '-U', PGUSER, '-h', PGHOST, '-p', PGPORT, '-d', PGDB, '-f', pathfile, '-v', 'schemaname={0}'.format(SCHEMANAME), '-v', 'schemasource={0}'.format(SCHEMASOURCE), '-v', 'annee={0}'.format(YEAR)])
                        except subprocess.CalledProcessError as e:
                            print(e.output)
                    else:
                        try:
                            print('Exécution de {0}'.format(pathfile))
                            subprocess.check_call(['psql', '-U', PGUSER, '-h', PGHOST, '-p', PGPORT, '-d', PGDB, '-f', pathfile, '-v', 'schemaname={0}'.format(SCHEMANAME), '-v', 'annee={0}'.format(YEAR)])
                        except subprocess.CalledProcessError as e:
                            print(e.output)
        
        # Transfert ogr2ogr des données de base présentes dans les schémas de production : diffusion, observatoire, travail

        os.chdir(QGISBINPATH)
        tables_dependencies = [
            'diffusion.batiment',
            'diffusion.dependance',
            'diffusion.local_activite',
            'diffusion.logement',
            'diffusion.parcelle_cadastrale_alpha',
            'diffusion.proprietaire',
            'diffusion.syndic',
            'diffusion.parcelle_cadastrale',
            'diffusion.parcelle_cadastrale_stat',
            'observatoire.rpls_logement',
            'diffusion.parcelle_cadastrale'
        ]
        # Import des sources URL - ne fonctionne que pour des ZIP, avec un seul fichier dans le ZIP (à améliorer)
        # obj['src'] correspond à l'URL
        # obj['extract'] correspond au nom du fichier à extraire (on ne peut donc extraire qu'un fichier de l'archive) 
        # obj['out'] correspond au point d'arrivée dans la BDD PostgreSQL
        if PATHSOURCE:
            download_folder = PATHSOURCE
        else: 
            download_folder = 'C:/data'

        urlImport = [
            { 'src': 'https://files.data.gouv.fr/insee-sirene/StockUniteLegale_utf8.zip', 'extract': 'StockUniteLegale_utf8.csv', 'out': 'donnees_bases.sirene_insee_ul' },
            { 'src': 'https://files.data.gouv.fr/insee-sirene/StockEtablissement_utf8.zip', 'extract': 'StockEtablissement_utf8.csv', 'out': 'donnees_bases.sirene_insee_etab' },
        ]

        if not os.path.exists(os.path.join(download_folder, 'downloads')):
            os.mkdir(os.path.join(download_folder, 'downloads'))

        def download_file_to_memory(url):
            print('Téléchargement de {0}'.format(url))
            with request.urlopen(url) as response:
                return io.BytesIO(response.read())

        def extract_all(packed_format):
            print('Extraction dans {0}/downloads'.format(download_folder))
            packed_format.extractall(path = os.path.join(download_folder, 'downloads'))

        def extract_zip(byte_obj):
            with zipfile.ZipFile(byte_obj) as zip:
                extract_all(zip)
        
        # Pour chaque URL de urlImport, on extrait le fichier, puis on l'importe dans PostgreSQL
        for el in urlImport:
            try:
                sirene_ul_cached = download_file_to_memory(el['src'])
                extract_zip(sirene_ul_cached)
            except (ValueError, URLError):
                print(URLError)
            try:
                # Import des données Sirene UL
                print('Import de {0}'.format(el['extract']))
                subprocess.check_call(['ogr2ogr', '-f', 'PostgreSQL', "PG:host={0} port={1} dbname={2} user={3} password={4}".format(PGHOST, PGPORT, PGDB, PGUSER, PGPASSWORD), '{0}/downloads/{1}'.format(download_folder, el['extract']), '-overwrite', '-nln', el['out']])
                print('{0} importé'.format(el['extract']))
            except subprocess.CalledProcessError as e:
                print(e.output)
        # Import des tables de tables_dependencies
        for t in tables_dependencies:  
            try:
                print('Import de {0}'.format(t))
                if t == 'observatoire.rpls_logement':
                    subprocess.check_call(['ogr2ogr', '-f', 'PostgreSQL', "PG:host={0} port={1} dbname={2} user={3} password={4}".format(PGHOST, PGPORT, PGDB, PGUSER, PGPASSWORD), "PG:host={0} port={1} dbname={2} user={3} password={4}".format(PGPRODHOST, PGPRODPORT, PGPRODDB, PGPRODUSER, PGPRODPASSWORD), '-progress', '-lco', 'OVERWRITE=yes', '-lco', 'schema={0}'.format(SCHEMANAME), '-nln', '{0}'.format(t.split('.')[1]), '-sql', "SELECT * FROM {0} WHERE depcom::text like '75%'".format(t)])
                # Pour les données PC de la mairie (normalement intégrée dans travail..parcelle_cadastrale_75_xxxx)
                elif t == 'diffusion.parcelle_cadastrale':
                    subprocess.check_call(['ogr2ogr', '-f', 'PostgreSQL', "PG:host={0} port={1} dbname={2} user={3} password={4}".format(PGHOST, PGPORT, PGDB, PGUSER, PGPASSWORD), "PG:host={0} port={1} dbname={2} user={3} password={4}".format(PGPRODHOST, PGPRODPORT, PGPRODDB, PGPRODUSER, PGPRODPASSWORD), '-progress', '-lco', 'OVERWRITE=yes', '-lco', 'schema={0}'.format(SCHEMANAME), '-nln', 'parcelle_cadastrale', '-sql', "SELECT * FROM {0} WHERE c_cainsee::text like '75%'".format(t)])
                else:
                    subprocess.check_call(['ogr2ogr', '-f', 'PostgreSQL', "PG:host={0} port={1} dbname={2} user={3} password={4}".format(PGHOST, PGPORT, PGDB, PGUSER, PGPASSWORD), "PG:host={0} port={1} dbname={2} user={3} password={4}".format(PGPRODHOST, PGPRODPORT, PGPRODDB, PGPRODUSER, PGPRODPASSWORD), '-progress', '-lco', 'OVERWRITE=yes', '-lco', 'schema={0}'.format(SCHEMANAME), '-nln', '{0}_{1}'.format(t.split('.')[1], PREVIOUSYEAR), '-sql', "SELECT * FROM {0} WHERE c_cainsee::text like '75%'".format(t)])

                print('{0} importé'.format(t))
            except subprocess.CalledProcessError as e:
                print(e.output)
        os.chdir(PGBINPATH)
        print('Exécution des scripts SQL 02_creation_tables_finales')
        for sqlfile in os.listdir(os.path.join(PATHSQL, '02_creation_tables_finales')):
            pathfile = os.path.join(PATHSQL, '02_creation_tables_finales', sqlfile)
            if os.path.isfile(pathfile):
                if (sqlfile.startswith("0") and sqlfile.endswith(".sql")) and sqlfile != '08_pdl_dgfip.sql' and sqlfile != '05_local_activite_driea_av2017.sql':
                    try:
                        print('Exécution de {0}'.format(pathfile))
                        subprocess.check_call(['psql', '-U', PGUSER, '-h', PGHOST, '-p', PGPORT, '-d', PGDB, '-f', pathfile, '-v', 'schemaname={0}'.format(SCHEMANAME), '-v', 'annee={0}'.format(YEAR), '-v', 'previousyear={0}'.format(PREVIOUSYEAR)])
                    except subprocess.CalledProcessError as e:
                        print(e.output)
        
        os.chdir(PGBINPATH)        
        print('Exécution des scripts SQL 03_traitements_proprietaire')
        for sqlfile in os.listdir(os.path.join(PATHSQL, '03_traitements_proprietaire')):
            pathfile = os.path.join(PATHSQL, '03_traitements_proprietaire', sqlfile)
            if os.path.isfile(pathfile):
                if (sqlfile.startswith("0") and sqlfile.endswith(".sql")) and sqlfile != '05_maj_dictionnaire_apres_categorisation.sql':
                    try:
                        print('Exécution de {0}'.format(pathfile))
                        subprocess.check_call(['psql', '-U', PGUSER, '-h', PGHOST, '-p', PGPORT, '-d', PGDB, '-f', pathfile, '-v', 'schemaname={0}'.format(SCHEMANAME), '-v', 'annee={0}'.format(YEAR), '-v', 'previousyear={0}'.format(PREVIOUSYEAR)])
                    except subprocess.CalledProcessError as e:
                        print(e.output)
        print('A vous de jouer maintenant, place à la qualification manuelle des propriétaires (select * from {0}.proprietaire_{1}). Une fois la qualification terminée, relancez ce script en répondant "n" à la question "Lancez-vous le script du début ?"'.format(SCHEMANAME, YEAR))
        
    if not start_script:

        print('Exécution du script à partir de la qualification manuelle des propriétaires')
        os.chdir(PGBINPATH)    
        print('Exécution des scripts SQL 03_traitements_proprietaire')
        for sqlfile in os.listdir(os.path.join(PATHSQL, '03_traitements_proprietaire')):
            pathfile = os.path.join(PATHSQL, '03_traitements_proprietaire', sqlfile)
            if os.path.isfile(pathfile):
                if (sqlfile.startswith("0") and sqlfile.endswith(".sql")) and sqlfile == '05_maj_dictionnaire_apres_categorisation_dgfip.sql':
                    try:
                        print('Exécution de {0}'.format(pathfile))
                        subprocess.check_call(['psql', '-U', PGUSER, '-h', PGHOST, '-p', PGPORT, '-d', PGDB, '-f', pathfile, '-v', 'schemaname={0}'.format(SCHEMANAME), '-v', 'annee={0}'.format(YEAR), '-v', 'previousyear={0}'.format(PREVIOUSYEAR)])
                    except subprocess.CalledProcessError as e:
                        print(e.output)
        print('Exécution des scripts SQL 04_tables_stat')
        for sqlfile in os.listdir(os.path.join(PATHSQL, '04_tables_stat')):
            pathfile = os.path.join(PATHSQL, '04_tables_stat', sqlfile)
            if os.path.isfile(pathfile):
                if (sqlfile.startswith("0") and sqlfile.endswith(".sql")):
                    try:
                        print('Exécution de {0}'.format(pathfile))
                        subprocess.check_call(['psql', '-U', PGUSER, '-h', PGHOST, '-p', PGPORT, '-d', PGDB, '-f', pathfile, '-v', 'schemaname={0}'.format(SCHEMANAME), '-v', 'annee={0}'.format(YEAR), '-v', 'previousyear={0}'.format(PREVIOUSYEAR)])
                    except subprocess.CalledProcessError as e:
                        print(e.output)
                        
        print('Exécution des scripts SQL 05_adaptation_donnees_mairie')
        for sqlfile in os.listdir(os.path.join(PATHSQL, '05_adaptation_donnees_mairie')):
            pathfile = os.path.join(PATHSQL, '05_adaptation_donnees_mairie', sqlfile)
            if (sqlfile.startswith("0") and sqlfile.endswith(".sql")) and (sqlfile == '01_correction_nsqpc_mairieParis.sql' or sqlfile == '02_controle_qualite.sql' or sqlfile == '03_renommage.sql'):
                try:
                    print('Exécution de {0}'.format(pathfile))
                    subprocess.check_call(['psql', '-U', PGUSER, '-h', PGHOST, '-p', PGPORT, '-d', PGDB, '-f', pathfile, '-v', 'schemaname={0}'.format(SCHEMANAME), '-v', 'annee={0}'.format(YEAR), '-v', 'previousyear={0}'.format(PREVIOUSYEAR)])
                except subprocess.CalledProcessError as e:
                    print(e.output)

    print('Terminé')
if not clean_schema:
    print('Programme terminé')