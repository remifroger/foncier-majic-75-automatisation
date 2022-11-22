#!/usr/local/bin/python
# -*- coding: utf-8 -*-
# Python 2.7.X

"""
Présentation et logique du programme
------------------------------------

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

parser = argparse.ArgumentParser(description='MAJ des fichiers fonciers Majic 3')
parser.add_argument("-s", "--schema", required=True, help="Nom du schéma de travail PostgreSQL")
parser.add_argument("-a", "--annee", required=True, help="Année de MAJ")
parser.add_argument("-f", "--sourcefile", required=True, help="Chemin vers les sources de données")
parser.add_argument("-sql", "--sqlpathfiles", required=True, help="Chemin vers les scripts SQL")
args = parser.parse_args()

# Pour permettre de lancer le script de zéro jusqu'à la qualification manuelle des propriétaires
# Ou de la qualification manuelle des propriétaires jusqu'à la fin
clean_schema = input('Avant de lancer le script, il est préférable que le schéma {0} soit vide, souhaitez-vous poursuivre ? (y/n)'.format(args.schema)).lower().strip() == 'y'

if clean_schema:
    
    start_script = input('y: lancement du script à partir du début jusqu\'à la qualification des propriétaires / n : lancement du script après la qualification manuelle des propriétaires ? (y/n): ').lower().strip() == 'y'
    
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
    PATHFILE = 'C:/Users/froger/OneDrive - APUR/data/foncier/01_majic3/01_import/01_chargeprop.sql'
    PATHSOURCE = args.sourcefile
    PATHSQL = args.sqlpathfiles

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
        CODESRC = 'N000627'
        TYPEFILES = ['PROP', 'PDLL', 'NBAT', 'LLOC', 'BATI']
        CODEFILES = ['754', '755', '756', '757', '758']

        list_import = []
        
        for t in TYPEFILES:
            for c in CODEFILES:
                if t == 'PROP':
                    list_import.append("\COPY {0}.chargepropglobal FROM '{6}/ART.DC21.W{3}{4}.{5}.A{1}.{2}' delimiter AS '|';".format(SCHEMANAME, YEAR, CODESRC, int(str(YEAR)[-2:]), c, t, PATHSOURCE))
                elif t == 'PDLL':
                    list_import.append("\COPY {0}.chargpdl FROM '{6}/ART.DC21.W{3}{4}.{5}.A{1}.{2}' delimiter AS '|';".format(SCHEMANAME, YEAR, CODESRC, int(str(YEAR)[-2:]), c, t, PATHSOURCE))
                elif t == 'NBAT':
                    list_import.append("\COPY {0}.chargenbatglobal FROM '{6}/ART.DC21.W{3}{4}.{5}.A{1}.{2}' delimiter AS '|';".format(SCHEMANAME, YEAR, CODESRC, int(str(YEAR)[-2:]), c, t, PATHSOURCE))
                elif t == 'LLOC':
                    list_import.append("\COPY {0}.charglot FROM '{6}/ART.DC21.W{3}{4}.{5}.A{1}.{2}' delimiter AS '|';".format(SCHEMANAME, YEAR, CODESRC, int(str(YEAR)[-2:]), c, t, PATHSOURCE))
                elif t == 'BATI':
                    list_import.append("\COPY {0}.chargebatiglobal FROM '{6}/ART.DC21.W{3}{4}.{5}.A{1}.{2}' delimiter AS '|';".format(SCHEMANAME, YEAR, CODESRC, int(str(YEAR)[-2:]), c, t, PATHSOURCE))
                
        try:
            for import_query in list_import:
                print('Exécution de l''import de {0}'.format(import_query))
                subprocess.check_call(['psql', '-U', PGUSER, '-h', PGHOST, '-p', PGPORT, '-d', PGDB, '-c', '{0}'.format(import_query)])
        except subprocess.CalledProcessError as e:
            print(e.output)

        print('Exécution des scripts SQL 01_import')
        for sqlfile in os.listdir(os.path.join(PATHSQL, '01_import')):
            pathfile = os.path.join(PATHSQL, '01_import', sqlfile)
            if os.path.isfile(pathfile):
                if (sqlfile.startswith("0") and sqlfile.endswith(".sql")):
                    try:
                        print('Exécution de {0}'.format(pathfile))
                        subprocess.check_call(['psql', '-U', PGUSER, '-h', PGHOST, '-p', PGPORT, '-d', PGDB, '-f', pathfile, '-v', 'schemaname={0}'.format(SCHEMANAME), '-v', 'annee={0}'.format(YEAR)])
                    except subprocess.CalledProcessError as e:
                        print(e.output)
        
        # Transfert ogr2ogr des données de base
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
            'observatoire.rpls_logement',
            'travail.parcelle_cadastrale_75_2022'
        ]

        # Import du Sirene
        sireneulzipurl = 'https://files.data.gouv.fr/insee-sirene/StockUniteLegale_utf8.zip'
        sireneetabzipurl = 'https://files.data.gouv.fr/insee-sirene/StockEtablissement_utf8.zip'

        if not os.path.exists(os.path.join(PATHSOURCE, 'sirene')):
            os.mkdir(os.path.join(PATHSOURCE, 'sirene'))

        def download_file_to_memory(url):
            print('Téléchargement du Sirene sur {0}'.format(url))
            with request.urlopen(url) as response:
                return io.BytesIO(response.read())

        def extract_all(packed_format):
            print('Extraction du Sirene (ZIP) dans {0}/sirene'.format(PATHSOURCE))
            packed_format.extractall(path = os.path.join(PATHSOURCE, 'sirene'))

        def extract_zip(byte_obj):
            with zipfile.ZipFile(byte_obj) as zip:
                extract_all(zip)

        try:
            sirene_ul_cached = download_file_to_memory(sireneulzipurl)
            extract_zip(sirene_ul_cached)
        except (ValueError, URLError):
            print(URLError)
            
        try:
            sirene_etab_cached = download_file_to_memory(sireneetabzipurl)
            extract_zip(sirene_etab_cached)
        except (ValueError, URLError):
            print(URLError)

        try:
            # Import des données Sirene UL
            print('Import du Sirene UL')
            subprocess.check_call(['ogr2ogr', '-f', 'PostgreSQL', "PG:host={0} port={1} dbname={2} user={3} password={4}".format(PGHOST, PGPORT, PGDB, PGUSER, PGPASSWORD), '{0}/sirene/StockUniteLegale_utf8.csv'.format(PATHSOURCE), '-overwrite', '-nln', 'donnees_bases.sirene_insee_ul'])
            print('Sirene UL importé')
        except subprocess.CalledProcessError as e:
            print(e.output)
            
        try:
            # Import des données Sirene établissements
            print('Import du Sirene établissements')
            subprocess.check_call(['ogr2ogr', '-f', 'PostgreSQL', "PG:host={0} port={1} dbname={2} user={3} password={4}".format(PGHOST, PGPORT, PGDB, PGUSER, PGPASSWORD), '{0}/sirene/StockEtablissement_utf8.csv'.format(PATHSOURCE), '-overwrite', '-nln', 'donnees_bases.sirene_insee_etab'])
            print('Sirene établissements importé')
        except subprocess.CalledProcessError as e:
            print(e.output)

        # Import des tables du serveur Apur
        for t in tables_dependencies:  
            try:
                print('Import de {0}'.format(t))
                if t == 'observatoire.rpls_logement':
                    subprocess.check_call(['ogr2ogr', '-f', 'PostgreSQL', "PG:host={0} port={1} dbname={2} user={3} password={4}".format(PGHOST, PGPORT, PGDB, PGUSER, PGPASSWORD), "PG:host={0} port={1} dbname={2} user={3} password={4}".format(PGPRODHOST, PGPRODPORT, PGPRODDB, PGPRODUSER, PGPRODPASSWORD), '-progress', '-lco', 'OVERWRITE=yes', '-lco', 'schema={0}'.format(SCHEMANAME), '-nln', '{0}'.format(t.split('.')[1]), '-sql', "SELECT * FROM {0} WHERE depcom::text like '75%'".format(t)])
                # Pour les données PC de la mairie (normalement intégrée dans travail..parcelle_cadastrale_75_xxxx)
                elif t == 'travail.parcelle_cadastrale_75_2022':
                    subprocess.check_call(['ogr2ogr', '-f', 'PostgreSQL', "PG:host={0} port={1} dbname={2} user={3} password={4}".format(PGHOST, PGPORT, PGDB, PGUSER, PGPASSWORD), "PG:host={0} port={1} dbname={2} user={3} password={4}".format(PGPRODHOST, PGPRODPORT, PGPRODDB, PGPRODUSER, PGPRODPASSWORD), '-progress', '-lco', 'OVERWRITE=yes', '-lco', 'schema={0}'.format(SCHEMANAME), '-nln', 'parcelle_cadastrale_mairie_75_{0}'.format(YEAR), '-sql', "SELECT * FROM {0} WHERE c_cainsee::text like '75%'".format(t)])
                else:
                    subprocess.check_call(['ogr2ogr', '-f', 'PostgreSQL', "PG:host={0} port={1} dbname={2} user={3} password={4}".format(PGHOST, PGPORT, PGDB, PGUSER, PGPASSWORD), "PG:host={0} port={1} dbname={2} user={3} password={4}".format(PGPRODHOST, PGPRODPORT, PGPRODDB, PGPRODUSER, PGPRODPASSWORD), '-progress', '-lco', 'OVERWRITE=yes', '-lco', 'schema={0}'.format(SCHEMANAME), '-nln', '{0}_75_{1}'.format(t.split('.')[1], PREVIOUSYEAR), '-sql', "SELECT * FROM {0} WHERE c_cainsee::text like '75%'".format(t)])

                print('{0} importé'.format(t))
            except subprocess.CalledProcessError as e:
                print(e.output)
        
        os.chdir(PGBINPATH)
        print('Exécution des scripts SQL 02_creation_tables_finales')
        for sqlfile in os.listdir(os.path.join(PATHSQL, '02_creation_tables_finales')):
            pathfile = os.path.join(PATHSQL, '02_creation_tables_finales', sqlfile)
            if os.path.isfile(pathfile):
                if (sqlfile.startswith("0") and sqlfile.endswith(".sql")) and sqlfile != '08_pdl_dgfip.sql':
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
                if (sqlfile.startswith("0") and sqlfile.endswith(".sql")) and sqlfile != '05_maj_dictinnaire_apres_categorisation_dgfip.sql':
                    try:
                        print('Exécution de {0}'.format(pathfile))
                        subprocess.check_call(['psql', '-U', PGUSER, '-h', PGHOST, '-p', PGPORT, '-d', PGDB, '-f', pathfile, '-v', 'schemaname={0}'.format(SCHEMANAME), '-v', 'annee={0}'.format(YEAR), '-v', 'previousyear={0}'.format(PREVIOUSYEAR)])
                    except subprocess.CalledProcessError as e:
                        print(e.output)
        print('A vous de jouer maintenant, place à la qualification manuelle des propriétaires (select * from {0}.proprietaire_75_{1}). Une fois la qualification terminée, relancez ce script en répondant "n" à la question "Lancez-vous le script du début ?"'.format(SCHEMANAME, YEAR))
    if not start_script:
        print('Exécution du script à partir de la qualification manuelle des propriétaires')
        os.chdir(PGBINPATH)    

        print('Exécution des scripts SQL 03_traitements_proprietaire')
        for sqlfile in os.listdir(os.path.join(PATHSQL, '03_traitements_proprietaire')):
            pathfile = os.path.join(PATHSQL, '03_traitements_proprietaire', sqlfile)
            if os.path.isfile(pathfile):
                if (sqlfile.startswith("0") and sqlfile.endswith(".sql")) and sqlfile == '05_maj_dictinnaire_apres_categorisation_dgfip.sql':
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
        """
        print('Exécution des scripts SQL 05_adaptation_donnees_mairie')
        for sqlfile in os.listdir(os.path.join(PATHSQL, '05_adaptation_donnees_mairie')):
            pathfile = os.path.join(PATHSQL, '05_adaptation_donnees_mairie', sqlfile)
            if (sqlfile.startswith("0") and sqlfile.endswith(".sql") and os.path.isfile(pathfile)) or sqlfile == '01_correction_nsqpc_mairieParis.sql':
                try:
                    print('Exécution de {0}'.format(pathfile))
                    subprocess.check_call(['psql', '-U', PGUSER, '-h', PGHOST, '-p', PGPORT, '-d', PGDB, '-f', pathfile, '-v', 'schemaname={0}'.format(SCHEMANAME), '-v', 'annee={0}'.format(YEAR), '-v', 'previousyear={0}'.format(PREVIOUSYEAR)])
                except subprocess.CalledProcessError as e:
                    print(e.output)
        """

    print('Terminé')
if not clean_schema:
    print('Programme terminé')