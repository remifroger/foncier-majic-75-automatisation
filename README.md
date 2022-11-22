# Procédure de MAJ des données foncières

Exécution de la MAJ des fichiers fonciers. Le script, dans scripts/foncier.py, gère les imports et exécutent les différents fichiers SQL situés dans le dossier 01_majic3

## Version

Python 2.7.x

### Usage

```shell
python scripts/foncier.py --help
```

```
foncier.py [-h] -s SCHEMA -a ANNEE -w WORKSPACE -f SOURCEFILE -sql SQLPATHFILES
```

Ici SQLPATHFILES correspond au contenu de [ce projet](https://github.com/remifroger/foncier-majic-75-procedures-sql)

### Exemple d'usage

```shell
python "C:\data\scripts\foncier.py" -s test_ff_2022 -a 2022 -f "C:/data/foncier/src/ff2022/majic" -sql "C:/data/01_majic3"
```