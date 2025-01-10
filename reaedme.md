Pour lancer les extractiond de DFE voici les étape à suivre



I--- IMPORTATION DU FICHIER EXCEL VERS LA BASE DE DONNEE
    a-- Enregistrer les fichier ".csv" en ".xls".
    b-- Renommer les fichier en suppriment la date à la fin de chaque nom du fichier
    c-- Copier tout les fichier dans le repertoir de l'application "..\project_2024\etat_des_encours\input\"
    d-- lancer le script dans le terminal "pyhton generate_table.py"




II--- EXTRACTION DU DFE

    1--- Etat des encours
        a-- Ouvrir le script "concat_etat_des_encours.py", allez dans la ligne '344' changeé la date '2024-12-31' à la date du  COB - 1
        b-- Eregister pius lancer le script  "pyhton etat_des_encours.py" dans le terminal
            une fois que tout les extraction de l'etat des encours sera terminée  le script "pyhton concat_etat_des_encours.py" dans le terminal 
        c-- le fichier final se trouve dans "..\project_2024\etat_des_encours\out_put_etat_des_encours" sous le nom de "ETAT_DES_ENCOURS.xlsx"

    2-- Etat de remboursement
        a--  lancer le script  "pyhton etat_remboursement.py" dans le terminal
             une fois que tout les extraction de l'etat des encours sera terminée  le script "pyhton concat_etat_remboursement.py" dans le terminal 
        b-- le fichier final se trouve dans "..\project_2024\etat_des_encours\out_put_etat_des_remboursement" sous le nom de "ETAT_DES_REMBOURSEMENT.xlsx"

    3-- Limit caution
        a-- Ouvrir le script  "limit_caution.py", allez dans la ligne '83' changeé la valeur en "2900" pour l'extraction du Limit_Caution et "8400" pour Limit_AVM_ESCOMPTE 
        b-- Eregister pius lancer le script  "pyhton limit_caution.py" dans le terminal
        c-- le fichier final se trouve dans "..\project_2024\etat_des_encours\out_put_limit" sous le nom de "out_put_Limit_AVM_ESCOMPTE.xlsx" et out_put_Limit_Caution.xlsx
    
    4-- Limit DAV
        a-- ouvrir terminal dans "..\project_2024\etat_des_encours\dav_\"
        b-- copier les fichier plats "ACCOUNT_MCBC_LIVE_FULL.xlsx" et "LIMIT_MCBC_LIVE_FULL.xlsx" dans "..\project_2024\etat_des_encours\dav_\input"
        c-- lancer le script  "pyhton dav_.py" dans le terminal. Le fichier se soute dans "..\project_2024\etat_des_encours\dav_\input" sous le nom de 
        "DAV2.xlsx"


