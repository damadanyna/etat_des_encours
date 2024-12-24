import os
import pandas as pd
import pymysql
import re

# Fonction pour convertir les noms de colonnes en snake_case et les raccourcir
def convert_to_snake_case_and_truncate(column_name, max_length=64):
    snake_case_name = column_name.strip().replace('.', '_').lower()
    if len(snake_case_name) > max_length:
        return snake_case_name[:max_length]
    return snake_case_name

# Fonction pour générer un nom de table à partir d'un fichier
def generate_table_name_from_file(file_path):
    file_name = os.path.basename(file_path)
    table_name, _ = os.path.splitext(file_name)
    return table_name.lower()

# Fonction pour renommer les fichiers en remplaçant les points par des underscores
def rename_files_in_folder(folder_path):
    renamed_files = []
    for file_name in os.listdir(folder_path):
        
        old_path = os.path.join(folder_path, file_name)
        if not os.path.isfile(old_path):
            continue
        
        # Remplacer les points par des underscores
        
        file_name= file_name[:-5]
        new_name =  re.sub(r'[.,;:]', '_', file_name)
        new_name= new_name+".xlsx"
        new_path = os.path.join(folder_path, new_name)
        os.rename(old_path, new_path)
        renamed_files.append(new_path)
    return renamed_files

# Fonction principale pour traiter un fichier et l'insérer dans MySQL
def process_file(file_path, connection):
    if not os.path.exists(file_path):
        print(f"Erreur : Le fichier '{file_path}' n'existe pas.")
        return

    try:
        df = pd.read_excel(file_path)
        df.columns = [convert_to_snake_case_and_truncate(col) for col in df.columns]
        print("Colonnes après transformation:")
        print(df.columns)

        table_name = generate_table_name_from_file(file_path)
        print(f"Nom de la table généré : {table_name}")

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join([f"`{col}` TEXT" for col in df.columns])}
        );
        """
        with connection.cursor() as cursor:
            cursor.execute(create_table_query)
            print(f"Table '{table_name}' vérifiée ou créée avec succès.")
            
            # Vider la table avant l'insertion
            truncate_table_query = f"TRUNCATE TABLE {table_name};"
            cursor.execute(truncate_table_query)
            print(f"Table '{table_name}' vidée avant l'insertion.")

            # Requête SQL d'insertion 
            insert_query = f"""
                INSERT INTO {table_name} ({', '.join(df.columns)})
                VALUES ({', '.join(['%s'] * len(df.columns))});
            """
            for _, row in df.iterrows():
                row_to_insert = tuple([None if pd.isna(val) else val for val in row])
                cursor.execute(insert_query, row_to_insert)

            connection.commit()
            print("Données insérées avec succès !")

    except Exception as e:
        print(f"Erreur lors de l'insertion ou de la création de la table : {e}")
        connection.rollback()

# Fonction pour traiter tous les fichiers dans un dossier
def process_all_files_in_folder(folder_path, connection):
    renamed_files = rename_files_in_folder(folder_path)
    for file_path in renamed_files:
        process_file(file_path, connection)

# Connexion MySQL
connection = pymysql.connect(
    host='localhost',
    user='root',
    password='',
    database='dfe'
)

# Appel de la fonction pour un dossier donné
folder_path = r'./input/'
process_all_files_in_folder(folder_path, connection)

# Fermeture de la connexion
connection.close()
