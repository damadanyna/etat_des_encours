import pandas as pd
import pymysql
import os

# Fonction pour convertir les noms de colonnes en snake_case
def convert_to_snake_case(column_name):
    return column_name.replace('.', '_').lower()

# Fonction pour générer le nom de la table automatiquement à partir du nom du fichier
def generate_table_name_from_file(file_path):
    # Extraire le nom du fichier sans extension
    file_name = os.path.splitext(os.path.basename(file_path))[0]
    # Convertir en snake_case et retourner
    return file_name.replace('.', '_').lower()

# Lire le fichier Excel (avec les entêtes automatiquement chargés)
file_path = r'./combined_output/etat_des_encours.xlsx'

# Vérifiez si le fichier existe à l'emplacement spécifié
if not os.path.exists(file_path):
    print(f"Erreur : Le fichier '{file_path}' n'existe pas.")
else:
    # Charger le fichier Excel dans un DataFrame
    df = pd.read_excel(file_path)

    # Appliquer la transformation des noms de colonnes en snake_case
    df.columns = [convert_to_snake_case(col) for col in df.columns]

    # Affichage des colonnes transformées pour vérification
    print("Colonnes après transformation:")
    print(df.columns)

    # Affichage des premières lignes du DataFrame pour vérification
    print("Exemple de données:")
    print(df.head())

    # Connexion à la base de données MySQL
    connection = pymysql.connect(
        host='localhost',
        user='root',
        password='',
        database='dfe'
    )

    # Générer le nom de la table automatiquement à partir du fichier
    table_name = generate_table_name_from_file(file_path)
    print(f"Nom de la table généré : {table_name}")

    # Créer la table si elle n'existe pas
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {', '.join([f"`{col}` TEXT" for col in df.columns])}
    );
    """
    
    try:
        with connection.cursor() as cursor:
            # Créer la table si elle n'existe pas
            cursor.execute(create_table_query)
            print(f"Table '{table_name}' vérifiée ou créée avec succès.")

            # Requête SQL d'insertion
            insert_query = f"""
                INSERT INTO {table_name} ({', '.join(df.columns)})
                VALUES ({', '.join(['%s'] * len(df.columns))});
            """

            # Remplacer NaN par NULL pour l'insertion dans MySQL
            # Nous gardons NaN mais les remplaçons par None avant l'insertion
            for index, row in df.iterrows():
                row_to_insert = tuple(
                    [None if pd.isna(val) else val for val in row]  # Remplacer NaN par None
                )
                cursor.execute(insert_query, row_to_insert)

            # Commit des changements
            connection.commit()
            print("Données insérées avec succès !")

    except Exception as e:
        print(f"Erreur lors de l'insertion ou de la création de la table : {e}")
        connection.rollback()

    finally:
        connection.close()
