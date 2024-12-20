import pymysql
import time

print("Début du script")

def test_database_connection():
    try:
        print("Tentative de connexion à la base de données...")
        conn = pymysql.connect(
            host="192.168.1.137",       # Remplacez par vos paramètres
            user="root",                # Nom d'utilisateur
            password="clvohama",        # Mot de passe
            database="dfe"              # Base de données
        )
        print("Connexion réussie !")
        conn.close()
    except pymysql.MySQLError as err:
        print(f"Erreur de connexion MySQL : {err}")
    except Exception as e:
        print(f"Erreur inattendue : {e}")
    finally:
        print("Fin du test de connexion.")

if __name__ == "__main__":
    print("Appel de la fonction de test de connexion...")
    test_database_connection()
