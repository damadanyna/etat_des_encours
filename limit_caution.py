import pymysql  # Assurez-vous que pymysql est importé correctement
import pandas as pd
import os

def connect_to_database(host, user, password, database):
    """Connect to the database and return the connection object."""
    try:
        conn = pymysql.connect(
            host=host,       # Utilisation des paramètres fournis
            user=user,       # Nom d'utilisateur
            password=password,  # Mot de passe
            database=database,  # Base de données
            cursorclass=pymysql.cursors.DictCursor  # Ajoutez cette ligne pour obtenir les résultats sous forme de dictionnaire
        )
        print("Connection to the database was successful!")
        return conn
    except pymysql.MySQLError as err:
        print(f"Error: Unable to connect to the database. Details: {err}")
        return None

def fetch_data_from_table(conn, query):
    """Fetch data from the database based on the query."""
    try:
        cursor = conn.cursor()  # Pas besoin de 'dictionary=True', DictCursor est déjà configuré
        print(f"Executing query in progress...")
        cursor.execute(query)
        results = cursor.fetchall()
        print(f"Query executed successfully. Fetched {len(results)} rows.")
        return results
    except pymysql.MySQLError as err:
        print(f"Error: Unable to execute query. Query: {query} | Error: {err}")
        return None
    finally:
        cursor.close()

def split_value(value, index, default=None):
    """Helper function to split values by '|' and return the part at the given index."""
    if value:
        parts = value.split('|')
        return parts[index] if len(parts) > index else default
    return default
def split_value_by_trailing(value, index, default=None):
    """Helper function to split values by '|' and return the part at the given index."""
    if value:
        parts = value.split('-')
        return parts[index] if len(parts) > index else default
    return default

def modify_column_data(data):
    """Modify the 'contract_balance.open_balance' column to keep only the first value before the pipe (|)."""

    return data

def data_base_query(offset,type):
       return f"""
           SELECT limit_cau.id,concat(customer.short_name,' ',customer.name_1) as Name,limit_cau.approval_date,limit_cau.expiry_date,limit_cau.internal_amount,limit_cau.total_os,limit_cau.avail_amt 
            from limit_mcbc_live_full as limit_cau
            INNER JOIN customer_mcbc_live_full_partie_1 as customer ON customer.id = limit_cau.liability_number
            WHERE limit_product={type}  
             LIMIT 200 OFFSET {offset};    """
 
def export_to_excel(data, file_name):
    """Export data to an Excel file."""
    if not data:
        print("No data to export!")
        return

    try:
        df = pd.DataFrame(data)
        df.to_excel(file_name, index=False, engine='openpyxl')
        print(f"Data has been exported to {file_name} successfully!")
    except Exception as e:
        print(f"Error while exporting to Excel: {e}")

if __name__ == "__main__":
    # Database configuration
    db_config = {
        "host": "localhost",      # Replace with your database host
        "user": "root",               # Replace with your database username
        "password": "",       # Replace with your database password
        "database": "dfe"             # Replace with your database name
    }
    type_limit='8400'
    type_text=''
    
    if type_limit=='2900': 
        type_text ='Limit_Caution'
        output_file_template = "./out_put_limit/out_put_"+type_text+"_{offset}.xlsx"
    elif type_limit=='8400':
        type_text='Limit_AVM_ESCOMPTE'
        output_file_template = "./out_put_limit/out_put_"+type_text+"_{offset}.xlsx"
    # elif type_limit=='300':
    #     type_text='Limit_AVM_ESCOMPTE'
    #     output_file_template = "./out_put_limit_avm_escompte/out_put_{type_text}_{offset}.xlsx"
     
    output_dir = os.path.dirname(output_file_template)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Connect to the database
    conn = connect_to_database(**db_config) 
    if conn:
        try:
            for offset in range(0, 1000, 200): 
                # limit DAV 300
                # limit Caution 2900
                # limit AVM/ESCOMPTE 8400
                data = fetch_data_from_table(conn,  data_base_query(offset,type_limit))
                if data:
                    k=offset
                    print("Data fetched successfully in page =============> ", k+200," / 10000") 
                    modified_data = modify_column_data(data) 
                    output_file = output_file_template.format(offset=(k+200))
                    export_to_excel(modified_data, output_file)
                else:
                    print("No data was fetched from the database.") 
        finally:
            conn.close()
    else:
        print("Database connection failed!")
 
