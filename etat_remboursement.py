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
    for row in data:   
        input_string = row['property'] 
        if input_string :
            entries = input_string.split('|')
            
            indices_principal_ind=[]
            for index, entry in enumerate(entries):
                if entry =="PRINCIPALINT" :
                    indices_principal_ind.append(index) 
                if not indices_principal_ind:
                    row['principal_int']=0
                else:
                    montant_principal_int=0
                    for index in indices_principal_ind:  
                        or_prop_amount=0
                        if split_value(row['or_prop_amount'],index):  
                            or_prop_amount = split_value(row['or_prop_amount'],index)
                            if or_prop_amount.strip() == '' or or_prop_amount is None:
                                or_prop_amount = '0.0'  
                        if split_value(row['os_prop_amount'],index):  
                            os_prop_amount = split_value(row['os_prop_amount'],index)
                            if os_prop_amount.strip() == '' or os_prop_amount is None:
                                os_prop_amount = '0.0'    
                        montant_principal_int+= float(or_prop_amount)    
                        montant_principal_int+= float(os_prop_amount)    
                    row['principal_int'] =montant_principal_int * -1 if montant_principal_int < 0 else montant_principal_int  
            
            indices_total_penality_int=[]
            for index, entry in enumerate(entries):
                if entry =="PENALTYINT" :
                    indices_total_penality_int.append(index) 
                if not indices_total_penality_int:
                    row['penality_int']=0
                else:
                    montant_penality_int=0
                    for index in indices_total_penality_int:  
                        or_prop_amount=0
                        if split_value(row['or_prop_amount'],index):  
                            or_prop_amount = split_value(row['or_prop_amount'],index)
                            if or_prop_amount.strip() == '' or or_prop_amount is None:
                                or_prop_amount = '0.0'  
                        if split_value(row['os_prop_amount'],index):  
                            os_prop_amount = split_value(row['os_prop_amount'],index)
                            if os_prop_amount.strip() == '' or os_prop_amount is None:
                                os_prop_amount = '0.0'    
                        montant_penality_int+= float(or_prop_amount)    
                        montant_penality_int+= float(os_prop_amount)    
                    row['penality_int'] =montant_penality_int * -1 if montant_penality_int < 0 else montant_penality_int  
                    
            indices_total_capital=[]
            for index, entry in enumerate(entries):
                if entry =="ACCOUNT" :
                    indices_total_capital.append(index) 
                if not indices_total_capital:
                    row['Capital']=0
                else:
                    montant_penality_int=0
                    for index in indices_total_capital:  
                        or_prop_amount=0
                        if split_value(row['or_prop_amount'],index):  
                            or_prop_amount = split_value(row['or_prop_amount'],index)
                            if or_prop_amount.strip() == '' or or_prop_amount is None:
                                or_prop_amount = '0.0'  
                        if split_value(row['os_prop_amount'],index):  
                            os_prop_amount = split_value(row['os_prop_amount'],index)
                            if os_prop_amount.strip() == '' or os_prop_amount is None:
                                os_prop_amount = '0.0'    
                        montant_penality_int+= float(or_prop_amount)    
                        montant_penality_int+= float(os_prop_amount)    
                    row['Capital'] =montant_penality_int * -1 if montant_penality_int < 0 else montant_penality_int  
      
    return data

def data_base_query(offset):
       return f"""
            SELECT  (SELECT opening_date FROM account_mcbc_live_full WHERE id= arrangement.linked_appl_id AND opening_date is not NULL LIMIT 1) as Date_pret, 
            arrangement.product,arrangement.co_code,arrangement.id,arrangement.customer,  CONCAT(customer.short_name," " ,customer.name_1), count(arrangement_id) as echeance ,bill_detail.payment_date,
            '' as Capital, 
            '' as principal_int,
            '' as penality_int,
            (bill_detail.payment_date+ (bill_detail.or_prop_amount- bill_detail.os_prop_amount)) as TOTAL,
            bill_detail.property,
            bill_detail.or_prop_amount,
            bill_detail.os_prop_amount,
            (SELECT type_sysdate FROM eb_cont_bal_mcbc_live_full WHERE id = arrangement.linked_appl_id LIMIT 1) as type_sysdate,
            (SELECT open_balance FROM eb_cont_bal_mcbc_live_full WHERE id = arrangement.linked_appl_id LIMIT 1) as open_balance
            FROM aa_arrangement_mcbc_live_full as arrangement
            INNER JOIN customer_mcbc_live_full_partie_1 as customer ON customer.id = arrangement.customer
            INNER JOIN aa_bill_details_mcbc_live_full as bill_detail ON bill_detail.arrangement_id = arrangement.id
            WHERE bill_detail.bill_status REGEXP 'SETTLED' 
            AND NOT bill_detail.property REGEXP 'DISBURSEMENTFEE|NEWARRANGEMENTFEE'
            AND bill_detail.property REGEXP 'ACCOUNT' 
            AND  bill_detail.bill_date<='20241230'  
            AND bill_detail.os_prop_amount>=0  
            GROUP BY arrangement.id  
            HAVING date_pret is NOT NULL
            ORDER BY echeance DESC
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
     
    output_file_template = "./out_put_etat_des_remboursement/etat_des_remboursement_{offset}.xlsx" 
    output_dir = os.path.dirname(output_file_template)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Connect to the database
    conn = connect_to_database(**db_config) 
    if conn:
        try:
            for offset in range(0, 10000, 200): 
                data = fetch_data_from_table(conn,  data_base_query(offset))
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
 
