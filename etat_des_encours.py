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
        if row['Nombre_de_jour_retard']:
            if  int(row['Nombre_de_jour_retard'])<0 :  
                row['Nombre_de_jour_retard'] = 0
            else:
                row['Nombre_de_jour_retard'] = int(row['Nombre_de_jour_retard'])
        else:
            row['Nombre_de_jour_retard'] = 0 
                
        input_string = row['curr_asset_type']
        # curr_asset_type = row['curr_asset_type']
        if input_string or curr_asset_type: 
            entries = input_string.split('|')  
            # entries_curr_asset_type = curr_asset_type.split('|')  
            
            # TERME A IGNORER
            # ignore_terms = ("CURACCOUNT", "DUEACCOUNT", "ACCOUNT-") 
            # matching_indices = [
            #     index for index, entry in enumerate(entries) 
            #     if "ACCOUNT" in entry and not any(term in entry for term in ignore_terms)
            # ] 
            # if not matching_indices:
            #     row['Capital_Appele_Non_verse']=0
            # else:  
            #     matching_indices=matching_indices[0]  
            #     value =float(split_value(row['Capital_Appele_Non_verse'],matching_indices) )
            #     row['Capital_Appele_Non_verse'] = value * -1 if value < 0 else value 
            
            matching_indice_montant_pret=[] 
            for index, entry in enumerate(entries):
                if entry.startswith("TOTCOMMITMENT"):  
                    matching_indice_montant_pret.append(index)  # Affiche chaque entrée
  
            if not matching_indice_montant_pret: 
                row['Montant_pret']=0
            else: 
                montant_pert_total=0
                for index in matching_indice_montant_pret: 
                    debit_mvmt=0
                    credit_mvmt=0
                    open_balance=0 
                    if split_value(row['debit_mvmt'],index):
                        debit_mvmt = split_value(row['debit_mvmt'],index) 
                        if debit_mvmt.strip() == '' or debit_mvmt is None:
                            debit_mvmt = '0.0' 
                        montant_pert_total= float(debit_mvmt)
                    if split_value(row['credit_mvmt'],index):
                        credit_mvmt = split_value(row['credit_mvmt'],index)
                        if credit_mvmt.strip() == '' or credit_mvmt is None:
                            credit_mvmt = '0.0' 
                        montant_pert_total= float(credit_mvmt)
                    if split_value(row['open_balance'],index):
                        open_balance = split_value(row['open_balance'],index)
                        if open_balance.strip() == '' or open_balance is None:
                            open_balance = '0.0' 
                        montant_pert_total= float(open_balance) 
                row['Montant_pret'] =  montant_pert_total * -1 if montant_pert_total < 0 else montant_pert_total 
                 
                
       
            
            matching_indice_Appele_Non_verse=[]   
            for index, entry in enumerate(entries):
                # if (entry == "CURACCOUNT" or entry.startswith("CURACCOUNT-202411") or entry.startswith("CURACCOUNT-202412")): 
                if entry == "CURACCOUNT" or entry.startswith("CURACCOUNT-2024") or entry.startswith("CURACCOUNT-2025"): 
                    matching_indice_Appele_Non_verse.append(index) 
                    # print("CURACCOUNT",index)
                # elif ( entry.startswith("CURACCOUNT-2024")  and  "TOTCOMMITMENTBL" in entry): 
                #     matching_indice_Appele_Non_verse.append(index) 
                #     print(tota,index)
            if not matching_indice_Appele_Non_verse:
                row['Capital_Non_appele_ech']=0
            else:     
                montant_pert_total=0 
              
                for index in matching_indice_Appele_Non_verse: 
                    debit_mvmt=0
                    credit_mvmt=0
                    open_balance=0 
                    if split_value(row['debit_mvmt'],index):
                        debit_mvmt = split_value(row['debit_mvmt'],index)  
                        if debit_mvmt.strip() == '' or debit_mvmt is None:
                            debit_mvmt = '0.0'  
                    if split_value(row['credit_mvmt'],index):
                        credit_mvmt = split_value(row['credit_mvmt'],index) 
                        if credit_mvmt.strip() == '' or credit_mvmt is None:
                            credit_mvmt = '0.0' 
                    if split_value(row['open_balance'],index):
                         
                        open_balance = split_value(row['open_balance'],index)
                        if open_balance.strip() == '' or open_balance is None:
                            open_balance = '0.0'  
                    montant_pert_total+= float(credit_mvmt)  
                    montant_pert_total+= float(debit_mvmt) 
                    montant_pert_total+= float(open_balance)  
                # print("APPELLE NON ECHUE")
                # print('credit_mvmt',float(credit_mvmt))
                # print('debit_mvmt',float(debit_mvmt))
                # print('open_balance',float(open_balance))
                    
                row['Capital_Non_appele_ech'] =montant_pert_total * -1 if montant_pert_total < 0 else montant_pert_total  
                if row['Capital_Non_appele_ech']==0 and   row['arr_status'] =='EXPIRED' and row['Nombre_de_jour_retard']==0: 
                    for index in matching_indice_Appele_Non_verse: 
                        debit_mvmt=0
                        credit_mvmt=0
                        open_balance=0 
                        if split_value(row['debit_mvmt'],index):
                            debit_mvmt = split_value(row['debit_mvmt'],index)  
                            if debit_mvmt.strip() == '' or debit_mvmt is None:
                                debit_mvmt = '0.0'  
                        if split_value(row['credit_mvmt'],index):
                            credit_mvmt = split_value(row['credit_mvmt'],index) 
                            if credit_mvmt.strip() == '' or credit_mvmt is None:
                                credit_mvmt = '0.0' 
                        if split_value(row['open_balance'],index):
                            
                            open_balance = split_value(row['open_balance'],index)
                            if open_balance.strip() == '' or open_balance is None:
                                open_balance = '0.0'  
                        max_value = max(float(safe_float(open_balance)), float(credit_mvmt), float(debit_mvmt))
                        # print('VALEUR FINAL=',max_value)
                        row['Capital_Non_appele_ech'] =max_value
                        # row['Capital_Non_appele_ech'] = montant_pert_total * -1 if montant_pert_total < 0 else montant_pert_total
                 
            indices_total_iterest_echus=[]
            for index, entry in enumerate(entries):    
               if any(f"PA{x}PRINCIPALINT" in entry for x in range(1, 5)) and "SP" not in entry: 
                    indices_total_iterest_echus.append(index) 
            if not indices_total_iterest_echus:
                row['Total_interet_echus']=0
            else:
                montant_pert_total=0
                for index in indices_total_iterest_echus:  
                    open_balance=0
                    if split_value(row['open_balance'],index):  
                        open_balance = split_value(row['open_balance'],index)
                        # print("open balance: ",open_balance)
                        if open_balance.strip() == '' or open_balance is None:
                            open_balance = '0.0'    
                    montant_pert_total= float(open_balance)    
                row['Total_interet_echus'] =montant_pert_total * -1 if montant_pert_total < 0 else montant_pert_total   
            
     
            # Initialisation de la liste de résultats
            matching_indice_Non_appele_verse = [] 
            
            for index, entry in enumerate(entries):
                # if entry == "CURACCOUNT" or entry.startswith("CURACCOUNT-202411") or entry.startswith("CURACCOUNT-202412"): # Si l'entrée est dans la liste des entrées valides
                if entry == "CURACCOUNT" or entry.startswith("CURACCOUNT-2024") or entry.startswith("CURACCOUNT-2025"): # Si l'entrée est dans la liste des entrées valides
                    continue  # Passer à l'itération suivante sans rien faire
                else: 
                    if entry in ["PA1ACCOUNT", "PA2ACCOUNT", "PA3ACCOUNT", "PA4ACCOUNT"] : 
                        matching_indice_Non_appele_verse.append(index) 
                    elif (entry not in ["PA1ACCOUNT", "PA2ACCOUNT", "PA3ACCOUNT", "PA4ACCOUNT"]) and entry == "DUEACCOUNT":
                        # print("DUEACCOUNT",index)
                        matching_indice_Non_appele_verse.append(index) 
                        
            
            if not matching_indice_Non_appele_verse: 
                row['Capital_Appele_Non_verse']=0
            elif row['Nombre_de_jour_retard'] == 0:  
                row['Capital_Appele_Non_verse']=0
            else:     
                montant_pert_total=0 
                for index in matching_indice_Non_appele_verse: 
                    debit_mvmt=0
                    credit_mvmt=0
                    open_balance=0 
                    
                    # print("open_balance: ",split_value(row['open_balance'],index))
                    # print("credit_mvmt: ", split_value(row['credit_mvmt'],index))
                    # print("debit_mvmt: ", split_value(row['debit_mvmt'],index)) 
                    # print('index= ',index)
                    if split_value(row['debit_mvmt'],index):
                        debit_mvmt = split_value(row['debit_mvmt'],index) 
                        if debit_mvmt.strip() == '' or debit_mvmt is None:
                            debit_mvmt = '0.0' 
                             
                    if split_value(row['credit_mvmt'],index):
                        credit_mvmt = split_value(row['credit_mvmt'],index)
                        if credit_mvmt.strip() == '' or credit_mvmt is None:
                            credit_mvmt = '0.0'  
                    if split_value(row['open_balance'],index):
                        open_balance = split_value(row['open_balance'],index)
                        if open_balance.strip() == '' or open_balance is None:
                            open_balance = '0.0'    
                # value = montant_pert_total * -1 if montant_pert_total < 0 else montant_pert_total  
                if (row['Produits'].startswith('AL.ESCO') and row['Nombre_de_jour_retard'] > 0):  
                    row['Capital_Appele_Non_verse'] = float(open_balance)+float(credit_mvmt)+ float(debit_mvmt)
                else: 
                    max_value = max(float(safe_float(open_balance)), float(credit_mvmt), float(debit_mvmt))
                    # pirnt(f"OPENBALANCE/ {float(safe_float(open_balance))} CREDIT= {float(credit_mvmt)} DEBIT= { float(debit_mvmt)}")
                    # print(f"max_value: {max_value}") 
                    if max_value == 0:
                        min_value = min(float(open_balance), float(credit_mvmt), float(debit_mvmt))  
                        row['Capital_Appele_Non_verse'] =min_value * -1 if min_value < 0 else min_value  
                        # print(f"min_value: { min_value}")
                    else: 
                        row['Capital_Appele_Non_verse'] = max_value 
                        
                row['Capital_Appele_Non_verse'] =row['Capital_Appele_Non_verse']  * -1 if row['Capital_Appele_Non_verse']  < 0 else row['Capital_Appele_Non_verse'] 
                
                
                if row['Capital_Appele_Non_verse']  == 0 and row['Nombre_de_jour_retard'] > 0 and row['arr_status'] =='EXPIRED' :
                    # print("IO ALOHA")
                    for index in matching_indice_Non_appele_verse: 
                        debit_mvmt=0
                        credit_mvmt=0
                        open_balance=0   
                        if split_value(row['debit_mvmt'],index):
                            debit_mvmt = split_value(row['debit_mvmt'],index) 
                            if debit_mvmt.strip() == '' or debit_mvmt is None:
                                debit_mvmt = '0.0'  
                        if split_value(row['credit_mvmt'],index):
                            credit_mvmt = split_value(row['credit_mvmt'],index)
                            if credit_mvmt.strip() == '' or credit_mvmt is None:
                                credit_mvmt = '0.0'  
                        if split_value(row['open_balance'],index):
                            open_balance = split_value(row['open_balance'],index)
                            if open_balance.strip() == '' or open_balance is None:
                                open_balance = '0.0'   
                    max_value = max(float(open_balance), float(credit_mvmt), float(debit_mvmt))
                    
                    # print("CAPTIAL APPELE NON VERSE",max_value)
                
            # test breack point
            # if (row['Numero_pret'] != 'AA243345BQV4' ): 
            #     exit()
            # else:
            #     pass

            # TERME A POUT TOTAL ACCOUT
            valeur_retard = row.get('Nombre_de_jour_retard', '')

            # if isinstance(valeur_retard, str):  # Si c'est une chaîne, nettoyez-la
            #     valeur_retard = valeur_retard.strip()

            # # Vérifier si la valeur est convertible en entier
            # try:
            #     retard = int(valeur_retard)  # Convertit la valeur en entier
            #     # Appliquez les conditions
            #     if 0 < retard <= 30:
            #         row['Statut_du_client'] = 'PA1'
            #         row['Statut_du_client'] = 'PA1'
            #     elif 30 < retard <= 60:
            #         row['Statut_du_client'] = 'PA2'
            #     elif 60 < retard <= 90:
            #         row['Statut_du_client'] = 'PA3'
            #     elif retard > 90:
            #         row['Statut_du_client'] = 'PA4'
            #     else:
            #         row['Statut_du_client'] = ''  # Optionnel pour les cas inattendus
            # except (ValueError, TypeError):  # Gérer les cas où la conversion échoue
            #     row['Statut_du_client'] = ''
            # # Gestion des cas invalides ou valides
            row['Statut_du_client'] = row['all_age_status']
            # print(f"Statut_du_client: {row['Statut_du_client']}")
            if  row['Statut_du_client']==None:
                row['Nombre_de_jour_retard'] = ''
            
   
             
        capital_appele =row['Capital_Appele_Non_verse']
        capital_non_appele = row['Capital_Non_appele_ech']
        if (row['Produits'].startswith('AL.ESCO') and row['Nombre_de_jour_retard'] > 0):
            row['Total_capital_echus_non_echus'] = row['Capital_Appele_Non_verse']
        else:
            row['Total_capital_echus_non_echus'] = capital_appele + capital_non_appele  
        # if (row['Numero_pret'] != 'AA243345BQV4' ): 
        #     exit()
        # else:
        #     pass
    return data
def safe_float(value):
    try:
        return float(value)
    except ValueError:
        return 0.0

def data_base_query(offset):
       return f"""
           SELECT 
            arrangement.co_code AS Agence,
            arrangement.customer AS identification_client,
            arrangement.id AS Numero_pret,
            arrangement.linked_appl_id AS linked_appl_id,
            (SELECT opening_date FROM account_mcbc_live_full WHERE id= arrangement.linked_appl_id LIMIT 1) as Date_pret,  
            ( SELECT CONCAT(customer.short_name, ' ', customer.name_1) FROM customer_mcbc_live_full_partie_1 AS customer  
            WHERE FIND_IN_SET(customer.id, REPLACE(arrangement.customer, '|', ',')) LIMIT 1) AS Nom_client,
            arrangement.product AS Produits,
            '' as Montant_pret,
            (SELECT DATEDIFF(maturity_date, base_date) FROM aa_account_details_mcbc_live_full WHERE id = arrangement.id LIMIT 1) as Duree_Remboursement,
            (SELECT effective_rate FROM aa_arr_interest_mcbc_live_full  WHERE id_comp_1 =arrangement.id and id_comp_2='PRINCIPALINT' LIMIT 1) as taux_d_interet,  
            (SELECT DATEDIFF('2024-12-31', payment_date) FROM aa_bill_details_mcbc_live_full WHERE arrangement_id = arrangement.id ORDER BY payment_date ASC LIMIT 1 ) as Nombre_de_jour_retard,
            '' as Statut_du_client,
            '' as Capital_Non_appele_ech,
            '' as Capital_Appele_Non_verse, 
            '' as Total_capital_echus_non_echus,
            '' as Total_interet_echus,
            (SELECT industry.description FROM industry_mcbc_live_full AS industry INNER JOIN  customer_mcbc_live_full_partie_1 AS
            customer ON industry.id = customer.industry WHERE FIND_IN_SET(customer.id, REPLACE(arrangement.customer, '|', ',')) LIMIT 1) AS Secteur_d_activité,
            (SELECT industry.id FROM industry_mcbc_live_full AS industry INNER JOIN  customer_mcbc_live_full_partie_1 AS
            customer ON industry.id = customer.industry WHERE FIND_IN_SET(customer.id, REPLACE(arrangement.customer, '|', ',')) LIMIT 1) AS Secteur_d_activité_code, 
            ( SELECT account_officer FROM customer_mcbc_live_full_partie_1 AS customer  
            WHERE FIND_IN_SET(customer.id, REPLACE(arrangement.customer, '|', ',')) LIMIT 1) AS Agent_de_gestion,
            (SELECT collateral_code FROM collateral_right_mcbc_live_full WHERE SUBSTRING(id, 1, LOCATE('.', id) - 1) = arrangement.customer LIMIT 1) as Code_Garantie,
            (SELECT all_age_status FROM aa_account_details_mcbc_live_full WHERE id = arrangement.id LIMIT 1) as all_age_status,
            (SELECT alt_acct_id FROM account_mcbc_live_full WHERE id= arrangement.linked_appl_id LIMIT 1) as Numero_compte,  
            arrangement.arr_status, 
            (SELECT settle_status FROM aa_bill_details_mcbc_live_full WHERE settle_status = 'UNPAID' LIMIT 1) as settle_status,
            (SELECT curr_asset_type FROM eb_cont_bal_mcbc_live_full WHERE id = arrangement.linked_appl_id LIMIT 1) as curr_asset_type,
            (SELECT credit_mvmt FROM eb_cont_bal_mcbc_live_full WHERE id = arrangement.linked_appl_id) as credit_mvmt, 
            (SELECT debit_mvmt FROM eb_cont_bal_mcbc_live_full WHERE id = arrangement.linked_appl_id) as debit_mvmt, 
            (SELECT credit_mvmt FROM eb_cont_bal_mcbc_live_full WHERE id = arrangement.linked_appl_id and last_ac_bal_upd='20241231') as credit_mvmt_29_nov, 
            (SELECT debit_mvmt FROM eb_cont_bal_mcbc_live_full WHERE id = arrangement.linked_appl_id and last_ac_bal_upd='20241231') as debit_mvmt_29_nov ,
            (SELECT type_sysdate FROM eb_cont_bal_mcbc_live_full WHERE id = arrangement.linked_appl_id LIMIT 1) as type_sysdate,
            (SELECT open_balance FROM eb_cont_bal_mcbc_live_full WHERE id = arrangement.linked_appl_id LIMIT 1) as open_balance
        FROM 
            aa_arrangement_mcbc_live_full AS arrangement 
        WHERE 
            arrangement.product_line = 'LENDING'
            AND arrangement.arr_status IN ('CURRENT','EXPIRED')  
            HAVING settle_status is not null
            LIMIT 200 OFFSET {offset}
        
            """

   
            # AND arrangement.linked_appl_id ='20000301338' 
            # AND arrangement.linked_appl_id ='20000968887'  
            # AND arrangement.linked_appl_id ='20000957403' 
            # AND arrangement.linked_appl_id ='20000319358' 
            # AND arrangement.linked_appl_id ='20000319358' 
            # AND arrangement.linked_appl_id ='20000648591' 
            # AND arrangement.linked_appl_id ='20000969417' 
            # AND arrangement.linked_appl_id ='20000349478' 
            # AND arrangement.linked_appl_id ='20000968537' 
            # AND arrangement.linked_appl_id ='20000303039'
            # AND arrangement.linked_appl_id ='20000965497'
            # AND arrangement.linked_appl_id ='20001320972'
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
    
    # SQL query to execute
 
 
    # output_file_template = "output_offset_{offset}.xlsx"
    # Define the path template for the output file
    output_file_template = "./out_put_etat_des_encours/etat_des_encours_offset_{offset}.xlsx"

    # Check if the directory exists, and if not, create it
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
 
