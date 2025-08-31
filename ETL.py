import json
import psycopg2

DB_NAME = "sport_stores"
DB_USER = "postgres"
DB_PASS = "philistine"
DB_HOST = "localhost"
DB_PORT = 5432


# creation d'une connexion

conn = psycopg2.connect(database=DB_NAME,
                        user=DB_USER,
                        password=DB_PASS,
                        host=DB_HOST,
                        port=DB_PORT)

# creation de la table si elle n'existe pas déjà
with conn:
    cur = conn.cursor()
    cur.execute("""DROP TABLE IF EXISTS exploded_products""")
    cur.execute("""
    create table exploded_products (
            transaction_id VARCHAR(255)
            , store_id VARCHAR(255)
            , store_name VARCHAR(255)
            , store_country VARCHAR(255)
            , store_city VARCHAR(255)
            , purchase_timestamp TIMESTAMP
            , payment_method VARCHAR(255)
            , currency VARCHAR(255)
            , total_amount NUMERIC(10,2)
            , total_quantity_sold INT
            , discount_applied NUMERIC(10,2)
            , return_status VARCHAR(255)
            , product_id VARCHAR(255)
            , product_name VARCHAR(255)
            , product_category VARCHAR(255)
            , quantity INT
            , price NUMERIC(10,2))
    """)
    
    # Chargement du fichier JSON
    with open("source/pos_sales_data_30k.json", "r") as f:
        data = json.load(f)
        #print(data)

        for txn in data:   # parcourt toutes les transactions
            for product in txn["products"]:   # parcourt tous les produits de la transaction
                cur.execute("""
                    INSERT INTO exploded_products (
                        transaction_id, store_id, store_name, store_country, store_city,
                        purchase_timestamp, payment_method, currency, total_amount, total_quantity_sold,
                        discount_applied, return_status, product_id, product_name, product_category, quantity, price
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    txn["transaction_id"], txn["store_id"], txn["store_name"], txn["store_country"],
                    txn["store_city"], txn["purchase_timestamp"], txn["payment_method"], txn["currency"],
                    txn["total_amount"], txn["total_quantity_sold"], txn["discount_applied"], txn["return_status"],
                    product["product_id"], product["product_name"], product["products_category"], product["quantity"], product["price"]
                ))







