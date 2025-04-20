from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, when, desc, date_format, month, year, round, concat, lit

# Initialiser une session Spark
spark = SparkSession.builder \
    .appName("GameSalesDataProcessing") \
    .config("spark.sql.legacy.createHiveTableByDefault", "false") \
    .getOrCreate()

# Lecture des fichiers CSV
games_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/data/games.csv")
sales_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/data/sales.csv")
customers_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/data/customers.csv")
stores_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/data/stores.csv")

# Créer le répertoire de sortie pour les données traitées
import os
os.makedirs("/data/processed", exist_ok=True)

# Enregistrer les DataFrames comme des tables temporaires
games_df.createOrReplaceTempView("games_temp")
sales_df.createOrReplaceTempView("sales_temp")
customers_df.createOrReplaceTempView("customers_temp")
stores_df.createOrReplaceTempView("stores_temp")

# Nettoyage des données au besoin
print("Vérification des données : ")
print(f"Nombre de jeux : {games_df.count()}")
print(f"Nombre de ventes : {sales_df.count()}")
print(f"Nombre de clients : {customers_df.count()}")
print(f"Nombre de magasins : {stores_df.count()}")

# Transformation 1: Enrichir les ventes avec des informations sur les jeux
sales_enriched_df = spark.sql("""
    SELECT 
        s.sale_id, 
        s.sale_date, 
        s.game_id,
        g.title as game_title,
        g.genre,
        g.platform,
        s.quantity,
        s.unit_price,
        s.discount_percentage,
        s.total_amount,
        s.sales_channel,
        s.store_id,
        s.customer_id
    FROM sales_temp s
    JOIN games_temp g ON s.game_id = g.game_id
""")
sales_enriched_df.createOrReplaceTempView("sales_enriched_temp")
print("Transformation 1 terminée.")

# Transformation 2: Ajouter les informations client et magasin
full_sales_df = spark.sql("""
    SELECT 
        s.sale_id, 
        s.sale_date, 
        s.game_id,
        s.game_title,
        s.genre,
        s.platform,
        g.publisher,
        g.age_rating,
        g.release_date,
        g.base_price,
        s.quantity,
        s.unit_price,
        s.discount_percentage,
        s.total_amount,
        s.sales_channel,
        CASE WHEN s.sales_channel = 'store' THEN st.store_name ELSE 'Online' END as store_name,
        CASE WHEN s.sales_channel = 'store' THEN st.country ELSE c.country END as sale_country,
        c.customer_id,
        c.segment,
        c.loyalty_status,
        c.age_group
    FROM sales_enriched_temp s
    JOIN games_temp g ON s.game_id = g.game_id
    JOIN customers_temp c ON s.customer_id = c.customer_id
    LEFT JOIN stores_temp st ON s.store_id = st.store_id
""")
full_sales_df.createOrReplaceTempView("game_sales_full")
print("Transformation 2 terminée.")

# Enregistrement comme table au format parquet
full_sales_df.write.mode("overwrite").parquet("/data/processed/game_sales_full")
print("Sauvegarde de la table principale terminée.")

# Création de vues agrégées pour l'analyse

# Vue 1: Ventes par mois et par plateforme
monthly_platform_sales = spark.sql("""
    SELECT 
        date_format(sale_date, 'yyyy-MM') as month,
        platform,
        count(distinct sale_id) as num_sales,
        sum(total_amount) as total_revenue,
        avg(total_amount) as avg_sale_value
    FROM game_sales_full
    GROUP BY date_format(sale_date, 'yyyy-MM'), platform
    ORDER BY month, platform
""")
monthly_platform_sales.write.mode("overwrite").parquet("/data/processed/monthly_platform_sales")
print("Vue 1 créée et sauvegardée.")

# Vue 2: Performance des jeux
game_performance = spark.sql("""
    SELECT 
        game_id,
        game_title,
        genre,
        platform,
        publisher,
        count(distinct sale_id) as num_sales,
        sum(quantity) as units_sold,
        sum(total_amount) as total_revenue,
        avg(discount_percentage) as avg_discount
    FROM game_sales_full
    GROUP BY game_id, game_title, genre, platform, publisher
    ORDER BY total_revenue DESC
""")
game_performance.write.mode("overwrite").parquet("/data/processed/game_performance")
print("Vue 2 créée et sauvegardée.")

# Vue 3: Segmentation client
customer_segments = spark.sql("""
    SELECT 
        segment,
        loyalty_status,
        age_group,
        country,
        count(distinct customer_id) as num_customers,
        count(distinct sale_id) as num_transactions,
        sum(total_amount) as total_spent,
        sum(total_amount)/count(distinct customer_id) as avg_spent_per_customer
    FROM game_sales_full
    GROUP BY segment, loyalty_status, age_group, country
    ORDER BY total_spent DESC
""")
customer_segments.write.mode("overwrite").parquet("/data/processed/customer_segments")
print("Vue 3 créée et sauvegardée.")

# Vue 4: Performance des canaux de vente
channel_performance = spark.sql("""
    SELECT 
        sales_channel,
        CASE WHEN sales_channel = 'store' THEN store_name ELSE 'Online' END as store_or_online,
        count(distinct sale_id) as num_sales,
        sum(total_amount) as total_revenue,
        sum(total_amount)/count(distinct sale_id) as avg_transaction_value
    FROM game_sales_full
    GROUP BY sales_channel, CASE WHEN sales_channel = 'store' THEN store_name ELSE 'Online' END
    ORDER BY total_revenue DESC
""")
channel_performance.write.mode("overwrite").parquet("/data/processed/channel_performance")
print("Vue 4 créée et sauvegardée.")

# Vue 5: Tendances des genres de jeux
genre_trends = spark.sql("""
    SELECT 
        date_format(sale_date, 'yyyy-MM') as month,
        genre,
        count(distinct sale_id) as num_sales,
        sum(total_amount) as total_revenue
    FROM game_sales_full
    GROUP BY date_format(sale_date, 'yyyy-MM'), genre
    ORDER BY month, total_revenue DESC
""")
genre_trends.write.mode("overwrite").parquet("/data/processed/genre_trends")
print("Vue 5 créée et sauvegardée.")

print("Toutes les transformations et vues ont été créées avec succès!")
spark.stop()
