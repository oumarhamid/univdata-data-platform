from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HiveSyncParquet") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# Chemins vers les fichiers Parquet (les tables Hudi écrivent dans ce répertoire)
tables = [
    ("faculty_profiles", "s3a://curated-zone/faculty_profiles/*/*.parquet"),
    ("course_catalog", "s3a://curated-zone/course_catalog/*/*.parquet")
]

for table_name, path in tables:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        df = spark.read.parquet(path)
        df.createOrReplaceTempView("temp_view")
        spark.sql(f"CREATE TABLE {table_name} USING parquet AS SELECT * FROM temp_view")
        count = spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()[0][0]
        print(f"✅ Table {table_name} créée avec {count} lignes (depuis Parquet).")
    except Exception as e:
        print(f"❌ Erreur pour {table_name}: {e}")

spark.stop()