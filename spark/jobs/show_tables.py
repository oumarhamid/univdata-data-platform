from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ShowHudiTables") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.catalogImplementation", "in-memory") \
    .getOrCreate()

tables = ["faculty_profiles", "course_catalog", "programs"]

for table in tables:
    print(f"\n===== Table: {table} =====")
    df = spark.read.format("hudi").load(f"s3a://curated-zone/{table}")
    df.show(50, truncate=False)  # Affiche jusqu'à 50 lignes, sans troncation
    print(f"Nombre de lignes : {df.count()}\n")

spark.stop()