#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, coalesce

spark = SparkSession.builder \
    .appName("IndexToElasticsearch") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .enableHiveSupport() \
    .getOrCreate()

df_faculty = spark.sql("SELECT * FROM default.faculty_profiles")
df_courses = spark.sql("SELECT * FROM default.course_catalog")

df_faculty_index = df_faculty.withColumn("doc_type", lit("faculty")) \
    .withColumn("_id", concat(lit("faculty_"), col("record_id")))
df_courses_index = df_courses.withColumn("doc_type", lit("course")) \
    .withColumn("_id", concat(lit("course_"), col("course_id")))

df_all = df_faculty_index.unionByName(df_courses_index, allowMissingColumns=True)

es_conf = {
    "es.nodes": "elasticsearch",
    "es.port": "9200",
    "es.resource": "university_search/_doc",
    "es.mapping.id": "_id",
    "es.write.operation": "index"
}
df_all.write.format("org.elasticsearch.spark.sql") \
    .options(**es_conf) \
    .mode("append") \
    .save()

spark.stop()