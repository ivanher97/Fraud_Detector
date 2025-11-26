import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from pyspark.sql.functions import from_json, col, window

# --- 1. LIMPIEZA DE ZOMBIS ---
# Intentamos obtener una sesión activa y matarla para asegurar que la nueva carga los JARs
try:
    SparkSession.getActiveSession().stop()
    print("💀 Sesión antigua eliminada.")
except:
    print("✅ No había sesión activa.")

# Borramos checkpoints corruptos para que empiece de cero (sin memoria de offsets viejos)
checkpoint_dir = "/tmp/checkpoints_fraud"
if os.path.exists(checkpoint_dir):
    shutil.rmtree(checkpoint_dir)
    print("🧹 Checkpoints antiguos borrados.")

# --- 2. CONFIGURACIÓN BLINDADA ---
jar_path = "/home/jovyan/work/src/postgresql-42.7.8.jar" 

print(f"🛠️ Cargando driver desde: {jar_path}")

spark = SparkSession.builder \
    .appName("FraudDetector") \
    .config("spark.jars", jar_path) \
    .config("spark.driver.extraClassPath", jar_path) \
    .config("spark.executor.extraClassPath", jar_path) \
    .config("spark.sql.streaming.checkpointLocation", checkpoint_dir) \
    .getOrCreate()

# Verificación de carga en la JVM
try:
    spark.sparkContext._jvm.java.lang.Class.forName("org.postgresql.Driver")
    print("✅ DRIVER CARGADO EN JVM CORRECTAMENTE")
except Exception as e:
    print("❌ ERROR CRÍTICO: El driver no está en el Classpath.")
    raise e

spark.sparkContext.setLogLevel("WARN")

# --- 3. ESQUEMA Y LÓGICA ---
json_schema = StructType([
    StructField("id", StringType(), False),
    StructField("card_id", StringType(), False),
    StructField("amount", FloatType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("merchant_id", StringType(), False),
    StructField("category", StringType(), False),
    StructField("location", StructType([
        StructField("lat", FloatType()),
        StructField("lon", FloatType())
    ]), False)
])

# Leemos desde Kafka (Earliest para pillar los datos que el producer acaba de enviar)
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_df = raw_df.select(from_json(col("value").cast("string"), json_schema).alias("data")).select("data.*")

# Agregación
fraud_df = parsed_df.filter(col("amount") > 1000).groupBy(
    window(col("timestamp"), "10 minutes", "1 minute"), 
    "category").count()

final_df = fraud_df.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("category"),
    col("count")
)

# --- 4. FUNCIÓN WORKER (SINK) ---
def write_to_postgres(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    
    print(f"Writing batch {batch_id} to Postgres...")
    
    # 1. ESCRIBIR EN STAGING (Tabla temporal sin restricciones)
    # Usamos 'overwrite' para limpiar los datos del batch anterior automáticamente
    # OJO: Esto sobreescribe la tabla staging, NO la tabla final.
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://fraud-postgres:5432/fraud_detection") \
        .option("dbtable", "fraud_staging_metrics") \
        .option("user", "myuser") \
        .option("password", "mypassword") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
    
    # 2. EJECUTAR EL MERGE (UPSERT) VÍA JDBC NATIVO
    # Recuperamos el driver manager de la JVM de Spark
    driver_manager = batch_df._sc._gateway.jvm.java.sql.DriverManager
    con = driver_manager.getConnection(
        "jdbc:postgresql://fraud-postgres:5432/fraud_detection", "myuser", "mypassword"
    )
    
    try:
        stmt = con.createStatement()
        
        # SQL DE UPSERT (Postgres Syntax)
        # "Intenta insertar. Si la clave (window_start, window_end, category) choca,
        # entonces ACTUALIZA el contador con el nuevo valor."
        sql_merge = """
        INSERT INTO fraud_metrics (window_start, window_end, category, count)
        SELECT window_start, window_end, category, count FROM fraud_staging_metrics
        ON CONFLICT (window_start, window_end, category) 
        DO UPDATE SET count = EXCLUDED.count;
        """
        
        stmt.execute(sql_merge)
        stmt.close()
        print(f"✅ Batch {batch_id} merged successfully.")
        
    except Exception as e:
        print(f"❌ Error during merge: {e}")
        raise e
    finally:
        con.close()

# --- 5. EJECUCIÓN ---
print("🚀 Iniciando Streaming...")
query = final_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("update") \
    .start()

query.awaitTermination()