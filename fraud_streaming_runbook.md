# Fraud Detector Streaming - Deployment Runbook

Este documento detalla los pasos manuales necesarios para desplegar y
ejecutar el pipeline de detección de fraude en tiempo real.

**Stack Tecnológico:** Docker, Kafka, Spark Structured Streaming
(PySpark), PostgreSQL.

## 1. Preparación de la Base de Datos (DDL)

Antes de iniciar el stream, debemos garantizar que la tabla destino
tiene una **Clave Primaria Compuesta** para soportar la lógica de
*Upserts* (Idempotencia).Markdown

# Fraud Detector Streaming - Deployment Runbook

Este documento detalla los pasos manuales necesarios para desplegar y ejecutar el pipeline de detección de fraude en tiempo real.

**Stack Tecnológico:** Docker, Kafka, Spark Structured Streaming (PySpark), PostgreSQL.

## 1. Preparación de la Base de Datos (DDL)
Antes de iniciar el stream, debemos garantizar que la tabla destino tiene una **Clave Primaria Compuesta** para soportar la lógica de *Upserts* (Idempotencia).

```bash
# Reiniciar tabla con restricciones de integridad
docker exec -it fraud-postgres psql -U myuser -d fraud_detection -c "
DROP TABLE IF EXISTS fraud_metrics;
CREATE TABLE fraud_metrics (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    category TEXT,
    count BIGINT,
    PRIMARY KEY (window_start, window_end, category)
);"
```

## 2. Inyección de Dependencias (Driver JDBC)

Debido al aislamiento de red y problemas con la carga dinámica de JARs
en tiempo de ejecución, el driver de PostgreSQL debe inyectarse
manualmente en el System Classpath de Spark.

Requisito: El archivo `postgresql-42.7.8.jar` debe existir en la carpeta
`src/`.

``` bash
# 1. Copiar el JAR al directorio de librerías del sistema de Spark (requiere root)
docker exec --user root -it spark-client cp /home/jovyan/work/src/postgresql-42.7.8.jar /usr/local/spark/jars/

# 2. Verificación de instalación
docker exec --user root -it spark-client ls -l /usr/local/spark/jars/postgresql-42.7.8.jar
```

> Nota: Este paso solo es necesario hacerlo una vez tras crear el
> contenedor (`docker-compose up`).

## 3. Ejecución del Pipeline (Runtime)

Ejecutamos el trabajo de Spark (spark-submit) inyectando el paquete de
Kafka dinámicamente. El driver de Postgres ya será cargado desde el
sistema.

``` bash
docker exec -it spark-client spark-submit   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0   /home/jovyan/work/src/detector.py
```

## 4. Verificación de Datos

Para confirmar que los datos se están escribiendo y actualizando
correctamente (sin duplicados):

``` bash
docker exec -it fraud-postgres psql -U myuser -d fraud_detection -c "SELECT * FROM fraud_metrics ORDER BY window_start DESC LIMIT 20;"
```

------------------------------------------------------------------------

**Siguiente paso:** Visualización.\
Prepara tu mente. Mañana conectaremos **Grafana**. Quiero ver esos datos
en gráficos de barras en tiempo real. Descansa.
