import os
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"

import json
from collections import deque
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RUTA_DATOS = "datos.json"
RUTA_ALERTAS = "alertas.json"

# 1) SESIÓN
spark = SparkSession.builder \
    .appName("DatosValentinaTatan") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 2) CONTEXTO / LECTURA
flujo = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

datos = flujo.select(
    F.split(flujo.value, ",").getItem(0).cast("double").alias("temperatura"),
    F.split(flujo.value, ",").getItem(1).cast("double").alias("ruido"),
    F.split(flujo.value, ",").getItem(2).cast("double").alias("vibracion")
).dropna()  # ignora líneas mal formadas

# 3) REGLAS DE ALERTA
alertas = datos.withColumn(
    "alerta",
    F.when(
        (F.col("temperatura") < 25) |
        (F.col("temperatura") > 90) |
        (F.col("ruido") < 30) |
        (F.col("ruido") > 120) |
        (F.col("vibracion") < 0.1) |
        (F.col("vibracion") > 5),
        "MANTENIMIENTO PREVENTIVO"
    ).otherwise("NORMAL")
)

# 4) SALIDA A JSON PARA EL HTML
historial = deque(maxlen=200)  # últimos 200 registros

def escribir_json(ruta, contenido):
    # escribe en un temporal y reemplaza, para que el HTML nunca lea un archivo a medias
    tmp = ruta + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(contenido, f, ensure_ascii=False)
    os.replace(tmp, ruta)

def guardar_lote(df, batch_id):
    filas = [r.asDict() for r in df.collect()]
    if not filas:
        return
    hora = datetime.now().strftime("%H:%M:%S")
    for fila in filas:
        fila["hora"] = hora
        historial.append(fila)

    escribir_json(RUTA_DATOS, list(historial))
    escribir_json(RUTA_ALERTAS, [f for f in historial if f["alerta"] != "NORMAL"])
    print(f"Lote {batch_id}: {len(filas)} registros guardados")

consulta = alertas.writeStream \
    .outputMode("append") \
    .foreachBatch(guardar_lote) \
    .start()

consulta.awaitTermination()