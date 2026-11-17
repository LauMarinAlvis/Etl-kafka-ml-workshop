import os
import json
import pandas as pd
from kafka_consumer import consumir_topic

RUTA_CACHE = os.path.join("data", "raw_messages.json")


def obtener_mensajes(forzar_consumo=False):
    """
    Si ya existe una copia local de los mensajes, la carga.
    Si no existe (o forzar_consumo=True), consume Kafka y la crea.
    """
    if os.path.exists(RUTA_CACHE) and not forzar_consumo:
        print(f"Cargando mensajes desde la copia local: {RUTA_CACHE}")
        with open(RUTA_CACHE, "r", encoding="utf-8") as f:
            return json.load(f)

    print("Consumiendo mensajes desde Kafka...")
    mensajes = consumir_topic()
    os.makedirs("data", exist_ok=True)
    with open(RUTA_CACHE, "w", encoding="utf-8") as f:
        json.dump(mensajes, f, ensure_ascii=False, default=str)
    print(f"Copia local guardada en: {RUTA_CACHE}")
    return mensajes


def construir_estado(mensajes):
    """
    Estado en memoria: diccionario {record_id: registro}.
    Usar record_id como clave evita duplicados si Kafka entrega un mensaje repetido.
    """
    estado = {}
    duplicados = 0

    for m in mensajes:
        meta = m["value"]["metadata"]
        record_id = meta["record_id"]

        if record_id in estado:
            duplicados += 1
            continue

        estado[record_id] = {
            "metadata": meta,
            "data": m["value"]["data"],
            "kafka": {
                "partition": m["partition"],
                "offset": m["offset"],
                "timestamp": m["timestamp"],
            },
        }

    return estado, duplicados


def estado_a_dataframes(estado):
    """
    Convierte el estado en un diccionario {año: DataFrame}.
    Cada año queda separado porque sus columnas pueden ser diferentes.
    """
    filas_por_anio = {}

    for record_id, reg in estado.items():
        fila = dict(reg["data"])                      # columnas originales del CSV
        fila["year"] = reg["metadata"]["year"]
        fila["source_file"] = reg["metadata"]["source_file"]
        fila["record_id"] = record_id
        filas_por_anio.setdefault(reg["metadata"]["year"], []).append(fila)

    return {anio: pd.DataFrame(filas) for anio, filas in sorted(filas_por_anio.items())}


def verificar_estado(mensajes, estado, duplicados, dfs):
    """Imprime la verificación del dataset resultante."""
    print("\n===== VERIFICACIÓN DEL ESTADO =====")
    print("Mensajes consumidos:", len(mensajes))
    print("Registros únicos en el estado:", len(estado))
    print("Mensajes duplicados descartados:", duplicados)
    print("Suma de filas de los DataFrames:", sum(len(d) for d in dfs.values()))

    print("\nRegistros por año:")
    for anio, df in dfs.items():
        archivo = df["source_file"].iloc[0]
        print(f"  {anio} ({archivo}): {len(df)} filas, {df.shape[1] - 3} columnas de datos")

    print("\nColumnas de datos por año:")
    for anio, df in dfs.items():
        cols = [c for c in df.columns if c not in ("year", "source_file", "record_id")]
        print(f"\n  {anio}: {cols}")

    print("\nValores nulos por año:")
    for anio, df in dfs.items():
        nulos = df.isnull().sum()
        nulos = nulos[nulos > 0]
        print(f"  {anio}: {dict(nulos) if len(nulos) else 'sin nulos'}")


if __name__ == "__main__":
    mensajes = obtener_mensajes()
    estado, duplicados = construir_estado(mensajes)
    dfs = estado_a_dataframes(estado)
    verificar_estado(mensajes, estado, duplicados, dfs)