import os
import sqlite3
from contextlib import closing

import pandas as pd

RUTA_DB = os.path.join("data", "felicidad.db")


def guardar_predicciones(df, ruta=RUTA_DB, tabla="predicciones"):
    """guardo las predicciones en SQLite, reemplazando la tabla si ya existía"""
    os.makedirs(os.path.dirname(ruta), exist_ok=True)
    with closing(sqlite3.connect(ruta)) as con:
        df.to_sql(tabla, con, if_exists="replace", index=False)
        con.commit()


def leer_tabla(consulta, ruta=RUTA_DB):
    """corro una consulta sobre la base y devuelvo un dataframe"""
    with closing(sqlite3.connect(ruta)) as con:
        return pd.read_sql_query(consulta, con)
