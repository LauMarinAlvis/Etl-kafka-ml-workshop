import pandas as pd

# nombre original de cada año -> nombre estándar
MAPEO = {
    # 2015 y 2016
    "Country": "country",
    "Happiness Score": "happiness_score",
    "Economy (GDP per Capita)": "gdp_per_capita",
    "Family": "social_support",
    "Health (Life Expectancy)": "life_expectancy",
    "Freedom": "freedom",
    "Trust (Government Corruption)": "corruption_perception",
    "Generosity": "generosity",
    # 2017
    "Happiness.Score": "happiness_score",
    "Economy..GDP.per.Capita.": "gdp_per_capita",
    "Health..Life.Expectancy.": "life_expectancy",
    "Trust..Government.Corruption.": "corruption_perception",
    # 2018 y 2019
    "Country or region": "country",
    "Score": "happiness_score",
    "GDP per capita": "gdp_per_capita",
    "Social support": "social_support",
    "Healthy life expectancy": "life_expectancy",
    "Freedom to make life choices": "freedom",
    "Perceptions of corruption": "corruption_perception",
}

# columnas que existen en los 5 años después de armonizar los nombres
COLUMNAS_COMUNES = [
    "country", "happiness_score", "gdp_per_capita", "social_support",
    "life_expectancy", "freedom", "generosity", "corruption_perception",
]

# países escritos distinto según el año -> un solo nombre.
# la etiqueta solo sirve para reconocer la misma unidad entre años,
# no es una postura política
PAISES = {
    "Hong Kong S.A.R., China": "Hong Kong",
    "Taiwan Province of China": "Taiwan",
    "Northern Cyprus": "North Cyprus",
    "Macedonia": "North Macedonia",
    "Trinidad & Tobago": "Trinidad and Tobago",
    "Somaliland region": "Somaliland Region",
}


def armonizar_anio(df):
    """renombro al estándar y dejo solo las columnas comunes"""
    df = df.rename(columns=MAPEO)
    descartadas = [
        c for c in df.columns
        if c not in COLUMNAS_COMUNES + ["year", "source_file", "record_id"]
    ]
    df = df[COLUMNAS_COMUNES + ["year", "record_id"]].copy()

    # país como texto limpio y unificado, el resto numérico
    df["country"] = df["country"].astype(str).str.strip().replace(PAISES)
    for col in COLUMNAS_COMUNES[1:]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df, descartadas


def unir_anios(dfs):
    """armonizo cada año y los apilo en un solo dataframe"""
    partes, descartadas_por_anio = [], {}
    for anio, df in dfs.items():
        df_arm, descartadas = armonizar_anio(df)
        partes.append(df_arm)
        descartadas_por_anio[anio] = descartadas
    df_total = pd.concat(partes, ignore_index=True)
    return df_total, descartadas_por_anio
