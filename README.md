Pipeline de ETL con Kafka y machine learning para predecir el happiness score de los países entre 2015 y 2019.

### qué hace

Consume desde Kafka los cinco archivos del World Happiness Report, arma el estado en memoria, unifica los años en un solo dataset, hace el EDA, entrena una regresión con validación estadística y guarda las predicciones en una base SQLite.

El flujo es este: Kafka, estado en memoria, armonización de columnas, EDA, selección de features, pruebas estadísticas, entrenamiento, evaluación, validación temporal, modelo guardado y carga en base de datos.

### estructura

```
workshop3-etl-kafka/
├── src/
│   ├── kafka_consumer.py   conexión al broker y lectura del topic
│   ├── state.py            estado en memoria y dataframes por año
│   ├── transform.py        armonización de columnas y nombres de país
│   └── database.py         carga y consulta en SQLite
├── notebooks/
│   └── eda_modelo.ipynb    EDA, modelo, evaluación y carga final
├── models/                 modelo entrenado
├── data/                   copia local de los mensajes, dataset unificado y base
├── .env.example
├── figuras/                gráficas del EDA y del modelo
└── requirements.txt
```

### cómo correrlo

Crear el entorno e instalar las librerías:

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

Crear un archivo `.env` en la raíz copiando `.env.example` y llenando la contraseña del broker y un group id propio, en minúsculas y con guiones:

```
KAFKA_PASSWORD=
KAFKA_GROUP_ID=
```

Consumir el topic y revisar que el estado quedó bien:

```bash
python src/state.py
```

La primera vez lee Kafka y guarda una copia en `data/raw_messages.json`. Las siguientes veces usa esa copia. Para volver a leer Kafka basta con borrarla.

Después se abre `notebooks/eda_modelo.ipynb` con el kernel del entorno y se corre completo.

### extracción

El consumer se conecta con SASL_SSL y SCRAM-SHA-512, se suscribe a `etl.records` y lee desde el primer mensaje. Paro cuando pasan cinco intentos seguidos sin recibir nada. Llegaron 782 mensajes, cada uno con su metadata de Kafka y su contenido, que trae el archivo de origen, el año, un `record_id` y los campos originales del csv.

El estado en memoria es un diccionario con el `record_id` como llave, así un mensaje repetido no se duplica. La verificación dio 782 mensajes, 782 registros únicos y 0 duplicados, que coincide con las filas de los cinco dataframes: 158, 157, 155, 156 y 156.

### preparación de los datos

- Los nombres de las columnas cambian entre años, por ejemplo `Economy (GDP per Capita)`, `Economy..GDP.per.Capita.` y `GDP per capita`. Los pasé todos a un esquema común.
- Dejé solo las siete columnas que existen en los cinco años: país, puntaje, pib, apoyo social, esperanza de vida, libertad, generosidad y percepción de corrupción.
- Descarté el ranking porque es el objetivo ordenado, el error estándar, los intervalos y los whiskers porque salen del puntaje y no están en todos los años, el dystopia residual porque completa la suma del puntaje y sería fuga de información, y la región porque solo está en 2015 y 2016.
- Unifiqué seis países que venían escritos distinto según el año, como Taiwan, Hong Kong, North Cyprus, North Macedonia, Trinidad and Tobago y Somaliland Region. La etiqueta solo sirve para reconocer la misma unidad entre años y no es una postura política. El país no entra al modelo como variable.
- Hay un solo nulo, United Arab Emirates 2018 en percepción de corrupción, que se imputa con la mediana dentro del pipeline.
- Hay 10 outliers en apoyo social, 17 en generosidad y 67 en percepción de corrupción por rango intercuartílico. Los dejé porque son valores reales de distribuciones asimétricas.
- Hay entre 5 y 6 ceros por variable, menos del 1 por ciento de las filas, en países como Somalia, República Centroafricana, Sierra Leona y Grecia en generosidad. Los dejé porque son mínimos reales de la escala.

Integré los años apilándolos en un solo dataset en formato largo, con 782 filas. Gano observaciones, pero un mismo país aparece hasta cinco veces y pierdo las columnas que solo existen en algunos años. Además hay 18 países que no están en los cinco informes, así que el panel no queda balanceado.

### modelo

El objetivo es `happiness_score` y los predictores son pib, apoyo social, esperanza de vida, libertad, generosidad y percepción de corrupción. En el EDA las más correlacionadas con el puntaje fueron pib con 0.79, esperanza de vida con 0.74 y apoyo social con 0.65, y la más débil fue generosidad con 0.14.

Dividí train y test por país con `GroupShuffleSplit`, 80 y 20 por ciento. Quedaron 626 filas de 131 países en train y 156 filas de 33 países en test, sin ningún país en los dos lados. Si hubiera dividido al azar, el mismo país estaría en los dos conjuntos con valores casi iguales y las métricas saldrían infladas.

Cada modelo va en un pipeline con imputación por mediana y escalado, así esos parámetros se aprenden solo con train.

### estadística

- Con anova por año vi que el puntaje promedio no cambia entre años, con p de 0.996, pero las variables sí, sobre todo el apoyo social con p de 1.4e-47. Cambió la escala de las variables y no los países, por eso meto el año en el modelo como categoría con one hot.
- El VIF queda entre 1.2 y 3.1 y el KMO global da 0.68, así que la multicolinealidad es leve.
- Ajusté la regresión lineal sobre train con variables estandarizadas. Las seis variables salen significativas, con el pib como la más fuerte, y el modelo explica el 76 por ciento de la varianza. Como un mismo país aparece varias veces, los p valores salen algo optimistas, por eso divido siempre por país.
- Revisé los residuos con el QQ plot y contra el valor ajustado.
- Comparé la regresión lineal base, la lineal con año y el random forest con año en 40 particiones por país distintas. La lineal con año tiene MAE de 0.421 contra 0.441 de la base y gana en 34 de las 40.

### resultados en test

| modelo | MAE | MSE | RMSE | R2 | R2 ajustado |
|---|---|---|---|---|---|
| Regresión lineal base | 0.4502 | 0.3485 | 0.5903 | 0.7737 | 0.7645 |
| Regresión lineal con año | 0.4276 | 0.3185 | 0.5644 | 0.7931 | 0.7788 |

Con 33 países de test la métrica depende de cuáles caen ahí, por eso repetí la comparación en 40 particiones. Me quedé con la regresión lineal con año porque gana en test y en las 40 particiones, y es fácil de explicar. El apoyo social y el pib son las variables que más pesan, seguidas por la libertad y la esperanza de vida. Botswana y Tanzania aparecen repetidas entre los errores más grandes, siempre sobreestimadas.

### validación en el tiempo

No hice series de tiempo clásicas porque cada país tiene solo cinco puntos. Hice una validación hacia adelante, entrenando con los años anteriores y prediciendo el siguiente, y la comparé con la persistencia, que repite el puntaje del año anterior, y con suavización exponencial simple por país. Las dos ganan por mucho, con MAE de 0.12 a 0.14 contra 0.38 a 0.51 de los modelos, porque el puntaje de un país casi no cambia de un año a otro. El modelo sirve para explicar qué variables mueven el puntaje, no para pronosticar el año que viene. El año como categoría falla con años nuevos, y estandarizar por año funciona mejor en ese caso.

### guardado y carga

El pipeline final se guarda con joblib en `models/modelo_felicidad.joblib`, con el imputador, el escalador y la codificación del año adentro, y unos metadatos con el algoritmo, la versión, las variables esperadas, la fecha y las métricas. Las predicciones del test se cargan en la tabla `predicciones` de `data/felicidad.db`. El notebook termina cargando el modelo desde el archivo y prediciendo un registro nuevo con su año.

### gráficas

Se guardan solas en `figuras/` al correr el notebook, por ejemplo la distribución del puntaje, las correlaciones, las medias por año, los supuestos de los residuos y los coeficientes del modelo final.

### limitaciones y mejoras

El test tiene solo 33 países, así que las métricas cambian si cambia la partición. Los seis factores del informe son contribuciones estimadas por los autores y no mediciones directas, así que parte del buen ajuste viene de ahí. El año como categoría no sirve para años que el modelo no vio, y el modelo no supera a la persistencia ni a la suavización exponencial para pronosticar. Como mejoras se pueden estandarizar las variables por año, probar ridge o lasso, gradient boosting con búsqueda de hiperparámetros, la región para los años que la tienen y revisar Botswana y Tanzania.
