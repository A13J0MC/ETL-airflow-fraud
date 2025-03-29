# --------------- #
# PACKAGE IMPORTS #
# --------------- #

import streamlit as st
import duckdb
import pandas as pd
import os
from datetime import date
import altair as alt
import json
from utils import sql_1, sql_2, sql_3, sql_4, sql_5
# import matplotlib.pyplot as plt

# --------- #
# VARIABLES #
# --------- #

duck_db_instance_name = (
    "dwh"  # when changing this value also change the db name in .env
)


# retrieving data
def client_data(db=f"/usr/local/airflow/{duck_db_instance_name}"):
    """Function to query a local DuckDB data processed through
    the data pipeline."""

    # Query duckdb database `dwh`
    cursor = duckdb.connect(db)

    # get local weather data
    client_data = cursor.execute(
        """
        with
        transacciones as (
            SELECT
                date_trunc('month', fecha_transaccion) mes
                , count(*) cantidad_transacciones
                , sum(puntos_ganados) puntos_ganados
            FROM transacciones_table
            where fecha_transaccion::date >= CURRENT_DATE - INTERVAL '1 year'
            group by 1
        )
        , eventos_fraude as (
            SELECT
                date_trunc('month', fecha_evento) mes
                , count(*) cantidad_eventos_fraude
            from eventos_fraude_table
            where fecha_evento::date >= CURRENT_DATE - INTERVAL '1 year'
            group by 1
        )
        SELECT t.*, e.cantidad_eventos_fraude
        FROM transacciones t
        LEFT JOIN eventos_fraude e on t.mes = e.mes
        ;"""
    ).fetchall()

    # client_data_col_names = cursor.execute(
    #     f"""SELECT column_name from information_schema.columns where table_name = 'clientes_table';"""
    # ).fetchall()

    cursor.close()

    # df = pd.DataFrame(
    #     client_data, columns=[x[0] for x in client_data_col_names]
    # )

    return client_data


data = client_data()
st.write(data)
# Título de la aplicación
st.title("Prueba Técnica: BI Data Engineer - Resultados")

# Introducción a la prueba
st.header("Descripción de la prueba técnica")
st.write("""
A continuación, se presenta la prueba técnica para la vacante de BI Data Engineer, que evalúa habilidades en SQL, Python e ingeniería de datos. 
El enfoque adicional está en la detección de fraudes dentro de un programa de fidelización.
""")

# Desafío 1: SQL
st.header("1. Desafío SQL")
st.write("""
**Contexto**: Se provee un conjunto de tablas representativas de un programa de fidelización.
- **Clientes**: Información de los clientes.
- **Transacciones**: Detalles sobre las transacciones realizadas por los clientes.
- **Redenciones**: Detalles sobre las redenciones de puntos.
- **Eventos_Fraude**: Información sobre los eventos de fraude reportados.

**Ejercicios**:
1. Agregación y análisis de comportamiento:
   - Escribe una consulta que obtenga, para cada cliente, la cantidad total de puntos ganados, puntos redimidos y el saldo actual de puntos (diferencia entre los puntos ganados y redimidos).
   - Extiende la consulta para incluir clientes que hayan realizado transacciones en los últimos 6 meses y ordena el resultado por saldo de puntos descendente.
""")

st.code(sql_1, language='sql')
st.code(sql_2, language='sql')

st.write("""
2. Detección de anomalías:
   - Con la tabla Transacciones, identifica posibles transacciones fraudulentas basadas en un umbral definido (por ejemplo, transacciones cuyo monto sea mayor a 3 veces el promedio del cliente).
   - Presenta la consulta que calcule el promedio de gasto por cliente y, a partir de ello, liste aquellas transacciones que superen el umbral.
""")

st.code(sql_3, language='sql')
st.code(sql_4, language='sql')

st.write("""
3. Análisis temporal:
   - Escribe una consulta que muestre la evolución mensual de la cantidad de transacciones, el total de puntos ganados y la cantidad de eventos de fraude reportados (si existen) en un período determinado (por ejemplo, el último año).
""")

st.code(sql_5, language='sql')

# Desafío 2: Python
st.header("2. Desafío Python")
st.write("""
**Contexto**: Se provee un dataset de transacciones y redenciones en formato CSV. 
El candidato debe realizar un proceso de ETL (Extract, Transform, Load) para explorar, limpiar y analizar estos datos.

**Ejercicios**:
1. Exploración y limpieza de datos:
   - Cargar el dataset y realizar un análisis exploratorio.
   - Detectar valores nulos y outliers, y aplicar técnicas de limpieza.

2. Análisis de fraude:
   - Implementar una función que detecte patrones inusuales en las transacciones (por ejemplo, montos altos o transacciones repetidas).

3. Automatización de reportes:
   - Crear un script que genere un reporte automatizado en formato PDF o HTML con los resultados del análisis.
""")

# Desafío 3: Ingeniería de Datos
st.header("3. Desafío de Ingeniería de Datos")
st.write("""
**Contexto**: Se requiere diseñar un pipeline ETL/ELT que maneje grandes volúmenes de datos en tiempo real, incluyendo transacciones, redenciones y eventos de fraude.

**Ejercicios**:
1. Diseño del pipeline:
   - Ingesta de datos desde APIs, archivos y bases de datos transaccionales.
   - Transformación, validación y enriquecimiento de datos.
   - Almacenamiento en un data warehouse y consumo mediante herramientas de BI.

2. Implementación de un componente ETL:
   - Simular la ingesta de datos y aplicar transformaciones básicas, como la agregación de puntos y la detección de transacciones atípicas.

3. Consideraciones de escalabilidad y seguridad:
   - Explicar cómo abordar aspectos como la escalabilidad, tolerancia a fallos y seguridad en el pipeline.
""")

st.image("diagrama_ELT.png", caption="Diagrama ELT", use_container_width=True)
st.write("""
Se muestra el diagrama del ETL utilizado. Este consta de las siguientes etapas teniendo a Airflow como orquestador
- **Ingesta**: La data se carga a Minio desde csv locales.
- **Almacenamiento**: Se almacenan los archivos en Minio (DataLake) y posteriormente se estructuran en DuckDB (DataWarehouse)
- **Procesamiento**: Se realizan las transformaciones en DuckDB
- **Cosumo**: Se refleja la información y reporteria directamente en Streamlit

Acontinuación se muestra el flujo usado por Airflow para la ejecución del ELT

**Consideraciones Escalabilidad y seguridad**:

**Escalabilidad**
El Proceso debería ser capaz de escalar dependiendo de la cantidad de información, la cual va directamente relacionada con el crecimiento del negocio. Por ello es importante tener una infrastructura robusta y flexible desde un inicio. Lo mejor sería con contar con servicios desplegados en la nube (AWS, GCP, Azure) que permitan una escalabilidad automatica en funcion de las demandas del negocio. Mas información suele traer mas desorden en la organización, por lo que diseñar una arquitectura de datos solidas se combierte un "must", podrían emplearse arquitecturas tipo data_mesh o medallion para categorizar y documentar la data de la compañia.

**Seguridad**
Cualquier negocio que maneja información de clientes esta expuesto a una fuga de información, por ello el sistema debería ser capaz de garantizar que ningún externo a la compañia pueda consumir la información por cuenta propia. Para ello servicios ofrecidos por la mayoría nubes como por ejemplo mediante IAM. De igual forma se debe garantizar que todos los productos/servicios conectados a la información estén también protegidos ante cualquier ataque.

**Tolerancia a Fallos**
Es necesario que el sistema sea capaz de seguir funcionando en caso de fallos. El uso de repliación de datos o almacenamiento distribuido, como Amazon S3 o Google Cloud Storage, garantiza que los datos se conserven incluso si alguna etapa falla. El monitoreo en tiempo real es otra medida clave para la tolerancia a fallos. Herramientas como Prometheus o Grafana podrian detectar y alertar sobre cualquier anomalía o caída en los servicios de procesamiento, permitiendo una respuesta rápida ante posibles problemas.
""")
