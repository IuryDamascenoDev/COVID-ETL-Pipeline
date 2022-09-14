from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Covid ETL").getOrCreate()

covid_data = spark.read.csv('gs://bucket-name/data/*.csv',
                            sep=';', header='true', inferSchema='true')


# #### Processing data ####


# Removing hour from "data" column

covid_data = covid_data.withColumn("data", split(col("data"), " ").getItem(0))

# Preparing total cases data to transformation
covid_data = covid_data.withColumn("casosAcumulado",
                                   covid_data['casosAcumulado'].cast(FloatType()))

# Data is updated to December 2021

data = "2021-12-31"  # date = 2021 December 31th
regiao = "Brasil"  # region columns with "Brasil" value

brasil_covid = covid_data.where((covid_data.regiao == regiao) &
                                (covid_data.data == data))


# #### Creating views ####


# View for the recovered and under monitoring, respectively

recuperados_brasil = brasil_covid\
    .select(brasil_covid["Recuperadosnovos"].alias("Casos_Recuperados"),
            brasil_covid["emAcompanhamentoNovos"].alias("Em_Acompanhamento"))


# View of accumulated cases, new cases and incidence per 100 thousand people, respectively

casos_brasil = brasil_covid\
    .select(brasil_covid["casosAcumulado"].alias("Casos_Acumulados"),
            brasil_covid["casosNovos"].alias("Casos_Novos"),
            (round(brasil_covid["casosAcumulado"] /
                   brasil_covid["populacaoTCU2019"]*100000, 1))
            .alias("Incidencia"))


# View for deaths, fatality and mortality rate per 100 thousand people, respectively

obitos_brasil = brasil_covid\
    .select(brasil_covid["obitosAcumulado"].alias("Obitos_acumulados"),
            brasil_covid["obitosNovos"].alias("Obitos_novos"),
            (round(brasil_covid["obitosAcumulado"] /
                   brasil_covid["casosAcumulado"]*100, 1)).alias("Letalidade"),
            (round(brasil_covid["obitosAcumulado"] /
                   brasil_covid["populacaoTCU2019"]*100000, 1)).alias("Mortalidade"))


# #### Uploading views to BigQuery dataset tables ####


recuperados_brasil.write \
  .format("bigquery") \
  .option("writeMethod", "direct",) \
  .mode("overwrite") \
  .save("covid_views.recuperados_brasil")

casos_brasil.write \
  .format("bigquery") \
  .option("writeMethod", "direct") \
  .mode("overwrite") \
  .save("covid_views.casos_brasil")

obitos_brasil.write \
  .format("bigquery") \
  .option("writeMethod", "direct") \
  .mode("overwrite") \
  .save("covid_views.obitos_brasil")
