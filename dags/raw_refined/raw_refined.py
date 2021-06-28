from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'Airflow',
    'email': ['your.email@domain.com'],
    'start_date': days_ago(1),
    'email_on_failure' : False
}

dag = DAG(
    dag_id = 'raw_refined-dag',
    default_args = default_args,
    catchup=False,
    max_active_runs = 1,
    schedule_interval = '05 19 * * *',
    tags=['raw_refined']
    )

def camada_raw():

    # Packages
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    
    ## Packages for melt function
    from pyspark.sql.functions import array, col, explode, lit
    from pyspark.sql.functions import create_map
    from pyspark.sql import DataFrame
    from typing import Iterable 
    from itertools import chain

    ## Packages for manage column types
    from pyspark.sql.types import DateType, LongType
    from pyspark.sql.functions import udf, to_timestamp
    from datetime import datetime

    ## Package for get year and month from data
    from pyspark.sql.functions import year, month

    # Session
    spark = SparkSession.builder \
                .master('local[*]') \
                .appName('airflow_app') \
                .config('spark.executor.memory', '6g') \
                .config('spark.driver.memory', '6g') \
                .config('spark.driver.maxResultSize', '1048MB') \
                .config('spark.port.maxRetries', '100') \
                .getOrCreate()

    # Import data
    ## case
    df_case = spark.read.options(inferSchema='true', header='true').csv('/home/airflow/datalake/landing/covid19/time_series_covid19_confirmed_global.csv')
    ### Rename df_case - estado, pais, latitude, longitude
    df_case = df_case.withColumnRenamed('Province/State', 'estado')\
                     .withColumnRenamed('Country/Region', 'pais')\
                     .withColumnRenamed('Lat', 'latitude')\
                     .withColumnRenamed('Long', 'longitude')

    ## death
    df_death = spark.read.options(inferSchema='true', header='true').csv('/home/airflow/datalake/landing/covid19/time_series_covid19_deaths_global.csv')
    ### Rename df_death - estado, pais, latitude, longitude
    df_death = df_death.withColumnRenamed('Province/State', 'estado')\
                       .withColumnRenamed('Country/Region', 'pais')\
                       .withColumnRenamed('Lat', 'latitude')\
                       .withColumnRenamed('Long', 'longitude')
    ## recovered
    df_rec = spark.read.options(inferSchema='true', header='true').csv('/home/airflow/datalake/landing/covid19/time_series_covid19_recovered_global.csv')
    ### Rename df_rec - estado, pais, latitude, longitude
    df_rec = df_rec.withColumnRenamed('Province/State', 'estado')\
                   .withColumnRenamed('Country/Region', 'pais')\
                   .withColumnRenamed('Lat', 'latitude')\
                   .withColumnRenamed('Long', 'longitude')

    # melt function (ref: https://stackoverflow.com/questions/41670103/how-to-melt-spark-dataframe)

    ## Function
    def melt(
            df: DataFrame, 
            id_vars: Iterable[str], value_vars: Iterable[str], 
            var_name: str="variable", value_name: str="value") -> DataFrame:
        """Convert :class:`DataFrame` from wide to long format."""

        # Create map<key: value>
        _vars_and_vals = create_map(
            list(chain.from_iterable([
                [lit(c), col(c)] for c in value_vars]
            ))
        )

        _tmp = df.select(*id_vars, explode(_vars_and_vals)) \
            .withColumnRenamed('key', var_name) \
            .withColumnRenamed('value', value_name)

        return _tmp

    # Transform data
    # Using melt function -----------------------------------------------------

    ## Get data colmuns names
    data_cols = df_case.columns[4:]
    ## Apply melt to case
    melt_case = melt(df_case, id_vars=['pais', 'estado', 'latitude', 'longitude'], value_vars=data_cols)
    ## Rename variable to data
    melt_case = melt_case.withColumnRenamed('variable', 'data')\
                         .withColumnRenamed('value', 'quantidade_confirmados')

    ## Get data colmuns names
    data_cols = df_death.columns[4:]
    ## Apply melt to death
    melt_death = melt(df_death, id_vars=['pais', 'estado', 'latitude', 'longitude'], value_vars=data_cols)
    ## Rename variable to data
    melt_death = melt_death.withColumnRenamed('variable', 'data')\
                           .withColumnRenamed('value', 'quantidade_mortes')

    ## Get data colmuns names
    data_cols = df_rec.columns[4:]
    ## Apply melt to rec
    melt_rec = melt(df_rec, id_vars=['pais', 'estado', 'latitude', 'longitude'], value_vars=data_cols)
    ## Rename variable to data
    melt_rec = melt_rec.withColumnRenamed('variable', 'data')\
                       .withColumnRenamed('value', 'quantidade_recuperados')

    # Join dataframes -----------------------------------------------------

    ## Handling null values into estado feature & Drop lat and long features for death and rec
    mc = melt_case.na.fill({'estado': 'unknown'})
    md = melt_death.drop('latitude', 'longitude').na.fill({'estado': 'unknown'})
    mr = melt_rec.drop('latitude', 'longitude').na.fill({'estado': 'unknown'})

    ## Inner join - case + death
    aux = mc.join(md, ['pais', 'estado', 'data'], 'inner')

    ## Left join - (case + death) + rec
    raw = aux.join(mr, ['pais', 'estado', 'data'], 'left')

    # Manage columns types -----------------------------------------------------

    ## String to date function
    func =  udf (lambda x: datetime.strptime(x, '%m/%d/%y'), DateType())

    ## convert from date to timestamp
    raw = raw.withColumn('data', func(col('data'))).withColumn('data', to_timestamp(col('data')))

    ## convert from int to bigint (long)
    raw = raw.withColumn('quantidade_confirmados',col('quantidade_confirmados').cast(LongType())) \
             .withColumn('quantidade_mortes',col('quantidade_mortes').cast(LongType()))\
             .withColumn('quantidade_recuperados',col('quantidade_recuperados').cast(LongType()))

    # Show final data -------------------------------------------------
    print('-------------------------- RAW DATAFRAME --------------------------')
    print((raw.count(), len(raw.columns)))

    # Save in parquet partition by Ano and Mês

    ## Ano feature
    raw = raw.withColumn('Ano', year(raw.data))
    ## Mes feature
    raw = raw.withColumn('Mes', month(raw.data))
    ## Save in parquet format by Ano and Mes partition
    raw.write.partitionBy('Ano','Mes').mode('overwrite').parquet('./datalake/raw')

    return ">>>>> CAMADA RAW <<<<<"

def camada_refined():
    # Packages
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    ## Packages for manage column types
    from pyspark.sql.types import LongType
    from pyspark.sql.functions import col

    ## Package for get year and month from data
    from pyspark.sql.functions import year

    ## Package for rolling window
    from pyspark.sql.window import Window
    from pyspark.sql.functions import avg

    # Session
    spark = SparkSession.builder \
                .master('local[*]') \
                .appName('airflow_app') \
                .config('spark.executor.memory', '6g') \
                .config('spark.driver.memory', '6g') \
                .config('spark.driver.maxResultSize', '1048MB') \
                .config('spark.port.maxRetries', '100') \
                .getOrCreate()

    # Import raw data
    raw = spark.read.parquet("./datalake/raw")
    raw = raw.repartition(1)

    # Tranform data

    # Preprocess -------------------------------------------------
    ## Feature selection & Summarize by pais
    refined_sum = raw.drop('estado', 'latitude', 'longitude', 'Ano', 'Mes')\
                     .groupBy('pais', 'data')\
                     .sum()

    refined_sum = refined_sum.withColumnRenamed('sum(quantidade_confirmados)', 'quantidade_confirmados')\
                             .withColumnRenamed('sum(quantidade_mortes)', 'quantidade_mortes')\
                             .withColumnRenamed('sum(quantidade_recuperados)', 'quantidade_recuperados')

    refined_sum = refined_sum.sort('pais', 'data')
    # Window rolling -------------------------------------------------
    ## (ref: https://stackoverflow.com/questions/33207164/spark-window-functions-rangebetween-dates)

    ## Hive timestamp is interpreted as UNIX timestamp in seconds*
    days = lambda i: i * 86400 

    ## Window instance
    w = (Window()
         .partitionBy(F.col("pais"))
         .orderBy(F.col("data").cast('long'))
         .rangeBetween(-days(7), 0))

    ## Create new columns with rolling windows
    refined = refined_sum.withColumn('media_movel_confirmados', F.avg('quantidade_confirmados').over(w))\
                         .withColumn('media_movel_mortes', F.avg('quantidade_mortes').over(w))\
                         .withColumn('media_movel_recuperados', F.avg('quantidade_recuperados').over(w))

    ## Drop columns and Sort dataframe
    refined = refined.drop('quantidade_confirmados', 'quantidade_mortes', 'quantidade_recuperados')\
                     .sort('pais', 'data')

    ## convert from double to bigint (long)
    refined = refined.withColumn('media_movel_confirmados',col('media_movel_confirmados').cast(LongType())) \
                     .withColumn('media_movel_mortes',col('media_movel_mortes').cast(LongType()))\
                     .withColumn('media_movel_recuperados',col('media_movel_recuperados').cast(LongType()))

    # Show final data -------------------------------------------------
    print('-------------------------- REFINED DATAFRAME --------------------------')
    print((refined.count(), len(refined.columns)))

    # Save in parquet partition by Ano
    ## Avoid mutiple parquet files - Define number of partitions
    # HELP -> df.rdd.getNumPartitions()
    refined = refined.repartition(1)
    ## Ano feature
    refined = refined.withColumn('Ano', year(refined.data))
    ## Save in parquet format by Ano partition
    refined.write.partitionBy('Ano').mode('overwrite').parquet('./datalake/refined')

    return ">>>>> CAMADA REFINED <<<<<"

camada_raw = PythonOperator(
    task_id = 'camada_raw',
    #python_callable param points to the function you want to run 
    python_callable = camada_raw,
    #dag param points to the DAG that this task is a part of
    dag = dag)

camada_refined = PythonOperator(
    task_id = 'camada_refined',
    python_callable = camada_refined,
    dag = dag)

#Assign the order of the tasks in our DAG
camada_raw >> camada_refined
