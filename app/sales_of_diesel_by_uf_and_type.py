from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from datetime import datetime
from functools import reduce
import pandas as pd


class SalesOfDiesel:
    def __init__(self):
        self.spark = SparkSession.builder \
                .appName("SalesOfDiesel") \
                .config("spark.master", "local") \
                .getOrCreate()

        self.sheet_file = '/usr/local/spark/resources/data/vendas-combustiveis-m3.xls'

        self.sheets = ['DPCache_m3', 'DPCache_m3_2', 'DPCache_m3_3']

        self.months = [('Jan','01'), ('Fev', '02'), ('Mar', '03'), ('Abr', '04'), ('Mai', '05'), ('Jun', '06'),
             ('Jul', '07'), ('Ago', '08'), ('Set', '09'), ('Out', '10'), ('Nov', '11'), ('Dez', '12')]

        self.path_export = '/usr/local/spark/resources/data/sales_of_diesel_by_uf_and_type'

    def excel_to_pandas_df(self):
        fuel_m3 = pd.concat([pd.read_excel(self.sheet_file, sheet_name=sheet) for sheet in self.sheets])
        return fuel_m3


    def process_spark(self):
        dataframe = self.excel_to_pandas_df()
        fuelDF = self.spark.createDataFrame(dataframe)
        fuelDF.registerTempTable('fuelView')
        fuel_dfs = dict()
        query = '''
                select 
                    cast(ANO as int) as year,
                    {} as volume,
                    cast(ESTADO as string) as uf,
                    cast(`COMBUSTÍVEL` as string) as product
                from fuelView
                where `COMBUSTÍVEL` like '%DIESEL%'
                group by ANO, {},`COMBUSTÍVEL`, ESTADO;
        '''
        for month in self.months:
            month_num = month[1]
            fuel_dfs[month_num] = self.spark.sql(query.format(month[0], month[0]))
            fuel_dfs[month_num] = fuel_dfs[month_num].withColumn('month', lit(month_num))
            fuel_dfs[month_num] = fuel_dfs[month_num].withColumn('unit', regexp_extract(col('product'),r'(?<= \().\d', 0))
            fuel_dfs[month_num] = fuel_dfs[month_num].withColumn('product', regexp_extract(col('product'),r'^[^\(]+', 0))
        return fuel_dfs

    def generate_output(self, df):
        fuelDieselDF = reduce(DataFrame.unionAll, [x for x in df.values()])
        fuelDieselDF.write.partitionBy('uf').parquet(self.path_export )


client = SalesOfDiesel()
fullDF = client.process_spark()
client.generate_output(fullDF)