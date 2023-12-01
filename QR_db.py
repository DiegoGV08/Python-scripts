# -*- coding: utf-8 -*-

import os
import sys
from time import gmtime, strftime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pymssql
from datetime import date, timedelta
from py4j.java_gateway import java_import

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

"""## Setting up a Spark Session"""

spark = SparkSession.builder.config("spark.driver.memory","6g").config("spark.executor.memory","6g").appName('QR').getOrCreate()

"""## Validation of dates for automatic executions"""

start_day_of_this_month = date.today().replace(day=1)
year = start_day_of_this_month.strftime("%Y")
month = start_day_of_this_month.strftime("%m")

"""## Cleaning folders and defining the save path (output directory)"""

java_import(spark._jvm, 'org.apache.hadoop.fs.FileSystem')
java_import(spark._jvm, 'org.apache.hadoop.fs.Path')


deliveryAddress = r'C:/Users/dg/Documents/scripts_server/QR/'+f'{year}'/+f'{month}'
fs = spark._jvm.FileSystem.get(spark._jsc.hadoopConfiguration())
folder = spark._jvm.Path(deliveryAddress)

fs.delete(folder, True)

"""## Connecting to SQL and extracting information to our local machine"""

# Reading login credentials
with open(r'C:\Users\dg\Documents\credentials.txt') as f:
    credentials = f.readlines()

# Connection details - dbd
server = r'serverentities:1041'
db = r'dbd'
user = r'%s' %credentials[0].strip()
password = credentials[1]
try:
    connection_datos = pymssql.connect(server,user,password,db)
    print('Successful connection, we are in dbd - database')
except ValueError as error:
    print(f'Could not connect to the database. The error is as follows: {error}')

# Query for legal entities and businesses - B
cursor = connection_datos.cursor()
cursor.execute("SELECT terminal.Terminal,\
                bussines.Retailer,\
                bussines.Name,\
                fiscal.Name,\
                afiliation.Code,\
                institution.Name\
                FROM [dbd].[Entities].[dbd_Terminals] as terminal\
                LEFT JOIN dbd.Entities.BussinessData as bussines on terminal.IdBussiness_Fk = bussines.Id\
                LEFT JOIN dbd.Entities.AfiliationData as afiliation on bussines.IdAfiliacion_Fk = afiliation.Id\
                LEFT JOIN dbd.Entities.FiscalData as fiscal on afiliation.IdIva_Fk = fiscal.Id\
                LEFT JOIN dbd.Institution.dbd_Institution as institution on afiliation.IdInstitution_Fk = institution.Id\
                WHERE afiliation.IdInstitution_Fk = 4")
columns = ['TERMINAL','RETAILER_C','COM_NAME','LEGAL_NAME','CODE_A','INSTITUTION/PAYFAC']
result = cursor.fetchall()
dfB = spark.createDataFrame(result,columns)

print(f'We have the DataFrame dfB in Spark ready to be consumed locally: {strftime("%a, %d %b %Y %H:%M:%S")}')

##################################################

# Query for legal entities and businesses - W
cursor = connection_datos.cursor()
cursor.execute("SELECT terminal.Terminal,\
                bussines.Retailer,\
                bussines.Name,\
                fiscal.Name,\
                afiliation.Code,\
                institution.Name\
                FROM [dbd].[Entities].[dbd_Terminals] as terminal\
                LEFT JOIN dbd.Entities.BussinessData as bussines on terminal.IdBussiness_Fk = bussines.Id\
                LEFT JOIN dbd.Entities.AfiliationData as afiliation on bussines.IdAfiliacion_Fk = afiliation.Id\
                LEFT JOIN dbd.Entities.FiscalData as fiscal on afiliation.IdIva_Fk = fiscal.Id\
                LEFT JOIN dbd.Institution.dbd_Institution as institution on afiliation.IdInstitution_Fk = institution.Id\
                WHERE afiliation.IdInstitution_Fk = 5")
columns = ['TERMINAL','RETAILER_C','COM_NAME','LEGAL_NAME','CODE_A','INSTITUTION/PAYFAC']
result = cursor.fetchall()
dfW = spark.createDataFrame(result,columns)

print(f'We have the DataFrame dfW in Spark ready to be consumed locally: {strftime("%a, %d %b %Y %H:%M:%S")}')

##################################################

# Query for legal entities and businesses - T
cursor = connection_datos.cursor()
cursor.execute("SELECT terminal.Terminal,\
                bussines.Retailer,\
                bussines.Name,\
                fiscal.Name,\
                afiliation.Code,\
                institution.Name\
                FROM [dbd].[Entities].[dbd_Terminals] as terminal\
                LEFT JOIN dbd.Entities.BussinessData as bussines on terminal.IdBussiness_Fk = bussines.Id\
                LEFT JOIN dbd.Entities.AfiliationData as afiliation on bussines.IdAfiliacion_Fk = afiliation.Id\
                LEFT JOIN dbd.Entities.FiscalData as fiscal on afiliation.IdIva_Fk = fiscal.Id\
                LEFT JOIN dbd.Institution.dbd_Institution as institution on afiliation.IdInstitution_Fk = institution.Id\
                WHERE afiliation.IdInstitution_Fk = 1")
columns = ['TERMINAL','RETAILER_C','COM_NAME','LEGAL_NAME','CODE_A','INSTITUTION/PAYFAC']
result = cursor.fetchall()
dfT = spark.createDataFrame(result,columns)

print(f'We have the DataFrame dfT in Spark ready to be consumed locally: {strftime("%a, %d %b %Y %H:%M:%S")}')

##################################################

# Query for legal entities and businesses - N
cursor = connection_datos.cursor()
cursor.execute("SELECT terminal.Terminal,\
                bussines.Retailer,\
                bussines.Name,\
                fiscal.Name,\
                afiliation.Code,\
                institution.Name\
                FROM [dbd].[Entities].[dbd_Terminals] as terminal\
                LEFT JOIN dbd.Entities.BussinessData as bussines on terminal.IdBussiness_Fk = bussines.Id\
                LEFT JOIN dbd.Entities.AfiliationData as afiliation on bussines.IdAfiliacion_Fk = afiliation.Id\
                LEFT JOIN dbd.Entities.FiscalData as fiscal on afiliation.IdIva_Fk = fiscal.Id\
                LEFT JOIN dbd.Institution.dbd_Institution as institution on afiliation.IdInstitution_Fk = institution.Id\
                WHERE afiliation.IdInstitution_Fk = 2")
columns = ['TERMINAL','RETAILER_C','COM_NAME','LEGAL_NAME','CODE_A','INSTITUTION/PAYFAC']
result = cursor.fetchall()
dfN = spark.createDataFrame(result,columns)

print(f'We have the DataFrame dfN in Spark ready to be consumed locally: {strftime("%a, %d %b %Y %H:%M:%S")}')

# Connection details - PE
server = r'servertrxn:1001'
db = r'PE'
user = r'%s' %credentials[0].strip()
password = credentials[1]
try:
    connection_test = pymssql.connect(server,user,password,db)
    print('Successful connection, we are in dbd - database')
except ValueError as error:
    print(f'Could not connect to the database. The error is as follows: {error}')

####################################################### - PE

cursor = connection_test.cursor()
cursor.execute(f"SELECT cast(trxn.FechaCorte as date),\
                trxn.term_id,\
                trxn.tran_time\
                trxn.retail_id\
                trxn.fiid_card\
                trxn.pan\
                trxn.Mount\
                case when trxn.Tech LIKE '%SQR%' then 'STATIC QR' else (case when trxn.Tech LIKE '%DQR%' then 'DINAMIC QR' else '0' end) end,\
                peAfiliation.B_NAME\
                trxn.resp_cde\
                case when trxn.Approval=1 then 'APPROVAL' else 'REJECTED' end AS APROBACION\
                FROM [PE].[db].[PE_Detail] as trxn\
                LEFT JOIN [PE].[db].[PE_Af] AS peAfiliation on peAfiliation.Id = trxn.AfiliationID\
                WHERE (CharIndex('QR', Fiid_Card) > 0 Or  CharIndex('=QR', Tech) > 0 )\
                AND TestTrxn = 0\
                AND ReversedTrxn = 0\
                AND Date >= '{start_day_of_this_month}'")
columns = ['DATE','TERMINAL_TRXN','TRXN_TIME','RETAILER_PE','FIID_CARD','PAN','MOUNT_TRXN','TYPE_TRXN','B_NAME','RESP_CDE','APPROVAL']
result = cursor.fetchall()
dfPE = spark.createDataFrame(result,columns)
print(f'We have the DataFrame dfPE in Spark ready to be consumed locally: {strftime("%a, %d %b %Y %H:%M:%S")}')


# Data extraction completed
print('Proceeding to close connections with the databases')
connection_datos.close()
connection_test.close()
print('Connections to the database have been closed')

"""## We will merge the dataframes to have a single dataframe """

df_entities = dfB.union(dfW)
df_entities = df_entities.union(dfT)
df_entities = df_entities.union(dfN)

"""## Merging dataframes and selecting data ##"""

df_Output = dfPE.join(df_entities, dfPE.terminal_transac == df_entities.terminal,'inner')
df_Output = df_Output.select('DATE','TERMINAL_TRXN','TRXN_TIME','RETAILER_PE','CODE_A','COM_NAME','LEGAL_NAME','INSTITUTION/PAYFAC','FIID_CARD','PAN','MOUNT_TRXN','TYPE_TRXN','B_NAME','RESP_CDE','APPROVAL')

"""## Data cleaning """

df_Output = df_Output.withColumn('COM_NAME', regexp_replace(df_Output['COM_NAME'],r'\\\\|[^a-zA-Z0-9\s]', '')) #Removing line breaks from the COM_NAME column
df_Output = df_Output.withColumn('COM_NAME', regexp_replace(df_Output['COM_NAME'],'<.*?>', ''))   #Using regular expressions to remove HTML tags from the COM_NAME column
df_Output = df_Output.withColumn('COM_NAME', regexp_replace(df_Output['COM_NAME'],'[.,;]', ''))   #Removing ".,;" from the COM_NAME column
df_Output = df_Output.withColumn('COM_NAME', regexp_replace(df_Output['COM_NAME'],'&nbsp', ''))   #Removing '&nbsp' from the COM_NAME column

df_Output = df_Output.withColumn('LEGAL_NAME', regexp_replace(df_Output['LEGAL_NAME'],r'\\\\|[^a-zA-Z0-9\s]', '')) #Removing line breaks from the LEGAL_NAME column
df_Output = df_Output.withColumn('LEGAL_NAME', regexp_replace(df_Output['LEGAL_NAME'],'<.*?>', ''))   #Using regular expressions to remove HTML tags from the LEGAL_NAME column
df_Output = df_Output.withColumn('LEGAL_NAME', regexp_replace(df_Output['LEGAL_NAME'],'[.,;]', ''))   #Removing ".,;" from the LEGAL_NAME column
df_Output = df_Output.withColumn('LEGAL_NAME', regexp_replace(df_Output['LEGAL_NAME'],'&nbsp', ''))   #Removing '&nbsp' from the LEGAL_NAME column

"""## Creating the document for extraction """

# Output on a local repository.
print(f'We have finished all transformations, and we will start writing the data to C: {strftime("%a, %d %b %Y %H:%M:%S")}')
df_Output.coalesce(1).write.format('csv').mode("overwrite").save(f'{deliveryAddress}',header = 'true')
print(f'We have finished writing; the file is now in the folder: {strftime("%a, %d %b %Y %H:%M:%S")}')