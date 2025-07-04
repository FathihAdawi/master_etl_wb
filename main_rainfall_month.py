# from pyspark import SparkContext, SparkConf, SQLContext
# from pyspark.sql import SparkSession, SQLContext
import pyspark.sql.functions as psql
from configure import db_init
import pyodbc
import psycopg2
from psycopg2 import extras
import pandas as pd
import polars as pl
import numpy as np
from datetime import datetime
import time
from packages import \
    que_fun_l2_aws_ars_unitinterval_daily, \
    que_get_rf_cln, \
    que_get_rf_raw, \
    que_insert_aws_ars
# import cProfile

print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
start_time = time.time()

remapping_cols_curah_hujan = [
    "Tahun","Bulan","KodeKebun","TipeKebun","RainyDays",
    "RainFallValueAVG","WaterReserve","WaterEvapotranspiration","WaterBalance",
    "WaterReserveAfterEvapor", "WaterDeficit"
]

def init_staging():
    # [VARIABLES DB]
    path_db = 'D:\All_app_code\Testing Apps\master-etl_pyspark\db_dwh.ini'
    section = 'staging_db'
    staging_info = db_init(path_db,section)

    # [CONNECTION STRING]
    staging_string = "driver={"+staging_info.get('driver')+"}; "\
                        "server="+staging_info.get('server')+"; " \
                        "database="+staging_info.get('database')+"; " \
                        "UID="+staging_info.get('user')+"; " \
                        "PWD="+staging_info.get('password')+";"
    
    # [CONNECTION & CURSOR]
    conx_app_staging = pyodbc.connect(staging_string)
    curs_app_staging = conx_app_staging.cursor()

    return conx_app_staging, curs_app_staging, staging_info

def init_dwh():
    # [VARIABLES DB]
    path_db = 'D:\All_app_code\Testing Apps\master_etl_wb\db_dwh.ini'
    section = 'dwh'
    db_info = db_init(path_db, section)

    # [CONNECTION & CURSOR]
    db_connection = psycopg2.connect(**db_info)
    db_cursor = db_connection.cursor()
    
    return db_cursor, db_connection

def preparation_delete_data_rainfall(df,curs,que):
    curs.execute(que)
    for x,y,z in (df[['Tahun','Bulan','KodeKebun']].drop_duplicates()).values:
        que_del_rainfall = 'DELETE FROM "L2_AWS_ARS_PLANTATION"' \
                            'where "Tahun" = '+'\''+str(x)+'\''+ \
                            ' and "Bulan" = '+'\''+str(y)+'\''+ \
                            ' and "KodeKebun" = '+'\''+str(z)+'\' ;'
        curs.execute(que_del_rainfall)
        print(
            'Delete Rainfall: \n' \
            '\tTahun: '+str(x)+'\n' \
            '\tBulan: '+str(y)+'\n' \
            '\tKodeKebun: '+str(z)+'\n'
        )

def preparation_insert_data_rainfall(df,curs,conx,que):
    for i in range(1,len(df)):
        df.loc[i, 'WaterBalance'] = \
            (df.loc[i, 'RainFallValueAVG'] + \
                df.loc[i-1,'WaterReserve']) - \
            df.loc[i,'WaterEvapotranspiration']

        if df.loc[i, 'WaterBalance'] >= 200:
            df.loc[i, 'WaterReserveAfterEvapor'] = 200
        elif df.loc[i, 'WaterBalance'] > 0 \
            and df.loc[i, 'WaterBalance'] < 200:
            df.loc[i, 'WaterReserveAfterEvapor'] = \
            df.loc[i, 'WaterBalance']
        elif df.loc[i, 'WaterBalance'] <= 0:
            df.loc[i, 'WaterReserveAfterEvapor'] = 0

        df.loc[i,'WaterReserve'] = \
            df.loc[i-1,'WaterReserveAfterEvapor']
    for i in range(0,len(df)):
        if df.loc[i,'WaterBalance'] < 0:
            df.loc[i, 'WaterDeficit'] = \
            df.loc[i, 'WaterBalance'] + \
            (df.loc[i, 'WaterBalance'] * -2)
        elif df.loc[i, 'WaterBalance'] >= 0:
            df.loc[i, 'WaterDeficit'] = 0
    df = pl.from_pandas(df)
    tpl_cln_curah_hujan = tuple(map(tuple, df.to_numpy()))
    print(df)
    extras.execute_values(
        curs, 
        que,
        tpl_cln_curah_hujan
    )    
    print('Insert Success!')
    conx.commit()

def calc_rainfall_data():

    curs_dwh, conx_dwh = init_dwh()
    
    curs_dwh.execute(que_get_rf_raw)
    tbl_rainfall_raw_dwh = pd.DataFrame(curs_dwh.fetchall())
    # print(tbl_rainfall_raw_dwh)

    curs_dwh.execute(que_get_rf_cln)
    tbl_rainfall_cln_dwh = pd.DataFrame(curs_dwh.fetchall())
    # print(tbl_rainfall_cln_dwh)

    # print(tbl_rainfall_cln_dwh[1].unique())
    # print(tbl_rainfall_cln_dwh[0].unique())
    
    if (datetime.today().year in tbl_rainfall_cln_dwh[0].values) & \
        (datetime.today().month-1 in tbl_rainfall_cln_dwh[1].values):

        tbl_rainfall_cln_dwh = tbl_rainfall_cln_dwh.loc[
            (tbl_rainfall_cln_dwh[1] == datetime.today().month-1) & 
            (tbl_rainfall_cln_dwh[0] == datetime.today().year)
        ]
        tbl_concat_rainfall = pd.concat(
            [tbl_rainfall_raw_dwh,tbl_rainfall_cln_dwh]
        )
        tbl_concat_rainfall.sort_values(
            by=[2, 1], 
            inplace=True
        )
        tbl_concat_rainfall.reset_index(
            drop=True, 
            inplace=True
        )
        tbl_concat_rainfall.columns = remapping_cols_curah_hujan

        preparation_delete_data_rainfall(
            tbl_concat_rainfall,
            curs_dwh,
            que_fun_l2_aws_ars_unitinterval_daily
        )
        preparation_insert_data_rainfall(
            tbl_concat_rainfall,
            curs_dwh,
            conx_dwh,
            que_insert_aws_ars
        )
        
    else:
        tbl_rainfall_cln_dwh = tbl_rainfall_cln_dwh.loc[
            (tbl_rainfall_cln_dwh[1] == 12) &
            (tbl_rainfall_cln_dwh[0] == datetime.toda().year-1)
        ]
        tbl_concat_rainfall = pd.concat(
            [tbl_rainfall_raw_dwh,tbl_rainfall_cln_dwh]
        )
        tbl_concat_rainfall.sort_values(
            by=[2, 1], 
            inplace=True
        )
        tbl_concat_rainfall.reset_index(
            drop=True, 
            inplace=True
        )
        tbl_concat_rainfall.columns = remapping_cols_curah_hujan
        
        preparation_delete_data_rainfall(
            tbl_concat_rainfall,
            curs_dwh,
            que_fun_l2_aws_ars_unitinterval_daily
        )
        preparation_insert_data_rainfall(
            tbl_concat_rainfall,
            curs_dwh,
            conx_dwh,
            que_insert_aws_ars
        )

    print("--- %s seconds ---" % (time.time() - start_time))
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


calc_rainfall_data()