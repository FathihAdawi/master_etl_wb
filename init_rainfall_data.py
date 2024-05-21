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
# from datetime import date
import time
# import cProfile

start_time = time.time()

remapping_cols_curah_hujan = [
    "RN","Tahun","Bulan","KodeKebun","TipeKebun","RainyDays",
    "RainFallValueAVG","WaterReserve","WaterEvapotranspiration","WaterBalance",
    "WaterReserveAfterEvapor"
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
    section = 'tpadw_db'
    db_info = db_init(path_db, section)

    # [CONNECTION & CURSOR]
    db_connection = psycopg2.connect(**db_info)
    db_cursor = db_connection.cursor()
    
    return db_cursor, db_connection

def calc_rainfall_data():

    curs_dwh, conx_dwh = init_dwh()

    que_fun_l2_aws_ars_unitinterval_daily = 'CALL public.synch_l2_aws_ars_unitinterval_daily() '

    que_del_curah_hujan = 'DELETE FROM "L2_AWS_ARS_PLANTATION";'

    que_curah_hujan =    'SELECT ' \
                            'row_number() over (partition by a_1."Estate" order by a_1."Estate") as "RN", ' \
                            'a_1."Tahun", ' \
                            'a_1."Bulan", ' \
                            'a_1."Estate" AS "KodeKebun", ' \
                            'b_1."Unit Type" AS "TipeKebun", ' \
                            'a_1."RainyDays", ' \
                            'a_1."AVG_CurahHujan", ' \
                            'a_1."WaterReserve", ' \
                            'a_1."WaterEvapor", ' \
                            'a_1."WaterBalance", ' \
                            '(case ' \
                                'when "WaterBalance" <= 0 then 0  ' \
                                'when "WaterBalance" >= 200 then 200 ' \
                                'when "WaterBalance" < 200 then "WaterBalance" ' \
                            'end) as "WaterReserverAfterEvapor" ' \
                        'FROM (' \
                            'SELECT ' \
                                'a_2."Tahun", ' \
                                'a_2."Bulan", ' \
                                'a_2."Estate", ' \
                                '(case ' \
                                    'when a_2."Bulan" = 11 and a_2."Tahun" = 2022 then (200 + sum(coalesce(a_2."RainFallValue",0))/count(distinct a_2."Lokasi"))-(case when count(distinct date_part(\'day\',"Date")) <= 10 then 150 else 120 end) ' \
                                'end) as "WaterBalance", ' \
                                '(case when a_2."Bulan" = 11 and a_2."Tahun" = 2022 then 200 else 0 end) as "WaterReserve", ' \
                                '(case when count(date_part(\'day\',"Date")) <= 10 then 150 else 120 end) as "WaterEvapor", ' \
                                'count(distinct date_part(\'day\', "Date")) as "RainyDays", ' \
                                'sum(coalesce(a_2."RainFallValue", 0)) AS "RainFallValue", ' \
                                'sum(coalesce(a_2."RainFallValue",0))/count(distinct a_2."Lokasi") as "AVG_CurahHujan" ' \
                            'FROM "L2_AWS_ARS_IntervalDaily" a_2 ' \
                            'full join "L1_Fact_AWS_ARS" b_2 on  ' \
                                'b_2."ESTATE" = a_2."Estate" ' \
                                'and b_2."LOKASI" = a_2."Lokasi" ' \
                            'where a_2."RainFallValue" > 0 ' \
                            'GROUP BY ' \
                                'a_2."Tahun", ' \
                                'a_2."Bulan", ' \
                                'a_2."Estate" ' \
                            'order by "Estate" ' \
                        ') a_1 ' \
                        'JOIN "L1_Dim_UnitType" b_1 ON b_1."Unit" = a_1."Estate" '
    
    que_insert_aws_ars ='INSERT INTO "L2_AWS_ARS_PLANTATION" ' \
                        '("Tahun","Bulan","KodeKebun","TipeKebun","RainyDays", ' \
                        '"RainFallValueAVG","WaterReserve","WaterEvapotranspiration", ' \
                        '"WaterBalance","WaterReserveAfterEvapor","WaterDeficit")' \
                        'VALUES %s'
    
    curs_dwh.execute(que_curah_hujan)
    tbl_dwh_curah_hujan = pd.DataFrame(curs_dwh.fetchall())
    tbl_dwh_curah_hujan.columns = remapping_cols_curah_hujan
    tbl_dwh_curah_hujan.loc[1,'WaterReserve'] = tbl_dwh_curah_hujan.loc[0,'WaterReserveAfterEvapor']
    for i in range(1,len(tbl_dwh_curah_hujan)):
        # tbl_dwh_curah_hujan.loc[i, 'WaterReserve'] = tbl_dwh_curah_hujan.loc[i-1,'WaterReserve']
        tbl_dwh_curah_hujan.loc[i, 'WaterBalance'] = (tbl_dwh_curah_hujan.loc[i, 'RainFallValueAVG'] + tbl_dwh_curah_hujan.loc[i-1,'WaterReserve']) - tbl_dwh_curah_hujan.loc[i,'WaterEvapotranspiration']

        if tbl_dwh_curah_hujan.loc[i, 'WaterBalance'] >= 200:
            tbl_dwh_curah_hujan.loc[i, 'WaterReserveAfterEvapor'] = 200
        elif tbl_dwh_curah_hujan.loc[i, 'WaterBalance'] > 0 and tbl_dwh_curah_hujan.loc[i, 'WaterBalance'] < 200:
            tbl_dwh_curah_hujan.loc[i, 'WaterReserveAfterEvapor'] = tbl_dwh_curah_hujan.loc[i, 'WaterBalance']
        elif tbl_dwh_curah_hujan.loc[i, 'WaterBalance'] <= 0:
            tbl_dwh_curah_hujan.loc[i, 'WaterReserveAfterEvapor'] = 0

        # tbl_dwh_curah_hujan.loc[i,'WaterReserve'] = tbl_dwh_curah_hujan['WaterReserveAfterEvapor'].shift(1,fill_value=tbl_dwh_curah_hujan.loc[i, 'WaterReserveAfterEvapor'])
        tbl_dwh_curah_hujan.loc[i,'WaterReserve'] = tbl_dwh_curah_hujan.loc[i-1,'WaterReserveAfterEvapor']

    for i in range(0,len(tbl_dwh_curah_hujan)):
        if tbl_dwh_curah_hujan.loc[i,'WaterBalance'] < 0:
            tbl_dwh_curah_hujan.loc[i, 'WaterDeficit'] = tbl_dwh_curah_hujan.loc[i, 'WaterBalance']+(tbl_dwh_curah_hujan.loc[i, 'WaterBalance'] * -2)
        elif tbl_dwh_curah_hujan.loc[i, 'WaterBalance'] >= 0:
            tbl_dwh_curah_hujan.loc[i, 'WaterDeficit'] = 0
    
    tbl_dwh_curah_hujan.drop(['RN'],axis=1,inplace=True)
    tbl_dwh_curah_hujan.reset_index(drop=True, inplace=True)
    tbl_dwh_curah_hujan = pl.from_pandas(tbl_dwh_curah_hujan)
    print(tbl_dwh_curah_hujan)
    # tbl_dwh_curah_hujan.write_excel(autofit=True)
    # tbl_dwh_curah_hujan.to_excel('Data Curah Huja/n Calculation.xlsx')
    # print(tbl_dwh_curah_hujan.dtypes)

    tpl_cln_curah_hujan = tuple(map(tuple, tbl_dwh_curah_hujan.to_numpy()))

    curs_dwh.execute(que_fun_l2_aws_ars_unitinterval_daily)

    curs_dwh.execute(que_del_curah_hujan)
    # curs_dwh.executemany(
    #     que_insert_aws_ars,
    #     tpl_cln_curah_hujan
    # )
    extras.execute_values(
        curs_dwh, 
        que_insert_aws_ars,
        tpl_cln_curah_hujan
    )

    conx_dwh.commit()

    print("--- %s seconds ---" % (time.time() - start_time))

        # tbl_dwh_curah_hujan.loc[i, 'WaterReserve'] = tbl_dwh_curah_hujan.loc[i-1,'WaterReserve']
        # tbl_dwh_curah_hujan.loc[(tbl_dwh_curah_hujan['WaterBalance'] <= 0), 'WaterReserverAfterEvapor'] = 0
        # tbl_dwh_curah_hujan.loc[(tbl_dwh_curah_hujan['WaterBalance'] >= 200), 'WaterReserverAfterEvapor'] = 200
        # tbl_dwh_curah_hujan.loc[(tbl_dwh_curah_hujan['WaterBalance'] > 0) | (tbl_dwh_curah_hujan['WaterBalance'] < 200), 'WaterReserverAfterEvapor'] = tbl_dwh_curah_hujan.loc[i,'WaterBalance']
        # print(tbl_dwh_curah_hujan)
    # print(tbl_dwh_curah_hujan)
    
calc_rainfall_data()