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
from packages import que_bjr, \
    que_wb_prod, \
    que_del_wb_act, \
    que_insert_wb_prop, \
    que_sp_wb_prop
# import cProfile

print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
start_time = time.time()

remapping_cols_as = [
    "Year", "Month", "KodeKebun", "KodeDivisi", "KodeBlok", 
    "TahunTanam", "JenisKegiatan", "LuasTanam", "BJRPabrik", "BJRSensus"
]

remapping_cols_wb = [
    "FullDate","KodeKebun", "ShortNameKebun", "KodeDivisi",
    "KodeBlok", "TahunTanam", "TotalPokok",
    "netto", "Janjang"
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

def calc_propotition_blok_wb():

    curs_dwh, conx_dwh = init_dwh()



    ##########################
    """ Fetch all BJR Data """
    ##########################
    exec_dwh_as = curs_dwh.execute(que_bjr)
    tbl_dwh_as = pd.DataFrame(curs_dwh.fetchall())
    tbl_dwh_as.columns = remapping_cols_as
    tbl_dwh_as['TahunTanam'] = tbl_dwh_as['TahunTanam'].astype(np.int64)
    tbl_dwh_as['BJRPabrik'] = tbl_dwh_as['BJRPabrik'].fillna(0)
    tbl_dwh_as['BJRSensus'] = tbl_dwh_as['BJRSensus'].fillna(0)
    tbl_dwh_as.sort_values(
        ['KodeKebun','KodeBlok','Year','Month'],
        inplace=True
    )



    ###########################
    """ Cleansing BJR Data """
    ###########################
    tbl_dwh_as[
        'BJRPabrik_Fill'
    ] = tbl_dwh_as[
        'BJRPabrik'
    ].mask(
        tbl_dwh_as['BJRPabrik'] == 0
    ).groupby(
        [tbl_dwh_as['KodeKebun'],tbl_dwh_as['KodeBlok']]
    ).ffill()
    tbl_dwh_as.drop(
        index=tbl_dwh_as[tbl_dwh_as['KodeDivisi'].isin(['DUMMY','AFDTR','PABRIK'])].index, 
        inplace=True
    )
    tbl_dwh_as.drop(
        index=tbl_dwh_as[
            ((tbl_dwh_as['KodeBlok'].str.len() > 4) | (tbl_dwh_as['KodeBlok'].str.len() < 4))
        ].index, 
        inplace=True
    )
    tbl_dwh_as['BJRPabrik_Fill'] = tbl_dwh_as['BJRPabrik_Fill'].fillna(4.99)
    tbl_dwh_as.drop(['BJRPabrik'],axis=1,inplace=True)
    tbl_dwh_as.rename(columns={'BJRPabrik_Fill':'BJRPabrik'},inplace=True)
    tbl_dwh_as.reset_index(drop=True, inplace=True)
    tbl_dwh_as = pl.from_pandas(tbl_dwh_as)



    ##############################
    """ Fetch all data WB Blok """
    ##############################
    exec_dwh_wb = curs_dwh.execute(que_wb_prod)
    tbl_dwh_wb = pl.DataFrame(curs_dwh.fetchall())
    tbl_dwh_wb.columns = remapping_cols_wb
    tbl_dwh_wb = tbl_dwh_wb.with_columns(
        Year = pl.lit((tbl_dwh_wb['FullDate'].dt.year()).cast(pl.Int64)),
        Month = pl.lit((tbl_dwh_wb['FullDate'].dt.month()).cast(pl.Int64))
    )



    #########################################
    """ Join between table BJR and WB Blok"""
    #########################################
    tbl_dwh_as_wb = tbl_dwh_as.join(
        tbl_dwh_wb, 
        left_on=['Year','Month','KodeKebun','KodeDivisi','KodeBlok'], 
        right_on=['Year','Month','KodeKebun','KodeDivisi','KodeBlok']
    )
    tbl_dwh_as_wb = tbl_dwh_as_wb.with_columns(
        KG_ACT = pl.col('Janjang') * pl.col('BJRPabrik')
    )
    tbl_dwh_as_wb = tbl_dwh_as_wb.with_columns(
        pl.col('BJRPabrik').alias('BJR')
    )
    tbl_dwh_as_netto = tbl_dwh_as_wb.select(
        ['FullDate','Year','Month','KodeKebun','KodeDivisi','netto']
    ).filter(pl.col('netto') != 0)
    tbl_dwh_as_kg = tbl_dwh_as_wb.select(
        ['FullDate','Year','Month','KodeKebun','KodeDivisi','KG_ACT']
    )
    tbl_dwh_as_kg_netto = tbl_dwh_as_netto.join(
        tbl_dwh_as_kg,
        left_on=['FullDate','Year','Month','KodeKebun','KodeDivisi'],
        right_on=['FullDate','Year','Month','KodeKebun','KodeDivisi']
    )
    tbl_dwh_as_kg_netto = tbl_dwh_as_kg_netto.group_by(
        ['FullDate','KodeKebun','KodeDivisi','netto']
    ).agg(
        pl.col('KG_ACT').sum().alias('KG_Total')
    )
    tbl_dwh_as_wb = tbl_dwh_as_wb.drop('netto')
    tbl_dwh_as_wb_prop = tbl_dwh_as_wb.join(
        tbl_dwh_as_kg_netto,
        left_on=['FullDate','KodeKebun','KodeDivisi'],
        right_on=['FullDate','KodeKebun','KodeDivisi']
    )
    tbl_dwh_as_wb_prop = tbl_dwh_as_wb_prop.with_columns(pl.col('KG_Total').fill_null(0))
    tbl_dwh_as_wb_prop = tbl_dwh_as_wb_prop.with_columns(pl.col('Janjang').fill_null(0))
    tbl_dwh_as_wb_prop = tbl_dwh_as_wb_prop.with_columns(
        KG_PKS = (pl.col('KG_ACT') * pl.col('netto'))/pl.col('KG_Total')
    )
    tbl_dwh_as_wb_prop = tbl_dwh_as_wb_prop.with_columns(
        BJR_PKS = pl.col('KG_PKS')/pl.col('Janjang')
    )
    tbl_dwh_as_wb_prop = tbl_dwh_as_wb_prop.with_columns(pl.col('KG_PKS').fill_nan(0))
    tbl_dwh_as_wb_prop = tbl_dwh_as_wb_prop.with_columns(pl.col('BJR_PKS').fill_nan(0))
    tbl_dwh_final_prop = tbl_dwh_as_wb_prop.select(
        [
            "FullDate","KodeKebun","ShortNameKebun","KodeDivisi","KodeBlok",
            "TahunTanam","TotalPokok","LuasTanam","netto","Janjang","BJR",
            "KG_Total","KG_ACT","KG_PKS","BJR_PKS"
        ]
    )
    
    print(tbl_dwh_final_prop)



    ##################################
    """ Insert Data WB Proposition """
    ##################################
    tuple_clean_tbl_dwh_wb_prop = tuple(map(tuple, tbl_dwh_final_prop.to_numpy()))
    curs_dwh.execute(que_del_wb_act)
    extras.execute_values(
        curs_dwh,
        que_insert_wb_prop,
        tuple_clean_tbl_dwh_wb_prop
    )
    curs_dwh.execute(que_sp_wb_prop)
    conx_dwh.commit()

    print("--- %s seconds ---" % (time.time() - start_time))
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


calc_propotition_blok_wb()
