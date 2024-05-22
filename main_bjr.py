# import pyspark.sql.functions as psql
from configure import db_init
# import pyodbc
import psycopg2
from psycopg2 import extras
import pandas as pd
import polars as pl
import numpy as np
import time
from datetime import datetime
from packages import que_as_bjr_pis,que_del_bjr_pis,que_insert_bjr_pis
# import cProfile

print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
start_time = time.time()

remapping_cols_as = [
    "Tahun", "Bulan", "KodeKebun", "KodeDivisi", "KodeBlok", 
    "TahunTanam", "JenisKegiatan", "LuasTanam", "BJRPabrik", "BJRSensus"
]

def init_dwh():
    # [VARIABLES DB]
    path_db = 'D:\All_app_code\Testing Apps\master_etl_wb\db_dwh.ini'
    section = 'tpadw_db'
    db_info = db_init(path_db, section)

    # [CONNECTION & CURSOR]
    db_connection = psycopg2.connect(**db_info)
    db_cursor = db_connection.cursor()
    
    return db_cursor, db_connection

def calc_bjr_pis():

    curs_dwh, conx_dwh = init_dwh()

    #####################################
    """ 
        Fetch all Data ArealStatement 
    """
    #####################################
    exec_dwh_as = curs_dwh.execute(que_as_bjr_pis)
    # tbl_dwh_as  = pl.DataFrame(curs_dwh.fetchall())
    tbl_dwh_as = pd.DataFrame(curs_dwh.fetchall())
    tbl_dwh_as.columns = remapping_cols_as
    tbl_dwh_as['TahunTanam'] = tbl_dwh_as['TahunTanam'].astype(np.int64)
    tbl_dwh_as['BJRPabrik'] = tbl_dwh_as['BJRPabrik'].fillna(0)
    tbl_dwh_as['BJRSensus'] = tbl_dwh_as['BJRSensus'].fillna(0)

    tbl_dwh_as.sort_values(
        ['KodeKebun','KodeBlok','Tahun','Bulan'],
        inplace=True
    )
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
    tuple_clean_tbl_dwh_as = tuple(map(tuple, tbl_dwh_as.to_numpy()))
    tbl_dwh_as.write_excel(autofit=True)

    print(tbl_dwh_as)

    curs_dwh.execute(que_del_bjr_pis)
    extras.execute_values(
        curs_dwh,
        que_insert_bjr_pis,
        tuple_clean_tbl_dwh_as
    )
    conx_dwh.commit()

    print("--- %s seconds ---" % (time.time() - start_time))
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


calc_bjr_pis()