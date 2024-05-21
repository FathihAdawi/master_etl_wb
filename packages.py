##############################
"""
   Query Rain Fall Plantation
"""
##############################

"""
    Query Delete Data L3_WB_ACT
"""
que_del_wb_act = 'DELETE FROM "L3_WB_ACT" ' \
                    'WHERE date_part(\'year\', "FullDate") = date_part(\'year\', current_date) '

"""
    Query Get Data BJR PIS
"""
que_bjr = 'SELECT ' \
            '"Tahun", ' \
            '"Bulan", ' \
            '"KodeKebun", ' \
            '"KodeDivisi", ' \
            '"KodeBlok", '  \
            '"TahunTanam", ' \
            '"JenisKegiatan", ' \
            '"LuasTanam", ' \
            '"BJRPabrik", ' \
            '"BJRSensus" ' \
        'FROM "L2_BJR_PIS" ' \
        'WHERE "Tahun" = date_part(\'year\', current_date) '

"""
    Query Get Data WB Blok
"""
que_wb_prod =   'select ' \
                    'a."FullDate",' \
                    'a."KodeKebun",' \
                    'a."ShortNameKebun",' \
                    'a."KodeDivisi",' \
                    'a."KodeBlok",' \
                    'a."TahunTanam",' \
                    'a."TotalPokok",' \
                    'a."netto",' \
                    'a."Janjang"' \
                'from "L1_Fact_WB_Produksi" a '\
                'where ' \
                'date_part(\'year\', a."FullDate") = date_part(\'year\', current_date) ' 

"""
    Query Insert Data WB Blok
"""
que_insert_wb_prop ='INSERT INTO "L3_WB_ACT" ' \
                    '("FullDate","KodeKebun","ShortNameKebun","KodeDivisi","KodeBlok",' \
                    '"TahunTanam","TotalPokok","LuasTanam","netto","Janjang","BJR",' \
                    '"KG_Total","KG_ACT","KG_PKS","BJR_PKS") ' \
                    'VALUES %s '

"""
    Query SP Update Table L2_WB_Act_Bgt_Blok
"""
que_sp_wb_prop = 'CALL public.synch_l2_wb_act_bgt_blok()'






##############################
"""
   Query Rain Fall Plantation
"""
##############################

"""
    Query SP Rain Fall Plantation
"""
que_fun_l2_rf_unitinterval_daily = 'CALL public.synch_l2_aws_ars_unitinterval_daily() '


"""
    Query Get RAW Rain Fall Plantation
"""
que_get_rf_raw =   'select ' \
                            'a_2."Tahun", ' \
                            'a_2."Bulan", ' \
                            'a_2."Estate" as "KodeKebun", ' \
                            'b_1."Unit Type" as "TipeKebun", ' \
                            'count(distinct date_part(\'day\', "Date")) as "RainyDays", ' \
                            'sum(coalesce(a_2."RainFallValue", 0)) / ' \
                            'count(distinct a_2."Lokasi") as "AVG_CurahHujan", ' \
                            '0 as "WaterReserve", ' \
                            '(case ' \
                                'when count(date_part(\'day\', "Date")) <= 10 then 150 ' \
                                'else 120 ' \
                            'end) as "WaterEvapor", ' \
                            '0 as "WaterBalance", ' \
                            '0 as "WaterReserverAfterEvapor", ' \
                            '0 as "WaterDeficit" ' \
                        'from ' \
                            '"L2_AWS_ARS_IntervalDaily" a_2 ' \
                        'full join "L1_Fact_AWS_ARS" b_2 on ' \
                            'b_2."ESTATE" = a_2."Estate" ' \
                            'and b_2."LOKASI" = a_2."Lokasi" ' \
                        'join "L1_Dim_UnitType" b_1 on ' \
                            'b_1."Unit" = a_2."Estate" ' \
                        'where ' \
                            'a_2."RainFallValue" > 0 ' \
                            'and a_2."Tahun" = date_part(\'year\', current_date) ' \
                            'and a_2."Bulan" in (date_part(\'month\', current_date)) ' \
                        'group by ' \
                            'a_2."Tahun", ' \
                            'a_2."Bulan", ' \
                            'a_2."Estate", ' \
                            'b_1."Unit Type" ' \
                        'order by ' \
                            '"Estate" '

"""
    Query Get Cleansing Rain Fall Plantation
"""
que_get_rf_cln =   '( '\
                        'select ' \
                            '"Tahun", ' \
                            '"Bulan", ' \
                            '"KodeKebun", ' \
                            '"TipeKebun", ' \
                            '"RainyDays", ' \
                            '"RainFallValueAVG", ' \
                            '"WaterReserve", ' \
                            '"WaterEvapotranspiration", ' \
                            '"WaterBalance", ' \
                            '"WaterReserveAfterEvapor", ' \
                            '"WaterDeficit" ' \
                        'from "L2_AWS_ARS_PLANTATION" ' \
                        'where ' \
                            '"Tahun" = date_part(\'year\', current_date) ' \
                            'and "Bulan" = date_part(\'month\', current_date)-1 ' \
                        ') ' \
                        'union ' \
                        '( ' \
                        'select ' \
                            '"Tahun", ' \
                            '"Bulan", ' \
                            '"KodeKebun", ' \
                            '"TipeKebun", ' \
                            '"RainyDays", ' \
                            '"RainFallValueAVG", ' \
                            '"WaterReserve", ' \
                            '"WaterEvapotranspiration", ' \
                            '"WaterBalance", ' \
                            '"WaterReserveAfterEvapor", ' \
                            '"WaterDeficit" ' \
                        'from "L2_AWS_ARS_PLANTATION"  ' \
                        'where "Tahun" = date_part(\'year\', current_date)-1 ' \
                        'and "Bulan" = 12 ' \
                        ') '

"""
    Query Insert Rain Fall Plantation
"""
que_insert_rf ='INSERT INTO "L2_AWS_ARS_PLANTATION" ' \
                    '("Tahun","Bulan","KodeKebun","TipeKebun","RainyDays", ' \
                    '"RainFallValueAVG","WaterReserve","WaterEvapotranspiration", ' \
                    '"WaterBalance","WaterReserveAfterEvapor","WaterDeficit")' \
                    'VALUES %s'






########################
"""
    Query BJR L2 BJR PIS
"""
########################

"""
    Query Delete BJR PIS
"""
que_del_bjr_pis = 'DELETE ' \
                'FROM "L2_BJR_PIS" x ' \
                'where "Tahun" = date_part(\'year\', current_date) ' \
                'and "Bulan" = date_part(\'month\', current_date) '


"""
    Query Get BJR PIS
"""
que_as_bjr_pis = 'select ' \
        'a."Year",' \
        'a."Month",' \
        'a."KodeKebun",' \
        'a."KodeDivisi",' \
        'a."KodeBlok",' \
        'a."TahunTanam",' \
        'a."JenisKegiatan", '\
        'sum(a."LuasTanam") as "LuasTanam",' \
        'sum(a."BJRPabrik") as "BJRPabrik",' \
        'sum(a."BJRSensus") as "BJRSensus"' \
    'from ( ' \
    'select '\
        'ROW_NUMBER() over (partition by a."Year", a."Month", b."UnitCode", ' \
        'c."DivisionCode", d."KodeBlok" order by a."BJRPabrik" desc) as "RN",' \
        'a."Year",' \
        'a."Month",' \
        'b."UnitCode" as "KodeKebun",' \
        'c."DivisionCode" as "KodeDivisi",' \
        'd."KodeBlok",' \
        'date_part(\'year\', "TanggalTanam") as "TahunTanam",' \
        '"LuasTanahYangDitanam" as "LuasTanam",' \
        'a."JenisKegiatan", ' \
        'a."BJRPabrik",' \
        'a."BJRSensus"' \
    'from "L1_Fact_Blok" a ' \
    'join "L1_Dim_Unit" b on a."UnitID" = b."UnitID"' \
    'join "L1_Dim_Division" c on a."DivisionID" = c."DivisionID"' \
    'join "L1_Dim_Blok" d on d."BlokID" = a."BlokID"' \
    'where a."Year" = date_part(\'year\', current_date) ' \
    'and a."Month" = date_part(\'month\', current_date) ' \
    ') a ' \
    'where "RN" = 1 ' \
    'group by ' \
        'a."Year",' \
        'a."Month",' \
        'a."KodeKebun",' \
        'a."KodeDivisi",' \
        'a."KodeBlok",' \
        'a."TahunTanam", ' \
        'a."JenisKegiatan" '

"""
    Query Insert BJR PIS
"""
que_insert_bjr_pis = 'INSERT INTO "L2_BJR_PIS" '\
                    '("Tahun","Bulan","KodeKebun","KodeDivisi","KodeBlok","TahunTanam",' \
                    '"JenisKegiatan","LuasTanam","BJRSensus","BJRPabrik") ' \
                    'VALUES %s'