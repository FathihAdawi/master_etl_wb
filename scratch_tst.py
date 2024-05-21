SELECT  
    row_number() over (partition by a_1."Estate" order by a_1."Estate") as "RN",  
    a_1."Tahun",  
    a_1."Bulan",  
    a_1."Estate" AS "KodeKebun",  
    b_1."Unit Type" AS "TipeKebun",  
    a_1."RainyDays",  
    a_1."AVG_CurahHujan",  
    a_1."WaterReserve",  
    a_1."WaterEvapor",  
    a_1."WaterBalance",  
    (case  
        when "WaterBalance" <= 0 then 0   
        when "WaterBalance" >= 200 then 200  
        when "WaterBalance" < 200 then "WaterBalance"  
    end) as "WaterReserverAfterEvapor"  
FROM ( 
    SELECT  
        a_2."Tahun",  
        a_2."Bulan",  
        a_2."Estate",  
        (case  
            when a_2."Bulan" = 11 and a_2."Tahun" = 2022 then (200 + sum(coalesce(a_2."RainFallValue",0))/count(distinct a_2."Lokasi"))-(case when count(distinct date_part(day,"Date")) <= 10 then 150 else 120 end)  
        end) as "WaterBalance",  
        (case when a_2."Bulan" = 11 and a_2."Tahun" = 2022 then 200 else 0 end) as "WaterReserve",  
        (case when count(date_part(day,"Date")) <= 10 then 150 else 120 end) as "WaterEvapor",  
        count(distinct date_part(day, "Date")) as "RainyDays",  
        sum(coalesce(a_2."RainFallValue", 0)) AS "RainFallValue",  
        sum(coalesce(a_2."RainFallValue",0))/count(distinct a_2."Lokasi") as "AVG_CurahHujan"  
    FROM "L2_AWS_ARS_IntervalDaily" a_2  
    full join "L1_Fact_AWS_ARS" b_2 on   
        b_2."ESTATE" = a_2."Estate"  
        and b_2."LOKASI" = a_2."Lokasi"  
    where a_2."RainFallValue" > 0  
    GROUP BY  
        a_2."Tahun",  
        a_2."Bulan",  
        a_2."Estate"  
    order by "Estate"  
) a_1  
JOIN "L1_Dim_UnitType" b_1 ON b_1."Unit" = a_1."Estate" 


for i in range(1,len(tbl_concat_rainfall)):
            tbl_concat_rainfall.loc[i, 'WaterBalance'] = \
                (tbl_concat_rainfall.loc[i, 'RainFallValueAVG'] + \
                 tbl_concat_rainfall.loc[i-1,'WaterReserve']) - \
                tbl_concat_rainfall.loc[i,'WaterEvapotranspiration']

            if tbl_concat_rainfall.loc[i, 'WaterBalance'] >= 200:
                tbl_concat_rainfall.loc[i, 'WaterReserveAfterEvapor'] = 200
            elif tbl_concat_rainfall.loc[i, 'WaterBalance'] > 0 \
                and tbl_concat_rainfall.loc[i, 'WaterBalance'] < 200:
                tbl_concat_rainfall.loc[i, 'WaterReserveAfterEvapor'] = \
                tbl_concat_rainfall.loc[i, 'WaterBalance']
            elif tbl_concat_rainfall.loc[i, 'WaterBalance'] <= 0:
                tbl_concat_rainfall.loc[i, 'WaterReserveAfterEvapor'] = 0

            tbl_concat_rainfall.loc[i,'WaterReserve'] = \
                tbl_concat_rainfall.loc[i-1,'WaterReserveAfterEvapor']

        for i in range(0,len(tbl_concat_rainfall)):
            if tbl_concat_rainfall.loc[i,'WaterBalance'] < 0:
                tbl_concat_rainfall.loc[i, 'WaterDeficit'] = \
                tbl_concat_rainfall.loc[i, 'WaterBalance'] + \
                (tbl_concat_rainfall.loc[i, 'WaterBalance'] * -2)
            elif tbl_concat_rainfall.loc[i, 'WaterBalance'] >= 0:
                tbl_concat_rainfall.loc[i, 'WaterDeficit'] = 0

        tbl_concat_rainfall.reset_index(drop=True, inplace=True)

        curs_dwh.execute(que_fun_l2_aws_ars_unitinterval_daily)
        for x,y,z in (tbl_concat_rainfall[['Tahun','Bulan','KodeKebun']].drop_duplicates()).values:
            que_del_rainfall = 'DELETE FROM "L2_AWS_ARS_PLANTATION"' \
                                'where "Tahun" = '+'\''+str(x)+'\''+ \
                                ' and "Bulan" = '+'\''+str(y)+'\''+ \
                                ' and "KodeKebun" = '+'\''+str(z)+'\' ;'
            curs_dwh.execute(que_del_rainfall)
            print(
                'Delete Rainfall: \n' \
                '\tTahun: '+str(x)+'\n' \
                '\tBulan: '+str(y)+'\n' \
                '\tKodeKebun: '+str(z)+'\n'
            )

        tbl_concat_rainfall = pl.from_pandas(tbl_concat_rainfall)
        tpl_cln_curah_hujan = tuple(map(tuple, tbl_concat_rainfall.to_numpy()))

        print(tbl_concat_rainfall)

        extras.execute_values(
            curs_dwh, 
            que_insert_aws_ars,
            tpl_cln_curah_hujan
        )
        
        print('Insert Success!')
        conx_dwh.commit()