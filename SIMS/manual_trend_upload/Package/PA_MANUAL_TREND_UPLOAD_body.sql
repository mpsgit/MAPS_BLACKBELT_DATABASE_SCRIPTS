CREATE OR REPLACE PACKAGE BODY pa_manual_trend_upload IS

  g_run_id                 NUMBER := 0;
  g_user_id                mps_plsql_log.user_id%TYPE;

  FUNCTION get_product_table(p_mrkt_id IN NUMBER,
                             p_sls_perd_id IN NUMBER,
                             p_trgt_perd_id IN NUMBER,
                             p_sls_typ_id IN NUMBER)
    RETURN obj_manl_trend_upload_table PIPELINED IS

    l_procedure_name         VARCHAR2(50) := 'GET_PRODUCT_TABLE';

  BEGIN
    g_run_id  := app_plsql_output.generate_new_run_id;
    g_user_id := RTRIM(sys_context('USERENV', 'OS_USER'), 35);

    app_plsql_output.set_run_id(g_run_id);
    app_plsql_log.set_context(g_user_id, g_package_name, g_run_id);
    app_plsql_log.info(l_procedure_name || ' start');

    FOR rec IN (
      WITH d AS
       (SELECT ROWNUM day, dt
          FROM (SELECT DISTINCT TRUNC(prcsng_dt) dt
                  FROM dly_bilng_trnd, dly_bilng_trnd_cntrl
                 WHERE dly_bilng_trnd.mrkt_id = p_mrkt_id
                   AND dly_Bilng_trnd.trnd_sls_perd_id =
                       DECODE(p_sls_typ_id,
                              104,
                              p_trgt_perd_id,
                              PA_MAPS_PUBLIC.PERD_PLUS(dly_bilng_trnd.mrkt_id, p_trgt_perd_id, -2))
                   AND dly_bilng_trnd_cntrl.sls_typ_id = 6
                   AND dly_bilng_trnd_cntrl.dly_bilng_mtch_id = 2401
                   AND NVL(dly_bilng_trnd_cntrl.lcl_bilng_actn_cd,
                           dly_bilng_trnd.lcl_bilng_actn_cd) =
                       dly_bilng_trnd.lcl_bilng_actn_cd
                   AND NVL(dly_bilng_trnd_cntrl.lcl_bilng_tran_typ,
                           dly_bilng_trnd.lcl_bilng_tran_typ) =
                       dly_bilng_trnd.lcl_bilng_tran_typ
                   AND NVL(dly_bilng_trnd_cntrl.lcl_bilng_offr_typ,
                           dly_bilng_trnd.lcl_bilng_offr_typ) =
                       dly_bilng_trnd.lcl_bilng_offr_typ
                   AND NVL(dly_bilng_trnd_cntrl.lcl_bilng_defrd_cd,
                           dly_bilng_trnd.lcl_bilng_defrd_cd) =
                       dly_bilng_trnd.lcl_bilng_defrd_cd
                   AND NVL(dly_bilng_trnd_cntrl.lcl_bilng_shpng_cd,
                           dly_bilng_trnd.lcl_bilng_shpng_cd) =
                       dly_bilng_trnd.lcl_bilng_shpng_cd
                 ORDER BY TRUNC(prcsng_dt))),
        mrkt_tmp_fsc_master AS
         (SELECT mrkt_id, sku_id, fsc_cd
            FROM (SELECT mrkt_id
                        ,sku_id
                        ,mstr_fsc_cd fsc_cd
                        ,strt_perd_id from_strt_perd_id
                        ,nvl(lead(strt_perd_id, 1)
                             over(PARTITION BY mrkt_id, sku_id ORDER BY strt_perd_id),
                             99999999) to_strt_perd_id
                    FROM mstr_fsc_asgnmt
                   WHERE p_mrkt_id = mrkt_id
                     AND p_trgt_perd_id >= strt_perd_id)
           WHERE p_trgt_perd_id >= from_strt_perd_id
             AND p_trgt_perd_id < to_strt_perd_id),
        mrkt_tmp_fsc AS
         (SELECT mrkt_id, sku_id, MAX(fsc_cd) fsc_cd
            FROM (SELECT mrkt_id
                        ,sku_id
                        ,fsc_cd fsc_cd
                        ,strt_perd_id from_strt_perd_id
                        ,nvl(lead(strt_perd_id, 1)
                             over(PARTITION BY mrkt_id, fsc_cd ORDER BY strt_perd_id),
                             99999999) to_strt_perd_id
                    FROM mrkt_fsc
                   WHERE p_mrkt_id = mrkt_id
                     AND p_trgt_perd_id >= strt_perd_id
                     AND 'N' = dltd_ind)
           WHERE p_trgt_perd_id >= from_strt_perd_id
             AND p_trgt_perd_id < to_strt_perd_id
           GROUP BY mrkt_id, sku_id)
      --
      SELECT 1 AS status,
             dms.mrkt_id,
             dms.sls_perd_id,
             mtu.last_updt_ts,
             mtu.last_updt_user_id,
             NVL(dms.fsc_cd, dms.mstr_fsc_cd) AS fsc_cd,
             sku.sku_nm AS desc_txt,
             (SELECT day FROM d WHERE d.dt = TRUNC(dms.dt)) AS day,
             dms.dt actual_day,
             dms.sum_units dly_unit_qty,
             SUM(dms.sum_units) OVER (PARTITION BY NVL(dms.fsc_cd, dms.mstr_fsc_cd)) AS total_dly_bilng_unit_qty,
             mtu.unit_qty AS trnd_unit_qty,
             --ROUND(NVL(mtu.unit_qty, 0) / SUM(dms.sum_units) OVER (PARTITION BY dms.fsc_cd), 4)
             0 AS r_factor,
             dms.sls_typ_id
        FROM sku,
             manual_trend_upload mtu,
             d,
            (SELECT osl.sku_id,
                    SUM(dms.unit_qty) sum_units,
                    dbt.prcsng_dt dt,
                    COALESCE(dbt.fsc_cd, fsc_mstr.fsc_cd, fsc.fsc_cd) AS fsc_cd,
                    MAX(mfa.mstr_fsc_cd) AS mstr_fsc_cd,
                    osl.mrkt_id,
                    dms.sls_perd_id,
                    dms.sls_typ_id
               FROM dstrbtd_mrkt_sls dms,
                    offr_sku_line osl,
                    dly_bilng_trnd_offr_sku_line dbtosl,
                    dly_bilng_trnd dbt,
                    mrkt_tmp_fsc fsc,
                    mrkt_tmp_fsc_master fsc_mstr,
                    mstr_fsc_asgnmt mfa
              WHERE dms.offr_sku_line_id = osl.offr_sku_line_id
                AND dms.offr_sku_line_id = dbtosl.offr_sku_line_id (+)
                AND dbt.dly_bilng_id = dbtosl.dly_bilng_id
                AND osl.sku_id = fsc_mstr.sku_id(+)
                AND osl.sku_id = fsc.sku_id(+)
                AND osl.mrkt_id = fsc_mstr.mrkt_id(+)
                AND osl.mrkt_id = fsc.mrkt_id(+)
                AND osl.sku_id = mfa.sku_id
                AND osl.mrkt_id = mfa.mrkt_id
                AND dbtosl.sls_typ_id = 6
                AND dms.sls_typ_id = p_sls_typ_id
                AND dms.ver_id = 0
                AND dms.sls_perd_id = p_sls_perd_id
                AND dms.mrkt_id = p_mrkt_id
             GROUP BY osl.sku_id, COALESCE(dbt.fsc_cd, fsc_mstr.fsc_cd, fsc.fsc_cd),
                      osl.mrkt_id, dms.sls_perd_id, dms.sls_typ_id, dbt.prcsng_dt
             ) dms
      WHERE sku.sku_id = dms.sku_id
        AND mtu.mrkt_id (+) = dms.mrkt_id
        AND mtu.sls_perd_id (+) = dms.sls_perd_id
        AND mtu.sls_typ_id (+) = dms.sls_typ_id
        AND mtu.fsc_cd (+) = dms.fsc_cd
        AND d.dt = TRUNC(dms.dt)
      UNION ALL
      SELECT 0 AS status, 
             sc_trnd_no_fsc_prod.mrkt_id,
             sc_trnd_no_fsc_prod.sls_perd_id,
             sc_trnd_no_fsc_prod.last_updt_ts,
             sc_trnd_no_fsc_prod.last_updt_user_id,
             sc_trnd_no_fsc_prod.fsc_cd,
             sc_trnd_no_fsc_prod.desc_txt,
             (SELECT day FROM d WHERE dt = TRUNC(dly_bilng_trnd.prcsng_dt)) day,
             TRUNC(dly_bilng_trnd.prcsng_dt) actual_day,
             dly_bilng_trnd.unit_qty dly_unit_qty,
             SUM(dly_bilng_trnd.unit_qty) OVER (PARTITION BY sc_trnd_no_fsc_prod.fsc_cd) total_dly_bilng_unit_qty,
             sc_trnd_no_fsc_prod.unit_qty trnd_unit_qty,
             ROUND(sc_trnd_no_fsc_prod.unit_qty / SUM(dly_bilng_trnd.unit_qty)
                   OVER (PARTITION BY sc_trnd_no_fsc_prod.fsc_cd), 4) r_factor,
             sc_trnd_no_fsc_prod.SLS_TYP_ID
        FROM sc_trnd_no_fsc_prod,
             (SELECT fsc_cd, TRUNC(prcsng_dt) prcsng_dt, SUM(unit_qty) unit_qty
                FROM dly_bilng_trnd, dly_bilng_trnd_cntrl
               WHERE dly_bilng_trnd.MRKT_ID = p_mrkt_id
                 AND dly_bilng_trnd.trnd_sls_perd_id =
                     DECODE(p_sls_typ_id,
                            104,
                            p_trgt_perd_id,
                            PA_MAPS_PUBLIC.PERD_PLUS(p_mrkt_id, p_trgt_perd_id, -2))
                 AND dly_bilng_trnd_cntrl.sls_typ_id = 6
                 AND dly_bilng_trnd_cntrl.dly_bilng_mtch_id = 2401
                 AND NVL(dly_bilng_trnd_cntrl.lcl_bilng_actn_cd,
                         dly_bilng_trnd.lcl_bilng_actn_cd) =
                     dly_bilng_trnd.lcl_bilng_actn_cd
                 AND NVL(dly_bilng_trnd_cntrl.lcl_bilng_tran_typ,
                         dly_bilng_trnd.lcl_bilng_tran_typ) =
                     dly_bilng_trnd.lcl_bilng_tran_typ
                 AND NVL(dly_bilng_trnd_cntrl.lcl_bilng_offr_typ,
                         dly_bilng_trnd.lcl_bilng_offr_typ) =
                     dly_bilng_trnd.lcl_bilng_offr_typ
                 AND NVL(dly_bilng_trnd_cntrl.lcl_bilng_defrd_Cd,
                         dly_bilng_trnd.lcl_bilng_defrd_cd) =
                     dly_bilng_trnd.lcl_bilng_defrd_cd
                 AND NVL(dly_bilng_trnd_cntrl.lcl_bilng_shpng_cd,
                         dly_bilng_trnd.lcl_bilng_shpng_cd) =
                     dly_bilng_trnd.lcl_bilng_shpng_cd
               GROUP BY fsc_cd, TRUNC(prcsng_dt)) dly_bilng_trnd
       WHERE sc_trnd_no_fsc_prod.mrkt_id = p_mrkt_id
         AND sc_trnd_no_fsc_prod.sls_perd_id = p_trgt_perd_id
         AND sc_trnd_no_fsc_prod.sls_typ_id = p_sls_typ_id
         AND dly_bilng_trnd.FSC_CD = sc_trnd_no_fsc_prod.FSC_CD
      ORDER BY fsc_cd, actual_day
    )
    LOOP
      PIPE ROW(obj_manl_trend_upload_line(rec.status,
                                          rec.mrkt_id,
                                          rec.sls_perd_id,
                                          rec.last_updt_ts,
                                          rec.last_updt_user_id,
                                          rec.fsc_cd,
                                          rec.desc_txt,
                                          rec.day,
                                          rec.actual_day,
                                          rec.dly_unit_qty,
                                          rec.total_dly_bilng_unit_qty,
                                          rec.trnd_unit_qty,
                                          rec.r_factor,
                                          rec.sls_typ_id));
    END LOOP;

    app_plsql_log.info(l_procedure_name || ' stop');

  EXCEPTION
    WHEN OTHERS THEN
      app_plsql_log.info(l_procedure_name || ': Error while getting product table');
      app_plsql_log.info(l_procedure_name || ': ' || SQLERRM(SQLCODE));
  END get_product_table;

END pa_manual_trend_upload;
/
