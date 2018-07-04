CREATE OR REPLACE PACKAGE BODY pa_manual_trend_upload IS

  FUNCTION get_product_table(p_mrkt_id IN NUMBER,
                             p_sls_perd_id IN NUMBER,
                             p_trgt_perd_id IN NUMBER,
                             p_sls_typ_id IN NUMBER)
    RETURN obj_manl_trend_upload_table PIPELINED IS
  BEGIN
    FOR rec IN (
      WITH d AS (
        SELECT ROWNUM day, dt
          FROM (SELECT DISTINCT TRUNC(prcsng_dt) dt
                  FROM dly_bilng_trnd, dly_bilng_trnd_cntrl
                 WHERE dly_bilng_trnd.mrkt_id = p_mrkt_id
                   AND dly_Bilng_trnd.trnd_sls_perd_id =
                       DECODE(p_sls_typ_id,
                              104,
                              p_trgt_perd_id,
                              pa_maps_public.perd_plus(dly_bilng_trnd.mrkt_id,
                                                       p_trgt_perd_id,
                                                       -2))
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
                 ORDER BY TRUNC(prcsng_dt)))
      SELECT NULL AS status,
             nofsc.mrkt_id,
             nofsc.sls_perd_id,
             nofsc.last_updt_ts,
             nofsc.last_updt_user_id,
             nofsc.fsc_cd,
             nofsc.desc_txt,
             (SELECT day FROM d WHERE dt = TRUNC(dbt.prcsng_dt)) day,
             TRUNC(dbt.prcsng_dt) actual_day,
             dbt.unit_qty dly_unit_qty,
             SUM(dbt.unit_qty) OVER (PARTITION BY nofsc.fsc_cd) total_dly_bilng_unit_qty,
             nofsc.unit_qty trnd_unit_qty,
             ROUND(nofsc.unit_qty / SUM(dbt.unit_qty)
                   OVER (PARTITION BY nofsc.fsc_cd), 4) r_factor,
             nofsc.SLS_TYP_ID
        FROM sc_trnd_no_fsc_prod nofsc,
             (SELECT fsc_cd, TRUNC(prcsng_dt) prcsng_dt, SUM(unit_qty) unit_qty
                FROM dly_bilng_trnd,
                     dly_bilng_trnd_cntrl
               WHERE dly_bilng_trnd.MRKT_ID = p_mrkt_id
                 AND dly_bilng_trnd.trnd_sls_perd_id =
                     DECODE(p_sls_typ_id,
                            104,
                            p_trgt_perd_id,
                            pa_maps_public.perd_plus(p_mrkt_id, p_trgt_perd_id, -2))
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
               GROUP BY fsc_cd, TRUNC(prcsng_dt)) dbt
       WHERE nofsc.mrkt_id = p_mrkt_id
         AND nofsc.sls_perd_id = p_trgt_perd_id
         AND nofsc.sls_typ_id = p_sls_typ_id
         AND dbt.fsc_cd = nofsc.fsc_cd
       ORDER BY nofsc.FSC_CD, TRUNC(dbt.prcsng_dt)
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

  END get_product_table;

END pa_manual_trend_upload;
/
