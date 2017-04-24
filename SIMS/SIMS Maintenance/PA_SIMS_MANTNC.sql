CREATE OR REPLACE PACKAGE pa_sims_mantnc AS

  g_package_name CONSTANT VARCHAR2(30) := 'PA_SIMS_MANTNC';
  --
  -- sales types for DBT
  estimate       CONSTANT NUMBER := 1;
  demand_actuals CONSTANT NUMBER := 6;
  billed_actuals CONSTANT NUMBER := 7;
  --
  --sales type ids
  marketing_bst_id CONSTANT NUMBER := 4;
  marketing_est_id CONSTANT NUMBER := 3;
  marketing_fst_id CONSTANT NUMBER := 5;

  supply_bst_id CONSTANT NUMBER := 104;
  supply_est_id CONSTANT NUMBER := 103;
  supply_fst_id CONSTANT NUMBER := 105;

  --------------------------- cash value maintenance ----------------------------------

  PROCEDURE set_cash_vlu_mantnc(p_mrkt_id     IN NUMBER,
                                p_sls_perd_id IN NUMBER,
                                p_cash_val    IN NUMBER,
                                p_r_factor    IN NUMBER,
                                p_user_id     IN VARCHAR2,
                                p_stus        OUT NUMBER);

  FUNCTION get_cash_vlu_mantnc(p_mrkt_id       IN NUMBER,
                               p_sls_perd_list IN number_array)
    RETURN obj_cash_val_mantnc_table
    PIPELINED;

  FUNCTION get_cash_vlu_mantnc_hist(p_mrkt_id       IN NUMBER,
                                    p_sls_perd_list IN number_array)
    RETURN obj_cash_val_mantnc_table
    PIPELINED;

  --------------------------- daily billing adjustment ----------------------------------

  PROCEDURE set_dly_bilng_adjstmnt(p_dly_bilng_id   IN NUMBER,
                                   p_new_bi24_units IN NUMBER,
                                   p_user_id        IN VARCHAR2,
                                   p_stus           OUT NUMBER);

  FUNCTION get_dly_bilng_adjstmnt(p_mrkt_id      IN NUMBER,
                                  p_sls_perd_id  IN NUMBER,
                                  p_offr_perd_id IN NUMBER,
                                  p_prcsng_dt    IN DATE)
    RETURN obj_dly_bilng_adjstmnt_table
    PIPELINED;

  FUNCTION get_dly_bilng_adjstmnt2(p_dly_bilng_id_list IN number_array)
    RETURN obj_dly_bilng_adjstmnt_table
    PIPELINED;

  --------------------------- manual trend adjustment ----------------------------------

  FUNCTION get_trend_type_list RETURN obj_trend_type_table
    PIPELINED;

  FUNCTION get_target_campaign(p_mrkt_id     IN NUMBER,
                               p_sls_perd_id IN NUMBER,
                               p_sls_typ_id  IN NUMBER) RETURN NUMBER;

  FUNCTION get_sales_campaign(p_mrkt_id      IN NUMBER,
                              p_trgt_perd_id IN NUMBER,
                              p_sls_typ_id   IN NUMBER) RETURN NUMBER;

  PROCEDURE set_manl_trend_adjstmnt_new(p_mrkt_id      IN NUMBER,
                                        p_sls_perd_id  IN NUMBER,
                                        p_sls_typ_id   IN NUMBER,
                                        p_offst_lbl_id IN NUMBER,
                                        p_fsc_cd       IN NUMBER,
                                        p_sct_unit_qty IN NUMBER,
                                        p_user_id      IN VARCHAR2,
                                        p_stus         OUT NUMBER);

  FUNCTION get_manl_trend_adjstmnt_new(p_mrkt_id      IN NUMBER,
                                       p_sls_perd_id  IN NUMBER,
                                       p_offst_lbl_id IN NUMBER,
                                       p_sls_typ_id   IN NUMBER)
    RETURN pa_manl_trend_adjstmnt_table
    PIPELINED;

  FUNCTION get_manl_trend_adjstmnt2_new(p_mrkt_id      IN NUMBER,
                                        p_sls_perd_id  IN NUMBER,
                                        p_offst_lbl_id IN NUMBER,
                                        p_sls_typ_id   IN NUMBER,
                                        p_fsc_cd_array IN number_array)
    RETURN pa_manl_trend_adjstmnt_table
    PIPELINED;
  --------------------------- sku bias maintenance ----------------------------------

  PROCEDURE set_sku_bias_new(p_mrkt_id      IN NUMBER,
                             p_sls_perd_id  IN NUMBER,
                             p_sls_typ_id   IN NUMBER,
                             p_sku_id       IN NUMBER,
                             p_new_sku_bias IN NUMBER,
                             p_user_id      IN VARCHAR2,
                             p_stus         OUT NUMBER);

  FUNCTION get_sku_bias_new(p_mrkt_id     IN NUMBER,
                            p_sls_typ_id  IN NUMBER,
                            p_sls_perd_id IN NUMBER)
    RETURN pa_sku_bias_mantnc_table
    PIPELINED;

  FUNCTION get_sku_bias2_new(p_mrkt_id      IN NUMBER,
                             p_sls_perd_id  IN NUMBER,
                             p_sls_typ_id   IN NUMBER,
                             p_sku_id_array IN number_array)
    RETURN pa_sku_bias_mantnc_table
    PIPELINED;

  --------------------------- reports ----------------------------------

  FUNCTION sct_trend_check_rpt(p_mrkt_id            IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                               p_campgn_sls_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE)
    RETURN sct_trend_check_rpt_table
    PIPELINED;

  FUNCTION sct_dly_updt_rpt(p_mrkt_id            IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                            p_campgn_sls_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE)
    RETURN sct_dly_updt_rpt_table
    PIPELINED;

  FUNCTION p94_rpt_head(p_mrkt_id        IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                        p_campgn_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                        p_sls_typ_id     IN sls_typ.sls_typ_id%TYPE,
                        p_prcsng_dt      IN dly_bilng.prcsng_dt%TYPE)
    RETURN p94_rpt_head_table
    PIPELINED;

  FUNCTION p94_rpt_dtls(p_mrkt_id        IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                        p_campgn_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                        p_sls_typ_id     IN sls_typ.sls_typ_id%TYPE,
                        p_prcsng_dt      IN dly_bilng.prcsng_dt%TYPE)
    RETURN p94_rpt_dtls_table
    PIPELINED;

END pa_sims_mantnc;
/
CREATE OR REPLACE PACKAGE BODY pa_sims_mantnc AS

  --------------------------- cash value maintenance ----------------------------------

  FUNCTION get_local_prcsng_dt(p_mrkt_id NUMBER, p_sls_perd NUMBER)
    RETURN DATE;

  FUNCTION get_latest_prcsng_dt(p_mrkt_id NUMBER, p_sls_perd NUMBER)
    RETURN DATE;

  FUNCTION get_cash_vlu_mantnc(p_mrkt_id       IN NUMBER,
                               p_sls_perd_list IN number_array)
    RETURN obj_cash_val_mantnc_table
    PIPELINED IS
  
    CURSOR cc IS
      SELECT obj_cash_val_mantnc_line(t.perd_id,
                                      sp.sct_cash_val,
                                      sp.sct_r_factor,
                                      CASE
                                        WHEN m.max_perd_id > t.perd_id THEN
                                         'FINISHED_TRENDING'
                                        WHEN m.max_perd_id = t.perd_id THEN
                                         'CURRENT_TRENDING'
                                        ELSE
                                         'PLANNING'
                                      END,
                                      sp.last_updt_ts,
                                      u.user_frst_nm || ' ' ||
                                      u.user_last_nm) cline
        FROM (SELECT MAX(sls_perd_id) AS max_perd_id
                FROM dly_bilng
               WHERE mrkt_id = p_mrkt_id) m,
             mrkt_perd t
        LEFT JOIN mrkt_sls_perd sp
          ON sp.mrkt_id = t.mrkt_id
         AND sp.sls_perd_id = t.perd_id
        LEFT JOIN mps_user u
          ON u.user_nm = sp.last_updt_user_id
         AND u.sys_id = 1
       WHERE t.perd_typ = 'SC'
         AND t.mrkt_id = p_mrkt_id
         AND t.perd_id IN
             ((SELECT column_value FROM TABLE(p_sls_perd_list)))
       ORDER BY t.mrkt_id, t.perd_id DESC;
    g_run_id  NUMBER := app_plsql_output.generate_new_run_id;
    g_user_id VARCHAR(35) := USER;
    min_perd  NUMBER := p_sls_perd_list(1);
    max_perd  NUMBER := p_sls_perd_list(p_sls_perd_list.last);
    signature VARCHAR(100) := 'GET_CASH_VLU_MANTNC(p_mrkt_id: ' ||
                              p_mrkt_id || ', ';
  BEGIN
    signature := signature || 'p_sls_perd_list: ' || min_perd || '-' ||
                 max_perd || ')';
    app_plsql_log.register(g_user_id);
    app_plsql_output.set_run_id(g_run_id);
    app_plsql_log.set_context(g_user_id, 'PA_CASH_VLU_MANTNC', g_run_id);
    app_plsql_log.info(signature || ' start');
    FOR rec IN cc LOOP
      PIPE ROW(rec.cline);
    END LOOP;
    app_plsql_log.info(signature || ' stop');
  END;

  FUNCTION get_cash_vlu_mantnc_hist(p_mrkt_id       IN NUMBER,
                                    p_sls_perd_list IN number_array)
    RETURN obj_cash_val_mantnc_table
    PIPELINED AS
    CURSOR cc IS
      SELECT obj_cash_val_mantnc_line(perd_id,
                                      sct_cash_val,
                                      sct_r_factor,
                                      value_type,
                                      last_updt_ts,
                                      username) cline
        FROM (SELECT t.perd_id,
                     sp.sct_cash_val,
                     sp.sct_r_factor,
                     CASE
                       WHEN m.max_perd_id > t.perd_id THEN
                        'FINISHED_TRENDING'
                       WHEN m.max_perd_id = t.perd_id THEN
                        'CURRENT_TRENDING'
                       ELSE
                        'PLANNING'
                     END value_type,
                     sp.last_updt_ts,
                     substr(u.user_frst_nm || ' ' || u.user_last_nm, 1, 35) username
                FROM (SELECT MAX(sls_perd_id) AS max_perd_id
                        FROM dly_bilng
                       WHERE mrkt_id = p_mrkt_id) m,
                     mrkt_perd t
                LEFT JOIN mrkt_sls_perd sp -- actual values
                  ON sp.mrkt_id = t.mrkt_id
                 AND sp.sls_perd_id = t.perd_id
                LEFT JOIN mps_user u
                  ON u.user_nm = sp.last_updt_user_id
                 AND u.sys_id = 1
               WHERE t.perd_typ = 'SC'
                 AND t.mrkt_id = p_mrkt_id
                 AND t.perd_id IN
                     ((SELECT column_value FROM TABLE(p_sls_perd_list)))
              UNION
              SELECT t.perd_id,
                     sp.cash_val,
                     sp.r_factor,
                     'PREVIOUS VALUES' value_type,
                     sp.last_updt_ts,
                     substr(u.user_frst_nm || ' ' || u.user_last_nm, 1, 35) username
                FROM mrkt_perd t
                JOIN cash_val_rf_hist sp -- historic values
                  ON sp.mrkt_id = t.mrkt_id
                 AND sp.sls_perd_id = t.perd_id
                LEFT JOIN mps_user u
                  ON u.user_nm = sp.last_updt_user_id
                 AND u.sys_id = 1
               WHERE t.perd_typ = 'SC'
                 AND t.mrkt_id = p_mrkt_id
                 AND t.perd_id IN
                     ((SELECT column_value FROM TABLE(p_sls_perd_list))))
      --    ORDER BY PERD_ID desc, LAST_UPDT_TS desc
      ;
    g_run_id  NUMBER := app_plsql_output.generate_new_run_id;
    g_user_id VARCHAR(35) := USER;
    min_perd  NUMBER := p_sls_perd_list(1);
    max_perd  NUMBER := p_sls_perd_list(p_sls_perd_list.last);
    signature VARCHAR(100) := 'GET_CASH_VLU_MANTNC_HIST(p_mrkt_id: ' ||
                              p_mrkt_id || ', ';
  BEGIN
    signature := signature || 'p_sls_perd_list: ' || min_perd || '-' ||
                 max_perd || ')';
    app_plsql_log.register(g_user_id);
    app_plsql_output.set_run_id(g_run_id);
    app_plsql_log.set_context(g_user_id, 'PA_CASH_VLU_MANTNC', g_run_id);
    app_plsql_log.info(signature || ' start');
    FOR rec IN cc LOOP
      PIPE ROW(rec.cline);
    END LOOP;
    app_plsql_log.info(signature || ' stop');
    RETURN;
  END get_cash_vlu_mantnc_hist;

  PROCEDURE set_cash_vlu_mantnc(p_mrkt_id     IN NUMBER,
                                p_sls_perd_id IN NUMBER,
                                p_cash_val    IN NUMBER,
                                p_r_factor    IN NUMBER,
                                p_user_id     IN VARCHAR2,
                                p_stus        OUT NUMBER) IS
    /*********************************************************
    * INPUT p_r_factor IS NULL handled as do nothing with this field
    *
    * Possible OUT Values
    * 0 - success
    * 1 - New CASH_VAL value is negative, or NULL
    * 2 - database error in UPDATE MRKT_SLS_PERD or INSERT INTO CASH_VAL_RF_HIST statements
    * 3 - record to update in table MRKT_SLS_PERD is readonly (has records in DLY_BILNG)
    * 4 - period to update is not defined yet in table MRKT_PERD
    ******************************************************/
    max_perd_id           mrkt_sls_perd.sls_perd_id%TYPE;
    local_prcsng_dt       DATE := NULL;
    old_sct_cash_val      mrkt_sls_perd.sct_cash_val%TYPE;
    old_sct_r_factor      mrkt_sls_perd.sct_r_factor%TYPE;
    old_last_updt_user_id mrkt_sls_perd.last_updt_user_id%TYPE;
    mrkt_perd_exists      NUMBER;
    wtd                   CHAR(1) := 'N';
    ignore_r_factor       BOOLEAN := p_r_factor IS NULL;
  BEGIN
    p_stus := 0;
    IF nvl(p_cash_val, -1) < 0 THEN
      p_stus := 1;
    ELSE
      SELECT MAX(sls_perd_id)
        INTO max_perd_id
        FROM dly_bilng
       WHERE mrkt_id = p_mrkt_id;
      IF p_sls_perd_id > max_perd_id THEN
        -- Read current record for supplied market/campaign from MRKT_SLS_PERD
        SELECT COUNT(*)
          INTO mrkt_perd_exists
          FROM mrkt_sls_perd
         WHERE mrkt_id = p_mrkt_id
           AND sls_perd_id = p_sls_perd_id;
        IF mrkt_perd_exists = 1 THEN
          SELECT sct_cash_val, sct_r_factor, last_updt_user_id
            INTO old_sct_cash_val, old_sct_r_factor, old_last_updt_user_id
            FROM mrkt_sls_perd
           WHERE mrkt_id = p_mrkt_id
             AND sls_perd_id = p_sls_perd_id;
          -- If either the Cash Value or the R-Factor is different to current values
          IF old_sct_cash_val = p_cash_val
             AND old_last_updt_user_id = p_user_id
             AND (ignore_r_factor OR old_sct_r_factor = p_r_factor) THEN
            wtd := 'N'; -- No changes, nothing to do
          ELSE
            wtd := 'U'; -- previous value exists and different, so update
          END IF;
        ELSE
          SELECT COUNT(*)
            INTO mrkt_perd_exists
            FROM mrkt_perd
           WHERE mrkt_id = p_mrkt_id
             AND perd_id = p_sls_perd_id;
          IF mrkt_perd_exists = 1 THEN
            wtd := 'I'; -- The period still exists, Insert
          ELSE
            wtd := 'E'; -- changing non-existing period
          END IF;
        END IF;
        IF wtd = 'N' THEN
          NULL;
        ELSIF wtd = 'E' THEN
          p_stus := 4;
        ELSE
          -- Create the missing local processing date value
          local_prcsng_dt := NULL; --CASH_VLU_MANTNC.get_local_prcsng_dt(p_mrkt_id, p_sls_perd_id);
          DECLARE
            r_factor_to_set NUMBER;
          BEGIN
            IF ignore_r_factor THEN
              r_factor_to_set := nvl(old_sct_r_factor, NULL);
            ELSE
              r_factor_to_set := p_r_factor;
            END IF;
            SAVEPOINT before_changes;
            -- Upsert the appropriate record in MRKT_SLS_PERD to set new values including local processing date
            IF wtd = 'U' THEN
              UPDATE mrkt_sls_perd
                 SET mrkt_sls_perd.sct_cash_val      = p_cash_val,
                     mrkt_sls_perd.sct_r_factor      = r_factor_to_set,
                     mrkt_sls_perd.sct_prcsng_dt     = local_prcsng_dt,
                     mrkt_sls_perd.last_updt_user_id = p_user_id
               WHERE mrkt_id = p_mrkt_id
                 AND sls_perd_id = p_sls_perd_id;
            ELSE
              INSERT INTO mrkt_sls_perd
                (mrkt_id,
                 sls_perd_id,
                 sct_cash_val,
                 sct_r_factor,
                 sct_prcsng_dt,
                 creat_user_id,
                 last_updt_user_id)
              VALUES
                (p_mrkt_id,
                 p_sls_perd_id,
                 p_cash_val,
                 r_factor_to_set,
                 local_prcsng_dt,
                 p_user_id,
                 p_user_id);
            END IF;
            -- Insert a record into CASH_VAL_RF_HIST to record the previous values
            INSERT INTO cash_val_rf_hist
              (mrkt_id,
               sls_perd_id,
               cash_val,
               r_factor,
               prcsng_dt,
               last_updt_user_id)
              SELECT mrkt_id,
                     sls_perd_id,
                     p_cash_val,
                     r_factor_to_set,
                     local_prcsng_dt,
                     p_user_id
                FROM mrkt_sls_perd
               WHERE mrkt_id = p_mrkt_id
                 AND sls_perd_id = p_sls_perd_id;
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO before_changes;
              p_stus := 2;
          END;
        END IF;
      ELSE
        p_stus := 3;
      END IF;
    END IF;
  END;

  FUNCTION get_local_prcsng_dt(p_mrkt_id NUMBER, p_sls_perd NUMBER)
    RETURN DATE IS
    processing_dt    DATE := NULL;
    actual_sls_perd  NUMBER;
    before2_sls_perd NUMBER;
    bst_bilng_dt     DATE;
    est_bilng_dt     DATE;
    latest_prcsng_dt DATE;
  BEGIN
    WITH mrkt_perd2 AS
     (SELECT mrkt_id,
             perd_id,
             row_number() over(ORDER BY perd_id) AS rn,
             bst_bilng_dt
        FROM mrkt_perd
       WHERE mrkt_id = p_mrkt_id
         AND perd_typ = 'SC'),
    mrkt_perd3 AS
     (SELECT mrkt_id,
             perd_id,
             row_number() over(ORDER BY perd_id) AS rn,
             est_bilng_dt
        FROM mrkt_perd
       WHERE mrkt_id = p_mrkt_id
         AND perd_typ = 'SC')
    SELECT a1.perd_id, a2.perd_id, a1.bst_bilng_dt, a2.est_bilng_dt
      INTO actual_sls_perd, before2_sls_perd, bst_bilng_dt, est_bilng_dt
      FROM mrkt_perd2 a1
      LEFT JOIN mrkt_perd3 a2
        ON a2.rn = a1.rn - 2
     WHERE a1.perd_id = p_sls_perd;
    latest_prcsng_dt := get_latest_prcsng_dt(p_mrkt_id, actual_sls_perd);
    IF latest_prcsng_dt IS NOT NULL THEN
      processing_dt := least(latest_prcsng_dt, bst_bilng_dt);
    ELSIF before2_sls_perd IS NOT NULL THEN
      latest_prcsng_dt := get_latest_prcsng_dt(p_mrkt_id, before2_sls_perd);
      IF latest_prcsng_dt IS NOT NULL THEN
        processing_dt := least(latest_prcsng_dt, est_bilng_dt);
      END IF;
    END IF;
    RETURN processing_dt;
  END;

  FUNCTION get_latest_prcsng_dt(p_mrkt_id NUMBER, p_sls_perd NUMBER)
    RETURN DATE IS
    latest_prcsng_dt DATE;
  BEGIN
    SELECT MAX(prcsng_dt)
      INTO latest_prcsng_dt
      FROM dly_bilng
     WHERE mrkt_id = p_mrkt_id
       AND sls_perd_id = p_sls_perd
     GROUP BY mrkt_id, sls_perd_id;
    RETURN latest_prcsng_dt;
  EXCEPTION
    WHEN no_data_found THEN
      RETURN NULL;
  END;

  --------------------------- daily bilng adjustment ----------------------------------

  FUNCTION get_dly_bilng_adjstmnt(p_mrkt_id      IN NUMBER,
                                  p_sls_perd_id  IN NUMBER,
                                  p_offr_perd_id IN NUMBER,
                                  p_prcsng_dt    IN DATE)
    RETURN obj_dly_bilng_adjstmnt_table
    PIPELINED AS
    CURSOR cc IS
      SELECT obj_dly_bilng_adjstmnt_line(dly_bilng.dly_bilng_id,
                                         dly_bilng.fsc_cd,
                                         dly_bilng.prod_desc_txt,
                                         dly_bilng.sku_id,
                                         mrkt_sku.lcl_sku_nm,
                                         dly_bilng.sls_prc_amt,
                                         dly_bilng.nr_for_qty,
                                         dly_bilng.sls_prc_amt /
                                         dly_bilng.nr_for_qty,
                                         decode((SELECT 1
                                                  FROM dual
                                                 WHERE EXISTS (SELECT sku_prc_amt
                                                          FROM mrkt_perd_sku_prc mpsp
                                                         WHERE mpsp.mrkt_id =
                                                               p_mrkt_id
                                                           AND mpsp.offr_perd_id =
                                                               p_sls_perd_id
                                                           AND mpsp.sku_id =
                                                               mrkt_sku.sku_id
                                                           AND mpsp.prc_lvl_typ_cd = 'RP')
                                                   AND EXISTS
                                                 (SELECT hold_costs_ind
                                                          FROM sku_cost sc
                                                         WHERE sc.mrkt_id =
                                                               p_mrkt_id
                                                           AND sc.offr_perd_id =
                                                               p_offr_perd_id
                                                           AND sc.sku_id =
                                                               mrkt_sku.sku_id
                                                           AND cost_typ = 'P')
                                                   AND (pa_maps_public.get_sls_cls_cd(p_offr_perd_id,
                                                                                      p_mrkt_id,
                                                                                      mrkt_sku.avlbl_perd_id,
                                                                                      mrkt_sku.intrdctn_perd_id,
                                                                                      mrkt_sku.demo_ofs_nr,
                                                                                      mrkt_sku.demo_durtn_nr,
                                                                                      mrkt_sku.new_durtn_nr,
                                                                                      mrkt_sku.stus_perd_id,
                                                                                      mrkt_sku.dspostn_perd_id,
                                                                                      mrkt_sku.on_stus_perd_id) != '-1')),
                                                '1',
                                                'A',
                                                'N'),
                                         nvl2(offr_sku_line.sku_id, 'P', 'N'),
                                         dly_bilng.unit_qty,
                                         dly_bilng_adjstmnt.unit_qty,
                                         dly_bilng_adjstmnt.last_updt_ts,
                                         dly_bilng_adjstmnt.user_frst_nm || ' ' ||
                                         dly_bilng_adjstmnt.user_last_nm) cline
        FROM (WITH act_fsc AS (SELECT fsc_cd, MAX(strt_perd_id) max_perd_id
                                 FROM mrkt_fsc
                                WHERE mrkt_id = p_mrkt_id
                                  AND strt_perd_id <= p_sls_perd_id
                                  AND dltd_ind <> 'Y'
                                  AND dltd_ind <> 'y'
                                GROUP BY fsc_cd), all_fsc AS (SELECT *
                                                                FROM mrkt_fsc
                                                               WHERE mrkt_id =
                                                                     p_mrkt_id
                                                                 AND strt_perd_id <=
                                                                     p_sls_perd_id
                                                                 AND dltd_ind <> 'Y'
                                                                 AND dltd_ind <> 'y')
               SELECT dly_bilng.dly_bilng_id,
                      dly_bilng.unit_qty,
                      dly_bilng.sls_prc_amt,
                      dly_bilng.nr_for_qty,
                      dly_bilng.prcsng_dt,
                      all_fsc.fsc_cd,
                      all_fsc.sku_id,
                      all_fsc.prod_desc_txt
                 FROM dly_bilng, act_fsc, all_fsc
                WHERE dly_bilng.mrkt_id = p_mrkt_id
                  AND dly_bilng.offr_perd_id = p_offr_perd_id
                  AND dly_bilng.sls_perd_id = p_sls_perd_id
                  AND dly_bilng.fsc_cd = act_fsc.fsc_cd
                  AND act_fsc.fsc_cd = all_fsc.fsc_cd
                  AND act_fsc.max_perd_id = all_fsc.strt_perd_id
                  AND trunc(prcsng_dt) = trunc(p_prcsng_dt)
                ORDER BY fsc_cd, prcsng_dt) dly_bilng, (SELECT DISTINCT sku_id
                                                          FROM offr,
                                                               offr_sku_line
                                                         WHERE offr_sku_line.mrkt_id =
                                                               p_mrkt_id
                                                           AND offr.mrkt_id =
                                                               p_mrkt_id
                                                           AND offr.ver_id = 0
                                                           AND offr.offr_perd_id =
                                                               p_offr_perd_id
                                                           AND offr.offr_typ =
                                                               'CMP'
                                                           AND offr.offr_id =
                                                               offr_sku_line.offr_id
                                                           AND offr_sku_line.offr_perd_id =
                                                               p_offr_perd_id
                                                           AND offr_sku_line.dltd_ind <> 'Y'
                                                           AND offr_sku_line.dltd_ind <> 'y') offr_sku_line, (SELECT mu.user_frst_nm,
                                                                                                                     mu.user_last_nm,
                                                                                                                     dly_bilng_adjstmnt.*
                                                                                                                FROM (SELECT *
                                                                                                                        FROM mps_user
                                                                                                                       WHERE sys_id = 1) mu,
                                                                                                                     dly_bilng_adjstmnt
                                                                                                               WHERE mu.user_nm =
                                                                                                                     dly_bilng_adjstmnt.last_updt_user_id(+)
                                                                                                              
                                                                                                              ) dly_bilng_adjstmnt, mrkt_sku
                WHERE mrkt_sku.mrkt_id = p_mrkt_id
                  AND dly_bilng.sku_id = mrkt_sku.sku_id
                  AND dly_bilng.sku_id = offr_sku_line.sku_id(+)
                  AND dly_bilng.dly_bilng_id =
                      dly_bilng_adjstmnt.dly_bilng_id(+)
                ORDER BY dly_bilng.fsc_cd * 1;
  
  
    g_run_id  NUMBER := app_plsql_output.generate_new_run_id;
    g_user_id VARCHAR(35) := USER;
    signature VARCHAR(200) := 'GET_DLY_BILNG_ADJSTMNT(p_mrkt_id: ' ||
                              p_mrkt_id || ', p_sls_perd_id: ' ||
                              p_sls_perd_id || ', p_offr_perd_id: ' ||
                              p_offr_perd_id || ',  p_prcsng_dt: ' ||
                              p_prcsng_dt || ')';
  BEGIN
    app_plsql_log.register(g_user_id);
    app_plsql_output.set_run_id(g_run_id);
    app_plsql_log.set_context(g_user_id, 'PA_DLY_BILNG_ADJSTMNT', g_run_id);
    app_plsql_log.info(signature || ' start');
    FOR rec IN cc LOOP
      PIPE ROW(rec.cline);
    END LOOP;
    app_plsql_log.info(signature || ' stop');
  END get_dly_bilng_adjstmnt;

  FUNCTION get_dly_bilng_adjstmnt2(p_dly_bilng_id_list IN number_array)
    RETURN obj_dly_bilng_adjstmnt_table
    PIPELINED AS
    CURSOR cc IS
      WITH sdb AS
       (SELECT tt.column_value dly_bilng_id,
               dly_bilng.mrkt_id,
               dly_bilng.sls_perd_id,
               dly_bilng.offr_perd_id,
               dly_bilng.fsc_cd,
               dly_bilng.unit_qty,
               dly_bilng.sls_prc_amt,
               dly_bilng.nr_for_qty,
               dly_bilng.sku_id,
               dly_bilng.offr_sku_line_id
          FROM TABLE(p_dly_bilng_id_list) tt, dly_bilng
         WHERE tt.column_value = dly_bilng.dly_bilng_id),
      fscd AS
       (SELECT DISTINCT t.mrkt_id, t.fsc_cd, mfsc.prod_desc_txt
          FROM (SELECT MAX(mf.fsc_cd) fsc_cd,
                       MAX(mf.mrkt_id) mrkt_id,
                       MAX(mf.strt_perd_id) max_perd_id
                  FROM sdb db
                 INNER JOIN mrkt_fsc mf
                    ON db.fsc_cd = mf.fsc_cd
                 WHERE db.mrkt_id = mf.mrkt_id
                   AND db.sls_perd_id >= mf.strt_perd_id
                   AND mf.dltd_ind <> 'Y'
                   AND mf.dltd_ind <> 'y'
                 GROUP BY db.dly_bilng_id) t
         INNER JOIN mrkt_fsc mfsc
            ON t.fsc_cd = mfsc.fsc_cd
           AND t.mrkt_id = mfsc.mrkt_id
           AND t.max_perd_id = mfsc.strt_perd_id)
      SELECT obj_dly_bilng_adjstmnt_line(db.dly_bilng_id,
                                         db.fsc_cd,
                                         fscd.prod_desc_txt,
                                         db.sku_id,
                                         s.lcl_sku_nm,
                                         db.sls_prc_amt,
                                         db.nr_for_qty,
                                         db.sls_prc_amt / db.nr_for_qty,
                                         decode((SELECT 1
                                                  FROM dual
                                                 WHERE EXISTS (SELECT mpsp.sku_prc_amt
                                                          FROM mrkt_perd_sku_prc mpsp
                                                         WHERE mpsp.mrkt_id =
                                                               db.mrkt_id
                                                           AND mpsp.offr_perd_id =
                                                               db.offr_perd_id
                                                           AND mpsp.sku_id =
                                                               s.sku_id
                                                           AND mpsp.prc_lvl_typ_cd = 'RP')
                                                   AND EXISTS
                                                 (SELECT sc.hold_costs_ind
                                                          FROM sku_cost sc
                                                         WHERE sc.mrkt_id =
                                                               db.mrkt_id
                                                           AND sc.offr_perd_id =
                                                               db.offr_perd_id
                                                           AND sc.sku_id =
                                                               s.sku_id
                                                           AND sc.cost_typ = 'P')
                                                   AND pa_maps_public.get_sls_cls_cd(db.offr_perd_id,
                                                                                     db.mrkt_id,
                                                                                     s.avlbl_perd_id,
                                                                                     s.intrdctn_perd_id,
                                                                                     s.demo_ofs_nr,
                                                                                     s.demo_durtn_nr,
                                                                                     s.new_durtn_nr,
                                                                                     s.stus_perd_id,
                                                                                     s.dspostn_perd_id,
                                                                                     s.on_stus_perd_id) != '-1'),
                                                '1',
                                                'A',
                                                'N'),
                                         decode((SELECT COUNT(*)
                                                  FROM offr_sku_line osl
                                                 WHERE osl.offr_id IN
                                                       (SELECT o.offr_id
                                                          FROM offr o
                                                         WHERE o.mrkt_id =
                                                               db.mrkt_id
                                                           AND o.ver_id = 0
                                                           AND o.offr_typ =
                                                               'CMP'
                                                           AND o.offr_perd_id =
                                                               db.offr_perd_id)
                                                   AND osl.dltd_ind NOT IN
                                                       ('Y', 'y')
                                                   AND osl.sku_id = s.sku_id),
                                                0,
                                                'N',
                                                'P'),
                                         db.unit_qty,
                                         dbat.unit_qty,
                                         dbat.last_updt_ts,
                                         u.user_frst_nm || ' ' ||
                                         u.user_last_nm) cline
        FROM sdb db,
             fscd,
             mrkt_sku s,
             dly_bilng_adjstmnt dbat,
             (SELECT * FROM mps_user WHERE sys_id = 1) u
       WHERE db.mrkt_id = s.mrkt_id(+)
         AND db.sku_id = s.sku_id(+)
         AND dbat.last_updt_user_id = u.user_nm(+)
         AND db.mrkt_id = fscd.mrkt_id(+)
         AND db.fsc_cd = fscd.fsc_cd(+)
         AND db.dly_bilng_id = dbat.dly_bilng_id(+)
       ORDER BY db.fsc_cd * 1;
    g_run_id  NUMBER := app_plsql_output.generate_new_run_id;
    g_user_id VARCHAR(35) := USER;
    first_id  NUMBER := p_dly_bilng_id_list(1);
    last_id   NUMBER := p_dly_bilng_id_list(p_dly_bilng_id_list.last);
    signature VARCHAR(200) := 'After SET, GET_DLY_BILNG_ADJSTMNT2(';
  BEGIN
    signature := signature || 'p_dly_bilng_id_list: ' || first_id || '~' ||
                 last_id || ')';
    app_plsql_log.register(g_user_id);
    app_plsql_output.set_run_id(g_run_id);
    app_plsql_log.set_context(g_user_id, 'PA_DLY_BILNG_ADJSTMNT', g_run_id);
    app_plsql_log.info(signature || ' start');
    FOR rec IN cc LOOP
      PIPE ROW(rec.cline);
    END LOOP;
    app_plsql_log.info(signature || ' stop');
  END get_dly_bilng_adjstmnt2;

  PROCEDURE set_dly_bilng_adjstmnt(p_dly_bilng_id   IN NUMBER,
                                   p_new_bi24_units IN NUMBER,
                                   p_user_id        IN VARCHAR2,
                                   p_stus           OUT NUMBER) AS
    /*********************************************************
    * INPUT p_new_bi24_units IS NULL handled as record has to be deleted
    *
    * Possible OUT Values
    * 0 - success
    * 1 - New BI24 Units value is negative (not allowed)
    * 2 - database error in DELETE, UPDATE or INSERT statements
    * 3 - Obligatory foreign keys (DLY_BILNG) not found
    ******************************************************/
    counter1 NUMBER;
  BEGIN
    p_stus := 0;
    IF nvl(p_new_bi24_units, 0) >= 0 THEN
      SELECT COUNT(*)
        INTO counter1
        FROM dly_bilng
       WHERE dly_bilng_id = p_dly_bilng_id;
      IF counter1 > 0 THEN
        -- p_new_bi24_units IS NULL, the record if exists
        IF p_new_bi24_units IS NULL THEN
          BEGIN
            SAVEPOINT before_delete;
            DELETE FROM dly_bilng_adjstmnt
             WHERE dly_bilng_id = p_dly_bilng_id;
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO before_delete;
              p_stus := 2;
          END;
          -- otherwise upsert using p_new_bi24_units
        ELSE
          BEGIN
            SAVEPOINT before_upsert;
            MERGE INTO dly_bilng_adjstmnt trgt
            USING (SELECT p_dly_bilng_id t1 FROM dual) src
            ON (trgt.dly_bilng_id = src.t1)
            WHEN MATCHED THEN
              UPDATE
                 SET trgt.unit_qty          = p_new_bi24_units,
                     trgt.last_updt_user_id = p_user_id
            WHEN NOT MATCHED THEN
              INSERT
                (dly_bilng_id, unit_qty, creat_user_id, last_updt_user_id)
              VALUES
                (p_dly_bilng_id, p_new_bi24_units, p_user_id, p_user_id);
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO before_changes;
              p_stus := 2;
          END;
        END IF;
      ELSE
        p_stus := 3;
      END IF;
    ELSE
      p_stus := 1;
    END IF;
  END set_dly_bilng_adjstmnt;

  --------------------------- manual trend adjustment ----------------------------------

  FUNCTION get_trend_type_list RETURN obj_trend_type_table
    PIPELINED AS
    CURSOR cc IS
      SELECT obj_trend_type_line(sls_typ_id, sls_typ_nm) cline
        FROM sls_typ
       WHERE sls_typ_grp_id = 2;
  BEGIN
    FOR rec IN cc LOOP
      PIPE ROW(rec.cline);
    END LOOP;
  END get_trend_type_list;

  FUNCTION get_target_campaign(p_mrkt_id     IN NUMBER,
                               p_sls_perd_id IN NUMBER,
                               p_sls_typ_id  IN NUMBER) RETURN NUMBER AS
    res NUMBER;
  BEGIN
    SELECT pa_maps_public.perd_plus(p_mrkt_id, p_sls_perd_id, offst) target_perd_id
      INTO res
      FROM (SELECT MAX(mrkt_id) mrkt_id,
                   MAX(eff_sls_perd_id) eff_sls_perd_id,
                   MAX(sls_typ_id) sls_typ_id
              FROM trend_offst
             WHERE mrkt_id = p_mrkt_id
               AND eff_sls_perd_id <= p_sls_perd_id
               AND sls_typ_id = p_sls_typ_id)
      JOIN trend_offst
     USING (mrkt_id, eff_sls_perd_id, sls_typ_id);
    RETURN res;
  END get_target_campaign;

  FUNCTION get_sales_campaign(p_mrkt_id      IN NUMBER,
                              p_trgt_perd_id IN NUMBER,
                              p_sls_typ_id   IN NUMBER) RETURN NUMBER AS
    res NUMBER;
  BEGIN
    BEGIN
      WITH mp AS
       (SELECT mrkt_id, perd_id FROM mrkt_perd WHERE mrkt_id = p_mrkt_id),
      mto AS
       (SELECT MAX(mp.perd_id) perd_id,
               MAX(tof.eff_sls_perd_id) eff_sls_perd_id
          FROM mp
          LEFT JOIN trend_offst tof
            ON tof.mrkt_id = mp.mrkt_id
           AND tof.eff_sls_perd_id <= mp.perd_id
           AND tof.sls_typ_id = p_sls_typ_id
         GROUP BY mp.perd_id),
      tt AS
       (SELECT mp.mrkt_id,
               mp.perd_id sales_perd,
               pa_maps_public.perd_plus(mp.mrkt_id, mp.perd_id, tof.offst) target_perd
          FROM mp
          JOIN mto
            ON mto.perd_id = mp.perd_id
          JOIN trend_offst tof
            ON tof.mrkt_id = p_mrkt_id
           AND tof.sls_typ_id = p_sls_typ_id
           AND tof.eff_sls_perd_id = mto.eff_sls_perd_id)
      SELECT sales_perd
        INTO res
        FROM (SELECT sales_perd
                FROM tt
               WHERE target_perd = p_trgt_perd_id
               ORDER BY sales_perd DESC)
       WHERE rownum = 1;
    EXCEPTION
      WHEN no_data_found THEN
        res := NULL;
    END;
    RETURN res;
  END get_sales_campaign;

  PROCEDURE set_manl_trend_adjstmnt_new(p_mrkt_id      IN NUMBER,
                                        p_sls_perd_id  IN NUMBER,
                                        p_sls_typ_id   IN NUMBER,
                                        p_offst_lbl_id IN NUMBER,
                                        p_fsc_cd       IN NUMBER,
                                        p_sct_unit_qty IN NUMBER,
                                        p_user_id      IN VARCHAR2,
                                        p_stus         OUT NUMBER) AS
    /*********************************************************
    * INPUT p_sct_unit_qty IS NULL handled as record has to be deleted
    *
    * Possible OUT Values
    * 0 - success
    * 1 - New sct_unit_qty value is negative (not allowed)
    * 2 - database error in DELETE, UPDATE or INSERT statements
    * 3 - Obligatory foreign keys (SLS_TYP,MRKT_PERD) not found
    ******************************************************/
    counter1 NUMBER;
    counter2 NUMBER;
  BEGIN
    p_stus := 0;
    -- check if calue to set is not negative
    IF nvl(p_sct_unit_qty, 0) >= 0 THEN
      -- precheck constraints
      SELECT COUNT(*)
        INTO counter1
        FROM sls_typ
       WHERE sls_typ_id = p_sls_typ_id;
      SELECT COUNT(*)
        INTO counter2
        FROM mrkt_perd
       WHERE mrkt_id = p_mrkt_id
         AND perd_id = p_sls_perd_id;
      IF counter1 + counter2 = 2 THEN
        -- if p_new_bi24_units IS NULL, delete the record
        IF p_sct_unit_qty IS NULL THEN
          BEGIN
            SAVEPOINT before_delete;
            DELETE FROM sct_fsc_ovrrd
             WHERE mrkt_id = p_mrkt_id
               AND sls_perd_id = p_sls_perd_id
               AND sls_typ_id = p_sls_typ_id
               AND offst_lbl_id = p_offst_lbl_id
               AND fsc_cd = p_fsc_cd;
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO before_delete;
              p_stus := 2;
          END;
          -- otherwise upsert using p_new_bi24_units
        ELSE
          BEGIN
            SAVEPOINT before_upsert;
            MERGE INTO sct_fsc_ovrrd trgt
            USING (SELECT p_mrkt_id      t1,
                          p_sls_perd_id  t2,
                          p_sls_typ_id   t3,
                          p_offst_lbl_id t4,
                          p_fsc_cd       t5
                     FROM dual) src
            ON (trgt.mrkt_id = src.t1 AND trgt.sls_perd_id = src.t2 AND trgt.sls_typ_id = src.t3 AND trgt.offst_lbl_id = src.t4 AND trgt.fsc_cd = src.t5)
            WHEN MATCHED THEN
              UPDATE
                 SET trgt.sct_unit_qty      = p_sct_unit_qty,
                     trgt.last_updt_user_id = p_user_id
            WHEN NOT MATCHED THEN
              INSERT
                (mrkt_id,
                 sls_perd_id,
                 sls_typ_id,
                 fsc_cd,
                 offst_lbl_id,
                 sct_unit_qty,
                 creat_user_id,
                 last_updt_user_id)
              VALUES
                (p_mrkt_id,
                 p_sls_perd_id,
                 p_sls_typ_id,
                 p_fsc_cd,
                 p_offst_lbl_id,
                 p_sct_unit_qty,
                 p_user_id,
                 p_user_id);
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO before_changes;
              p_stus := 2;
          END;
        END IF;
      ELSE
        p_stus := 3;
      END IF;
    ELSE
      p_stus := 1;
    END IF;
  END set_manl_trend_adjstmnt_new;

  FUNCTION get_manl_trend_adjstmnt_new(p_mrkt_id      IN NUMBER,
                                       p_sls_perd_id  IN NUMBER,
                                       p_offst_lbl_id IN NUMBER,
                                       p_sls_typ_id   IN NUMBER)
    RETURN pa_manl_trend_adjstmnt_table
    PIPELINED AS
    l_module_name    VARCHAR2(30) := 'GET_MANL_TREND_ADJSTMNT';
    l_parameter_list VARCHAR2(2048) := '(p_mrkt_id: ' || to_char(p_mrkt_id) || ', ' ||
                                       'p_sls_perd_id: ' ||
                                       to_char(p_sls_perd_id) || ', ' ||
                                       'p_offst_lbl_id: ' ||
                                       to_char(p_offst_lbl_id) || ', ' ||
                                       'p_sls_typ_id: ' ||
                                       to_char(p_sls_typ_id) || ')';
    --
    l_run_id  NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id VARCHAR(35) := USER();
    --
    CURSOR cc IS
      WITH act_fsc AS
       (SELECT fsc_cd, MAX(strt_perd_id) max_perd_id
          FROM mrkt_fsc
         WHERE mrkt_id = p_mrkt_id
           AND strt_perd_id <= p_sls_perd_id
           AND dltd_ind <> 'Y'
           AND dltd_ind <> 'y'
         GROUP BY fsc_cd),
      all_fsc AS
       (SELECT *
          FROM mrkt_fsc
         WHERE mrkt_id = p_mrkt_id
           AND strt_perd_id <= p_sls_perd_id
           AND dltd_ind <> 'Y'
           AND dltd_ind <> 'y')
      SELECT pa_manl_trend_adjstmnt_line(all_fsc.fsc_cd,
                                         all_fsc.prod_desc_txt,
                                         all_fsc.sku_id,
                                         mrkt_sku.lcl_sku_nm,
                                         sct_fsc_ovrrd.offst_lbl_id,
                                         db.unit_qty,
                                         sct_fsc_ovrrd.sct_unit_qty,
                                         sct_fsc_ovrrd.last_updt_ts,
                                         substr(sct_fsc_ovrrd.user_frst_nm || ' ' ||
                                                sct_fsc_ovrrd.user_last_nm,
                                                1,
                                                35)) cline
        FROM (WITH mesp AS (SELECT dly_bilng_mtch_id
                              FROM mrkt_eff_sls_perd
                             WHERE mrkt_id = p_mrkt_id
                               AND eff_sls_perd_id =
                                   (SELECT MAX(mesp.eff_sls_perd_id)
                                      FROM mrkt_eff_sls_perd mesp
                                     WHERE mesp.mrkt_id = p_mrkt_id
                                       AND mesp.eff_sls_perd_id <=
                                           p_sls_perd_id))
               SELECT db.fsc_cd, SUM(db.unit_qty) unit_qty, dbc.sls_typ_id
                 FROM dly_bilng        db,
                      dly_bilng_cntrl  dbc,
                      mesp,
                      mrkt_config_item mci
                WHERE nvl(dbc.lcl_bilng_actn_cd, db.lcl_bilng_actn_cd) =
                      db.lcl_bilng_actn_cd
                  AND nvl(dbc.lcl_bilng_tran_typ, db.lcl_bilng_tran_typ) =
                      db.lcl_bilng_tran_typ
                  AND nvl(dbc.lcl_bilng_offr_typ, db.lcl_bilng_offr_typ) =
                      db.lcl_bilng_offr_typ
                  AND nvl(dbc.lcl_bilng_defrd_cd, db.lcl_bilng_defrd_cd) =
                      db.lcl_bilng_defrd_cd
                  AND nvl(dbc.lcl_bilng_shpng_cd, db.lcl_bilng_shpng_cd) =
                      db.lcl_bilng_shpng_cd
                  AND mesp.dly_bilng_mtch_id = dbc.dly_bilng_mtch_id
                  AND mci.mrkt_id = db.mrkt_id
                  AND mci.mrkt_config_item_val_txt = dbc.sls_typ_id
                  AND mci.config_item_id = 10000
                  AND db.mrkt_id = p_mrkt_id
                  AND db.sls_perd_id = p_sls_perd_id
                GROUP BY db.fsc_cd, dbc.sls_typ_id) db, act_fsc, all_fsc, mrkt_sku, (SELECT sct_fsc_ovrrd.*,
                                                                                            nvl(mu.user_frst_nm,
                                                                                                'USER_ID:') user_frst_nm,
                                                                                            nvl(mu.user_last_nm,
                                                                                                sct_fsc_ovrrd.last_updt_user_id) user_last_nm
                                                                                       FROM sct_fsc_ovrrd,
                                                                                            (SELECT DISTINCT user_nm,
                                                                                                             user_frst_nm,
                                                                                                             user_last_nm
                                                                                               FROM mps_user
                                                                                              WHERE mps_user.sys_id = 1) mu
                                                                                      WHERE sct_fsc_ovrrd.last_updt_user_id =
                                                                                            mu.user_nm(+)
                                                                                        AND sct_fsc_ovrrd.mrkt_id =
                                                                                            p_mrkt_id
                                                                                        AND sct_fsc_ovrrd.sls_perd_id =
                                                                                            p_sls_perd_id
                                                                                        AND sct_fsc_ovrrd.sls_typ_id =
                                                                                            p_sls_typ_id
                                                                                        AND sct_fsc_ovrrd.offst_lbl_id =
                                                                                            p_offst_lbl_id) sct_fsc_ovrrd
                WHERE db.fsc_cd = act_fsc.fsc_cd
                  AND act_fsc.fsc_cd = all_fsc.fsc_cd
                  AND act_fsc.max_perd_id = all_fsc.strt_perd_id
                  AND all_fsc.sku_id = mrkt_sku.sku_id
                  AND mrkt_sku.mrkt_id = p_mrkt_id
                  AND sct_fsc_ovrrd.fsc_cd(+) * 1 = db.fsc_cd * 1
                ORDER BY db.fsc_cd * 1;
  
  
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || l_parameter_list || ' start');
    FOR rec IN cc LOOP
      PIPE ROW(rec.cline);
    END LOOP;
    app_plsql_log.info(l_module_name || l_parameter_list || ' stop');
  END get_manl_trend_adjstmnt_new;

  FUNCTION get_manl_trend_adjstmnt2_new(p_mrkt_id      IN NUMBER,
                                        p_sls_perd_id  IN NUMBER,
                                        p_offst_lbl_id IN NUMBER,
                                        p_sls_typ_id   IN NUMBER,
                                        p_fsc_cd_array IN number_array)
    RETURN pa_manl_trend_adjstmnt_table
    PIPELINED AS
    -- for LOG
    l_module_name    VARCHAR2(30) := 'GET_MANL_TREND_ADJSTMNT2';
    l_parameter_list VARCHAR2(2048) := '(p_mrkt_id: ' || to_char(p_mrkt_id) || ', ' ||
                                       'p_sls_perd_id: ' ||
                                       to_char(p_sls_perd_id) || ', ' ||
                                       'p_offst_lbl_id: ' ||
                                       to_char(p_offst_lbl_id) || ', ' ||
                                       'p_sls_typ_id: ' ||
                                       to_char(p_sls_typ_id) || ', ' ||
                                       'p_fsc_cd_array: ' ||
                                       to_char(p_fsc_cd_array(p_fsc_cd_array.first)) || '~' ||
                                       to_char(p_fsc_cd_array(p_fsc_cd_array.last)) || ')';
    --
    l_run_id  NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id VARCHAR(35) := USER();
    --
    CURSOR cc IS
      WITH act_fsc AS
       (SELECT fsc_cd, MAX(strt_perd_id) max_perd_id
          FROM mrkt_fsc
         WHERE mrkt_id = p_mrkt_id
           AND strt_perd_id <= p_sls_perd_id
           AND dltd_ind <> 'Y'
           AND dltd_ind <> 'y'
           AND mrkt_fsc.fsc_cd IN
               (SELECT column_value FROM TABLE(p_fsc_cd_array))
         GROUP BY fsc_cd),
      all_fsc AS
       (SELECT *
          FROM mrkt_fsc
         WHERE mrkt_id = p_mrkt_id
           AND strt_perd_id <= p_sls_perd_id
           AND dltd_ind <> 'Y'
           AND dltd_ind <> 'y'
           AND mrkt_fsc.fsc_cd IN
               (SELECT column_value FROM TABLE(p_fsc_cd_array)))
      SELECT pa_manl_trend_adjstmnt_line(all_fsc.fsc_cd,
                                         all_fsc.prod_desc_txt,
                                         all_fsc.sku_id,
                                         mrkt_sku.lcl_sku_nm,
                                         sct_fsc_ovrrd.offst_lbl_id,
                                         db.unit_qty,
                                         sct_fsc_ovrrd.sct_unit_qty,
                                         sct_fsc_ovrrd.last_updt_ts,
                                         substr(sct_fsc_ovrrd.user_frst_nm || ' ' ||
                                                sct_fsc_ovrrd.user_last_nm,
                                                1,
                                                35)) cline
        FROM (WITH mesp AS (SELECT dly_bilng_mtch_id
                              FROM mrkt_eff_sls_perd
                             WHERE mrkt_id = p_mrkt_id
                               AND eff_sls_perd_id =
                                   (SELECT MAX(mesp.eff_sls_perd_id)
                                      FROM mrkt_eff_sls_perd mesp
                                     WHERE mesp.mrkt_id = p_mrkt_id
                                       AND mesp.eff_sls_perd_id <=
                                           p_sls_perd_id))
               SELECT db.fsc_cd, SUM(db.unit_qty) unit_qty, dbc.sls_typ_id
                 FROM dly_bilng        db,
                      dly_bilng_cntrl  dbc,
                      mesp,
                      mrkt_config_item mci
                WHERE nvl(dbc.lcl_bilng_actn_cd, db.lcl_bilng_actn_cd) =
                      db.lcl_bilng_actn_cd
                  AND nvl(dbc.lcl_bilng_tran_typ, db.lcl_bilng_tran_typ) =
                      db.lcl_bilng_tran_typ
                  AND nvl(dbc.lcl_bilng_offr_typ, db.lcl_bilng_offr_typ) =
                      db.lcl_bilng_offr_typ
                  AND nvl(dbc.lcl_bilng_defrd_cd, db.lcl_bilng_defrd_cd) =
                      db.lcl_bilng_defrd_cd
                  AND nvl(dbc.lcl_bilng_shpng_cd, db.lcl_bilng_shpng_cd) =
                      db.lcl_bilng_shpng_cd
                  AND mesp.dly_bilng_mtch_id = dbc.dly_bilng_mtch_id
                  AND mci.mrkt_id = db.mrkt_id
                  AND mci.mrkt_config_item_val_txt = dbc.sls_typ_id
                  AND mci.config_item_id = 10000
                  AND db.mrkt_id = p_mrkt_id
                  AND db.sls_perd_id = p_sls_perd_id
                GROUP BY db.fsc_cd, dbc.sls_typ_id) db, act_fsc, all_fsc, mrkt_sku, (SELECT sct_fsc_ovrrd.*,
                                                                                            nvl(mu.user_frst_nm,
                                                                                                'USER_ID:') user_frst_nm,
                                                                                            nvl(mu.user_last_nm,
                                                                                                sct_fsc_ovrrd.last_updt_user_id) user_last_nm
                                                                                       FROM sct_fsc_ovrrd,
                                                                                            (SELECT DISTINCT user_nm,
                                                                                                             user_frst_nm,
                                                                                                             user_last_nm
                                                                                               FROM mps_user
                                                                                              WHERE mps_user.sys_id = 1) mu
                                                                                      WHERE sct_fsc_ovrrd.last_updt_user_id =
                                                                                            mu.user_nm(+)
                                                                                        AND sct_fsc_ovrrd.mrkt_id =
                                                                                            p_mrkt_id
                                                                                        AND sct_fsc_ovrrd.sls_perd_id =
                                                                                            p_sls_perd_id
                                                                                        AND sct_fsc_ovrrd.sls_typ_id =
                                                                                            p_sls_typ_id
                                                                                        AND sct_fsc_ovrrd.offst_lbl_id =
                                                                                            p_offst_lbl_id) sct_fsc_ovrrd
                WHERE db.fsc_cd = act_fsc.fsc_cd
                  AND act_fsc.fsc_cd = all_fsc.fsc_cd
                  AND act_fsc.max_perd_id = all_fsc.strt_perd_id
                  AND all_fsc.sku_id = mrkt_sku.sku_id
                  AND mrkt_sku.mrkt_id = p_mrkt_id
                  AND sct_fsc_ovrrd.fsc_cd(+) * 1 = db.fsc_cd * 1
                  AND db.fsc_cd IN
                      (SELECT column_value FROM TABLE(p_fsc_cd_array))
                ORDER BY db.fsc_cd * 1;
  
  
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info('After SET, ' || l_module_name || l_parameter_list ||
                       ' start');
    FOR rec IN cc LOOP
      PIPE ROW(rec.cline);
    END LOOP;
    app_plsql_log.info('After SET, ' || l_module_name || l_parameter_list ||
                       ' stop');
  END get_manl_trend_adjstmnt2_new;

  --------------------------- sku bias maintenance ----------------------------------

  FUNCTION get_sku_bias_new(p_mrkt_id     IN NUMBER, -- 68
                            p_sls_typ_id  IN NUMBER, -- 3
                            p_sls_perd_id IN NUMBER -- 20170305
                            ) RETURN pa_sku_bias_mantnc_table
    PIPELINED AS
    -- for LOG
    l_module_name    VARCHAR2(30) := 'GET_SKU_BIAS';
    l_parameter_list VARCHAR2(512) := '(p_mrkt_id: ' || to_char(p_mrkt_id) || ', ' ||
                                      'p_sls_typ_id: ' ||
                                      to_char(p_mrkt_id) || ', ' ||
                                      'p_sls_perd_id: ' ||
                                      to_char(p_sls_perd_id) || ')';
    --
    l_run_id  NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id VARCHAR(35) := USER();
    --
    CURSOR cc IS
      WITH mpsb AS
       (SELECT psb.sku_id,
               psb.sls_typ_id,
               psb.bias_pct,
               psb.last_updt_ts,
               u.user_frst_nm || ' ' || u.user_last_nm last_updt_nm
          FROM mrkt_perd_sku_bias psb
          LEFT JOIN mps_user u
            ON psb.last_updt_user_id = u.user_nm
           AND u.sys_id = 1
         WHERE psb.mrkt_id = p_mrkt_id
           AND psb.sls_perd_id = p_sls_perd_id
           AND psb.sls_typ_id = p_sls_typ_id),
      act_fsc AS
       (SELECT mrkt_fsc.sku_id, MAX(mrkt_fsc.strt_perd_id) max_perd_id
          FROM mrkt_fsc
         WHERE mrkt_fsc.mrkt_id = p_mrkt_id
           AND mrkt_fsc.strt_perd_id <= p_sls_perd_id
         GROUP BY mrkt_fsc.sku_id),
      all_fsc AS
       (SELECT mrkt_fsc.strt_perd_id,
               mrkt_fsc.fsc_cd,
               mrkt_fsc.sku_id,
               mrkt_fsc.prod_desc_txt
          FROM mrkt_fsc
         WHERE mrkt_fsc.strt_perd_id <= p_sls_perd_id
           AND mrkt_fsc.mrkt_id = p_mrkt_id)
      SELECT pa_sku_bias_mantnc_line(all_fsc.fsc_cd,
                                     all_fsc.prod_desc_txt,
                                     sku.sku_id,
                                     sku.lcl_sku_nm,
                                     'A',
                                     nvl2(offr_sku_line.sku_id, 'P', 'N'),
                                     p_sls_typ_id,
                                     mpsb.bias_pct,
                                     mpsb.last_updt_ts,
                                     mpsb.last_updt_nm) cline
        FROM act_fsc,
             all_fsc,
             mpsb,
             (SELECT sms.mrkt_id, sms.sku_id, sms.lcl_sku_nm, p_sls_typ_id
                FROM (SELECT mrkt_sku.mrkt_id AS mrkt_id,
                             mrkt_sku.sku_id,
                             mrkt_sku.lcl_sku_nm
                        FROM mrkt_sku
                       WHERE mrkt_sku.mrkt_id = p_mrkt_id
                         AND mrkt_sku.dltd_ind NOT IN ('Y', 'y')
                         AND pa_maps.get_sls_cls_cd(p_sls_perd_id,
                                                    p_mrkt_id,
                                                    mrkt_sku.avlbl_perd_id,
                                                    mrkt_sku.intrdctn_perd_id,
                                                    mrkt_sku.demo_ofs_nr,
                                                    mrkt_sku.demo_durtn_nr,
                                                    mrkt_sku.new_durtn_nr,
                                                    mrkt_sku.stus_perd_id,
                                                    mrkt_sku.dspostn_perd_id,
                                                    mrkt_sku.on_stus_perd_id) > -1) sms,
                     (SELECT DISTINCT sku_id
                        FROM sku_reg_prc
                       WHERE sku_reg_prc.mrkt_id = p_mrkt_id
                         AND sku_reg_prc.offr_perd_id = p_sls_perd_id) sku_reg_prc,
                     (SELECT DISTINCT sku_id
                        FROM sku_cost
                       WHERE sku_cost.mrkt_id = p_mrkt_id
                         AND sku_cost.offr_perd_id = p_sls_perd_id
                         AND sku_cost.cost_typ = 'P') sku_cost
               WHERE sms.sku_id = sku_reg_prc.sku_id
                 AND sms.sku_id = sku_cost.sku_id) sku,
             (SELECT DISTINCT offr_sku_line.sku_id
                FROM offr, offr_sku_line
               WHERE offr.offr_id = offr_sku_line.offr_id
                 AND offr_sku_line.mrkt_id = p_mrkt_id
                 AND offr.mrkt_id = p_mrkt_id
                 AND offr.ver_id = 0
                 AND offr.offr_perd_id = p_sls_perd_id
                 AND offr.offr_typ = 'CMP'
                 AND offr_sku_line.offr_perd_id = p_sls_perd_id
                 AND offr_sku_line.dltd_ind NOT IN ('Y', 'y')) offr_sku_line
       WHERE sku.sku_id = offr_sku_line.sku_id(+)
         AND sku.sku_id = act_fsc.sku_id(+)
         AND act_fsc.sku_id = all_fsc.sku_id
         AND act_fsc.max_perd_id = all_fsc.strt_perd_id
         AND sku.sku_id = mpsb.sku_id(+)
      --         AND sku.sls_typ_id = mpsb.sls_typ_id(+)
       ORDER BY all_fsc.fsc_cd * 1;
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || l_parameter_list || ' start');
    FOR rec IN cc LOOP
      PIPE ROW(rec.cline);
    END LOOP;
    app_plsql_log.info(l_module_name || l_parameter_list || ' stop');
  END get_sku_bias_new;

  FUNCTION get_sku_bias2_new(p_mrkt_id      IN NUMBER,
                             p_sls_perd_id  IN NUMBER,
                             p_sls_typ_id   IN NUMBER,
                             p_sku_id_array IN number_array)
    RETURN pa_sku_bias_mantnc_table
    PIPELINED AS
    -- for LOG
    l_module_name    VARCHAR2(30) := 'GET_SKU_BIAS2';
    l_parameter_list VARCHAR2(512) := '(p_mrkt_id: ' || to_char(p_mrkt_id) || ', ' ||
                                      'p_sls_perd_id: ' ||
                                      to_char(p_sls_perd_id) || ', ' ||
                                      'p_sls_typ_id: ' ||
                                      to_char(p_sls_typ_id) || ', ' ||
                                      'p_sku_id_array: ' ||
                                      to_char(p_sku_id_array.first) || '~' ||
                                      to_char(p_sku_id_array.last) || ')';
    --
    l_run_id  NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id VARCHAR(35) := USER();
    --
    CURSOR cc IS
      WITH mpsb AS
       (SELECT psb.sku_id,
               psb.bias_pct,
               psb.last_updt_ts,
               u.user_frst_nm || ' ' || u.user_last_nm last_updt_nm
          FROM mrkt_perd_sku_bias psb
          LEFT JOIN mps_user u
            ON psb.last_updt_user_id = u.user_nm
           AND u.sys_id = 1
         WHERE psb.mrkt_id = p_mrkt_id
           AND psb.sls_perd_id = p_sls_perd_id
           AND psb.sls_typ_id = p_sls_typ_id
           AND psb.sku_id IN
               (SELECT column_value FROM TABLE(p_sku_id_array))),
      act_fsc AS
       (SELECT mrkt_fsc.sku_id, MAX(mrkt_fsc.strt_perd_id) max_perd_id
          FROM mrkt_fsc
         WHERE mrkt_fsc.mrkt_id = p_mrkt_id
           AND mrkt_fsc.strt_perd_id <= p_sls_perd_id
           AND sku_id IN (SELECT column_value FROM TABLE(p_sku_id_array))
         GROUP BY mrkt_fsc.sku_id),
      all_fsc AS
       (SELECT mrkt_fsc.strt_perd_id,
               mrkt_fsc.fsc_cd,
               mrkt_fsc.sku_id,
               mrkt_fsc.prod_desc_txt
          FROM mrkt_fsc
         WHERE mrkt_fsc.strt_perd_id <= p_sls_perd_id
           AND mrkt_fsc.mrkt_id = p_mrkt_id
           AND sku_id IN (SELECT column_value FROM TABLE(p_sku_id_array)))
      SELECT pa_sku_bias_mantnc_line(all_fsc.fsc_cd,
                                     all_fsc.prod_desc_txt,
                                     sku.sku_id,
                                     sku.lcl_sku_nm,
                                     'A',
                                     nvl2(offr_sku_line.sku_id, 'P', 'N'),
                                     p_sls_typ_id,
                                     mpsb.bias_pct,
                                     mpsb.last_updt_ts,
                                     mpsb.last_updt_nm) cline
        FROM act_fsc,
             all_fsc,
             mpsb,
             (SELECT mrkt_sku.*
                FROM (SELECT *
                        FROM mrkt_sku
                       WHERE mrkt_sku.mrkt_id = p_mrkt_id
                         AND mrkt_sku.dltd_ind NOT IN ('Y', 'y')
                         AND pa_maps.get_sls_cls_cd(p_sls_perd_id,
                                                    p_mrkt_id,
                                                    mrkt_sku.avlbl_perd_id,
                                                    mrkt_sku.intrdctn_perd_id,
                                                    mrkt_sku.demo_ofs_nr,
                                                    mrkt_sku.demo_durtn_nr,
                                                    mrkt_sku.new_durtn_nr,
                                                    mrkt_sku.stus_perd_id,
                                                    mrkt_sku.dspostn_perd_id,
                                                    mrkt_sku.on_stus_perd_id) > -1) mrkt_sku,
                     (SELECT *
                        FROM sku_reg_prc
                       WHERE mrkt_id = p_mrkt_id
                         AND offr_perd_id = p_sls_perd_id) sku_reg_prc,
                     (SELECT *
                        FROM sku_cost
                       WHERE mrkt_id = p_mrkt_id
                         AND offr_perd_id = p_sls_perd_id
                         AND cost_typ = 'P') sku_cost
               WHERE mrkt_sku.sku_id = sku_reg_prc.sku_id
                 AND mrkt_sku.sku_id = sku_cost.sku_id
                 AND mrkt_sku.sku_id IN
                     (SELECT column_value FROM TABLE(p_sku_id_array))) sku,
             (SELECT DISTINCT sku_id
                FROM offr, offr_sku_line
               WHERE offr.offr_id = offr_sku_line.offr_id
                 AND offr_sku_line.mrkt_id = p_mrkt_id
                 AND offr.mrkt_id = p_mrkt_id
                 AND offr.ver_id = 0
                 AND offr.offr_perd_id = p_sls_perd_id
                 AND offr.offr_typ = 'CMP'
                 AND offr_sku_line.offr_perd_id = p_sls_perd_id
                 AND offr_sku_line.dltd_ind NOT IN ('Y', 'y')
                 AND offr_sku_line.sku_id IN
                     (SELECT column_value FROM TABLE(p_sku_id_array))) offr_sku_line
       WHERE sku.sku_id = offr_sku_line.sku_id(+)
         AND sku.sku_id = act_fsc.sku_id(+)
         AND act_fsc.sku_id = all_fsc.sku_id
         AND act_fsc.max_perd_id = all_fsc.strt_perd_id
         AND sku.sku_id = mpsb.sku_id(+)
       ORDER BY all_fsc.fsc_cd * 1;
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info('After SET, ' || l_module_name || l_parameter_list ||
                       ' start');
    FOR rec IN cc LOOP
      PIPE ROW(rec.cline);
    END LOOP;
    app_plsql_log.info('After SET, ' || l_module_name || l_parameter_list ||
                       ' stop');
  END get_sku_bias2_new;

  PROCEDURE set_sku_bias_new(p_mrkt_id      IN NUMBER,
                             p_sls_perd_id  IN NUMBER,
                             p_sls_typ_id   IN NUMBER,
                             p_sku_id       IN NUMBER,
                             p_new_sku_bias IN NUMBER,
                             p_user_id      IN VARCHAR2,
                             p_stus         OUT NUMBER) AS
    -- TODO - check if the sku is active, if not refuse handling the record
    /*********************************************************
    * INPUT p_new_sku_bias IS NULL means do nothing
    *
    * Possible OUT Values
    * 0 - success
    * 1 - New BIAS_PCT value is out of range 0 to 1000
    * 2 - database error in DELETE, UPDATE or INSERT statements
    * 3 - Obligatory foreign keys (MRKT_SKU and MRKT_PERD) not found
    * 4 - Inactive SKU, change refused
    ******************************************************/
    counter1  NUMBER;
    counter2  NUMBER;
    counter3  NUMBER;
    phcounter NUMBER;
  BEGIN
    p_stus := 0;
    -- p_new_sku_bias IS NULL means do nothing
    IF p_new_sku_bias IS NULL THEN
      NULL;
      -- p_new_sku_bias must be between 0 and 1000
    ELSIF p_new_sku_bias BETWEEN 0 AND 1000 THEN
      SELECT COUNT(*)
        INTO counter1
        FROM mrkt_perd_sku_prc mpsp
       WHERE mpsp.mrkt_id = p_mrkt_id
         AND mpsp.offr_perd_id = p_sls_perd_id
         AND mpsp.sku_id = p_sku_id
         AND mpsp.prc_lvl_typ_cd = 'RP';
      SELECT COUNT(*)
        INTO counter2
        FROM sku_cost sc
       WHERE sc.mrkt_id = p_mrkt_id
         AND sc.offr_perd_id = p_sls_perd_id
         AND sc.sku_id = p_sku_id
         AND cost_typ = 'P';
      SELECT ph,
             CASE
               WHEN pa_maps_public.get_sls_cls_cd(p_sls_perd_id,
                                                  p_mrkt_id,
                                                  s.avlbl_perd_id,
                                                  s.intrdctn_perd_id,
                                                  s.demo_ofs_nr,
                                                  s.demo_durtn_nr,
                                                  s.new_durtn_nr,
                                                  s.stus_perd_id,
                                                  s.dspostn_perd_id,
                                                  s.on_stus_perd_id) > '-1' THEN
                1
               ELSE
                0
             END ind
        INTO phcounter, counter3
        FROM (SELECT 1 ph FROM dual) dd
        LEFT JOIN mrkt_sku s
          ON s.mrkt_id = p_mrkt_id
         AND s.sku_id = p_sku_id;
      IF counter1 + counter2 + counter3 = 3 THEN
        -- active SKU
        -- upsert using p_new_bias
        SELECT COUNT(*)
          INTO counter1
          FROM mrkt_perd
         WHERE mrkt_id = p_mrkt_id
           AND perd_id = p_sls_perd_id;
        SELECT COUNT(*)
          INTO counter2
          FROM mrkt_sku
         WHERE mrkt_id = p_mrkt_id
           AND sku_id = p_sku_id;
        IF counter1 + counter2 = 2 THEN
          BEGIN
            SAVEPOINT before_upsert;
            MERGE INTO mrkt_perd_sku_bias trgt
            USING (SELECT p_mrkt_id     t1,
                          p_sls_perd_id t2,
                          p_sku_id      t3,
                          p_sls_typ_id  AS t4
                     FROM dual) src
            ON (trgt.mrkt_id = src.t1 AND trgt.sls_perd_id = src.t2 AND trgt.sku_id = src.t3 AND trgt.sls_typ_id = src.t4)
            WHEN MATCHED THEN
              UPDATE
                 SET trgt.bias_pct          = p_new_sku_bias,
                     trgt.last_updt_user_id = p_user_id
            WHEN NOT MATCHED THEN
              INSERT
                (mrkt_id,
                 sls_perd_id,
                 sls_typ_id,
                 sku_id,
                 bias_pct,
                 creat_user_id,
                 last_updt_user_id)
              VALUES
                (p_mrkt_id,
                 p_sls_perd_id,
                 p_sls_typ_id,
                 p_sku_id,
                 p_new_sku_bias,
                 p_user_id,
                 p_user_id);
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO before_changes;
              p_stus := 2; -- Database Error
          END;
        ELSE
          p_stus := 3; -- Missing foreign keys
        END IF; -- cc2
      ELSE
        p_stus := 4; -- Passive SKU
      END IF; --ccc3
    ELSE
      p_stus := 1; -- Negative value
    END IF; -- 1 1000
  END set_sku_bias_new;

  --------------------------- reports ----------------------------------

  FUNCTION sct_trend_check_rpt(p_mrkt_id            IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                               p_campgn_sls_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE)
    RETURN sct_trend_check_rpt_table
    PIPELINED AS
  
    l_actual_perd_id NUMBER := p_campgn_sls_perd_id;
    l_next_perd_id   NUMBER := pa_maps_public.perd_plus(p_mrkt_id,
                                                        p_campgn_sls_perd_id,
                                                        1);
    l_next1_perd_id  NUMBER := pa_maps_public.perd_plus(p_mrkt_id,
                                                        p_campgn_sls_perd_id,
                                                        2);
    l_prev_perd_id   NUMBER := pa_maps_public.perd_plus(p_mrkt_id,
                                                        p_campgn_sls_perd_id,
                                                        -1);
  
    l_last_accs_dt DATE;
  
    CURSOR cc IS
      WITH details AS
       (SELECT CASE
                 WHEN dstrbtd_mrkt_sls.sls_perd_id = l_actual_perd_id
                      AND dstrbtd_mrkt_sls.sls_typ_id = supply_bst_id THEN
                  1
                 WHEN dstrbtd_mrkt_sls.sls_perd_id = l_next_perd_id
                      AND dstrbtd_mrkt_sls.sls_typ_id = supply_est_id THEN
                  2
                 WHEN dstrbtd_mrkt_sls.sls_perd_id = l_next1_perd_id
                      AND dstrbtd_mrkt_sls.sls_typ_id = supply_est_id THEN
                  3
                 ELSE
                  4
               END otr,
               pa_maps_public.get_mstr_fsc_cd(p_mrkt_id => p_mrkt_id,
                                              p_sku_id  => offr_sku_line.sku_id,
                                              p_perd_id => dstrbtd_mrkt_sls.sls_perd_id) fsc_cd,
               dstrbtd_mrkt_sls.sls_perd_id,
               dstrbtd_mrkt_sls.sls_typ_id,
               round(SUM(nvl(dstrbtd_mrkt_sls.unit_qty, 0))) units
          FROM dstrbtd_mrkt_sls, offr_sku_line, offr_prfl_prc_point, offr
         WHERE dstrbtd_mrkt_sls.mrkt_id = p_mrkt_id
           AND ((dstrbtd_mrkt_sls.sls_perd_id = l_actual_perd_id AND
               dstrbtd_mrkt_sls.sls_typ_id = supply_bst_id) OR
               (dstrbtd_mrkt_sls.sls_perd_id = l_next_perd_id AND
               dstrbtd_mrkt_sls.sls_typ_id = supply_est_id) OR
               (dstrbtd_mrkt_sls.sls_perd_id = l_next1_perd_id AND
               dstrbtd_mrkt_sls.sls_typ_id = supply_est_id) OR
               (dstrbtd_mrkt_sls.sls_perd_id = l_actual_perd_id AND
               dstrbtd_mrkt_sls.sls_typ_id = 2))
           AND dstrbtd_mrkt_sls.offr_sku_line_id =
               offr_sku_line.offr_sku_line_id
           AND offr_sku_line.offr_prfl_prcpt_id =
               offr_prfl_prc_point.offr_prfl_prcpt_id
           AND offr_sku_line.dltd_ind <> 'Y'
           AND offr_sku_line.offr_prfl_prcpt_id =
               offr_prfl_prc_point.offr_prfl_prcpt_id
           AND dstrbtd_mrkt_sls.ver_id = 0
           AND offr_prfl_prc_point.offr_id = offr.offr_id
           AND offr.offr_typ = 'CMP'
        --           AND offr.ver_id = 0
         GROUP BY pa_maps_public.get_mstr_fsc_cd(p_mrkt_id => p_mrkt_id,
                                                 p_sku_id  => offr_sku_line.sku_id,
                                                 p_perd_id => dstrbtd_mrkt_sls.sls_perd_id),
                  dstrbtd_mrkt_sls.sls_perd_id,
                  dstrbtd_mrkt_sls.sls_typ_id)
      SELECT d1.fsc_cd,
             pa_maps_public.get_fsc_desc(p_mrkt_id      => p_mrkt_id,
                                         p_offr_perd_id => d1.sls_perd_id,
                                         p_fsc_cd       => d1.fsc_cd) fsc_nm,
             d1.sls_typ_id d1_sls_typ_id,
             d1.units d1_units,
             d2.sls_typ_id d2_sls_typ_id,
             d2.units d2_units,
             CASE
               WHEN (SELECT COUNT(1) FROM details WHERE otr = 3) = 0 THEN
                d4.sls_typ_id
               ELSE
                d3.sls_typ_id
             END d3_sls_typ_id,
             CASE
               WHEN (SELECT COUNT(1) FROM details WHERE otr = 3) = 0 THEN
                d4.units
               ELSE
                d3.units
             END d3_units
        FROM details d1
        LEFT JOIN details d2
          ON d1.fsc_cd = d2.fsc_cd
         AND d2.otr = 2
        LEFT JOIN details d4
          ON d1.fsc_cd = d4.fsc_cd
         AND d4.otr = 4
        LEFT JOIN details d3
          ON d1.fsc_cd = d3.fsc_cd
         AND d3.otr = 3
       WHERE d1.otr = 1;
  BEGIN
    SELECT MAX(prcsng_dt)
      INTO l_last_accs_dt
      FROM dly_bilng_trnd
     WHERE mrkt_id = p_mrkt_id
       AND offr_perd_id = p_campgn_sls_perd_id;
  
    FOR rec IN cc LOOP
      PIPE ROW(sct_trend_check_rpt_line(l_last_accs_dt,
                                        rec.fsc_cd,
                                        rec.fsc_nm,
                                        l_actual_perd_id,
                                        l_actual_perd_id,
                                        rec.d1_units,
                                        l_next_perd_id,
                                        l_prev_perd_id,
                                        rec.d2_units,
                                        l_next1_perd_id,
                                        l_actual_perd_id,
                                        rec.d3_units));
    END LOOP;
  
  END sct_trend_check_rpt;

  FUNCTION sct_dly_updt_rpt(p_mrkt_id            IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                            p_campgn_sls_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE)
    RETURN sct_dly_updt_rpt_table
    PIPELINED AS
  
    l_actual_perd_id NUMBER := p_campgn_sls_perd_id;
    l_next_perd_id   NUMBER := pa_maps_public.perd_plus(p_mrkt_id,
                                                        p_campgn_sls_perd_id,
                                                        1);
    l_next1_perd_id  NUMBER := pa_maps_public.perd_plus(p_mrkt_id,
                                                        p_campgn_sls_perd_id,
                                                        2);
    l_prev_perd_id   NUMBER := pa_maps_public.perd_plus(p_mrkt_id,
                                                        p_campgn_sls_perd_id,
                                                        -1);
  
    l_last_accs_dt DATE;
    l_cash_value   NUMBER;
    l_r_factor     NUMBER;
  
    CURSOR cc IS
      WITH perd_data AS
       (SELECT DISTINCT lr.mrkt_id,
                        lr.sls_perd_id,
                        lr.sls_typ_id,
                        lr.prcsng_dt,
                        first_value(cv.cash_val) over(PARTITION BY cv.mrkt_id, cv.sls_perd_id, cv.sls_typ_id ORDER BY cv.last_updt_ts DESC) cash_val,
                        first_value(cv.r_factor) over(PARTITION BY cv.mrkt_id, cv.sls_perd_id, cv.sls_typ_id ORDER BY cv.last_updt_ts DESC) r_factor
          FROM (SELECT mrkt_id,
                       sls_perd_id,
                       sls_typ_id,
                       MAX(trunc(last_updt_ts)) prcsng_dt
                  FROM cash_val_rf_hist
                 WHERE mrkt_id = p_mrkt_id
                   AND ((sls_perd_id = l_actual_perd_id AND
                       sls_typ_id = supply_bst_id) OR
                       (sls_perd_id = l_next_perd_id AND
                       sls_typ_id = supply_est_id) OR
                       (sls_perd_id = l_next1_perd_id AND
                       sls_typ_id = supply_est_id))
                 GROUP BY mrkt_id, sls_perd_id, sls_typ_id) lr
          JOIN cash_val_rf_hist cv
            ON cv.mrkt_id = lr.mrkt_id
           AND cv.sls_perd_id = lr.sls_perd_id
           AND cv.sls_typ_id = lr.sls_typ_id),
      last_run AS
       (SELECT pd.*,
               pa_maps_public.perd_plus(p_mrkt_id   => pd.mrkt_id,
                                        p_perd1     => pd.sls_perd_id,
                                        p_perd_diff => nvl(tof.offst, 0)) offr_perd_id
          FROM perd_data pd
          LEFT JOIN trend_offst tof
            ON pd.mrkt_id = tof.mrkt_id
           AND pd.sls_typ_id = tof.sls_typ_id)
      SELECT sls_perd_id,
             sls_typ_id,
             round(SUM(nvl(unit_qty, 0) * nvl(sls_prc_amt, 0) /
                       decode(nvl(nr_for_qty, 0), 0, 1, nr_for_qty) *
                       decode(nvl(net_to_avon_fct, 0),
                              0,
                              1,
                              net_to_avon_fct))) sales,
             MAX(cash_val) cash_val,
             MAX(r_factor) r_factor,
             MAX(offr_perd_id) offr_perd_id,
             MAX(prcsng_dt) prcsng_dt
        FROM (SELECT dly_bilng_trnd.dly_bilng_id,
                     dly_bilng_trnd.trnd_sls_perd_id sls_perd_id,
                     last_run.sls_typ_id,
                     MAX(dly_bilng_trnd.unit_qty) unit_qty,
                     MAX(offr_prfl_prc_point.sls_prc_amt) sls_prc_amt,
                     MAX(offr_prfl_prc_point.nr_for_qty) nr_for_qty,
                     MAX(offr_prfl_prc_point.net_to_avon_fct) net_to_avon_fct,
                     MAX(last_run.cash_val) cash_val,
                     MAX(last_run.r_factor) r_factor,
                     MAX(dly_bilng_trnd.offr_perd_id) offr_perd_id,
                     MAX(dly_bilng_trnd.prcsng_dt) prcsng_dt
                FROM dly_bilng_trnd,
                     dly_bilng_trnd_offr_sku_line,
                     dstrbtd_mrkt_sls,
                     offr_sku_line,
                     offr_prfl_prc_point,
                     last_run
               WHERE dly_bilng_trnd.mrkt_id = last_run.mrkt_id
                 AND dly_bilng_trnd.trnd_sls_perd_id = last_run.sls_perd_id
                 AND dly_bilng_trnd.offr_perd_id = last_run.offr_perd_id
                 AND dly_bilng_trnd.dly_bilng_id =
                     dly_bilng_trnd_offr_sku_line.dly_bilng_id
                 AND dly_bilng_trnd_offr_sku_line.offr_sku_line_id =
                     dstrbtd_mrkt_sls.offr_sku_line_id
                 AND dstrbtd_mrkt_sls.offr_sku_line_id =
                     offr_sku_line.offr_sku_line_id
                 AND offr_sku_line.offr_prfl_prcpt_id =
                     offr_prfl_prc_point.offr_prfl_prcpt_id
                 AND dstrbtd_mrkt_sls.sls_typ_id = estimate
                 AND dly_bilng_trnd_offr_sku_line.sls_typ_id =
                     demand_actuals
                 AND dly_bilng_trnd.mrkt_id = last_run.mrkt_id(+)
                 AND dly_bilng_trnd.trnd_sls_perd_id =
                     last_run.sls_perd_id(+)
                 AND trunc(dly_bilng_trnd.prcsng_dt) <=
                     nvl(last_run.prcsng_dt, SYSDATE)
               GROUP BY dly_bilng_trnd.dly_bilng_id,
                        dly_bilng_trnd.trnd_sls_perd_id,
                        last_run.sls_typ_id)
       GROUP BY sls_perd_id, sls_typ_id;
  
    -- for LOG
    l_run_id         NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id        VARCHAR(35) := USER();
    l_module_name    VARCHAR2(30) := 'sct_dly_updt_rpt';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       to_char(p_mrkt_id) || ', ' ||
                                       'p_campgn_SLS_perd_id: ' ||
                                       to_char(p_campgn_sls_perd_id) || ')';
    --
  BEGIN
    --
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start' || l_parameter_list);
  
    FOR rec IN cc LOOP
      PIPE ROW(sct_dly_updt_rpt_line(rec.prcsng_dt,
                                     rec.sls_perd_id,
                                     rec.sls_typ_id,
                                     rec.cash_val,
                                     rec.sales,
                                     rec.r_factor));
    
    END LOOP;
  END sct_dly_updt_rpt;

  FUNCTION p94_rpt_head(p_mrkt_id        IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                        p_campgn_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                        p_sls_typ_id     IN sls_typ.sls_typ_id%TYPE,
                        p_prcsng_dt      IN dly_bilng.prcsng_dt%TYPE)
    RETURN p94_rpt_head_table
    PIPELINED AS
    CURSOR cc IS
      SELECT mrkt_id,
             sls_perd_id,
             MAX(trgt_perd_id) trgt_perd_id,
             sls_typ_nm,
             MAX(cash_val) totl_cash_frcst,
             round(SUM(nvl(unit_qty, 0) * nvl(sls_prc_amt, 0) /
                       decode(nvl(nr_for_qty, 0), 0, 1, nr_for_qty) *
                       decode(nvl(net_to_avon_fct, 0),
                              0,
                              1,
                              net_to_avon_fct))) sales,
             MAX(r_factor) r_factor,
             MAX(prcsng_dt) prcsng_dt,
             MAX(offr_perd_id) offr_perd_id_on,
             MAX(offr_perd_id_off) off_perd_id_off
        FROM (SELECT dly_bilng_trnd.dly_bilng_id,
                     MAX(dly_bilng_trnd.mrkt_id) mrkt_id,
                     dly_bilng_trnd.trnd_sls_perd_id sls_perd_id,
                     MAX(last_run.trgt_perd_id) trgt_perd_id,
                     last_run.sls_typ_id,
                     MAX(dly_bilng_trnd.unit_qty) unit_qty,
                     MAX(offr_prfl_prc_point.sls_prc_amt) sls_prc_amt,
                     MAX(offr_prfl_prc_point.nr_for_qty) nr_for_qty,
                     MAX(offr_prfl_prc_point.net_to_avon_fct) net_to_avon_fct,
                     MAX(last_run.cash_val) cash_val,
                     MAX(last_run.r_factor) r_factor,
                     MAX(last_run.offr_perd_id) offr_perd_id,
                     MAX(last_run.offr_perd_id_off) offr_perd_id_off,
                     MAX(dly_bilng_trnd.prcsng_dt) prcsng_dt
                FROM dly_bilng_trnd,
                     dly_bilng_trnd_offr_sku_line,
                     dstrbtd_mrkt_sls,
                     offr_sku_line,
                     offr_prfl_prc_point,
                     (SELECT DISTINCT pd.*,
                                      first_value(cv.cash_val) over(PARTITION BY cv.mrkt_id, cv.sls_perd_id, cv.sls_typ_id ORDER BY cv.last_updt_ts DESC) cash_val,
                                      first_value(cv.r_factor) over(PARTITION BY cv.mrkt_id, cv.sls_perd_id, cv.sls_typ_id ORDER BY cv.last_updt_ts DESC) r_factor
                        FROM (SELECT lr.*,
                                     pa_maps_public.perd_plus(p_mrkt_id   => lr.mrkt_id,
                                                              p_perd1     => lr.sls_perd_id,
                                                              p_perd_diff => nvl(tof.offst,
                                                                                 0)) trgt_perd_id,
                                     pa_maps_public.perd_plus(p_mrkt_id   => lr.mrkt_id,
                                                              p_perd1     => lr.sls_perd_id,
                                                              p_perd_diff => nvl(tof.offst,
                                                                                 0)) offr_perd_id,
                                     pa_maps_public.perd_plus(p_mrkt_id   => lr.mrkt_id,
                                                              p_perd1     => lr.sls_perd_id,
                                                              p_perd_diff => nvl(tof.offst,
                                                                                 0) - 1) offr_perd_id_off
                                FROM (SELECT p_mrkt_id        mrkt_id,
                                             p_campgn_perd_id sls_perd_id,
                                             p_sls_typ_id     sls_typ_id,
                                             p_prcsng_dt      prcsng_dt
                                        FROM dual) lr
                                JOIN trend_offst tof
                                  ON tof.mrkt_id = lr.mrkt_id
                                 AND tof.sls_typ_id = lr.sls_typ_id) pd
                        LEFT JOIN cash_val_rf_hist cv
                          ON cv.mrkt_id = pd.mrkt_id
                         AND cv.sls_perd_id = pd.trgt_perd_id
                         AND cv.sls_typ_id = pd.sls_typ_id) last_run
               WHERE dly_bilng_trnd.mrkt_id = last_run.mrkt_id
                 AND dly_bilng_trnd.trnd_sls_perd_id = last_run.sls_perd_id
                 AND (dly_bilng_trnd.offr_perd_id = last_run.offr_perd_id OR
                     dly_bilng_trnd.offr_perd_id =
                     last_run.offr_perd_id_off)
                 AND dly_bilng_trnd.dly_bilng_id =
                     dly_bilng_trnd_offr_sku_line.dly_bilng_id
                 AND dly_bilng_trnd_offr_sku_line.offr_sku_line_id =
                     dstrbtd_mrkt_sls.offr_sku_line_id
                 AND dstrbtd_mrkt_sls.offr_sku_line_id =
                     offr_sku_line.offr_sku_line_id
                 AND offr_sku_line.offr_prfl_prcpt_id =
                     offr_prfl_prc_point.offr_prfl_prcpt_id
                 AND dstrbtd_mrkt_sls.sls_typ_id = estimate
                 AND dly_bilng_trnd_offr_sku_line.sls_typ_id =
                     demand_actuals
                 AND trunc(dly_bilng_trnd.prcsng_dt) <=
                     trunc(nvl(last_run.prcsng_dt, SYSDATE))
               GROUP BY dly_bilng_trnd.dly_bilng_id,
                        dly_bilng_trnd.trnd_sls_perd_id,
                        last_run.sls_typ_id) t
        JOIN sls_typ
          ON t.sls_typ_id = sls_typ.sls_typ_id
       GROUP BY mrkt_id, sls_perd_id, sls_typ_nm;
    -- for LOG
    l_run_id         NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id        VARCHAR(35) := USER();
    l_module_name    VARCHAR2(30) := 'p94_rpt_head';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       to_char(p_mrkt_id) || ', ' ||
                                       'p_campgn_perd_id: ' ||
                                       to_char(p_campgn_perd_id) || ', ' ||
                                       'p_sls_typ_id: ' ||
                                       to_char(p_sls_typ_id) || ', ' ||
                                       'p_prcsng_dt: ' ||
                                       to_char(p_prcsng_dt) || ')';
  
    --
  BEGIN
    --
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start' || l_parameter_list);
  
    FOR rec IN cc LOOP
      PIPE ROW(p94_rpt_head_line(rec.mrkt_id,
                                 rec.sls_perd_id,
                                 rec.trgt_perd_id,
                                 rec.sls_typ_nm,
                                 rec.totl_cash_frcst,
                                 rec.sales,
                                 rec.r_factor,
                                 rec.prcsng_dt,
                                 rec.offr_perd_id_on,
                                 rec.off_perd_id_off));
    END LOOP;
  
  END p94_rpt_head;

  FUNCTION p94_rpt_dtls(p_mrkt_id        IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                        p_campgn_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                        p_sls_typ_id     IN sls_typ.sls_typ_id%TYPE,
                        p_prcsng_dt      IN dly_bilng.prcsng_dt%TYPE)
    RETURN p94_rpt_dtls_table
    PIPELINED AS
    CURSOR cc IS
      SELECT res.mrkt_id,
             res.sls_perd_id,
             res.trgt_perd_id,
             res.sls_typ_id,
             res.fsc_cd,
             pa_maps_public.get_fsc_desc(p_mrkt_id      => res.mrkt_id,
                                         p_offr_perd_id => res.trgt_perd_id,
                                         p_fsc_cd       => res.fsc_cd) AS fsc_desc,
             res.veh_id,
             res.pg_no,
             res.totl_estimt,
             res.totl_op_est,
             res.totl_op_est_on,
             res.totl_op_est_off,
             CASE
               WHEN res.sls_typ_id IN (3, 103) THEN
                NULL
               ELSE
                res.totl_units
             END totl_units,
             res.totl_bst,
             res.totl_bst_on,
             res.totl_bst_off,
             CASE
               WHEN res.totl_bst = 0 THEN
                NULL
               ELSE
                round(res.totl_bst_pct, 0)
             END totl_bst_pct,
             res.bias,
             res.variation,
             CASE
               WHEN res.totl_bst = 0 THEN
                NULL
               ELSE
                round(res.variation_pct, 0)
             END variation_pct,
             res.intrdctn_perd_id,
             res.stus_perd_id stat_dt,
             catgry.catgry_nm,
             sgmt.sgmt_nm,
             form.form_desc_txt,
             form_grp.form_grp_desc_txt,
             brnd.brnd_nm,
             res.itemid,
             res.conceptid,
             sc.sls_cls_desc_txt
        FROM (WITH lr AS (SELECT p_mrkt_id        mrkt_id,
                                 p_campgn_perd_id sls_perd_id,
                                 p_sls_typ_id     sls_typ_id,
                                 p_prcsng_dt      prcsng_dt
                            FROM dual), perd_data AS (SELECT lr.*,
                                                             pa_maps_public.perd_plus(lr.mrkt_id,
                                                                                      lr.sls_perd_id,
                                                                                      tof.offst) trgt_perd_id
                                                        FROM lr
                                                        LEFT JOIN trend_offst tof
                                                          ON lr.mrkt_id =
                                                             tof.mrkt_id
                                                         AND lr.sls_typ_id =
                                                             tof.sls_typ_id)
               SELECT lr.mrkt_id,
                      lr.sls_perd_id,
                      lr.trgt_perd_id,
                      lr.sls_typ_id,
                      pa_maps_public.get_mstr_fsc_cd(lr.mrkt_id,
                                                     osl.sku_id,
                                                     lr.trgt_perd_id) fsc_cd,
                      MAX(dms.veh_id) veh_id,
                      MAX(decode(offr.mrkt_veh_perd_sctn_id,
                                 NULL,
                                 999999,
                                 nvl(mvps.strtg_page_nr, 0) +
                                 nvl(offr.sctn_page_ofs_nr, 0) +
                                 oppp.pg_ofs_nr +
                                 decode(oppp.featrd_side_cd, 1, 1, 0))) pg_no, -- 'page number'
                      SUM(CASE
                            WHEN dms.sls_typ_id = 1 THEN
                             dms.unit_qty
                            ELSE
                             0
                          END) totl_estimt,
                      SUM(CASE
                            WHEN dms.sls_typ_id = 2 THEN
                             dms.unit_qty
                            ELSE
                             0
                          END) totl_op_est,
                      SUM(CASE
                            WHEN dms.sls_typ_id = 2
                                 AND dms.sls_perd_id = dms.offr_perd_id THEN
                             dms.unit_qty
                            ELSE
                             0
                          END) totl_op_est_on,
                      SUM(CASE
                            WHEN dms.sls_typ_id = 2
                                 AND dms.sls_perd_id <> dms.offr_perd_id THEN
                             dms.unit_qty
                            ELSE
                             0
                          END) totl_op_est_off,
                      SUM(CASE
                            WHEN dms.sls_typ_id = 3 THEN
                             dms.unit_qty
                            ELSE
                             0
                          END) totl_units,
                      SUM(CASE
                            WHEN dms.sls_typ_id = lr.sls_typ_id THEN
                             dms.unit_qty
                            ELSE
                             0
                          END) totl_bst,
                      SUM(CASE
                            WHEN dms.sls_typ_id = lr.sls_typ_id
                                 AND dms.sls_perd_id = dms.offr_perd_id THEN
                             dms.unit_qty
                            ELSE
                             0
                          END) totl_bst_on,
                      SUM(CASE
                            WHEN dms.sls_typ_id = lr.sls_typ_id
                                 AND dms.sls_perd_id <> dms.offr_perd_id THEN
                             dms.unit_qty
                            ELSE
                             0
                          END) totl_bst_off,
                      SUM(CASE
                            WHEN dms.sls_typ_id = lr.sls_typ_id
                                 AND dms.sls_perd_id <> dms.offr_perd_id THEN
                             dms.unit_qty
                            ELSE
                             0
                          END) / CASE
                        WHEN SUM(CASE
                                   WHEN dms.sls_typ_id = lr.sls_typ_id THEN
                                    dms.unit_qty
                                   ELSE
                                    0
                                 END) = 0 THEN
                         0.01
                        ELSE
                         SUM(CASE
                               WHEN dms.sls_typ_id = lr.sls_typ_id THEN
                                dms.unit_qty
                               ELSE
                                0
                             END)
                      END * 100.0 totl_bst_pct,
                      MAX(nvl(mpsb.bias_pct, 100)) bias,
                      SUM(CASE
                            WHEN dms.sls_typ_id = lr.sls_typ_id THEN
                             dms.unit_qty
                            ELSE
                             0
                          END) - CASE
                        WHEN lr.sls_typ_id IN (3, 103) THEN
                         SUM(CASE
                               WHEN dms.sls_typ_id = 2 THEN
                                dms.unit_qty
                               ELSE
                                0
                             END)
                        ELSE
                         SUM(CASE
                               WHEN dms.sls_typ_id = 3 THEN
                                dms.unit_qty
                               ELSE
                                0
                             END)
                      END variation,
                      (SUM(CASE
                             WHEN dms.sls_typ_id = lr.sls_typ_id THEN
                              dms.unit_qty
                             ELSE
                              0
                           END) - CASE
                        WHEN lr.sls_typ_id IN (3, 103) THEN
                         SUM(CASE
                               WHEN dms.sls_typ_id = 2 THEN
                                dms.unit_qty
                               ELSE
                                0
                             END)
                        ELSE
                         SUM(CASE
                               WHEN dms.sls_typ_id = 3 THEN
                                dms.unit_qty
                               ELSE
                                0
                             END)
                      END) / CASE
                        WHEN SUM(CASE
                                   WHEN dms.sls_typ_id = lr.sls_typ_id THEN
                                    dms.unit_qty
                                   ELSE
                                    0
                                 END) = 0 THEN
                         0.01
                        ELSE
                         SUM(CASE
                               WHEN dms.sls_typ_id = lr.sls_typ_id THEN
                                dms.unit_qty
                               ELSE
                                0
                             END)
                      END * 100.0 variation_pct,
                      MAX(mrkt_sku.intrdctn_perd_id) intrdctn_perd_id,
                      MAX(mrkt_sku.stus_perd_id) stus_perd_id,
                      MAX(prfl.catgry_id) catgry_id,
                      MAX(prfl.sgmt_id) sgmt_id,
                      MAX(prfl.form_id) form_id,
                      MAX(prfl.brnd_id) brnd_id,
                      MAX(osl.sku_id) itemid,
                      MAX(osl.prfl_cd) conceptid,
                      MAX(osl.sls_cls_cd) sls_cls_cd
                 FROM perd_data lr
                 JOIN dstrbtd_mrkt_sls dms
                   ON dms.mrkt_id = lr.mrkt_id
                  AND dms.sls_typ_id IN (1, 2, 3, lr.sls_typ_id)
                  AND dms.sls_perd_id = lr.trgt_perd_id
                  AND pa_maps_public.perd_diff(lr.mrkt_id,
                                               dms.sls_perd_id,
                                               dms.offr_perd_id) IN (0, -1)
                  AND dms.ver_id = 0
                 JOIN offr_sku_line osl
                   ON osl.offr_sku_line_id = dms.offr_sku_line_id
                  AND osl.dltd_ind <> 'Y'
                 JOIN offr_prfl_prc_point oppp
                   ON oppp.offr_prfl_prcpt_id = osl.offr_prfl_prcpt_id
                 JOIN offr
                   ON oppp.offr_id = offr.offr_id
                  AND offr.offr_typ = 'CMP'
                  AND offr.ver_id = 0
                 LEFT JOIN mrkt_perd_sku_bias mpsb
                   ON dms.mrkt_id = mpsb.mrkt_id
                  AND osl.sku_id = mpsb.sku_id
                  AND lr.trgt_perd_id = mpsb.sls_perd_id
                 LEFT JOIN prfl
                   ON osl.prfl_cd = prfl.prfl_cd
                 LEFT JOIN form
                   ON form.form_id = prfl.form_id
                 JOIN mrkt_sku
                   ON lr.mrkt_id = mrkt_sku.mrkt_id
                  AND osl.sku_id = mrkt_sku.sku_id
                 JOIN mrkt_veh_perd_sctn mvps
                   ON offr.mrkt_id = mvps.mrkt_id
                  AND offr.veh_id = mvps.veh_id
                  AND offr.offr_perd_id = mvps.offr_perd_id
                  AND offr.ver_id = mvps.ver_id
                  AND offr.mrkt_veh_perd_sctn_id =
                      mvps.mrkt_veh_perd_sctn_id
                GROUP BY lr.mrkt_id,
                         lr.sls_perd_id,
                         lr.trgt_perd_id,
                         lr.sls_typ_id,
                         pa_maps_public.get_mstr_fsc_cd(lr.mrkt_id,
                                                        osl.sku_id,
                                                        lr.trgt_perd_id)) res
                 LEFT JOIN sls_cls sc
                   ON res.sls_cls_cd = sc.sls_cls_cd
                 LEFT JOIN catgry
                   ON res.catgry_id = catgry.catgry_id
                 LEFT JOIN sgmt
                   ON res.sgmt_id = sgmt.sgmt_id
                 LEFT JOIN form
                   ON form.form_id = res.form_id
                 LEFT JOIN form_grp
                   ON form_grp.form_grp_id = form.form_grp_id
                 LEFT JOIN brnd
                   ON brnd.brnd_id = res.brnd_id;
  
  
    -- for LOG
    l_run_id         NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id        VARCHAR(35) := USER();
    l_module_name    VARCHAR2(30) := 'p94_rpt_dtls';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       to_char(p_mrkt_id) || ', ' ||
                                       'p_campgn_perd_id: ' ||
                                       to_char(p_campgn_perd_id) || ', ' ||
                                       'p_sls_typ_id: ' ||
                                       to_char(p_sls_typ_id) || ', ' ||
                                       'p_prcsng_dt: ' ||
                                       to_char(p_prcsng_dt) || ')';
  
    --
  BEGIN
    --
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start' || l_parameter_list);
  
    FOR rec IN cc LOOP
      PIPE ROW(p94_rpt_dtls_line(rec.mrkt_id,
                                 rec.sls_perd_id,
                                 rec.trgt_perd_id,
                                 rec.sls_typ_id,
                                 rec.fsc_cd,
                                 rec.fsc_desc,
                                 rec.veh_id,
                                 rec.pg_no,
                                 rec.totl_estimt,
                                 rec.totl_op_est,
                                 rec.totl_op_est_on,
                                 rec.totl_op_est_off,
                                 rec.totl_units,
                                 rec.totl_bst,
                                 rec.totl_bst_on,
                                 rec.totl_bst_off,
                                 rec.totl_bst_pct,
                                 rec.bias,
                                 rec.variation,
                                 rec.variation_pct,
                                 rec.intrdctn_perd_id,
                                 rec.stat_dt,
                                 rec.catgry_nm,
                                 rec.sgmt_nm,
                                 rec.form_desc_txt,
                                 rec.form_grp_desc_txt,
                                 rec.brnd_nm,
                                 rec.itemid,
                                 rec.conceptid,
                                 rec.sls_cls_desc_txt));
    END LOOP;
  
  END p94_rpt_dtls;

END pa_sims_mantnc;
/
