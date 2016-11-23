create or replace PACKAGE pa_trend_alloc AS

  g_package_name CONSTANT VARCHAR2(30) := 'PA_TREND_ALLOC';

  FUNCTION get_trend_alloc_hist_head(p_mrkt_id     IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                                     p_sls_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                     p_trg_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                     p_sls_typ_id  IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                                     p_bilng_day   IN dly_bilng_trnd.prcsng_dt%TYPE)
    RETURN obj_trend_alloc_hist_hd_table
    PIPELINED;

   FUNCTION get_trend_alloc_hist_dtls(p_mrkt_id     IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                                     p_sls_perd_id IN dly_bilng_trnd.trnd_sls_perd_id%TYPE,
                                     p_trg_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                     p_sls_typ_id  IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                                     p_bilng_day   IN dly_bilng.prcsng_dt%TYPE)
    RETURN obj_trend_alloc_hist_dt_table
    PIPELINED;

END pa_trend_alloc;

create or replace PACKAGE BODY pa_trend_alloc AS

  FUNCTION get_trend_alloc_hist_head(p_mrkt_id     IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                                     p_sls_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                     p_trg_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                     p_sls_typ_id  IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                                     p_bilng_day   IN dly_bilng_trnd.prcsng_dt%TYPE)
    RETURN obj_trend_alloc_hist_hd_table
    PIPELINED AS
    -- once
    l_sls_typ_id_from_config dly_bilng_trnd_offr_sku_line.sls_typ_id%TYPE;
    --
    --l_sct_cash_val mrkt_sls_perd.sct_cash_val%TYPE;
    l_sct_r_factor mrkt_sls_perd.sct_r_factor%TYPE;
    --
    -- for LOG
    l_module_name    VARCHAR2(30) := 'GET_MANL_TREND_ADJSTMNT';
    l_parameter_list VARCHAR2(2048) := '(p_mrkt_id: ' || to_char(p_mrkt_id) || ', ' ||
                                       'p_sls_perd_id: ' ||
                                       to_char(p_sls_perd_id) || ', ' ||
                                       'p_trg_perd_id: ' ||
                                       to_char(p_trg_perd_id) || ', ' ||
                                       'p_sls_typ_id: ' ||
                                       to_char(p_sls_typ_id) || ', ' ||
                                       'p_bilng_day: ' ||
                                       to_char(p_bilng_day) || ')';
    --
    l_run_id  NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id VARCHAR(35) := USER();
  

  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || l_parameter_list || ' start');
    
    -- MRKT_CONFIG_ITEM_VAL_TXT
    BEGIN
      SELECT mrkt_config_item_val_txt
        INTO l_sls_typ_id_from_config
        FROM mrkt_config_item
       WHERE mrkt_id = p_mrkt_id
         AND config_item_id = 10000;
    EXCEPTION
      WHEN OTHERS THEN
        app_plsql_log.info(l_module_name || l_parameter_list ||
                           ' error sls_typ_id_from_config not found');
        l_sls_typ_id_from_config := -999;
    END;
    -- iterations
    FOR i_prd IN (SELECT -- dbt
                   c_sls_perd_id dbt_on_sls_perd_id,
                   c_trg_perd_id dbt_on_offr_perd_id,
                   c_sls_perd_id dbt_off_sls_perd_id,
                   pa_maps_public.perd_plus(p_mrkt_id, c_trg_perd_id, -1) dbt_off_offr_perd_id,
                   -- dms
                   c_trg_perd_id dms_on_sls_perd_id,
                   c_trg_perd_id dms_on_offr_perd_id,
                   c_trg_perd_id dms_off_sls_perd_id,
                   pa_maps_public.perd_plus(p_mrkt_id, c_trg_perd_id, -1) dms_off_offr_perd_id,
                   -- billing day
                   (SELECT MAX(trnd_bilng_days.prcsng_dt)
                      FROM trnd_bilng_days
                     WHERE trnd_bilng_days.mrkt_id = p_mrkt_id
                       AND trnd_bilng_days.sls_perd_id = c_sls_perd_id
                       AND trnd_bilng_days.day_num <=
                           (SELECT trnd_bilng_days.day_num
                              FROM trnd_bilng_days
                             WHERE trnd_bilng_days.mrkt_id = p_mrkt_id
                               AND trnd_bilng_days.sls_perd_id = p_sls_perd_id
                               AND trnd_bilng_days.prcsng_dt = p_bilng_day)) dbt_bilng_day,
                   -- cash value
                   c_cash_val
                    FROM (SELECT perd_id c_sls_perd_id,
                                 pa_manl_trend_adjstmnt.get_target_campaign(p_mrkt_id     => p_mrkt_id,
                                                                            p_sls_perd_id => perd_id,
                                                                            p_sls_typ_id  => p_sls_typ_id) c_trg_perd_id,
                                 mrkt_sls_perd.sct_cash_val c_cash_val
                            FROM (WITH yrs AS (SELECT p_sls_perd_id - 10000 perd_id
                                                 FROM dual
                                               UNION ALL
                                               SELECT p_sls_perd_id - 20000 perd_id
                                                 FROM dual
                                               UNION ALL
                                               SELECT p_sls_perd_id - 30000 perd_id
                                                 FROM dual)
                                   SELECT pa_maps_public.perd_plus(p_mrkt_id,
                                                                   yrs.perd_id,
                                                                   -1) perd_id
                                     FROM yrs
                                   UNION
                                   SELECT perd_id
                                     FROM yrs
                                   UNION
                                   SELECT pa_maps_public.perd_plus(p_mrkt_id,
                                                                   yrs.perd_id,
                                                                   1) perd_id
                                     FROM yrs), mrkt_sls_perd
                                    WHERE p_mrkt_id = mrkt_sls_perd.mrkt_id(+)
                                      AND perd_id =
                                          mrkt_sls_perd.sls_perd_id(+)
                          )
                   ORDER BY 1) LOOP
      --
      FOR i_prd_sch IN (SELECT i_prd.dbt_on_sls_perd_id sls_perd_id,
                               i_prd.dms_on_sls_perd_id trg_perd_id,
                               i_prd.dbt_bilng_day bilng_day,
/*                               round(SUM(decode(dstrbtd_mrkt_sls.sls_typ_id,
                                                1,
                                                dly_bilng_trnd_offr_sku_line.unit_qty,
                                                0))) bi24_unts,*/
                               round(SUM(case when dstrbtd_mrkt_sls.sls_typ_id=1 
                                                and i_prd.dbt_on_offr_perd_id=dly_bilng_trnd.offr_perd_id
                                              then dly_bilng_trnd_offr_sku_line.unit_qty
                                              else 0 end)) bi24_unts,
                               round(SUM(case when dstrbtd_mrkt_sls.sls_typ_id=1 
                                                and i_prd.dbt_on_offr_perd_id=dly_bilng_trnd.offr_perd_id
                                              then dly_bilng_trnd_offr_sku_line.unit_qty *
                                                dly_bilng_trnd.sls_prc_amt /
                                                decode(dly_bilng_trnd.nr_for_qty,
                                                       0,
                                                       1,
                                                       dly_bilng_trnd.nr_for_qty) -
                                                dly_bilng_trnd_offr_sku_line.comsn_amt -
                                                dly_bilng_trnd_offr_sku_line.tax_amt
                                                else 0 end)) bi24_sls,
                               round(SUM(case when dstrbtd_mrkt_sls.sls_typ_id=1 
                                                and i_prd.dbt_off_offr_perd_id=dly_bilng_trnd.offr_perd_id
                                              then dly_bilng_trnd_offr_sku_line.unit_qty
                                              else 0 end)) bi24_unts_off,
                               round(SUM(case when dstrbtd_mrkt_sls.sls_typ_id=1 
                                                and i_prd.dbt_off_offr_perd_id=dly_bilng_trnd.offr_perd_id
                                              then dly_bilng_trnd_offr_sku_line.unit_qty *
                                                dly_bilng_trnd.sls_prc_amt /
                                                decode(dly_bilng_trnd.nr_for_qty,
                                                       0,
                                                       1,
                                                       dly_bilng_trnd.nr_for_qty) -
                                                dly_bilng_trnd_offr_sku_line.comsn_amt -
                                                dly_bilng_trnd_offr_sku_line.tax_amt
                                                else 0 end)) bi24_sls_off,
                               i_prd.c_cash_val cash_vlu,
                               l_sct_r_factor r_factor,
                               round(SUM(case when dstrbtd_mrkt_sls.sls_typ_id=p_sls_typ_id 
                                                and i_prd.dms_on_offr_perd_id=dstrbtd_mrkt_sls.offr_perd_id
                                              then dstrbtd_mrkt_sls.unit_qty
                                              else 0 end)) trend_unts,
                               round(SUM(case when dstrbtd_mrkt_sls.sls_typ_id=p_sls_typ_id 
                                                and i_prd.dms_on_offr_perd_id=dstrbtd_mrkt_sls.offr_perd_id
                                              then dstrbtd_mrkt_sls.unit_qty *
                                                offr_prfl_prc_point.sls_prc_amt /
                                                decode(offr_prfl_prc_point.nr_for_qty,
                                                       0,
                                                       1,
                                                       offr_prfl_prc_point.nr_for_qty) *
                                                offr_prfl_prc_point.net_to_avon_fct
                                              else 0 end)) trend_sls,
                               round(SUM(case when dstrbtd_mrkt_sls.sls_typ_id=p_sls_typ_id 
                                                and i_prd.dms_off_offr_perd_id=dstrbtd_mrkt_sls.offr_perd_id
                                              then dstrbtd_mrkt_sls.unit_qty
                                              else 0 end)) trend_unts_off,
                               round(SUM(case when dstrbtd_mrkt_sls.sls_typ_id=p_sls_typ_id 
                                                and i_prd.dms_off_offr_perd_id=dstrbtd_mrkt_sls.offr_perd_id
                                              then dstrbtd_mrkt_sls.unit_qty *
                                                offr_prfl_prc_point.sls_prc_amt /
                                                decode(offr_prfl_prc_point.nr_for_qty,
                                                       0,
                                                       1,
                                                       offr_prfl_prc_point.nr_for_qty) *
                                                offr_prfl_prc_point.net_to_avon_fct
                                              else 0 end)) trend_sls_off,
                               round(SUM(case when dstrbtd_mrkt_sls.sls_typ_id=1 
                                                and i_prd.dms_on_offr_perd_id=dstrbtd_mrkt_sls.offr_perd_id
                                              then dstrbtd_mrkt_sls.unit_qty
                                              else 0 end)) estimate_unts,
                               round(SUM(case when dstrbtd_mrkt_sls.sls_typ_id=1 
                                                and i_prd.dms_on_offr_perd_id=dstrbtd_mrkt_sls.offr_perd_id
                                              then dstrbtd_mrkt_sls.unit_qty *
                                                offr_prfl_prc_point.sls_prc_amt /
                                                decode(offr_prfl_prc_point.nr_for_qty,
                                                       0,
                                                       1,
                                                       offr_prfl_prc_point.nr_for_qty) *
                                                offr_prfl_prc_point.net_to_avon_fct
                                              else 0 end)) estimate_sls,
                               round(SUM(case when dstrbtd_mrkt_sls.sls_typ_id=1 
                                                and i_prd.dms_off_offr_perd_id=dstrbtd_mrkt_sls.offr_perd_id
                                              then dstrbtd_mrkt_sls.unit_qty
                                              else 0 end)) estimate_unts_off,
                               round(SUM(case when dstrbtd_mrkt_sls.sls_typ_id=1 
                                                and i_prd.dms_off_offr_perd_id=dstrbtd_mrkt_sls.offr_perd_id
                                              then dstrbtd_mrkt_sls.unit_qty *
                                                offr_prfl_prc_point.sls_prc_amt /
                                                decode(offr_prfl_prc_point.nr_for_qty,
                                                       0,
                                                       1,
                                                       offr_prfl_prc_point.nr_for_qty) *
                                                offr_prfl_prc_point.net_to_avon_fct
                                              else 0 end)) estimate_sls_off,
                               round(SUM(case when dstrbtd_mrkt_sls.sls_typ_id=6 
                                                and i_prd.dms_on_offr_perd_id=dstrbtd_mrkt_sls.offr_perd_id
                                              then dstrbtd_mrkt_sls.unit_qty
                                              else 0 end)) actual_unts,
                               round(SUM(case when dstrbtd_mrkt_sls.sls_typ_id=6 
                                                and i_prd.dms_on_offr_perd_id=dstrbtd_mrkt_sls.offr_perd_id
                                              then dstrbtd_mrkt_sls.unit_qty *
                                                offr_prfl_prc_point.sls_prc_amt /
                                                decode(offr_prfl_prc_point.nr_for_qty,
                                                       0,
                                                       1,
                                                       offr_prfl_prc_point.nr_for_qty) *
                                                offr_prfl_prc_point.net_to_avon_fct
                                              else 0 end)) actual_sls,
                               round(SUM(case when dstrbtd_mrkt_sls.sls_typ_id=6 
                                                and i_prd.dms_off_offr_perd_id=dstrbtd_mrkt_sls.offr_perd_id
                                              then dstrbtd_mrkt_sls.unit_qty
                                              else 0 end)) actual_unts_off,
                               round(SUM(case when dstrbtd_mrkt_sls.sls_typ_id=6 
                                                and i_prd.dms_off_offr_perd_id=dstrbtd_mrkt_sls.offr_perd_id
                                              then dstrbtd_mrkt_sls.unit_qty *
                                                offr_prfl_prc_point.sls_prc_amt /
                                                decode(offr_prfl_prc_point.nr_for_qty,
                                                       0,
                                                       1,
                                                       offr_prfl_prc_point.nr_for_qty) *
                                                offr_prfl_prc_point.net_to_avon_fct
                                              else 0 end)) actual_sls_off

                          FROM dly_bilng_trnd,
                               dly_bilng_trnd_offr_sku_line,
                               dstrbtd_mrkt_sls,
                               offr_sku_line,
                               offr_prfl_prc_point,
                               offr
                         WHERE 1 = 1
                              -- filter: dly_bilng_trnd
                           AND dly_bilng_trnd.mrkt_id = p_mrkt_id
                           AND dly_bilng_trnd.trnd_sls_perd_id =
                               i_prd.dbt_on_sls_perd_id
                           AND (( --on-schedule
                                dly_bilng_trnd.trnd_sls_perd_id =
                                i_prd.dbt_on_sls_perd_id AND
                                dly_bilng_trnd.offr_perd_id =
                                i_prd.dbt_on_offr_perd_id) OR
                               ( --off-schedule
                                dly_bilng_trnd.trnd_sls_perd_id =
                                i_prd.dbt_off_sls_perd_id AND
                                dly_bilng_trnd.offr_perd_id =
                                i_prd.dbt_off_offr_perd_id))
                           AND trunc(dly_bilng_trnd.prcsng_dt) <=
                               i_prd.dbt_bilng_day
                              -- join: dly_bilng_trnd -> dly_bilng_trnd_offr_sku_line
                           AND dly_bilng_trnd.dly_bilng_id =
                               dly_bilng_trnd_offr_sku_line.dly_bilng_id
                              -- filter: p_sls_typ_id
                           AND dly_bilng_trnd_offr_sku_line.sls_typ_id =
                               l_sls_typ_id_from_config
                              -- join: dly_bilng_trnd_offr_sku_line -> dstrbtd_mrkt_sls
                           AND dly_bilng_trnd_offr_sku_line.offr_sku_line_id =
                               dstrbtd_mrkt_sls.offr_sku_line_id
                              -- filter: mrkt_id, 
                           AND dstrbtd_mrkt_sls.mrkt_id = p_mrkt_id
                           AND (( --on-schedule
                                dstrbtd_mrkt_sls.sls_perd_id =
                                i_prd.dms_on_sls_perd_id AND
                                dstrbtd_mrkt_sls.offr_perd_id =
                                i_prd.dms_on_offr_perd_id) OR
                               ( --off-schedule
                                dstrbtd_mrkt_sls.sls_perd_id =
                                i_prd.dms_off_sls_perd_id AND
                                dstrbtd_mrkt_sls.offr_perd_id =
                                i_prd.dms_off_offr_perd_id))
                           AND dstrbtd_mrkt_sls.sls_typ_id IN
                               (1, p_sls_typ_id, 6)
                              -- additional filtering...
                           AND dstrbtd_mrkt_sls.offr_sku_line_id =
                               offr_sku_line.offr_sku_line_id
                           AND offr_sku_line.dltd_ind <> 'Y'
                           AND offr_sku_line.offr_prfl_prcpt_id =
                               offr_prfl_prc_point.offr_prfl_prcpt_id
                           AND offr_prfl_prc_point.offr_id = offr.offr_id
                           AND offr.offr_typ = 'CMP'
                           AND offr.ver_id = 0
                         GROUP BY i_prd.dbt_on_sls_perd_id,
                                  i_prd.dms_on_sls_perd_id,
                                  i_prd.dbt_bilng_day,
                                  i_prd.c_cash_val,
                                  l_sct_r_factor
                         ORDER BY 1, 2 DESC) LOOP
        PIPE ROW(obj_trend_alloc_hist_head_line(i_prd_sch.sls_perd_id,
                                              i_prd_sch.trg_perd_id,
                                              i_prd_sch.bilng_day,
                                              i_prd_sch.bi24_unts,
                                              i_prd_sch.bi24_sls,
                                              i_prd_sch.bi24_unts_off,
                                              i_prd_sch.bi24_sls_off,
                                              i_prd_sch.cash_vlu,
                                              i_prd_sch.r_factor,
                                              i_prd_sch.estimate_unts,
                                              i_prd_sch.estimate_sls,
                                              i_prd_sch.estimate_unts_off,
                                              i_prd_sch.estimate_sls_off,
                                              i_prd_sch.trend_unts,
                                              i_prd_sch.trend_sls,
                                              i_prd_sch.trend_unts_off,
                                              i_prd_sch.trend_sls_off,
                                              i_prd_sch.actual_unts,
                                              i_prd_sch.actual_sls,
                                              i_prd_sch.actual_unts_off,
                                              i_prd_sch.actual_sls_off
                                              ));
      END LOOP;
      --
    END LOOP;
    -- DUMMY records (2)
    PIPE ROW(obj_trend_alloc_hist_head_line(p_sls_perd_id,
                                          p_trg_perd_id,
                                          p_bilng_day,
                                          11,
                                          333,
                                          1,
                                          33,
                                          6666,
                                          45,
                                          777,
                                          22,
                                          77,
                                          2,
                                          888,
                                          33,
                                          88,
                                          3,
                                          9999,
                                          444,
                                          999,
                                          44));
    --
    app_plsql_log.info(l_module_name || l_parameter_list || ' stop');
  END get_trend_alloc_hist_head;

  FUNCTION get_trend_alloc_hist_dtls(p_mrkt_id     IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                                     p_sls_perd_id IN dly_bilng_trnd.trnd_sls_perd_id%TYPE,
                                     p_trg_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                     p_sls_typ_id  IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                                     p_bilng_day   IN dly_bilng.prcsng_dt%TYPE)
    RETURN obj_trend_alloc_hist_dt_table
    PIPELINED AS
    l_sls_typ_id_from_config dly_bilng_trnd_offr_sku_line.sls_typ_id%TYPE;
    
    l_sct_r_factor mrkt_sls_perd.sct_r_factor%TYPE;
  
    l_dbt_sls_perd_id  dly_bilng_trnd.trnd_sls_perd_id%TYPE;
    l_dbt_offr_perd_id dly_bilng_trnd.offr_perd_id%TYPE;
    l_dbt_off_offr_perd_id dly_bilng_trnd.offr_perd_id%TYPE;
    l_dms_sls_perd_id  dstrbtd_mrkt_sls.sls_perd_id%TYPE;
    l_dms_offr_perd_id dstrbtd_mrkt_sls.offr_perd_id%TYPE;
    l_dms_off_offr_perd_id dstrbtd_mrkt_sls.offr_perd_id%TYPE;
    l_bilng_day        DATE;
  
    -- for LOG
    l_module_name    VARCHAR2(30) := 'get_trend_alloc_hist_dtls';
    l_parameter_list VARCHAR2(2048) := '(p_mrkt_id: ' || to_char(p_mrkt_id) || ', ' ||
                                       'p_sls_perd_id: ' ||
                                       to_char(p_sls_perd_id) || ', ' ||
                                       'p_trg_perd_id: ' ||
                                       to_char(p_trg_perd_id) || ', ' ||
                                       'p_sls_typ_id: ' ||
                                       to_char(p_sls_typ_id) || ', ' ||
                                       'p_prcsng_dt: ' ||
                                       to_char(p_bilng_day) || ')';
    --
    l_run_id  NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id VARCHAR(35) := USER();
  
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || l_parameter_list || ' start');
    
    -- MRKT_CONFIG_ITEM_VAL_TXT
    BEGIN
      SELECT mrkt_config_item_val_txt
        INTO l_sls_typ_id_from_config
        FROM mrkt_config_item
       WHERE mrkt_id = p_mrkt_id
         AND config_item_id = 10000;
    EXCEPTION
      WHEN OTHERS THEN
        app_plsql_log.info(l_module_name || l_parameter_list ||
                           ' error sls_typ_id_from_config not found');
        l_sls_typ_id_from_config := -999;
    END;

FOR i_prd IN (SELECT -- dbt
                   c_sls_perd_id dbt_on_sls_perd_id,
                   c_trg_perd_id dbt_on_offr_perd_id,
                   c_sls_perd_id dbt_off_sls_perd_id,
                   pa_maps_public.perd_plus(p_mrkt_id, c_trg_perd_id, -1) dbt_off_offr_perd_id,
                   -- dms
                   c_trg_perd_id dms_on_sls_perd_id,
                   c_trg_perd_id dms_on_offr_perd_id,
                   c_trg_perd_id dms_off_sls_perd_id,
                   pa_maps_public.perd_plus(p_mrkt_id, c_trg_perd_id, -1) dms_off_offr_perd_id,
                   -- billing day
                   p_bilng_day dbt_bilng_day,
                   -- cash value
                   c_cash_val
                    FROM (SELECT p_sls_perd_id c_sls_perd_id,
                                 p_trg_perd_id c_trg_perd_id,
                                 mrkt_sls_perd.sct_cash_val c_cash_val
                            FROM mrkt_sls_perd
                                    WHERE p_mrkt_id = mrkt_sls_perd.mrkt_id
                                      AND p_sls_perd_id =  mrkt_sls_perd.sls_perd_id
                          )) LOOP
          FOR i_agg_line IN (SELECT
                              offr_sku_line.sku_id,
                              offr.mrkt_id,
                              offr.veh_id,
                              offr.offr_desc_txt,
                              offr_prfl_prc_point.promtn_id,
                              offr_prfl_prc_point.promtn_clm_id,
                              offr_prfl_prc_point.sls_cls_cd,
                              round(SUM(case when dstrbtd_mrkt_sls.sls_typ_id=1 
                                                and i_prd.dbt_on_offr_perd_id=dly_bilng_trnd.offr_perd_id
                                              then dly_bilng_trnd_offr_sku_line.unit_qty
                                              else 0 end)) bi24_unts,
                              round(SUM(case when dstrbtd_mrkt_sls.sls_typ_id=1 
                                                and i_prd.dbt_on_offr_perd_id=dly_bilng_trnd.offr_perd_id
                                              then dly_bilng_trnd_offr_sku_line.unit_qty *
                                                dly_bilng_trnd.sls_prc_amt /
                                                decode(dly_bilng_trnd.nr_for_qty,
                                                       0,
                                                       1,
                                                       dly_bilng_trnd.nr_for_qty) -
                                                dly_bilng_trnd_offr_sku_line.comsn_amt -
                                                dly_bilng_trnd_offr_sku_line.tax_amt
                                                else 0 end)) bi24_sls,
                              round(SUM(case when dstrbtd_mrkt_sls.sls_typ_id=1 
                                                and i_prd.dbt_off_offr_perd_id=dly_bilng_trnd.offr_perd_id
                                              then dly_bilng_trnd_offr_sku_line.unit_qty
                                              else 0 end)) bi24_unts_off,
                              round(SUM(case when dstrbtd_mrkt_sls.sls_typ_id=1 
                                                and i_prd.dbt_off_offr_perd_id=dly_bilng_trnd.offr_perd_id
                                              then dly_bilng_trnd_offr_sku_line.unit_qty *
                                                dly_bilng_trnd.sls_prc_amt /
                                                decode(dly_bilng_trnd.nr_for_qty,
                                                       0,
                                                       1,
                                                       dly_bilng_trnd.nr_for_qty) -
                                                dly_bilng_trnd_offr_sku_line.comsn_amt -
                                                dly_bilng_trnd_offr_sku_line.tax_amt
                                                else 0 end)) bi24_sls_off,
                               i_prd.c_cash_val cash_vlu,
                               l_sct_r_factor r_factor,
                               round(SUM(case when dstrbtd_mrkt_sls.sls_typ_id=p_sls_typ_id 
                                                and i_prd.dms_on_offr_perd_id=dstrbtd_mrkt_sls.offr_perd_id
                                              then dstrbtd_mrkt_sls.unit_qty
                                              else 0 end)) trend_unts,
                              round(SUM(case when dstrbtd_mrkt_sls.sls_typ_id=p_sls_typ_id 
                                                and i_prd.dms_on_offr_perd_id=dstrbtd_mrkt_sls.offr_perd_id
                                              then dstrbtd_mrkt_sls.unit_qty *
                                                offr_prfl_prc_point.sls_prc_amt /
                                                decode(offr_prfl_prc_point.nr_for_qty,
                                                       0,
                                                       1,
                                                       offr_prfl_prc_point.nr_for_qty) *
                                                offr_prfl_prc_point.net_to_avon_fct
                                              else 0 end)) trend_sls,
                              round(SUM(case when dstrbtd_mrkt_sls.sls_typ_id=p_sls_typ_id 
                                                and i_prd.dms_off_offr_perd_id=dstrbtd_mrkt_sls.offr_perd_id
                                              then dstrbtd_mrkt_sls.unit_qty
                                              else 0 end)) trend_unts_off,
                              round(SUM(case when dstrbtd_mrkt_sls.sls_typ_id=p_sls_typ_id 
                                                and i_prd.dms_off_offr_perd_id=dstrbtd_mrkt_sls.offr_perd_id
                                              then dstrbtd_mrkt_sls.unit_qty *
                                                offr_prfl_prc_point.sls_prc_amt /
                                                decode(offr_prfl_prc_point.nr_for_qty,
                                                       0,
                                                       1,
                                                       offr_prfl_prc_point.nr_for_qty) *
                                                offr_prfl_prc_point.net_to_avon_fct
                                              else 0 end)) trend_sls_off,
                              round(SUM(case when dstrbtd_mrkt_sls.sls_typ_id=1 
                                                and i_prd.dms_on_offr_perd_id=dstrbtd_mrkt_sls.offr_perd_id
                                              then dstrbtd_mrkt_sls.unit_qty
                                              else 0 end)) estimate_unts,
                              round(SUM(case when dstrbtd_mrkt_sls.sls_typ_id=1 
                                                and i_prd.dms_on_offr_perd_id=dstrbtd_mrkt_sls.offr_perd_id
                                              then dstrbtd_mrkt_sls.unit_qty *
                                                offr_prfl_prc_point.sls_prc_amt /
                                                decode(offr_prfl_prc_point.nr_for_qty,
                                                       0,
                                                       1,
                                                       offr_prfl_prc_point.nr_for_qty) *
                                                offr_prfl_prc_point.net_to_avon_fct
                                              else 0 end)) estimate_sls,
                              round(SUM(case when dstrbtd_mrkt_sls.sls_typ_id=1 
                                                and i_prd.dms_off_offr_perd_id=dstrbtd_mrkt_sls.offr_perd_id
                                              then dstrbtd_mrkt_sls.unit_qty
                                              else 0 end)) estimate_unts_off,
                              round(SUM(case when dstrbtd_mrkt_sls.sls_typ_id=1 
                                                and i_prd.dms_off_offr_perd_id=dstrbtd_mrkt_sls.offr_perd_id
                                              then dstrbtd_mrkt_sls.unit_qty *
                                                offr_prfl_prc_point.sls_prc_amt /
                                                decode(offr_prfl_prc_point.nr_for_qty,
                                                       0,
                                                       1,
                                                       offr_prfl_prc_point.nr_for_qty) *
                                                offr_prfl_prc_point.net_to_avon_fct
                                              else 0 end)) estimate_sls_off,
                              round(SUM(case when dstrbtd_mrkt_sls.sls_typ_id=6 
                                                and i_prd.dms_on_offr_perd_id=dstrbtd_mrkt_sls.offr_perd_id
                                              then dstrbtd_mrkt_sls.unit_qty
                                              else 0 end)) actual_unts,
                              round(SUM(case when dstrbtd_mrkt_sls.sls_typ_id=6 
                                                and i_prd.dms_on_offr_perd_id=dstrbtd_mrkt_sls.offr_perd_id
                                              then dstrbtd_mrkt_sls.unit_qty *
                                                offr_prfl_prc_point.sls_prc_amt /
                                                decode(offr_prfl_prc_point.nr_for_qty,
                                                       0,
                                                       1,
                                                       offr_prfl_prc_point.nr_for_qty) *
                                                offr_prfl_prc_point.net_to_avon_fct
                                              else 0 end)) actual_sls,
                              round(SUM(case when dstrbtd_mrkt_sls.sls_typ_id=6 
                                                and i_prd.dms_off_offr_perd_id=dstrbtd_mrkt_sls.offr_perd_id
                                              then dstrbtd_mrkt_sls.unit_qty
                                              else 0 end)) actual_unts_off,
                              round(SUM(case when dstrbtd_mrkt_sls.sls_typ_id=6 
                                                and i_prd.dms_off_offr_perd_id=dstrbtd_mrkt_sls.offr_perd_id
                                              then dstrbtd_mrkt_sls.unit_qty *
                                                offr_prfl_prc_point.sls_prc_amt /
                                                decode(offr_prfl_prc_point.nr_for_qty,
                                                       0,
                                                       1,
                                                       offr_prfl_prc_point.nr_for_qty) *
                                                offr_prfl_prc_point.net_to_avon_fct
                                              else 0 end)) actual_sls_off

                          FROM dly_bilng_trnd,
                               dly_bilng_trnd_offr_sku_line,
                               dstrbtd_mrkt_sls,
                               offr_sku_line,
                               offr_prfl_prc_point,
                               offr
                         WHERE 1 = 1
                              -- filter: dly_bilng_trnd
                           AND dly_bilng_trnd.mrkt_id = p_mrkt_id
                           AND dly_bilng_trnd.trnd_sls_perd_id =
                               i_prd.dbt_on_sls_perd_id
                           AND (( --on-schedule
                                dly_bilng_trnd.trnd_sls_perd_id =
                                i_prd.dbt_on_sls_perd_id AND
                                dly_bilng_trnd.offr_perd_id =
                                i_prd.dbt_on_offr_perd_id) OR
                               ( --off-schedule
                                dly_bilng_trnd.trnd_sls_perd_id =
                                i_prd.dbt_off_sls_perd_id AND
                                dly_bilng_trnd.offr_perd_id =
                                i_prd.dbt_off_offr_perd_id))
                           AND trunc(dly_bilng_trnd.prcsng_dt) <=
                               i_prd.dbt_bilng_day
                              -- join: dly_bilng_trnd -> dly_bilng_trnd_offr_sku_line
                           AND dly_bilng_trnd.dly_bilng_id =
                               dly_bilng_trnd_offr_sku_line.dly_bilng_id
                              -- filter: p_sls_typ_id
                           AND dly_bilng_trnd_offr_sku_line.sls_typ_id =
                               l_sls_typ_id_from_config
                              -- join: dly_bilng_trnd_offr_sku_line -> dstrbtd_mrkt_sls
                           AND dly_bilng_trnd_offr_sku_line.offr_sku_line_id =
                               dstrbtd_mrkt_sls.offr_sku_line_id
                              -- filter: mrkt_id, 
                           AND dstrbtd_mrkt_sls.mrkt_id = p_mrkt_id
                           AND (( --on-schedule
                                dstrbtd_mrkt_sls.sls_perd_id =
                                i_prd.dms_on_sls_perd_id AND
                                dstrbtd_mrkt_sls.offr_perd_id =
                                i_prd.dms_on_offr_perd_id) OR
                               ( --off-schedule
                                dstrbtd_mrkt_sls.sls_perd_id =
                                i_prd.dms_off_sls_perd_id AND
                                dstrbtd_mrkt_sls.offr_perd_id =
                                i_prd.dms_off_offr_perd_id))
                           AND dstrbtd_mrkt_sls.sls_typ_id IN
                               (1, p_sls_typ_id, 6)
                              -- additional filtering...
                           AND dstrbtd_mrkt_sls.offr_sku_line_id =
                               offr_sku_line.offr_sku_line_id
                           AND offr_sku_line.dltd_ind <> 'Y'
                           AND offr_sku_line.offr_prfl_prcpt_id =
                               offr_prfl_prc_point.offr_prfl_prcpt_id
                           AND offr_prfl_prc_point.offr_id = offr.offr_id
                           AND offr.offr_typ = 'CMP'
                           AND offr.ver_id = 0
                         GROUP BY --       dstrbtd_mrkt_sls.sls_typ_id,
                                 offr_sku_line.sku_id,
                                 -- just for syntax ?
                                 offr.mrkt_id,
                                 offr.veh_id,
                                 offr.offr_desc_txt,
                                 offr_prfl_prc_point.promtn_id,
                                 offr_prfl_prc_point.promtn_clm_id,
                                 offr_prfl_prc_point.sls_cls_cd
                      ) LOOP
      FOR i_prd_sch IN (SELECT mrkt_veh.lcl_veh_desc_txt,
                               catgry.catgry_nm,
                               i_agg_line.offr_desc_txt,
                               pa_maps_public.get_mstr_fsc_cd(p_mrkt_id,
                                                              sku.sku_id,
                                                              p_sls_perd_id) ||
                               ' - ' ||
                               pa_maps_public.get_fsc_desc(p_mrkt_id,
                                                           p_sls_perd_id,
                                                           pa_maps_public.get_mstr_fsc_cd(p_mrkt_id,
                                                                                          sku.sku_id,
                                                                                          p_sls_perd_id)) fsc_nm,
                               pa_maps_public.get_mstr_fsc_cd(p_mrkt_id,
                                                              sku.sku_id,
                                                              p_sls_perd_id) fsc_cd,
                               v_brnd.brnd_nm,
                               sgmt.sgmt_nm,
                               form.form_desc_txt,
                               form_grp.form_grp_desc_txt,
                               promtn.promtn_desc_txt,
                               promtn_clm.promtn_clm_desc_txt,
                               sls_cls.sls_cls_desc_txt,
                               prfl.prfl_cd,
                               prfl.prfl_nm,
                               sku.sku_id,
                               sku.sku_nm,
                               i_agg_line.bi24_unts,
                               i_agg_line.bi24_sls,
                               i_agg_line.bi24_unts_off,
                               i_agg_line.bi24_sls_off,
                               i_agg_line.trend_unts,
                               i_agg_line.trend_sls,
                               i_agg_line.trend_unts_off,
                               i_agg_line.trend_sls_off,
                               i_agg_line.estimate_unts,
                               i_agg_line.estimate_sls,
                               i_agg_line.estimate_unts_off,
                               i_agg_line.estimate_sls_off,
                               i_agg_line.actual_unts,
                               i_agg_line.actual_sls,
                               i_agg_line.actual_unts_off,
                               i_agg_line.actual_sls_off
                          FROM mrkt_veh,
                               catgry,
                               sku,
                               v_brnd,
                               sgmt,
                               form,
                               form_grp,
                               promtn,
                               promtn_clm,
                               sls_cls,
                               prfl
                       
                         WHERE i_agg_line.sku_id = sku.sku_id
                           AND sku.prfl_cd = prfl.prfl_cd
                           AND prfl.catgry_id = catgry.catgry_id
                           AND mrkt_veh.mrkt_id = i_agg_line.mrkt_id
                           AND mrkt_veh.veh_id = i_agg_line.veh_id
                           AND prfl.brnd_id = v_brnd.brnd_id
                           AND prfl.sgmt_id = sgmt.sgmt_id
                           AND prfl.form_id = form.form_id
                           AND form.form_grp_id = form_grp.form_grp_id
                           AND i_agg_line.promtn_id = promtn.promtn_id(+)
                           AND i_agg_line.promtn_clm_id =
                               promtn_clm.promtn_clm_id(+)
                           AND i_agg_line.sls_cls_cd = sls_cls.sls_cls_cd
                         ORDER BY sku_id) LOOP
      
        PIPE ROW(obj_trend_alloc_hist_dt_line(i_prd_sch.lcl_veh_desc_txt,
                                              i_prd_sch.catgry_nm,
                                              i_prd_sch.offr_desc_txt,
                                              i_prd_sch.fsc_nm,
                                              i_prd_sch.fsc_cd,
                                              i_prd_sch.brnd_nm,
                                              i_prd_sch.sgmt_nm,
                                              i_prd_sch.form_desc_txt,
                                              i_prd_sch.form_grp_desc_txt,
                                              i_prd_sch.promtn_desc_txt,
                                              i_prd_sch.promtn_clm_desc_txt,
                                              i_prd_sch.sls_cls_desc_txt,
                                              i_prd_sch.prfl_cd,
                                              i_prd_sch.prfl_nm,
                                              i_prd_sch.sku_id,
                                              i_prd_sch.sku_nm,
                                              i_prd_sch.bi24_unts,
                                              i_prd_sch.bi24_sls,
                                              i_prd_sch.bi24_unts_off,
                                              i_prd_sch.bi24_sls_off,
                                              i_prd_sch.estimate_unts,
                                              i_prd_sch.estimate_sls,
                                              i_prd_sch.estimate_unts_off,
                                              i_prd_sch.estimate_sls_off,
                                              i_prd_sch.trend_unts,
                                              i_prd_sch.trend_sls,
                                              i_prd_sch.trend_unts_off,
                                              i_prd_sch.trend_sls_off,
                                              i_prd_sch.actual_unts,
                                              i_prd_sch.actual_sls,
                                              i_prd_sch.actual_unts_off,
                                              i_prd_sch.actual_sls_off
                                              ));
        END LOOP;
      END LOOP;
    END LOOP;
    app_plsql_log.info(l_module_name || l_parameter_list || ' stop');
  END get_trend_alloc_hist_dtls;

END pa_trend_alloc;