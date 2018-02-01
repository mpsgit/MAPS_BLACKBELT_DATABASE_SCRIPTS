create or replace PACKAGE BODY pa_trend_alloc AS

  -- get_value_from_config
  FUNCTION get_value_from_config(p_mrkt_id        IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                                 p_config_item_id IN mrkt_config_item.config_item_id%TYPE,
                                 p_user_id        IN VARCHAR2 DEFAULT NULL,
                                 p_run_id         IN NUMBER DEFAULT NULL)
    RETURN mrkt_config_item.mrkt_config_item_val_txt%TYPE IS
    -- local variables
    l_value_from_config mrkt_config_item.mrkt_config_item_val_txt%TYPE;
    -- for LOG
    l_run_id         NUMBER := nvl(p_run_id,
                                   app_plsql_output.generate_new_run_id);
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'GET_VALUE_FROM_CONFIG';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       to_char(p_mrkt_id) || ', ' ||
                                       'p_config_item_id: ' ||
                                       to_char(p_config_item_id) || ', ' ||
                                       'p_user_id: ' || l_user_id || ', ' ||
                                       'p_run_id: ' || to_char(l_run_id) || ')';
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    -- VALUE_from_config
    BEGIN
      SELECT mrkt_config_item_val_txt
        INTO l_value_from_config
        FROM mrkt_config_item
       WHERE mrkt_id = p_mrkt_id
         AND config_item_id = p_config_item_id;
    EXCEPTION
      WHEN OTHERS THEN
        app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
        app_plsql_log.info(l_module_name ||
                           ' warning: "MRKT_CONFIG_ITEM_VAL_TXT" not found in table: mrkt_config_item where mrkt_id=' ||
                           to_char(p_mrkt_id) || 'and config_item_id=' ||
                           to_char(p_config_item_id) || l_parameter_list);
        l_value_from_config := NULL;
    END;
    RETURN l_value_from_config;
  END get_value_from_config;

  -- get_cache_flag
  FUNCTION get_cache_flag(p_mrkt_id        IN ta_config.mrkt_id%TYPE,
                          p_sls_perd_id    IN ta_config.eff_sls_perd_id%TYPE,
                          p_sls_typ_id     IN ta_config.trgt_sls_typ_id%TYPE,
                          p_sls_typ_grp_nm IN ta_config.sls_typ_grp_nm%TYPE,
                          p_bilng_day      IN dly_bilng_trnd.prcsng_dt%TYPE,
                          p_user_id        IN VARCHAR2 DEFAULT NULL,
                          p_run_id         IN NUMBER DEFAULT NULL)
    RETURN BOOLEAN IS
    -- local variables
    l_dms_last_updt_ts dstrbtd_mrkt_sls.last_updt_ts%TYPE;
    l_dbt_last_updt_ts dly_bilng_trnd.last_updt_ts%TYPE;
    l_log_last_updt_ts trend_alloc_hist_dtls_log.last_updt_ts%TYPE;
    -- for LOG
    l_run_id         NUMBER := nvl(p_run_id,
                                   app_plsql_output.generate_new_run_id);
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'GET_CACHE_FLAG';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       to_char(p_mrkt_id) || ', ' ||
                                       'p_sls_perd_id: ' ||
                                       to_char(p_sls_perd_id) || ', ' ||
                                       'p_sls_typ_id: ' ||
                                       to_char(p_sls_typ_id) || ', ' ||
                                       'p_sls_typ_grp_nm: ' ||
                                       p_sls_typ_grp_nm || ', ' ||
                                       'p_bilng_day: ' ||
                                       to_char(p_bilng_day, 'yyyy-mm-dd') || ', ' ||
                                       'p_user_id: ' || l_user_id || ', ' ||
                                       'p_run_id: ' || to_char(l_run_id) || ')';
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    -- CHECK c_sls_typ_grp_nm_dms
    IF p_sls_typ_grp_nm = c_sls_typ_grp_nm_dms THEN
      BEGIN
        SELECT greatest(MAX(dstrbtd_mrkt_sls.last_updt_ts),
                        MAX(offr_sku_line.last_updt_ts),
                        MAX(offr_prfl_prc_point.last_updt_ts))
          INTO l_dms_last_updt_ts
          FROM dstrbtd_mrkt_sls,
               offr_prfl_prc_point,
               offr_sku_line,
               (SELECT src_sls_typ_id,
                       x_src_sls_typ_id,
                       offst_lbl_id,
                       sls_typ_lbl_id,
                       x_sls_typ_lbl_id,
                       pa_maps_public.perd_plus(p_mrkt_id,
                                                p_sls_perd_id,
                                                offst_val_src_sls) src_sls_perd_id,
                       pa_maps_public.perd_plus(p_mrkt_id,
                                                p_sls_perd_id,
                                                offst_val_trgt_sls) trgt_sls_perd_id,
                       pa_maps_public.perd_plus(p_mrkt_id,
                                                p_sls_perd_id,
                                                offst_val_src_offr) src_offr_perd_id,
                       pa_maps_public.perd_plus(p_mrkt_id,
                                                p_sls_perd_id,
                                                offst_val_trgt_offr) trgt_offr_perd_id,
                       r_factor
                  FROM (SELECT src_sls_typ_id,
                               est_src_sls_typ_id AS x_src_sls_typ_id,
                               offst_lbl_id,
                               sls_typ_lbl_id,
                               x_sls_typ_lbl_id,
                               offst_val_src_sls,
                               offst_val_trgt_sls,
                               offst_val_src_offr,
                               offst_val_trgt_offr,
                               r_factor,
                               eff_sls_perd_id,
                               lead(eff_sls_perd_id, 1) over(PARTITION BY offst_lbl_id, sls_typ_grp_nm ORDER BY eff_sls_perd_id) AS next_eff_sls_perd_id
                          FROM ta_config
                         WHERE mrkt_id = p_mrkt_id
                           AND trgt_sls_typ_id = p_sls_typ_id
                           AND upper(REPLACE(TRIM(sls_typ_grp_nm), '  ', ' ')) IN
                               (SELECT TRIM(regexp_substr(col,
                                                          '[^,]+',
                                                          1,
                                                          LEVEL)) RESULT
                                  FROM (SELECT c_sls_typ_grp_nm_dms col
                                          FROM dual)
                                CONNECT BY LEVEL <=
                                           length(regexp_replace(col, '[^,]+')) + 1)
                           AND eff_sls_perd_id <= p_sls_perd_id)
                 WHERE p_sls_perd_id BETWEEN eff_sls_perd_id AND
                       nvl(next_eff_sls_perd_id, p_sls_perd_id)) tc_dms
         WHERE dstrbtd_mrkt_sls.mrkt_id = p_mrkt_id
           AND dstrbtd_mrkt_sls.sls_perd_id = tc_dms.trgt_sls_perd_id
           AND dstrbtd_mrkt_sls.offr_perd_id = tc_dms.trgt_offr_perd_id
           AND dstrbtd_mrkt_sls.sls_typ_id = tc_dms.src_sls_typ_id
           AND dstrbtd_mrkt_sls.ver_id = 0
           AND dstrbtd_mrkt_sls.offr_sku_line_id = offr_sku_line.offr_sku_line_id
           AND offr_sku_line.offr_prfl_prcpt_id = offr_prfl_prc_point.offr_prfl_prcpt_id;
      EXCEPTION
        WHEN OTHERS THEN
          l_dms_last_updt_ts := SYSDATE;
      END;
      BEGIN
        SELECT MAX(last_updt_ts)
          INTO l_log_last_updt_ts
          FROM trend_alloc_hist_dtls_log
         WHERE mrkt_id = p_mrkt_id
           AND sls_perd_id = p_sls_perd_id
           AND sls_typ_id = p_sls_typ_id
           AND sls_typ_grp_nm = c_sls_typ_grp_nm_dms
           AND bilng_day = p_bilng_day;
      EXCEPTION
        WHEN OTHERS THEN
          l_log_last_updt_ts := l_dms_last_updt_ts - 999;
      END;
      IF l_log_last_updt_ts > l_dms_last_updt_ts THEN
        app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
        app_plsql_log.info(l_module_name || ' (TRUE) - ' || c_sls_typ_grp_nm_dms || l_parameter_list);
        RETURN TRUE;
      ELSE
        app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
        app_plsql_log.info(l_module_name || ' (FALSE) - ' || c_sls_typ_grp_nm_dms || l_parameter_list);
        RETURN FALSE;
      END IF;
    END IF;
    -- CHECK c_sls_typ_grp_nm_bi24
    IF p_sls_typ_grp_nm = c_sls_typ_grp_nm_bi24 THEN
      BEGIN
        SELECT MAX(dly_bilng_trnd.last_updt_ts) last_updated_ts
          INTO l_dbt_last_updt_ts
          FROM dly_bilng_trnd,
               (SELECT src_sls_typ_id,
                       x_src_sls_typ_id,
                       offst_lbl_id,
                       sls_typ_lbl_id,
                       x_sls_typ_lbl_id,
                       pa_maps_public.perd_plus(p_mrkt_id,
                                                p_sls_perd_id,
                                                offst_val_src_sls) src_sls_perd_id,
                       pa_maps_public.perd_plus(p_mrkt_id,
                                                p_sls_perd_id,
                                                offst_val_trgt_sls) trgt_sls_perd_id,
                       pa_maps_public.perd_plus(p_mrkt_id,
                                                p_sls_perd_id,
                                                offst_val_src_offr) src_offr_perd_id,
                       pa_maps_public.perd_plus(p_mrkt_id,
                                                p_sls_perd_id,
                                                offst_val_trgt_offr) trgt_offr_perd_id,
                       r_factor
                  FROM (SELECT src_sls_typ_id,
                               est_src_sls_typ_id AS x_src_sls_typ_id,
                               offst_lbl_id,
                               sls_typ_lbl_id,
                               x_sls_typ_lbl_id,
                               offst_val_src_sls,
                               offst_val_trgt_sls,
                               offst_val_src_offr,
                               offst_val_trgt_offr,
                               r_factor,
                               eff_sls_perd_id,
                               lead(eff_sls_perd_id, 1) over(PARTITION BY offst_lbl_id, sls_typ_grp_nm ORDER BY eff_sls_perd_id) AS next_eff_sls_perd_id
                          FROM ta_config
                         WHERE mrkt_id = p_mrkt_id
                           AND trgt_sls_typ_id = p_sls_typ_id
                           AND upper(REPLACE(TRIM(sls_typ_grp_nm), '  ', ' ')) =
                               c_sls_typ_grp_nm_bi24
                           AND eff_sls_perd_id <= p_sls_perd_id)
                 WHERE p_sls_perd_id BETWEEN eff_sls_perd_id AND
                       nvl(next_eff_sls_perd_id, p_sls_perd_id)) tc_bi24
         WHERE dly_bilng_trnd.mrkt_id = p_mrkt_id
           AND dly_bilng_trnd.trnd_sls_perd_id = tc_bi24.src_sls_perd_id;
      EXCEPTION
        WHEN OTHERS THEN
          l_dbt_last_updt_ts := SYSDATE;
      END;
      BEGIN
        SELECT MAX(last_updt_ts)
          INTO l_log_last_updt_ts
          FROM trend_alloc_hist_dtls_log
         WHERE mrkt_id = p_mrkt_id
           AND sls_perd_id = p_sls_perd_id
           AND sls_typ_id = p_sls_typ_id
           AND sls_typ_grp_nm = c_sls_typ_grp_nm_bi24
           AND bilng_day = p_bilng_day;
      EXCEPTION
        WHEN OTHERS THEN
          l_log_last_updt_ts := l_dbt_last_updt_ts - 999;
      END;
      IF l_log_last_updt_ts > l_dbt_last_updt_ts THEN
        app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
        app_plsql_log.info(l_module_name || ' (TRUE) - ' || c_sls_typ_grp_nm_bi24 || l_parameter_list);
        RETURN TRUE;
      ELSE
        app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
        app_plsql_log.info(l_module_name || ' (FALSE) - ' || c_sls_typ_grp_nm_bi24 || l_parameter_list);
        RETURN FALSE;
      END IF;
    END IF;
  END get_cache_flag;

  -- get_ta_config
  FUNCTION get_ta_config(p_mrkt_id        IN ta_config.mrkt_id%TYPE,
                         p_sls_perd_id    IN ta_config.eff_sls_perd_id%TYPE,
                         p_sls_typ_id     IN ta_config.trgt_sls_typ_id%TYPE,
                         p_sls_typ_grp_nm IN ta_config.sls_typ_grp_nm%TYPE DEFAULT NULL,
                         p_user_id        IN VARCHAR2 DEFAULT NULL,
                         p_run_id         IN NUMBER DEFAULT NULL)
    RETURN obj_ta_config_table
    PIPELINED AS
    -- for LOG
    l_run_id         NUMBER := nvl(p_run_id,
                                   app_plsql_output.generate_new_run_id);
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'GET_TA_CONFIG';
    l_sls_typ_grp_nm VARCHAR2(128) := nvl(p_sls_typ_grp_nm, c_sls_typ_grp_nm_bi24 || ', ' || c_sls_typ_grp_nm_dms || ', ' || c_sls_typ_grp_nm_fc_dbt || ', ' || c_sls_typ_grp_nm_fc_dms);
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       to_char(p_mrkt_id) || ', ' ||
                                       'p_sls_perd_id: ' ||
                                       to_char(p_sls_perd_id) || ', ' ||
                                       'p_sls_typ_id: ' ||
                                       to_char(p_sls_typ_id) || ', ' ||
                                       'p_sls_typ_grp_nm: ' || nvl(p_sls_typ_grp_nm, '(null)') || ', ' ||
                                       'p_user_id: ' || l_user_id || ', ' ||
                                       'p_run_id: ' || to_char(l_run_id) || ')';
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    --
    BEGIN
      FOR i IN (SELECT src_sls_typ_id,
                       x_src_sls_typ_id,
                       offst_lbl_id,
                       sls_typ_lbl_id,
                       x_sls_typ_lbl_id,
                       pa_maps_public.perd_plus(p_mrkt_id,
                                                p_sls_perd_id,
                                                offst_val_src_sls) src_sls_perd_id,
                       pa_maps_public.perd_plus(p_mrkt_id,
                                                p_sls_perd_id,
                                                offst_val_trgt_sls) trgt_sls_perd_id,
                       pa_maps_public.perd_plus(p_mrkt_id,
                                                p_sls_perd_id,
                                                offst_val_src_offr) src_offr_perd_id,
                       pa_maps_public.perd_plus(p_mrkt_id,
                                                p_sls_perd_id,
                                                offst_val_trgt_offr) trgt_offr_perd_id,
                       r_factor
                  FROM (SELECT src_sls_typ_id,
                               est_src_sls_typ_id AS x_src_sls_typ_id,
                               offst_lbl_id,
                               sls_typ_lbl_id,
                               x_sls_typ_lbl_id,
                               offst_val_src_sls,
                               offst_val_trgt_sls,
                               offst_val_src_offr,
                               offst_val_trgt_offr,
                               r_factor,
                               eff_sls_perd_id,
                               lead(eff_sls_perd_id, 1) over(PARTITION BY offst_lbl_id, sls_typ_grp_nm ORDER BY eff_sls_perd_id) AS next_eff_sls_perd_id
                          FROM ta_config
                         WHERE mrkt_id = p_mrkt_id
                           AND trgt_sls_typ_id = p_sls_typ_id
                           AND upper(REPLACE(TRIM(sls_typ_grp_nm), '  ', ' ')) IN
                               (SELECT TRIM(regexp_substr(col,
                                                          '[^,]+',
                                                          1,
                                                          LEVEL)) RESULT
                                  FROM (SELECT l_sls_typ_grp_nm col FROM dual)
                                CONNECT BY LEVEL <=
                                           length(regexp_replace(col, '[^,]+')) + 1)
                           AND eff_sls_perd_id <= p_sls_perd_id)
                 WHERE p_sls_perd_id BETWEEN eff_sls_perd_id AND
                       nvl(next_eff_sls_perd_id, p_sls_perd_id)) LOOP
        PIPE ROW(obj_ta_config_line(i.src_sls_typ_id,
                                    i.x_src_sls_typ_id,
                                    i.offst_lbl_id,
                                    i.sls_typ_lbl_id,
                                    i.x_sls_typ_lbl_id,
                                    i.src_sls_perd_id,
                                    i.trgt_sls_perd_id,
                                    i.src_offr_perd_id,
                                    i.trgt_offr_perd_id,
                                    i.r_factor));
      END LOOP;
    EXCEPTION
      WHEN OTHERS THEN
        app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
        app_plsql_log.info(l_module_name ||
                           ' warning: RECORD not found in table: ta_config where mrkt_id=' ||
                           to_char(p_mrkt_id) || ' and trgt_sls_typ_id = ' ||
                           to_char(p_sls_typ_id) ||
                           ' and sls_typ_grp_nm in (' || l_sls_typ_grp_nm || ')' ||
                           ' and eff_sls_perd_id <= ' ||
                           to_char(p_sls_perd_id) ||
                           ' < next_eff_sls_perd_id' || l_parameter_list);
    END;
  END get_ta_config;

  -- get_periods
  FUNCTION get_periods(p_mrkt_id       IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                       p_orig_perd_id  IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                       p_bilng_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                       p_sls_typ_id    IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                       p_bilng_day     IN dly_bilng_trnd.prcsng_dt%TYPE,
                       p_user_id       IN VARCHAR2 DEFAULT NULL,
                       p_run_id        IN NUMBER DEFAULT NULL)
    RETURN r_periods IS
    l_periods r_periods;
    -- for LOG
    l_run_id         NUMBER := nvl(p_run_id,
                                   app_plsql_output.generate_new_run_id);
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'GET_PERIODS';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       to_char(p_mrkt_id) || ', ' ||
                                       'p_orig_perd_id: ' ||
                                       to_char(p_orig_perd_id) || ', ' ||
                                       'p_bilng_perd_id: ' ||
                                       to_char(p_bilng_perd_id) || ', ' ||
                                       'p_sls_typ_id: ' ||
                                       to_char(p_sls_typ_id) || ', ' ||
                                       'p_bilng_day: ' ||
                                       to_char(p_bilng_day, 'yyyy-mm-dd') || ', ' ||
                                       'p_user_id: ' || l_user_id || ', ' ||
                                       'p_run_id: ' || to_char(l_run_id) || ')';
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    --
    l_periods.sls_perd_id := p_bilng_perd_id;
    BEGIN
      SELECT MAX(trgt_sls_perd_id)
        INTO l_periods.trg_perd_id
        FROM TABLE(get_ta_config(p_mrkt_id        => p_mrkt_id,
                                 p_sls_perd_id    => p_bilng_perd_id,
                                 p_sls_typ_id     => p_sls_typ_id,
                                 p_sls_typ_grp_nm => c_sls_typ_grp_nm_bi24));
    EXCEPTION
      WHEN OTHERS THEN
        app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
        app_plsql_log.info(l_module_name ||
                           ' warning: RECORD not found in table: ta_config where mrkt_id = ' ||
                           to_char(p_mrkt_id) || ' and sls_perd_id = ' ||
                           to_char(p_bilng_perd_id) ||
                           ' and sls_typ_id = ' || p_sls_typ_id ||
                           ' and sls_typ_grp_nm = ' ||
                           c_sls_typ_grp_nm_bi24 || l_parameter_list);
        l_periods.trg_perd_id := NULL;
    END;
    -- billing day
    IF p_orig_perd_id = p_bilng_perd_id THEN
      l_periods.bilng_day := p_bilng_day;
    ELSE
      -- has to be recalculated for billing period 
      BEGIN
        SELECT MAX(trnd_bilng_days.prcsng_dt)
          INTO l_periods.bilng_day
          FROM trnd_bilng_days
         WHERE trnd_bilng_days.mrkt_id = p_mrkt_id
           AND trnd_bilng_days.sls_perd_id = p_bilng_perd_id
           AND trnd_bilng_days.day_num <=
               (SELECT trnd_bilng_days.day_num
                  FROM trnd_bilng_days
                 WHERE trnd_bilng_days.mrkt_id = p_mrkt_id
                   AND trnd_bilng_days.sls_perd_id = p_orig_perd_id
                   AND trnd_bilng_days.prcsng_dt = p_bilng_day);
      EXCEPTION
        WHEN OTHERS THEN
          app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
          app_plsql_log.info(l_module_name ||
                             ' warning: RECORD not found in table: trnd_bilng_days where mrkt_id = ' ||
                             to_char(p_mrkt_id) || ' and orig_perd_id = ' ||
                             to_char(p_orig_perd_id) ||
                             ' and bilng_perd_id = ' ||
                             to_char(p_bilng_perd_id) ||
                             ' and prcsng_dt = ' ||
                             to_char(p_bilng_day, 'yyyy-mm-dd') ||
                             l_parameter_list);
          l_periods.bilng_day := NULL;
      END;
    END IF;
    -- sct_cash_value_on, sct_cash_value_off
    BEGIN
      SELECT sct_cash_value_on, sct_cash_value_off
        INTO l_periods.sct_cash_value_on, l_periods.sct_cash_value_off
        FROM mrkt_trnd_sls_perd_sls_typ
       WHERE p_mrkt_id = mrkt_id
         AND l_periods.trg_perd_id = trnd_sls_perd_id
         AND p_sls_typ_id = sls_typ_id;
    EXCEPTION
      WHEN OTHERS THEN
        app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
        app_plsql_log.info(l_module_name ||
                           ' warning: RECORD not found in table: MRKT_TRND_SLS_PERD_SLS_TYP where mrkt_id = ' ||
                           to_char(p_mrkt_id) ||
                           ' and trnd_sls_perd_id = ' ||
                           to_char(l_periods.trg_perd_id) ||
                           ' and sls_typ_id = ' || to_char(p_sls_typ_id) ||
                           l_parameter_list);
        l_periods.sct_cash_value_on := NULL;
        l_periods.sct_cash_value_off := NULL;
    END;
    RETURN l_periods;
  END get_periods;

  -- get_rule_nm
  FUNCTION get_rule_nm(p_mrkt_id        IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                       p_campgn_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                       p_sls_typ_id     IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                       p_offst_lbl_id   IN custm_seg_mstr.offst_lbl_id%TYPE,
                       p_catgry_id      IN custm_seg_mstr.catgry_id%TYPE,
                       p_sls_cls_cd     IN custm_seg_mstr.sls_cls_cd%TYPE,
                       p_veh_id         IN custm_seg_mstr.veh_id%TYPE,
                       p_perd_part      IN custm_seg_mstr.perd_part%TYPE,
                       p_sku_id         IN dly_bilng_trnd.sku_id%TYPE)
    RETURN custm_seg_mstr.rul_nm%TYPE
    IS
    -- local variables
    l_rul_nm  custm_rul_mstr.rul_nm%TYPE;
  BEGIN
    BEGIN
      SELECT rul_nm
        INTO l_rul_nm
        FROM (SELECT rul_id,
                     rul_nm,
                     offst_lbl_id,
                     catgry_id,
                     sls_cls_cd,
                     veh_id,
                     perd_part,
                     sku_list,
                     prirty,
                     period_list,
                     r_factor,
                     r_factor_manual,
                     cash_value,
                     use_estimate,
                     row_number() OVER(ORDER BY prirty) AS primary_rule
                FROM (SELECT rm.rul_id,
                             rm.rul_nm,
                             CAST(NULL AS NUMBER) offst_lbl_id,
                             CAST(NULL AS NUMBER) catgry_id,
                             CAST(NULL AS NUMBER) sls_cls_cd,
                             CAST(NULL AS NUMBER) veh_id,
                             CAST(NULL AS NUMBER) perd_part,
                             listagg(rs.sku_id, ',') within GROUP(ORDER BY rs.sku_id) sku_list,
                             0 prirty,
                             r.period_list,
                             r.r_factor,
                             r.r_factor_manual,
                             r.cash_value,
                             upper(r.use_estimate) use_estimate
                        FROM custm_rul_mstr     rm,
                             custm_rul_perd     r,
                             custm_rul_sku_list rs
                       WHERE rm.mrkt_id = p_mrkt_id
                         AND r.campgn_perd_id = p_campgn_perd_id
                         AND rm.rul_id = rs.rul_id
                         AND rm.rul_id = r.rul_id
                       GROUP BY rm.rul_id,
                                rm.rul_nm,
                                r.period_list,
                                r.r_factor,
                                r.r_factor_manual,
                                r.cash_value,
                                upper(r.use_estimate)
                      UNION
                      SELECT ms.rul_id,
                             ms.rul_nm,
                             ms.offst_lbl_id,
                             ms.catgry_id,
                             ms.sls_cls_cd,
                             ms.veh_id,
                             ms.perd_part,
                             CAST(NULL AS VARCHAR(2048)) sku_list,
                             nvl(s.prirty, 9999) prirty,
                             s.period_list,
                             s.r_factor,
                             s.r_factor_manual,
                             s.cash_value,
                             upper(s.use_estimate) use_estimate
                        FROM custm_seg_mstr ms, custm_seg_perd s
                       WHERE ms.mrkt_id = p_mrkt_id
                         AND ms.rul_id = s.rul_id(+)
                         AND p_campgn_perd_id = s.campgn_perd_id(+)
                         AND p_sls_typ_id = s.sls_typ_id(+))
               WHERE nvl(p_offst_lbl_id, -1) = nvl(offst_lbl_id(+), nvl(p_offst_lbl_id, -1))
                 AND nvl(p_catgry_id, -1) = nvl(catgry_id(+), nvl(p_catgry_id, -1))
                 AND nvl(p_sls_cls_cd, '-1') = nvl(sls_cls_cd(+), nvl(p_sls_cls_cd, '-1'))
                 AND nvl(p_veh_id, -1) = nvl(veh_id(+), nvl(p_veh_id, -1))
                 AND nvl(p_perd_part, -1) = nvl(perd_part(+), nvl(p_perd_part, -1))
                 AND instr(nvl(sku_list(+), nvl(p_sku_id, '-1')), nvl(p_sku_id, '-1')) > 0)
       WHERE primary_rule = 1;
    EXCEPTION
      WHEN OTHERS THEN
        l_rul_nm := NULL;
    END;
    --
    RETURN l_rul_nm;
  END get_rule_nm;

  -- get_r_factor
  FUNCTION get_r_factor(p_mrkt_id        IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                        p_campgn_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                        p_sls_typ_id     IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                        p_offst_lbl_id   IN custm_seg_mstr.offst_lbl_id%TYPE,
                        p_catgry_id      IN custm_seg_mstr.catgry_id%TYPE,
                        p_sls_cls_cd     IN custm_seg_mstr.sls_cls_cd%TYPE,
                        p_veh_id         IN custm_seg_mstr.veh_id%TYPE,
                        p_perd_part      IN custm_seg_mstr.perd_part%TYPE,
                        p_sku_id         IN dly_bilng_trnd.sku_id%TYPE)
    RETURN custm_rul_perd.r_factor%TYPE
    IS
    -- local variables
    l_r_factor  custm_rul_perd.r_factor%TYPE;
  BEGIN
    BEGIN
      SELECT nvl(r_factor, r_factor_manual)
        INTO l_r_factor
        FROM (SELECT rul_id,
                     rul_nm,
                     offst_lbl_id,
                     catgry_id,
                     sls_cls_cd,
                     veh_id,
                     perd_part,
                     sku_list,
                     prirty,
                     period_list,
                     r_factor,
                     r_factor_manual,
                     cash_value,
                     use_estimate,
                     row_number() OVER(ORDER BY prirty, rul_id) AS primary_rule
                FROM (SELECT rm.rul_id,
                             rm.rul_nm,
                             CAST(NULL AS NUMBER) offst_lbl_id,
                             CAST(NULL AS NUMBER) catgry_id,
                             CAST(NULL AS NUMBER) sls_cls_cd,
                             CAST(NULL AS NUMBER) veh_id,
                             CAST(NULL AS NUMBER) perd_part,
                             listagg(rs.sku_id, ',') within GROUP(ORDER BY rs.sku_id) sku_list,
                             0 prirty,
                             r.period_list,
                             r.r_factor,
                             r.r_factor_manual,
                             r.cash_value,
                             upper(r.use_estimate) use_estimate
                        FROM custm_rul_mstr     rm,
                             custm_rul_perd     r,
                             custm_rul_sku_list rs
                       WHERE rm.mrkt_id = p_mrkt_id
                         AND r.campgn_perd_id = p_campgn_perd_id
                         AND rm.rul_id = rs.rul_id
                         AND rm.rul_id = r.rul_id
                       GROUP BY rm.rul_id,
                                rm.rul_nm,
                                r.period_list,
                                r.r_factor,
                                r.r_factor_manual,
                                r.cash_value,
                                upper(r.use_estimate)
                      UNION
                      SELECT ms.rul_id,
                             ms.rul_nm,
                             ms.offst_lbl_id,
                             ms.catgry_id,
                             ms.sls_cls_cd,
                             ms.veh_id,
                             ms.perd_part,
                             CAST(NULL AS VARCHAR(2048)) sku_list,
                             nvl(s.prirty, 9999) prirty,
                             s.period_list,
                             s.r_factor,
                             s.r_factor_manual,
                             s.cash_value,
                             upper(s.use_estimate) use_estimate
                        FROM custm_seg_mstr ms, custm_seg_perd s
                       WHERE ms.mrkt_id = p_mrkt_id
                         AND ms.rul_id = s.rul_id(+)
                         AND p_campgn_perd_id = s.campgn_perd_id(+)
                         AND p_sls_typ_id = s.sls_typ_id(+))
               WHERE nvl(p_offst_lbl_id, -1) = nvl(offst_lbl_id(+), nvl(p_offst_lbl_id, -1))
                 AND nvl(p_catgry_id, -1) = nvl(catgry_id(+), nvl(p_catgry_id, -1))
                 AND nvl(p_sls_cls_cd, '-1') = nvl(sls_cls_cd(+), nvl(p_sls_cls_cd, '-1'))
                 AND nvl(p_veh_id, -1) = nvl(veh_id(+), nvl(p_veh_id, -1))
                 AND nvl(p_perd_part, -1) = nvl(perd_part(+), nvl(p_perd_part, -1))
                 AND instr(nvl(sku_list(+), nvl(p_sku_id, '-1')), nvl(p_sku_id, '-1')) > 0)
       WHERE primary_rule = 1;
    EXCEPTION
      WHEN OTHERS THEN
        l_r_factor := NULL;
    END;
    --
    RETURN l_r_factor;
  END get_r_factor;

  -- get_use_estimate
  FUNCTION get_use_estimate(p_mrkt_id        IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                            p_campgn_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                            p_sls_typ_id     IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                            p_offst_lbl_id   IN custm_seg_mstr.offst_lbl_id%TYPE,
                            p_catgry_id      IN custm_seg_mstr.catgry_id%TYPE,
                            p_sls_cls_cd     IN custm_seg_mstr.sls_cls_cd%TYPE,
                            p_veh_id         IN custm_seg_mstr.veh_id%TYPE,
                            p_perd_part      IN custm_seg_mstr.perd_part%TYPE,
                            p_sku_id         IN dly_bilng_trnd.sku_id%TYPE)
    RETURN custm_rul_perd.use_estimate%TYPE
    IS
    -- local variables
    l_use_estimate  custm_rul_mstr.rul_nm%TYPE;
  BEGIN
    BEGIN
      SELECT use_estimate
        INTO l_use_estimate
        FROM (SELECT rul_id,
                     rul_nm,
                     offst_lbl_id,
                     catgry_id,
                     sls_cls_cd,
                     veh_id,
                     perd_part,
                     sku_list,
                     prirty,
                     period_list,
                     r_factor,
                     r_factor_manual,
                     cash_value,
                     use_estimate,
                     row_number() OVER(ORDER BY prirty) AS primary_rule
                FROM (SELECT rm.rul_id,
                             rm.rul_nm,
                             CAST(NULL AS NUMBER) offst_lbl_id,
                             CAST(NULL AS NUMBER) catgry_id,
                             CAST(NULL AS NUMBER) sls_cls_cd,
                             CAST(NULL AS NUMBER) veh_id,
                             CAST(NULL AS NUMBER) perd_part,
                             listagg(rs.sku_id, ',') within GROUP(ORDER BY rs.sku_id) sku_list,
                             0 prirty,
                             r.period_list,
                             r.r_factor,
                             r.r_factor_manual,
                             r.cash_value,
                             upper(r.use_estimate) use_estimate
                        FROM custm_rul_mstr     rm,
                             custm_rul_perd     r,
                             custm_rul_sku_list rs
                       WHERE rm.mrkt_id = p_mrkt_id
                         AND r.campgn_perd_id = p_campgn_perd_id
                         AND rm.rul_id = rs.rul_id
                         AND rm.rul_id = r.rul_id
                       GROUP BY rm.rul_id,
                                rm.rul_nm,
                                r.period_list,
                                r.r_factor,
                                r.r_factor_manual,
                                r.cash_value,
                                upper(r.use_estimate)
                      UNION
                      SELECT ms.rul_id,
                             ms.rul_nm,
                             ms.offst_lbl_id,
                             ms.catgry_id,
                             ms.sls_cls_cd,
                             ms.veh_id,
                             ms.perd_part,
                             CAST(NULL AS VARCHAR(2048)) sku_list,
                             nvl(s.prirty, 9999) prirty,
                             s.period_list,
                             s.r_factor,
                             s.r_factor_manual,
                             s.cash_value,
                             upper(s.use_estimate) use_estimate
                        FROM custm_seg_mstr ms, custm_seg_perd s
                       WHERE ms.mrkt_id = p_mrkt_id
                         AND ms.rul_id = s.rul_id(+)
                         AND p_campgn_perd_id = s.campgn_perd_id(+)
                         AND p_sls_typ_id = s.sls_typ_id(+))
               WHERE nvl(p_offst_lbl_id, -1) = nvl(offst_lbl_id(+), nvl(p_offst_lbl_id, -1))
                 AND nvl(p_catgry_id, -1) = nvl(catgry_id(+), nvl(p_catgry_id, -1))
                 AND nvl(p_sls_cls_cd, '-1') = nvl(sls_cls_cd(+), nvl(p_sls_cls_cd, '-1'))
                 AND nvl(p_veh_id, -1) = nvl(veh_id(+), nvl(p_veh_id, -1))
                 AND nvl(p_perd_part, -1) = nvl(perd_part(+), nvl(p_perd_part, -1))
                 AND instr(nvl(sku_list(+), nvl(p_sku_id, '-1')), nvl(p_sku_id, '-1')) > 0)
       WHERE primary_rule = 1;
    EXCEPTION
      WHEN OTHERS THEN
        l_use_estimate := NULL;
    END;
    --
    RETURN l_use_estimate;
  END get_use_estimate;

  -- get_bi24
  FUNCTION get_bi24(p_mrkt_id              IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                    p_sls_perd_id          IN dly_bilng_trnd.trnd_sls_perd_id%TYPE,
                    p_sls_typ_id           IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                    p_bilng_day            IN dly_bilng_trnd.prcsng_dt%TYPE,
                    p_offst_lbl_id         IN ta_dict.lbl_id%TYPE DEFAULT NULL,
                    p_cash_value           IN NUMBER DEFAULT NULL,
                    p_r_factor             IN NUMBER DEFAULT NULL,
                    p_x_sls_typ_lbl_id_flg IN CHAR DEFAULT 'N',
                    p_perd_part_flg        IN CHAR DEFAULT 'N',
                    p_user_id              IN VARCHAR2 DEFAULT NULL,
                    p_run_id               IN NUMBER DEFAULT NULL)
    RETURN t_hist_detail IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    -- local variables
    l_tbl_bi24_get_cache_flag BOOLEAN;
    l_tbl_bi24_perd           t_hist_detail := t_hist_detail();
    l_tbl_bi24_ind            t_bi24_ind;
    l_tbl_bi24                t_hist_detail := t_hist_detail();
    c_key                     VARCHAR2(512);
    l_sls_typ_lbl_id          ta_dict.lbl_id%TYPE;
    l_ins_cnt                 PLS_INTEGER := 0;
    l_stus                    PLS_INTEGER := 0;
    l_sql_rowcount            NUMBER;
    -- for LOG
    l_run_id         NUMBER := nvl(p_run_id,
                                   app_plsql_output.generate_new_run_id);
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'GET_BI24';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       to_char(p_mrkt_id) || ', ' ||
                                       'p_sls_perd_id: ' ||
                                       to_char(p_sls_perd_id) || ', ' ||
                                       'p_sls_typ_id: ' ||
                                       to_char(p_sls_typ_id) || ', ' ||
                                       'p_bilng_day: ' ||
                                       to_char(p_bilng_day, 'yyyy-mm-dd') || ', ' ||
                                       'p_offst_lbl_id: ' ||
                                       to_char(p_offst_lbl_id) || ', ' ||
                                       'p_cash_value: ' ||
                                       to_char(p_cash_value) || ', ' ||
                                       'p_r_factor: ' ||
                                       to_char(p_r_factor) || ', ' ||
                                       'p_x_sls_typ_lbl_id_flg: ' ||
                                       p_x_sls_typ_lbl_id_flg || ', ' ||
                                       'p_user_id: ' || l_user_id || ', ' ||
                                       'p_run_id: ' || to_char(l_run_id) || ')';
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start' || l_parameter_list);
    BEGIN
      l_tbl_bi24_get_cache_flag := get_cache_flag(p_mrkt_id        => p_mrkt_id,
                                                  p_sls_perd_id    => p_sls_perd_id,
                                                  p_sls_typ_id     => p_sls_typ_id,
                                                  p_sls_typ_grp_nm => c_sls_typ_grp_nm_bi24,
                                                  p_bilng_day      => p_bilng_day,
                                                  p_user_id        => l_user_id,
                                                  p_run_id         => l_run_id);
      IF l_tbl_bi24_get_cache_flag THEN
        -- re-READ
        SELECT trend_alloc_hist_dtls.sku_id,
               trend_alloc_hist_dtls.veh_id,
               trend_alloc_hist_dtls.offr_id,
               trend_alloc_hist_dtls.promtn_id,
               trend_alloc_hist_dtls.promtn_clm_id,
               trend_alloc_hist_dtls.sls_cls_cd,
               trend_alloc_hist_dtls.perd_part,
               trend_alloc_hist_dtls.offst_lbl_id,
               trend_alloc_hist_dtls.cash_value,
               trend_alloc_hist_dtls.r_factor,
               CASE
                 WHEN p_x_sls_typ_lbl_id_flg = 'Y' THEN
                  tc_bi24.x_sls_typ_lbl_id
                 ELSE
                  tc_bi24.sls_typ_lbl_id
               END AS sls_typ_lbl_id,
               trend_alloc_hist_dtls.units,
               trend_alloc_hist_dtls.sales
          BULK COLLECT
          INTO l_tbl_bi24_perd
          FROM trend_alloc_hist_dtls,
               (SELECT offst_lbl_id, sls_typ_lbl_id, x_sls_typ_lbl_id
                  FROM (SELECT src_sls_typ_id,
                               est_src_sls_typ_id AS x_src_sls_typ_id,
                               offst_lbl_id,
                               sls_typ_lbl_id,
                               x_sls_typ_lbl_id,
                               offst_val_src_sls,
                               offst_val_trgt_sls,
                               offst_val_src_offr,
                               offst_val_trgt_offr,
                               r_factor,
                               eff_sls_perd_id,
                               lead(eff_sls_perd_id, 1) over(PARTITION BY offst_lbl_id, sls_typ_grp_nm ORDER BY eff_sls_perd_id) AS next_eff_sls_perd_id
                          FROM ta_config
                         WHERE mrkt_id = p_mrkt_id
                           AND trgt_sls_typ_id = p_sls_typ_id
                           AND upper(REPLACE(TRIM(sls_typ_grp_nm), '  ', ' ')) =
                               c_sls_typ_grp_nm_bi24
                           AND eff_sls_perd_id <= p_sls_perd_id)
                 WHERE p_sls_perd_id BETWEEN eff_sls_perd_id AND
                       nvl(next_eff_sls_perd_id, p_sls_perd_id)) tc_bi24
         WHERE trend_alloc_hist_dtls.mrkt_id = p_mrkt_id
           AND trend_alloc_hist_dtls.sls_perd_id = p_sls_perd_id
           AND trend_alloc_hist_dtls.sls_typ_id = p_sls_typ_id
           AND trend_alloc_hist_dtls.sls_typ_grp_nm = c_sls_typ_grp_nm_bi24
           AND trend_alloc_hist_dtls.bilng_day = p_bilng_day
           AND trend_alloc_hist_dtls.offst_lbl_id = tc_bi24.offst_lbl_id;
        app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
        app_plsql_log.info(l_module_name || ' cache: BI24' ||
                           l_parameter_list);
      ELSE
        -- re-CALCULATE
        WITH dbt AS
         (SELECT /*+ INDEX(DLY_BILNG_TRND FK_MRKTPERD_DLYBLTRNDSLSPERD) */
           dly_bilng_trnd.dly_bilng_id,
           tc_bi24.offst_lbl_id,
           tc_bi24.x_sls_typ_lbl_id,
           tc_bi24.sls_typ_lbl_id,
           tc_bi24.src_sls_typ_id,
           mpp.perd_part
            FROM dly_bilng_trnd,
                 (SELECT src_sls_typ_id,
                         x_src_sls_typ_id,
                         offst_lbl_id,
                         sls_typ_lbl_id,
                         x_sls_typ_lbl_id,
                         pa_maps_public.perd_plus(p_mrkt_id,
                                                  p_sls_perd_id,
                                                  offst_val_src_sls) src_sls_perd_id,
                         pa_maps_public.perd_plus(p_mrkt_id,
                                                  p_sls_perd_id,
                                                  offst_val_trgt_sls) trgt_sls_perd_id,
                         pa_maps_public.perd_plus(p_mrkt_id,
                                                  p_sls_perd_id,
                                                  offst_val_src_offr) src_offr_perd_id,
                         pa_maps_public.perd_plus(p_mrkt_id,
                                                  p_sls_perd_id,
                                                  offst_val_trgt_offr) trgt_offr_perd_id,
                         r_factor
                    FROM (SELECT src_sls_typ_id,
                                 est_src_sls_typ_id AS x_src_sls_typ_id,
                                 offst_lbl_id,
                                 sls_typ_lbl_id,
                                 x_sls_typ_lbl_id,
                                 offst_val_src_sls,
                                 offst_val_trgt_sls,
                                 offst_val_src_offr,
                                 offst_val_trgt_offr,
                                 r_factor,
                                 eff_sls_perd_id,
                                 lead(eff_sls_perd_id, 1) over(PARTITION BY offst_lbl_id, sls_typ_grp_nm ORDER BY eff_sls_perd_id) AS next_eff_sls_perd_id
                            FROM ta_config
                           WHERE mrkt_id = p_mrkt_id
                             AND trgt_sls_typ_id = p_sls_typ_id
                             AND upper(REPLACE(TRIM(sls_typ_grp_nm), '  ', ' ')) =
                                 c_sls_typ_grp_nm_bi24
                             AND eff_sls_perd_id <= p_sls_perd_id)
                   WHERE p_sls_perd_id BETWEEN eff_sls_perd_id AND
                         nvl(next_eff_sls_perd_id, p_sls_perd_id)) tc_bi24,
                 (SELECT trnd_bilng_days.prcsng_dt,
                         CASE
                           WHEN trnd_bilng_days.day_num < p.lte_day_num THEN
                            1
                           WHEN trnd_bilng_days.day_num > p.gte_day_num THEN
                            3
                           ELSE
                            2
                         END AS perd_part
                    FROM trnd_bilng_days,
                         (SELECT lte_day_num, gte_day_num
                            FROM (SELECT mrkt_id,
                                         eff_campgn_perd_id,
                                         lte_day_num,
                                         gte_day_num,
                                         lead(eff_campgn_perd_id) over(ORDER BY eff_campgn_perd_id) next_eff_campgn_perd_id
                                    FROM mrkt_perd_parts
                                   WHERE mrkt_perd_parts.mrkt_id = p_mrkt_id
                                     AND mrkt_perd_parts.eff_campgn_perd_id <=
                                         p_sls_perd_id)
                           WHERE p_sls_perd_id BETWEEN eff_campgn_perd_id AND
                                 nvl(next_eff_campgn_perd_id, p_sls_perd_id)) p
                   WHERE trnd_bilng_days.mrkt_id = p_mrkt_id
                     AND trnd_bilng_days.sls_perd_id = p_sls_perd_id
                     AND trnd_bilng_days.prcsng_dt <= p_bilng_day) mpp
           WHERE dly_bilng_trnd.mrkt_id = p_mrkt_id
             AND dly_bilng_trnd.trnd_sls_perd_id = tc_bi24.src_sls_perd_id
             AND dly_bilng_trnd.offr_perd_id = tc_bi24.src_offr_perd_id
             AND trunc(dly_bilng_trnd.prcsng_dt) <= p_bilng_day
             AND dly_bilng_trnd.trnd_aloctn_auto_stus_id IN
                 (auto_matched, auto_suggested_single, auto_suggested_multi)
             AND trunc(dly_bilng_trnd.prcsng_dt) = mpp.prcsng_dt(+))
        SELECT offr_sku_line.sku_id,
               offr.veh_id,
               offr.offr_id,
               offr_prfl_prc_point.promtn_id,
               nvl(offr_prfl_prc_point.promtn_clm_id, -999),
               offr_prfl_prc_point.sls_cls_cd,
               dbt.perd_part,
               dbt.offst_lbl_id,
               p_cash_value,
               p_r_factor,
               CASE
                 WHEN p_x_sls_typ_lbl_id_flg = 'Y' THEN
                  dbt.x_sls_typ_lbl_id
                 ELSE
                  dbt.sls_typ_lbl_id
               END AS sls_typ_lbl_id,
               round(SUM(nvl(dly_bilng_trnd_offr_sku_line.unit_qty, 0))) units,
               round(SUM(nvl(dly_bilng_trnd_offr_sku_line.unit_qty, 0) *
                         nvl(offr_prfl_prc_point.sls_prc_amt, 0) /
                         decode(nvl(offr_prfl_prc_point.nr_for_qty, 0),
                                0,
                                1,
                                offr_prfl_prc_point.nr_for_qty) *
                         decode(nvl(offr_prfl_prc_point.net_to_avon_fct, 0),
                                0,
                                1,
                                offr_prfl_prc_point.net_to_avon_fct))) sales
          BULK COLLECT
          INTO l_tbl_bi24_perd
          FROM dbt,
               dly_bilng_trnd_offr_sku_line,
               offr_sku_line,
               offr_prfl_prc_point,
               offr
         WHERE dbt.dly_bilng_id = dly_bilng_trnd_offr_sku_line.dly_bilng_id
           AND dly_bilng_trnd_offr_sku_line.sls_typ_id =
               (SELECT MAX(src_sls_typ_id)
                  FROM TABLE(get_ta_config(p_mrkt_id        => p_mrkt_id,
                                           p_sls_perd_id    => p_sls_perd_id,
                                           p_sls_typ_id     => p_sls_typ_id,
                                           p_sls_typ_grp_nm => c_sls_typ_grp_nm_bi24)))
           AND dly_bilng_trnd_offr_sku_line.offr_sku_line_id =
               offr_sku_line.offr_sku_line_id
           AND offr_sku_line.dltd_ind <> 'Y'
           AND offr_sku_line.offr_prfl_prcpt_id =
               offr_prfl_prc_point.offr_prfl_prcpt_id
           AND offr_prfl_prc_point.offr_id = offr.offr_id
           AND offr.offr_typ = 'CMP'
           AND offr.ver_id = 0
         GROUP BY offr_sku_line.sku_id,
                  offr.veh_id,
                  offr.offr_id,
                  offr_prfl_prc_point.promtn_id,
                  offr_prfl_prc_point.promtn_clm_id,
                  offr_prfl_prc_point.sls_cls_cd,
                  dbt.perd_part,
                  dbt.offst_lbl_id,
                  CASE
                    WHEN p_x_sls_typ_lbl_id_flg = 'Y' THEN
                     dbt.x_sls_typ_lbl_id
                    ELSE
                     dbt.sls_typ_lbl_id
                  END;
      -- write TREND_ALLOC_HIST_DTLS
        BEGIN
          DELETE FROM trend_alloc_hist_dtls
           WHERE mrkt_id = p_mrkt_id
             AND sls_perd_id = p_sls_perd_id
             AND sls_typ_id = p_sls_typ_id
             AND sls_typ_grp_nm = c_sls_typ_grp_nm_bi24
             AND bilng_day = p_bilng_day;
          l_sql_rowcount := SQL%ROWCOUNT;
          app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
          app_plsql_log.info(l_module_name || ' ' || l_sql_rowcount ||
                             ' records DELETED from trend_alloc_hist_dtls - ' ||
                             c_sls_typ_grp_nm_bi24 || l_parameter_list);
          IF l_tbl_bi24_perd.count > 0 THEN
            SELECT MAX(sls_typ_lbl_id)
              INTO l_sls_typ_lbl_id
              FROM TABLE(get_ta_config(p_mrkt_id        => p_mrkt_id,
                                       p_sls_perd_id    => p_sls_perd_id,
                                       p_sls_typ_id     => p_sls_typ_id,
                                       p_sls_typ_grp_nm => c_sls_typ_grp_nm_bi24));
            FOR i IN l_tbl_bi24_perd.first .. l_tbl_bi24_perd.last LOOP
              l_stus := 2;
              BEGIN
                INSERT INTO trend_alloc_hist_dtls
                  (mrkt_id,
                   sls_perd_id,
                   sls_typ_id,
                   sls_typ_grp_nm,
                   bilng_day,
                   sku_id,
                   veh_id,
                   offr_id,
                   promtn_id,
                   promtn_clm_id,
                   sls_cls_cd,
                   perd_part,
                   offst_lbl_id,
                   cash_value,
                   r_factor,
                   sls_typ_lbl_id,
                   units,
                   sales)
                VALUES
                  (p_mrkt_id,
                   p_sls_perd_id,
                   p_sls_typ_id,
                   c_sls_typ_grp_nm_bi24,
                   p_bilng_day,
                   l_tbl_bi24_perd(i).sku_id,
                   l_tbl_bi24_perd(i).veh_id,
                   l_tbl_bi24_perd(i).offr_id,
                   l_tbl_bi24_perd(i).promtn_id,
                   l_tbl_bi24_perd(i).promtn_clm_id,
                   l_tbl_bi24_perd(i).sls_cls_cd,
                   l_tbl_bi24_perd(i).perd_part,
                   l_tbl_bi24_perd(i).offst_lbl_id,
                   l_tbl_bi24_perd(i).cash_value,
                   l_tbl_bi24_perd(i).r_factor,
                   l_sls_typ_lbl_id,
                   l_tbl_bi24_perd(i).units,
                   l_tbl_bi24_perd(i).sales);
                -- count REAL insert
                l_ins_cnt := l_ins_cnt + 1;
                l_stus    := 0;
              EXCEPTION
                WHEN OTHERS
                -- log
                 THEN
                  app_plsql_log.error('ERROR at INSERT INTO trend_alloc_hist_dtls (mrkt_id: ' ||
                                      p_mrkt_id || ', sls_perd_id: ' ||
                                      p_sls_perd_id || ', sls_typ_id: ' ||
                                      p_sls_typ_id || ', sls_typ_grp_nm: ' ||
                                      c_sls_typ_grp_nm_bi24 ||
                                      ', bilng_day: ' || p_bilng_day ||
                                      ', sku_id: ' || l_tbl_bi24_perd(i)
                                      .sku_id || ', veh_id: ' || l_tbl_bi24_perd(i)
                                      .veh_id || ', offr_id: ' || l_tbl_bi24_perd(i)
                                      .offr_id || ', promtn_id: ' || l_tbl_bi24_perd(i)
                                      .promtn_id || ', promtn_clm_id: ' || l_tbl_bi24_perd(i)
                                      .promtn_clm_id || ', sls_cls_cd: ' || l_tbl_bi24_perd(i)
                                      .sls_cls_cd || ', perd_part: ' || l_tbl_bi24_perd(i)
                                      .perd_part || ', offst_lbl_id: ' || l_tbl_bi24_perd(i)
                                      .offst_lbl_id || ', sls_typ_lbl_id: ' || l_tbl_bi24_perd(i)
                                      .sls_typ_lbl_id);
                  RAISE;
              END;
            END LOOP;
          END IF;
          -- write into TREND_ALLOC_HIST_DTLS_LOG
          MERGE INTO trend_alloc_hist_dtls_log l
          USING (SELECT p_mrkt_id             AS mrkt_id,
                        p_sls_perd_id         AS sls_perd_id,
                        p_sls_typ_id          AS sls_typ_id,
                        c_sls_typ_grp_nm_bi24 AS sls_typ_grp_nm,
                        p_bilng_day           AS bilng_day,
                        SYSDATE               AS last_updt_ts
                   FROM dual) m
          ON (l.mrkt_id = m.mrkt_id AND l.sls_perd_id = m.sls_perd_id AND l.sls_typ_id = m.sls_typ_id AND l.sls_typ_grp_nm = m.sls_typ_grp_nm AND l.bilng_day = m.bilng_day)
          WHEN MATCHED THEN
            UPDATE SET l.last_updt_ts = m.last_updt_ts
          WHEN NOT MATCHED THEN
            INSERT
              (mrkt_id,
               sls_perd_id,
               sls_typ_id,
               sls_typ_grp_nm,
               bilng_day,
               last_updt_ts)
            VALUES
              (m.mrkt_id,
               m.sls_perd_id,
               m.sls_typ_id,
               m.sls_typ_grp_nm,
               m.bilng_day,
               m.last_updt_ts);
        EXCEPTION
          WHEN OTHERS THEN
            ROLLBACK;
            l_stus := 2;
            app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
            app_plsql_log.info(l_module_name || ' FAILED, error code: ' ||
                               SQLCODE || ' error message: ' || SQLERRM ||
                               l_parameter_list);
        END;
        IF l_stus = 0 THEN
          COMMIT;
          app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
          app_plsql_log.info(l_module_name || ' COMMIT, status_code: ' ||
                             to_char(l_stus) ||
                             ' trend_alloc_hist_dtls (insert): ' ||
                             l_ins_cnt || l_parameter_list);
        ELSE
          ROLLBACK;
          app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
          app_plsql_log.info(l_module_name || ' ROLLBACK, status_code: ' ||
                             to_char(l_stus) || l_parameter_list);
        END IF;
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
        app_plsql_log.info(l_module_name ||
                           ' warning: BI24 calculation! error code:' ||
                           SQLCODE || ' error message: ' || SQLERRM ||
                           l_parameter_list);
    END;
    --
    IF p_perd_part_flg = 'Y' THEN
      app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
      app_plsql_log.info(l_module_name || ' end with l_tbl_bi24_perd CNT(' ||
                         l_tbl_bi24_perd.count || ')' || l_parameter_list);
      RETURN l_tbl_bi24_perd;
    ELSE
      -- SUM (HEAD)
      IF l_tbl_bi24_perd.count > 0 THEN
        FOR i IN l_tbl_bi24_perd.first .. l_tbl_bi24_perd.last LOOP
          c_key := to_char(l_tbl_bi24_perd(i).sku_id) || '_' ||
                   to_char(l_tbl_bi24_perd(i).veh_id) || '_' ||
                   to_char(l_tbl_bi24_perd(i).offr_id) || '_' ||
                   to_char(l_tbl_bi24_perd(i).promtn_id) || '_' ||
                   to_char(l_tbl_bi24_perd(i).promtn_clm_id) || '_' || l_tbl_bi24_perd(i).sls_cls_cd || '_' ||
                   to_char(l_tbl_bi24_perd(i).offst_lbl_id) || '_' ||
                   to_char(l_tbl_bi24_perd(i).cash_value) || '_' ||
                   to_char(l_tbl_bi24_perd(i).r_factor) || '_' ||
                   to_char(l_tbl_bi24_perd(i).sls_typ_lbl_id);
          --
          l_tbl_bi24_ind(c_key).sku_id := l_tbl_bi24_perd(i).sku_id;
          l_tbl_bi24_ind(c_key).veh_id := l_tbl_bi24_perd(i).veh_id;
          l_tbl_bi24_ind(c_key).offr_id := l_tbl_bi24_perd(i).offr_id;
          l_tbl_bi24_ind(c_key).promtn_id := l_tbl_bi24_perd(i).promtn_id;
          l_tbl_bi24_ind(c_key).promtn_clm_id := l_tbl_bi24_perd(i).promtn_clm_id;
          l_tbl_bi24_ind(c_key).sls_cls_cd := l_tbl_bi24_perd(i).sls_cls_cd;
          l_tbl_bi24_ind(c_key).offst_lbl_id := l_tbl_bi24_perd(i).offst_lbl_id;
          l_tbl_bi24_ind(c_key).cash_value := l_tbl_bi24_perd(i).cash_value;
          l_tbl_bi24_ind(c_key).r_factor := l_tbl_bi24_perd(i).r_factor;
          l_tbl_bi24_ind(c_key).sls_typ_lbl_id := l_tbl_bi24_perd(i).sls_typ_lbl_id;
          l_tbl_bi24_ind(c_key).units := nvl(l_tbl_bi24_ind(c_key).units, 0) + l_tbl_bi24_perd(i).units;
          l_tbl_bi24_ind(c_key).sales := nvl(l_tbl_bi24_ind(c_key).sales, 0) + l_tbl_bi24_perd(i).sales;
        END LOOP;
        l_tbl_bi24_perd.delete;
      END IF;
      -- PIPE (HEAD)
      IF l_tbl_bi24_ind.count > 0 THEN
        c_key := l_tbl_bi24_ind.first;
        WHILE c_key IS NOT NULL LOOP
          l_tbl_bi24.extend;
          l_tbl_bi24(l_tbl_bi24.count).sku_id := l_tbl_bi24_ind(c_key)
                                                 .sku_id;
          l_tbl_bi24(l_tbl_bi24.count).veh_id := l_tbl_bi24_ind(c_key)
                                                 .veh_id;
          l_tbl_bi24(l_tbl_bi24.count).offr_id := l_tbl_bi24_ind(c_key)
                                                  .offr_id;
          l_tbl_bi24(l_tbl_bi24.count).promtn_id := l_tbl_bi24_ind(c_key)
                                                    .promtn_id;
          l_tbl_bi24(l_tbl_bi24.count).promtn_clm_id := l_tbl_bi24_ind(c_key)
                                                        .promtn_clm_id;
          l_tbl_bi24(l_tbl_bi24.count).sls_cls_cd := l_tbl_bi24_ind(c_key)
                                                     .sls_cls_cd;
          l_tbl_bi24(l_tbl_bi24.count).offst_lbl_id := l_tbl_bi24_ind(c_key)
                                                       .offst_lbl_id;
          l_tbl_bi24(l_tbl_bi24.count).cash_value := l_tbl_bi24_ind(c_key)
                                                     .cash_value;
          l_tbl_bi24(l_tbl_bi24.count).r_factor := l_tbl_bi24_ind(c_key)
                                                   .r_factor;
          l_tbl_bi24(l_tbl_bi24.count).sls_typ_lbl_id := l_tbl_bi24_ind(c_key)
                                                         .sls_typ_lbl_id;
          l_tbl_bi24(l_tbl_bi24.count).units := l_tbl_bi24_ind(c_key).units;
          l_tbl_bi24(l_tbl_bi24.count).sales := l_tbl_bi24_ind(c_key).sales;
          c_key := l_tbl_bi24_ind.next(c_key);
        END LOOP;
        l_tbl_bi24_ind.delete;
      END IF;
      app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
      app_plsql_log.info(l_module_name || ' end with l_tbl_bi24 CNT(' ||
                         l_tbl_bi24.count || ')' || l_parameter_list);
      RETURN l_tbl_bi24;
    END IF;
  END get_bi24;

  -- get_rules
  FUNCTION get_rules(p_mrkt_id        IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                     p_campgn_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                     p_sls_typ_id     IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                     p_bilng_day      IN dly_bilng_trnd.prcsng_dt%TYPE,
                     p_user_id        IN VARCHAR2 DEFAULT NULL,
                     p_run_id         IN NUMBER DEFAULT NULL)
    RETURN obj_pa_trend_alloc_rules_table
    PIPELINED IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    -- local variables
    l_trgt_sls_perd_id  mrkt_trnd_sls_perd_sls_typ.trnd_sls_perd_id%TYPE;
    -- for LOG
    l_run_id         NUMBER := nvl(p_run_id,
                                   app_plsql_output.generate_new_run_id);
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'GET_RULES';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       to_char(p_mrkt_id) || ', ' ||
                                       'p_campgn_perd_id: ' ||
                                       to_char(p_campgn_perd_id) || ', ' ||
                                       'p_sls_typ_id: ' ||
                                       to_char(p_sls_typ_id) || ', ' ||
                                       'p_bilng_day: ' ||
                                       to_char(p_bilng_day, 'yyyy-mm-dd') || ', ' ||
                                       'p_user_id: ' || l_user_id || ', ' ||
                                       'p_run_id: ' || to_char(l_run_id) || ')';
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start' || l_parameter_list);
    BEGIN
      -- get period
      BEGIN
        SELECT MAX(trgt_sls_perd_id)
          INTO l_trgt_sls_perd_id
          FROM TABLE(get_ta_config(p_mrkt_id        => p_mrkt_id,
                                   p_sls_perd_id    => p_campgn_perd_id,
                                   p_sls_typ_id     => p_sls_typ_id,
                                   p_sls_typ_grp_nm => c_sls_typ_grp_nm_bi24));
      EXCEPTION
        WHEN OTHERS THEN l_trgt_sls_perd_id := NULL;
      END;
      -- ETC rules
      -- custm_seg_mstr
      MERGE INTO custm_seg_mstr sm
      USING (SELECT p_mrkt_id AS mrkt_id,
                    lbl_desc AS rul_nm,
                    lbl_desc AS rul_desc,
                    td.lbl_id AS offst_lbl_id
               FROM (SELECT DISTINCT offst_lbl_id
                       FROM TABLE(get_ta_config(p_mrkt_id        => p_mrkt_id,
                                                p_sls_perd_id    => p_campgn_perd_id,
                                                p_sls_typ_id     => p_sls_typ_id,
                                                p_sls_typ_grp_nm => NULL))) tc,
                    ta_dict td
              WHERE td.lbl_id = tc.offst_lbl_id
                AND upper(td.lbl_wght_dir) = 'O') x
      ON (sm.mrkt_id = x.mrkt_id AND sm.rul_nm = x.rul_nm)
      WHEN NOT MATCHED THEN
        INSERT
          (mrkt_id, rul_nm, rul_desc, offst_lbl_id)
        VALUES
          (x.mrkt_id, x.rul_nm, x.rul_desc, x.offst_lbl_id);
      -- custm_seg_perd
      MERGE INTO custm_seg_perd sp
      USING (SELECT p_campgn_perd_id AS campgn_perd_id,
                    m.rul_id,
                    p_sls_typ_id AS sls_typ_id,
                    xm.r_factor AS r_factor,
                    CASE
                      WHEN xm.int_lbl_desc = 'ON-SCHEDULE' THEN mtspst.sct_cash_value_on
                      WHEN xm.int_lbl_desc = 'OFF-SCHEDULE' THEN mtspst.sct_cash_value_off
                      ELSE NULL
                    END AS cash_value,
                    row_number() over(ORDER BY m.offst_lbl_id) AS prirty
               FROM custm_seg_mstr m,
                    (SELECT p_mrkt_id AS mrkt_id,
                            lbl_desc AS rul_nm,
                            lbl_desc AS rul_desc,
                            td.lbl_id AS offst_lbl_id,
                            td.int_lbl_desc,
                            tc.r_factor AS r_factor
                       FROM (SELECT offst_lbl_id,
                                    LISTAGG(r_factor) WITHIN GROUP (ORDER BY r_factor) AS r_factor
                               FROM TABLE(get_ta_config(p_mrkt_id        => p_mrkt_id,
                                                        p_sls_perd_id    => p_campgn_perd_id,
                                                        p_sls_typ_id     => p_sls_typ_id,
                                                        p_sls_typ_grp_nm => NULL))
                              GROUP BY offst_lbl_id) tc,
                            ta_dict td
                      WHERE td.lbl_id = tc.offst_lbl_id
                        AND upper(td.lbl_wght_dir) = 'O') xm,
                    (SELECT mrkt_id, sct_cash_value_on, sct_cash_value_off
                       FROM mrkt_trnd_sls_perd_sls_typ
                      WHERE p_mrkt_id = mrkt_id 
                        AND l_trgt_sls_perd_id = trnd_sls_perd_id
                        AND p_sls_typ_id = sls_typ_id) mtspst
              WHERE m.mrkt_id = p_mrkt_id
                AND m.rul_nm = xm.rul_nm
                AND m.mrkt_id = mtspst.mrkt_id(+)) x
      ON (sp.campgn_perd_id = x.campgn_perd_id AND sp.rul_id = x.rul_id AND sp.sls_typ_id = x.sls_typ_id)
      WHEN NOT MATCHED THEN
        INSERT
          (campgn_perd_id, rul_id, sls_typ_id, prirty, r_factor_manual, cash_value, use_estimate)
        VALUES
          (x.campgn_perd_id, x.rul_id, x.sls_typ_id, x.prirty, x.r_factor, x.cash_value, 'N');
    EXCEPTION
      WHEN OTHERS THEN
        ROLLBACK;
        app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
        app_plsql_log.info(l_module_name || ' FAILED, error code: ' ||
                           SQLCODE || ' error message: ' || SQLERRM ||
                           l_parameter_list);
    END;
    --
    COMMIT;
    --
    FOR i IN (SELECT rm.rul_id,
                     rm.rul_nm,
                     CAST(NULL AS NUMBER) offst_lbl_id,
                     CAST(NULL AS NUMBER) catgry_id,
                     CAST(NULL AS NUMBER) sls_cls_cd,
                     CAST(NULL AS NUMBER) veh_id,
                     CAST(NULL AS NUMBER) perd_part,
                     listagg(rs.sku_id, ',') within GROUP(ORDER BY rs.sku_id) sku_list,
                     0 prirty,
                     r.period_list,
                     r.r_factor,
                     r.r_factor_manual,
                     r.cash_value,
                     upper(r.use_estimate) use_estimate
                FROM custm_rul_mstr     rm,
                     custm_rul_perd     r,
                     custm_rul_sku_list rs
               WHERE rm.mrkt_id = p_mrkt_id
                 AND r.campgn_perd_id = p_campgn_perd_id
                 AND rm.rul_id = rs.rul_id
                 AND rm.rul_id = r.rul_id
               GROUP BY rm.rul_id,
                        rm.rul_nm,
                        r.period_list,
                        r.r_factor,
                        r.r_factor_manual,
                        r.cash_value,
                        upper(r.use_estimate)
              UNION
              SELECT ms.rul_id,
                     ms.rul_nm,
                     ms.offst_lbl_id,
                     ms.catgry_id,
                     ms.sls_cls_cd,
                     ms.veh_id,
                     ms.perd_part,
                     CAST(NULL AS VARCHAR(2048)) sku_list,
                     s.prirty,
                     s.period_list,
                     s.r_factor,
                     s.r_factor_manual,
                     s.cash_value,
                     upper(s.use_estimate) use_estimate
                FROM custm_seg_mstr ms, custm_seg_perd s
               WHERE ms.mrkt_id = p_mrkt_id
                 AND ms.rul_id = s.rul_id(+)
                 AND p_campgn_perd_id = s.campgn_perd_id(+)
                 AND p_sls_typ_id = s.sls_typ_id(+)) LOOP
      PIPE ROW(obj_pa_trend_alloc_rules_line(i.rul_id,
                                             i.rul_nm,
                                             i.offst_lbl_id,
                                             i.catgry_id,
                                             i.sls_cls_cd,
                                             i.veh_id,
                                             i.perd_part,
                                             i.sku_list,
                                             i.prirty,
                                             i.period_list,
                                             i.r_factor,
                                             i.r_factor_manual,
                                             i.cash_value,
                                             i.use_estimate));
    END LOOP;
    --
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' end' || l_parameter_list);
  END get_rules;

  -- get_trend_alloc_head_view
  FUNCTION get_trend_alloc_head_view(p_mrkt_id        IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                                     p_campgn_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                     p_sls_typ_id     IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                                     p_bilng_day      IN dly_bilng_trnd.prcsng_dt%TYPE,
                                     p_user_id        IN VARCHAR2 DEFAULT NULL)
    RETURN obj_pa_trend_alloc_view_table
    PIPELINED AS
    -- local variables
    l_periods              r_periods;
    l_trgt_sls_perd_id     mrkt_trnd_sls_perd_sls_typ.trnd_sls_perd_id%TYPE;
    l_not_planned          NUMBER := NULL;
    l_last_run             DATE;
    l_is_started           CHAR(1);
    l_is_complete          CHAR(1);
    l_is_saved             CHAR(1);
    -- for LOG
    l_run_id         NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'GET_TREND_ALLOC_HEAD_VIEW';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       to_char(p_mrkt_id) || ', ' ||
                                       'p_campgn_perd_id: ' ||
                                       to_char(p_campgn_perd_id) || ', ' ||
                                       'p_sls_typ_id: ' ||
                                       to_char(p_sls_typ_id) || ', ' ||
                                       'p_bilng_day: ' ||
                                       to_char(p_bilng_day, 'yyyy-mm-dd') || ', ' ||
                                       'p_user_id: ' || l_user_id || ', ' || ')';
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start' || l_parameter_list);
    -- get trg_perd_id from TA_CONFIG
    BEGIN
      SELECT MAX(trgt_sls_perd_id)
        INTO l_trgt_sls_perd_id
        FROM TABLE(get_ta_config(p_mrkt_id        => p_mrkt_id,
                                 p_sls_perd_id    => p_campgn_perd_id,
                                 p_sls_typ_id     => p_sls_typ_id,
                                 p_sls_typ_grp_nm => c_sls_typ_grp_nm_bi24));
    EXCEPTION
      WHEN OTHERS THEN
        app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
        app_plsql_log.info(l_module_name ||
                           ' warning: RECORD not found in table: ta_config where mrkt_id=' ||
                           to_char(p_mrkt_id) || ' and sls_perd_id=' ||
                           to_char(l_trgt_sls_perd_id) || ' error code: ' ||
                           SQLCODE || ' error message: ' || SQLERRM ||
                           l_parameter_list);
        l_trgt_sls_perd_id := NULL;
    END;
    BEGIN
      SELECT sct_aloctn_strt_ts,
             CASE
               WHEN p_sls_typ_id IN (marketing_est_id, supply_est_id) THEN
                sct_autclc_ind
               WHEN p_sls_typ_id IN (marketing_bst_id, supply_bst_id) THEN
                sct_autclc_ind
               ELSE
                NULL
             END
        INTO l_last_run,
             l_is_started
        FROM mrkt_trnd_sls_perd_sls_typ
       WHERE mrkt_id = p_mrkt_id
         AND trnd_sls_perd_id = l_trgt_sls_perd_id
         AND sls_typ_id = p_sls_typ_id;
    EXCEPTION
      WHEN OTHERS THEN
        app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
        app_plsql_log.info(l_module_name ||
                           ' warning: RECORD not found in table: MRKT_TRND_SLS_PERD_SLS_TYP where mrkt_id=' ||
                           to_char(p_mrkt_id) || ' and trnd_sls_perd_id=' ||
                           to_char(l_trgt_sls_perd_id) ||
                           ' and sls_typ_id=' || to_char(p_sls_typ_id) ||
                           l_parameter_list);
        l_last_run             := NULL;
        l_is_started           := NULL;
    END;
    -- IS COMPLETE
    BEGIN
      SELECT CASE
               WHEN COUNT(1) <> 0 THEN
                'Y'
               ELSE
                'N'
             END
        INTO l_is_complete
        FROM mrkt_trnd_sls_perd_sls_typ
       WHERE mrkt_id = p_mrkt_id
         AND trnd_sls_perd_id = l_trgt_sls_perd_id
         AND sls_typ_id IN (demand_actuals, billed_actuals);
    EXCEPTION
      WHEN OTHERS THEN
        app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
        app_plsql_log.info(l_module_name ||
                           ' warning: RECORD not found in table: MRKT_TRND_SLS_PERD_SLS_TYP where mrkt_id=' ||
                           to_char(p_mrkt_id) || ' and trnd_sls_perd_id=' ||
                           to_char(l_trgt_sls_perd_id) ||
                           ' and sls_typ_id=' || to_char(p_sls_typ_id) ||
                           ' error code: ' || SQLCODE ||
                           ' error message: ' || SQLERRM ||
                           l_parameter_list);
        l_is_complete := NULL;
    END;
    -- IS SAVED
    BEGIN
      SELECT CASE
               WHEN MAX(last_saved) < l_last_run THEN
                'Y'
               ELSE
                'N'
             END
        INTO l_is_saved
        FROM (SELECT greatest(MAX(rm.last_updt_ts),
                              MAX(r.last_updt_ts),
                              MAX(rs.last_updt_ts)) AS last_saved
                FROM custm_rul_mstr     rm,
                     custm_rul_perd     r,
                     custm_rul_sku_list rs
               WHERE rm.mrkt_id = p_mrkt_id
                 AND r.campgn_perd_id = p_campgn_perd_id
                 AND rm.rul_id = rs.rul_id
                 AND rm.rul_id = r.rul_id
               GROUP BY rm.rul_nm,
                        r.period_list,
                        r.r_factor,
                        r.r_factor_manual
              UNION
              SELECT greatest(MAX(ms.last_updt_ts), MAX(s.last_updt_ts)) AS last_saved
                FROM custm_seg_mstr ms, custm_seg_perd s
               WHERE ms.mrkt_id = p_mrkt_id
                 AND ms.rul_id = s.rul_id
                 AND p_campgn_perd_id = s.campgn_perd_id
                 AND p_sls_typ_id = s.sls_typ_id);
    EXCEPTION
      WHEN OTHERS THEN
        app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
        app_plsql_log.info(l_module_name ||
                           ' warning: Other exception when selecting from: RULE tables where mrkt_id=' ||
                           to_char(p_mrkt_id) || ' and campgn_perd_id=' ||
                           to_char(p_campgn_perd_id) || ' and sls_typ_id=' ||
                           to_char(p_sls_typ_id) || ' error code: ' ||
                           SQLCODE || ' error message: ' || SQLERRM ||
                           l_parameter_list);
        l_is_saved := NULL;
    END;
    -- NOT PLANNED
    -- get CURRENT periods
    l_periods := get_periods(p_mrkt_id       => p_mrkt_id,
                             p_orig_perd_id  => p_campgn_perd_id,
                             p_bilng_perd_id => p_campgn_perd_id,
                             p_sls_typ_id    => p_sls_typ_id,
                             p_bilng_day     => p_bilng_day,
                             p_user_id       => l_user_id,
                             p_run_id        => l_run_id);
    BEGIN
      SELECT SUM(dly_bilng_trnd.unit_qty)
        INTO l_not_planned
        FROM dly_bilng_trnd,
             dly_bilng_offr_sku_line,
             (SELECT src_sls_typ_id,
                     x_src_sls_typ_id,
                     offst_lbl_id,
                     sls_typ_lbl_id,
                     x_sls_typ_lbl_id,
                     pa_maps_public.perd_plus(p_mrkt_id,
                                              p_campgn_perd_id,
                                              offst_val_src_sls) src_sls_perd_id,
                     pa_maps_public.perd_plus(p_mrkt_id,
                                              p_campgn_perd_id,
                                              offst_val_trgt_sls) trgt_sls_perd_id,
                     pa_maps_public.perd_plus(p_mrkt_id,
                                              p_campgn_perd_id,
                                              offst_val_src_offr) src_offr_perd_id,
                     pa_maps_public.perd_plus(p_mrkt_id,
                                              p_campgn_perd_id,
                                              offst_val_trgt_offr) trgt_offr_perd_id,
                     r_factor
                FROM (SELECT src_sls_typ_id,
                             est_src_sls_typ_id AS x_src_sls_typ_id,
                             offst_lbl_id,
                             sls_typ_lbl_id,
                             x_sls_typ_lbl_id,
                             offst_val_src_sls,
                             offst_val_trgt_sls,
                             offst_val_src_offr,
                             offst_val_trgt_offr,
                             r_factor,
                             eff_sls_perd_id,
                             lead(eff_sls_perd_id, 1) over(PARTITION BY offst_lbl_id, sls_typ_grp_nm ORDER BY eff_sls_perd_id) AS next_eff_sls_perd_id
                        FROM ta_config
                       WHERE mrkt_id = p_mrkt_id
                         AND trgt_sls_typ_id = p_sls_typ_id
                         AND upper(REPLACE(TRIM(sls_typ_grp_nm), '  ', ' ')) =
                             c_sls_typ_grp_nm_bi24
                         AND eff_sls_perd_id <= p_campgn_perd_id)
               WHERE p_campgn_perd_id BETWEEN eff_sls_perd_id AND
                     nvl(next_eff_sls_perd_id, p_campgn_perd_id)) tc_bi24
       WHERE dly_bilng_trnd.mrkt_id = p_mrkt_id
         AND dly_bilng_trnd.trnd_sls_perd_id = tc_bi24.src_sls_perd_id
         AND dly_bilng_trnd.offr_perd_id = tc_bi24.src_offr_perd_id
         AND trunc(dly_bilng_trnd.prcsng_dt) <= l_periods.bilng_day
         AND dly_bilng_trnd.trnd_aloctn_auto_stus_id IN
             (auto_matched, auto_suggested_single, auto_suggested_multi)
         AND dly_bilng_trnd.dly_bilng_id =
             dly_bilng_offr_sku_line.dly_bilng_id(+)
         AND dly_bilng_offr_sku_line.dly_bilng_id IS NULL;
    EXCEPTION
      WHEN OTHERS THEN
        app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
        app_plsql_log.info(l_module_name ||
                           ' warning: RECORD not found in table: dly_bilng_trnd where mrkt_id=' ||
                           to_char(p_mrkt_id) || ' and trnd_sls_perd_id=' ||
                           to_char(p_campgn_perd_id) || ' error code: ' ||
                           SQLCODE || ' error message: ' || SQLERRM ||
                           l_parameter_list);
        l_not_planned := NULL;
    END;
    PIPE ROW(obj_pa_trend_alloc_view_line(l_not_planned,
                                          NULL,
                                          l_last_run,
                                          l_is_started,
                                          l_is_complete,
                                          l_is_saved,
                                          NULL,
                                          NULL));
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' end' || l_parameter_list);
  END get_trend_alloc_head_view;

  -- get_head_details
  FUNCTION get_head_details(p_mrkt_id       IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                            p_sls_perd_id   IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                            p_sls_typ_id    IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                            p_bilng_day     IN dly_bilng_trnd.prcsng_dt%TYPE,
                            p_offst_lbl_id  IN ta_dict.lbl_id%TYPE DEFAULT NULL,
                            p_cash_value    IN NUMBER DEFAULT NULL,
                            p_r_factor      IN NUMBER DEFAULT NULL,
                            p_perd_part_flg IN CHAR DEFAULT 'N',
                            p_user_id       IN VARCHAR2 DEFAULT NULL,
                            p_run_id        IN NUMBER DEFAULT NULL)
    RETURN t_hist_detail IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    -- local variables
    l_tbl_bi24               t_hist_detail := t_hist_detail();
    l_tbl_dms_get_cache_flag BOOLEAN;
    l_tbl_dms                t_hist_detail := t_hist_detail();
    l_tbl_hist_detail        t_hist_detail := t_hist_detail();
    l_ins_cnt                PLS_INTEGER := 0;
    l_stus                   PLS_INTEGER := 0;
    l_sql_rowcount           NUMBER;
    -- for LOG
    l_run_id         NUMBER := nvl(p_run_id,
                                   app_plsql_output.generate_new_run_id);
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'GET_HEAD_DETAILS';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       to_char(p_mrkt_id) || ', ' ||
                                       'p_sls_perd_id: ' ||
                                       to_char(p_sls_perd_id) || ', ' ||
                                       'p_sls_typ_id: ' ||
                                       to_char(p_sls_typ_id) || ', ' ||
                                       'p_bilng_day: ' ||
                                       to_char(p_bilng_day, 'yyyy-mm-dd') || ', ' ||
                                       'p_offst_lbl_id: ' ||
                                       to_char(p_offst_lbl_id) || ', ' ||
                                       'p_cash_value: ' ||
                                       to_char(p_cash_value) || ', ' ||
                                       'p_r_factor: ' ||
                                       to_char(p_r_factor) || ', ' ||
                                       'p_user_id: ' || l_user_id || ', ' ||
                                       'p_run_id: ' || to_char(l_run_id) || ')';
    -- exception
    e_get_head_details EXCEPTION;
    --
  BEGIN
    --
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start' || l_parameter_list);
    -- AGGR: head_details
    BEGIN
      -- BI24
      l_tbl_bi24 := get_bi24(p_mrkt_id              => p_mrkt_id,
                             p_sls_perd_id          => p_sls_perd_id,
                             p_sls_typ_id           => p_sls_typ_id,
                             p_bilng_day            => p_bilng_day,
                             p_offst_lbl_id         => NULL,
                             p_cash_value           => p_cash_value,
                             p_r_factor             => p_r_factor,
                             p_x_sls_typ_lbl_id_flg => 'N',
                             p_perd_part_flg        => p_perd_part_flg,
                             p_user_id              => l_user_id,
                             p_run_id               => l_run_id);
      -- DMS
      l_tbl_dms_get_cache_flag := get_cache_flag(p_mrkt_id        => p_mrkt_id,
                                                 p_sls_perd_id    => p_sls_perd_id,
                                                 p_sls_typ_id     => p_sls_typ_id,
                                                 p_sls_typ_grp_nm => c_sls_typ_grp_nm_dms,
                                                 p_bilng_day      => p_bilng_day,
                                                 p_user_id        => l_user_id,
                                                 p_run_id         => l_run_id);
      IF l_tbl_dms_get_cache_flag THEN
        -- re-READ
        SELECT sku_id,
               veh_id,
               offr_id,
               promtn_id,
               promtn_clm_id,
               sls_cls_cd,
               perd_part,
               offst_lbl_id,
               cash_value,
               r_factor,
               sls_typ_lbl_id,
               units,
               sales
          BULK COLLECT
          INTO l_tbl_dms
          FROM trend_alloc_hist_dtls
         WHERE mrkt_id = p_mrkt_id
           AND sls_perd_id = p_sls_perd_id
           AND sls_typ_id = p_sls_typ_id
           AND sls_typ_grp_nm = c_sls_typ_grp_nm_dms
           AND bilng_day = p_bilng_day;
        app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
        app_plsql_log.info(l_module_name || ' cache: DMS' ||
                           l_parameter_list);
      ELSE
        -- re-CALCULATE
        SELECT offr_sku_line.sku_id,
               offr.veh_id,
               offr.offr_id,
               offr_prfl_prc_point.promtn_id,
               nvl(offr_prfl_prc_point.promtn_clm_id, -999),
               offr_prfl_prc_point.sls_cls_cd,
               NULL AS perd_part,
               tc_dms.offst_lbl_id,
               p_cash_value,
               p_r_factor,
               tc_dms.sls_typ_lbl_id,
               round(SUM(nvl(dstrbtd_mrkt_sls.unit_qty, 0))) units,
               round(SUM(nvl(dstrbtd_mrkt_sls.unit_qty, 0) *
                         nvl(offr_prfl_prc_point.sls_prc_amt, 0) /
                         decode(nvl(offr_prfl_prc_point.nr_for_qty, 0),
                                0,
                                1,
                                offr_prfl_prc_point.nr_for_qty) *
                         decode(nvl(offr_prfl_prc_point.net_to_avon_fct, 0),
                                0,
                                1,
                                offr_prfl_prc_point.net_to_avon_fct))) sales
          BULK COLLECT
          INTO l_tbl_dms
          FROM dstrbtd_mrkt_sls,
               offr_sku_line,
               offr_prfl_prc_point,
               offr,
               (SELECT src_sls_typ_id,
                       x_src_sls_typ_id,
                       offst_lbl_id,
                       sls_typ_lbl_id,
                       x_sls_typ_lbl_id,
                       pa_maps_public.perd_plus(p_mrkt_id,
                                                p_sls_perd_id,
                                                offst_val_src_sls) src_sls_perd_id,
                       pa_maps_public.perd_plus(p_mrkt_id,
                                                p_sls_perd_id,
                                                offst_val_trgt_sls) trgt_sls_perd_id,
                       pa_maps_public.perd_plus(p_mrkt_id,
                                                p_sls_perd_id,
                                                offst_val_src_offr) src_offr_perd_id,
                       pa_maps_public.perd_plus(p_mrkt_id,
                                                p_sls_perd_id,
                                                offst_val_trgt_offr) trgt_offr_perd_id,
                       r_factor
                  FROM (SELECT src_sls_typ_id,
                               est_src_sls_typ_id AS x_src_sls_typ_id,
                               offst_lbl_id,
                               sls_typ_lbl_id,
                               x_sls_typ_lbl_id,
                               offst_val_src_sls,
                               offst_val_trgt_sls,
                               offst_val_src_offr,
                               offst_val_trgt_offr,
                               r_factor,
                               eff_sls_perd_id,
                               lead(eff_sls_perd_id, 1) over(PARTITION BY offst_lbl_id, sls_typ_grp_nm ORDER BY eff_sls_perd_id) AS next_eff_sls_perd_id
                          FROM ta_config
                         WHERE mrkt_id = p_mrkt_id
                           AND trgt_sls_typ_id = p_sls_typ_id
                           AND upper(REPLACE(TRIM(sls_typ_grp_nm), '  ', ' ')) IN
                               (SELECT TRIM(regexp_substr(col,
                                                          '[^,]+',
                                                          1,
                                                          LEVEL)) RESULT
                                  FROM (SELECT c_sls_typ_grp_nm_dms col
                                          FROM dual)
                                CONNECT BY LEVEL <=
                                           length(regexp_replace(col, '[^,]+')) + 1)
                           AND eff_sls_perd_id <= p_sls_perd_id)
                 WHERE p_sls_perd_id BETWEEN eff_sls_perd_id AND
                       nvl(next_eff_sls_perd_id, p_sls_perd_id)) tc_dms
         WHERE dstrbtd_mrkt_sls.mrkt_id = p_mrkt_id
           AND dstrbtd_mrkt_sls.sls_perd_id = tc_dms.trgt_sls_perd_id
           AND dstrbtd_mrkt_sls.offr_perd_id = tc_dms.trgt_offr_perd_id
           AND dstrbtd_mrkt_sls.sls_typ_id = tc_dms.src_sls_typ_id
           AND dstrbtd_mrkt_sls.offr_sku_line_id =
               offr_sku_line.offr_sku_line_id
           AND offr_sku_line.dltd_ind <> 'Y'
           AND offr_sku_line.offr_prfl_prcpt_id =
               offr_prfl_prc_point.offr_prfl_prcpt_id
           AND offr_prfl_prc_point.offr_id = offr.offr_id
           AND offr.offr_typ = 'CMP'
           AND offr.ver_id = 0
         GROUP BY offr_sku_line.sku_id,
                  offr.veh_id,
                  offr.offr_id,
                  offr_prfl_prc_point.promtn_id,
                  nvl(offr_prfl_prc_point.promtn_clm_id, -999),
                  offr_prfl_prc_point.sls_cls_cd,
                  tc_dms.offst_lbl_id,
                  p_cash_value,
                  p_r_factor,
                  tc_dms.sls_typ_lbl_id;
        -- write TREND_ALLOC_HIST_DTLS
        IF p_mrkt_id IS NOT NULL
           AND p_sls_perd_id IS NOT NULL
           AND p_sls_typ_id IS NOT NULL
           AND p_bilng_day IS NOT NULL THEN
          -- DMS
          BEGIN
            DELETE FROM trend_alloc_hist_dtls
             WHERE mrkt_id = p_mrkt_id
               AND sls_perd_id = p_sls_perd_id
               AND sls_typ_id = p_sls_typ_id
               AND sls_typ_grp_nm = c_sls_typ_grp_nm_dms
               AND bilng_day = p_bilng_day;
            l_sql_rowcount := SQL%ROWCOUNT;
            app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
            app_plsql_log.info(l_module_name || ' ' || l_sql_rowcount ||
                               ' records DELETED from trend_alloc_hist_dtls - ' ||
                               c_sls_typ_grp_nm_dms || l_parameter_list);
            IF l_tbl_dms.count > 0 THEN
              FOR i IN l_tbl_dms.first .. l_tbl_dms.last LOOP
                l_stus := 2;
                BEGIN
                  INSERT INTO trend_alloc_hist_dtls
                    (mrkt_id,
                     sls_perd_id,
                     sls_typ_id,
                     sls_typ_grp_nm,
                     bilng_day,
                     sku_id,
                     veh_id,
                     offr_id,
                     promtn_id,
                     promtn_clm_id,
                     sls_cls_cd,
                     offst_lbl_id,
                     cash_value,
                     r_factor,
                     sls_typ_lbl_id,
                     units,
                     sales)
                  VALUES
                    (p_mrkt_id,
                     p_sls_perd_id,
                     p_sls_typ_id,
                     c_sls_typ_grp_nm_dms,
                     p_bilng_day,
                     l_tbl_dms(i).sku_id,
                     l_tbl_dms(i).veh_id,
                     l_tbl_dms(i).offr_id,
                     l_tbl_dms(i).promtn_id,
                     l_tbl_dms(i).promtn_clm_id,
                     l_tbl_dms(i).sls_cls_cd,
                     l_tbl_dms(i).offst_lbl_id,
                     l_tbl_dms(i).cash_value,
                     l_tbl_dms(i).r_factor,
                     l_tbl_dms(i).sls_typ_lbl_id,
                     l_tbl_dms(i).units,
                     l_tbl_dms(i).sales);
                  -- count REAL insert
                  l_ins_cnt := l_ins_cnt + 1;
                  l_stus    := 0;
                EXCEPTION
                  WHEN OTHERS
                  -- log
                   THEN
                    app_plsql_log.error('ERROR at INSERT INTO trend_alloc_hist_dtls (mrkt_id: ' ||
                                        p_mrkt_id || ', sls_perd_id: ' ||
                                        p_sls_perd_id || ', sls_typ_id: ' ||
                                        p_sls_typ_id ||
                                        ', sls_typ_grp_nm: ' ||
                                        c_sls_typ_grp_nm_dms ||
                                        ', bilng_day: ' || p_bilng_day ||
                                        ', sku_id: ' || l_tbl_dms(i)
                                        .sku_id || ', veh_id: ' || l_tbl_dms(i)
                                        .veh_id || ', offr_id: ' || l_tbl_dms(i)
                                        .offr_id || ', promtn_id: ' || l_tbl_dms(i)
                                        .promtn_id || ', promtn_clm_id: ' || l_tbl_dms(i)
                                        .promtn_clm_id || ', sls_cls_cd: ' || l_tbl_dms(i)
                                        .sls_cls_cd || ', offst_lbl_id: ' || l_tbl_dms(i)
                                        .offst_lbl_id ||
                                        ', sls_typ_lbl_id: ' || l_tbl_dms(i)
                                        .sls_typ_lbl_id);
                    RAISE;
                END;
              END LOOP;
            END IF;
            -- write into TREND_ALLOC_HIST_DTLS_LOG
            MERGE INTO trend_alloc_hist_dtls_log l
            USING (SELECT p_mrkt_id            AS mrkt_id,
                          p_sls_perd_id        AS sls_perd_id,
                          p_sls_typ_id         AS sls_typ_id,
                          c_sls_typ_grp_nm_dms AS sls_typ_grp_nm,
                          p_bilng_day          AS bilng_day,
                          SYSDATE              AS last_updt_ts
                     FROM dual) m
            ON (l.mrkt_id = m.mrkt_id AND l.sls_perd_id = m.sls_perd_id AND l.sls_typ_id = m.sls_typ_id AND l.sls_typ_grp_nm = m.sls_typ_grp_nm AND l.bilng_day = m.bilng_day)
            WHEN MATCHED THEN
              UPDATE SET l.last_updt_ts = m.last_updt_ts
            WHEN NOT MATCHED THEN
              INSERT
                (mrkt_id,
                 sls_perd_id,
                 sls_typ_id,
                 sls_typ_grp_nm,
                 bilng_day,
                 last_updt_ts)
              VALUES
                (m.mrkt_id,
                 m.sls_perd_id,
                 m.sls_typ_id,
                 m.sls_typ_grp_nm,
                 m.bilng_day,
                 m.last_updt_ts);
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK;
              l_stus := 2;
              app_plsql_log.set_context(l_user_id,
                                        g_package_name,
                                        l_run_id);
              app_plsql_log.info(l_module_name || ' FAILED, error code: ' ||
                                 SQLCODE || ' error message: ' || SQLERRM ||
                                 l_parameter_list);
          END;
          IF l_stus = 0 THEN
            COMMIT;
            app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
            app_plsql_log.info(l_module_name || ' COMMIT, status_code: ' ||
                               to_char(l_stus) ||
                               ' trend_alloc_hist_dtls (insert): ' ||
                               l_ins_cnt || l_parameter_list);
          ELSE
            ROLLBACK;
            app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
            app_plsql_log.info(l_module_name || ' ROLLBACK, status_code: ' ||
                               to_char(l_stus) || l_parameter_list);
          END IF;
        END IF;
      END IF;
      -- join COLLECTIONS
      l_tbl_hist_detail := l_tbl_bi24 MULTISET UNION l_tbl_dms;
    EXCEPTION
      WHEN OTHERS THEN
        ROLLBACK;
        app_plsql_log.error(l_module_name || ' error code:' || SQLCODE ||
                            ' error message: ' || SQLERRM ||
                            l_parameter_list);
        RAISE e_get_head_details;
    END;
    --
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' end' || l_parameter_list);
    --
    RETURN l_tbl_hist_detail;
  END get_head_details;

  -- get_trend_alloc_hist_head
  FUNCTION get_trend_alloc_hist_head(p_mrkt_id        IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                                     p_campgn_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                     p_sls_typ_id     IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                                     p_bilng_day      IN dly_bilng_trnd.prcsng_dt%TYPE,
                                     p_perd_from      IN NUMBER,
                                     p_perd_to        IN NUMBER,
                                     p_user_id        IN VARCHAR2 DEFAULT NULL)
    RETURN obj_pa_trend_alloc_hist_hd_tbl
    PIPELINED IS
    -- local variables
    l_tbl_hist_detail t_hist_detail := t_hist_detail();
    l_tbl_hist_head   t_hist_head;
    l_periods         r_periods;
    --
    l_multplyr  NUMBER;
    l_perd_list mrkt_config_item.mrkt_config_item_val_txt%TYPE;
    c_key       VARCHAR2(128);
    -- for LOG
    l_run_id         NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'GET_TREND_ALLOC_HIST_HEAD';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       to_char(p_mrkt_id) || ', ' ||
                                       'p_campgn_perd_id: ' ||
                                       to_char(p_campgn_perd_id) || ', ' ||
                                       'p_sls_typ_id: ' ||
                                       to_char(p_sls_typ_id) || ', ' ||
                                       'p_bilng_day: ' ||
                                       to_char(p_bilng_day, 'yyyy-mm-dd') || ', ' ||
                                       'p_perd_from: ' ||
                                       to_char(p_perd_from) || ', ' ||
                                       'p_perd_to: ' || to_char(p_perd_to) || ', ' ||
                                       'p_user_id: ' || l_user_id || ', ' ||
                                       'p_run_id: ' || to_char(l_run_id) || ')';
    --
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start' || l_parameter_list);
    --
    l_multplyr := to_number(substr(to_char(p_campgn_perd_id), 1, 4)) - p_perd_to;
    BEGIN
      SELECT mrkt_config_item_val_txt
        INTO l_perd_list
        FROM mrkt_config_item
       WHERE mrkt_id = p_mrkt_id
         AND mrkt_config_item_desc_txt = 'PA_TREND_ALLOCATION period list';
    EXCEPTION
      WHEN OTHERS THEN
        l_perd_list := '0';
    END;
    FOR p IN p_perd_to .. p_perd_from LOOP
      -- iterations for FROM periods to TO periods
      FOR i_prd IN (SELECT pa_maps_public.perd_plus(p_mrkt_id,
                                                    p_campgn_perd_id - (10000 * l_multplyr),
                                                    period) perd_id
                      FROM (SELECT DISTINCT TRIM(regexp_substr(col, '[^,]+', 1, LEVEL)) *
                                            CASE
                                              WHEN l_multplyr = 0 THEN 0
                                              ELSE 1
                                            END AS period
                              FROM (SELECT l_perd_list col
                                      FROM dual)
                            CONNECT BY LEVEL <=
                                       length(regexp_replace(col, '[^,]+')) + 1)
                     ORDER BY perd_id) LOOP
        -- get CURRENT periods
        l_periods := get_periods(p_mrkt_id       => p_mrkt_id,
                                 p_orig_perd_id  => p_campgn_perd_id,
                                 p_bilng_perd_id => i_prd.perd_id,
                                 p_sls_typ_id    => p_sls_typ_id,
                                 p_bilng_day     => p_bilng_day,
                                 p_user_id       => l_user_id,
                                 p_run_id        => l_run_id);
        IF (l_periods.bilng_day IS NOT NULL) THEN
          -- get_head_details
          l_tbl_hist_detail := get_head_details(p_mrkt_id       => p_mrkt_id,
                                                p_sls_perd_id   => i_prd.perd_id,
                                                p_sls_typ_id    => p_sls_typ_id,
                                                p_bilng_day     => l_periods.bilng_day,
                                                p_offst_lbl_id  => NULL,
                                                p_cash_value    => NULL,
                                                p_r_factor      => NULL,
                                                p_perd_part_flg => 'N',
                                                p_user_id       => l_user_id,
                                                p_run_id        => l_run_id);
          -- SUM (HEAD)
          IF l_tbl_hist_detail.count > 0 THEN
            FOR i IN l_tbl_hist_detail.first .. l_tbl_hist_detail.last LOOP
              c_key := to_char(l_tbl_hist_detail(i).offst_lbl_id) || '_' || to_char(l_tbl_hist_detail(i).sls_typ_lbl_id);
              l_tbl_hist_head(c_key).offst_lbl_id := l_tbl_hist_detail(i).offst_lbl_id;
              l_tbl_hist_head(c_key).units := nvl(l_tbl_hist_head(c_key).units, 0) + l_tbl_hist_detail(i).units;
              l_tbl_hist_head(c_key).sales := nvl(l_tbl_hist_head(c_key).sales, 0) + l_tbl_hist_detail(i).sales;
              l_tbl_hist_head(c_key).sls_typ_lbl_id := l_tbl_hist_detail(i).sls_typ_lbl_id;
            END LOOP;
            l_tbl_hist_detail.delete;
          END IF;
          -- PIPE (HEAD)
          IF l_tbl_hist_head.count > 0 THEN
            c_key := l_tbl_hist_head.first;
            WHILE c_key IS NOT NULL LOOP
              PIPE ROW(obj_pa_trend_alloc_hist_hd_ln(i_prd.perd_id,
                                                     l_periods.trg_perd_id,
                                                     l_periods.bilng_day,
                                                     l_tbl_hist_head(c_key).offst_lbl_id,
                                                     NULL,
                                                     NULL,
                                                     l_tbl_hist_head(c_key).sls_typ_lbl_id,
                                                     l_tbl_hist_head(c_key).units,
                                                     l_tbl_hist_head(c_key).sales));
              c_key := l_tbl_hist_head.next(c_key);
            END LOOP;
            l_tbl_hist_head.delete;
          END IF;
        END IF;
      END LOOP;
      l_multplyr := l_multplyr - 1;
    END LOOP;
    --
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' end' || l_parameter_list);
  END get_trend_alloc_hist_head;

  -- get_reproc_trnd
  FUNCTION get_reproc_trnd(p_mrkt_id     IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                           p_sls_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                           p_sls_typ_id  IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                           p_bilng_day   IN dly_bilng_trnd.prcsng_dt%TYPE,
                           p_user_id     IN VARCHAR2 DEFAULT NULL,
                           p_run_id      IN NUMBER DEFAULT NULL)
    RETURN t_reproc IS
    -- local variables
    l_table_reproc_trnd t_reproc;
    -- for LOG
    l_run_id         NUMBER := nvl(p_run_id,
                                   app_plsql_output.generate_new_run_id);
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'GET_REPROC_TRND';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       to_char(p_mrkt_id) || ', ' ||
                                       'p_sls_perd_id: ' ||
                                       to_char(p_sls_perd_id) || ', ' ||
                                       'p_sls_typ_id: ' ||
                                       to_char(p_sls_typ_id) || ', ' ||
                                       'p_bilng_day: ' ||
                                       to_char(p_bilng_day, 'yyyymmdd') || ', ' ||
                                       'p_user_id: ' || l_user_id || ', ' ||
                                       'p_run_id: ' || to_char(l_run_id) || ')';
    -- exception
    e_get_reproc_trnd EXCEPTION;
    --
  BEGIN
    --
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start' || l_parameter_list);
    -- forecasted - BIAS, BI24, FSC_CD
    BEGIN
      SELECT offr_sku_line_id,
             rul_nm,
             offst_lbl_id,
             sls_typ_lbl_id,
             round(SUM(CASE
                         WHEN sct_unit_qty IS NOT NULL THEN
                          CASE
                            WHEN total_unit_qty_by_fsc_cd = 0 THEN
                             (1 / total_cnt_by_fsc_cd) * sct_unit_qty
                            ELSE
                             (osl_unit_qty / total_unit_qty_by_fsc_cd) * sct_unit_qty
                          END
                         ELSE
                          r_factor * bias * bi24_adj * osl_unit_qty
                       END)) units,
             round(SUM(CASE
                         WHEN sct_unit_qty IS NOT NULL THEN
                          CASE
                            WHEN total_unit_qty_by_fsc_cd = 0 THEN
                             (1 / total_cnt_by_fsc_cd) * sct_unit_qty
                            ELSE
                             (osl_unit_qty / total_unit_qty_by_fsc_cd) * sct_unit_qty
                          END *
                          (opp_sls_prc_amt / opp_nr_for_qty * opp_net_to_avon_fct)
                         ELSE
                          r_factor * bias * bi24_adj * osl_unit_qty *
                          (opp_sls_prc_amt / opp_nr_for_qty * opp_net_to_avon_fct)
                       END)) sales,
             veh_id,
             SUM(CASE
                   WHEN sct_unit_qty IS NOT NULL THEN
                    CASE
                      WHEN osl_unit_qty = 0 THEN
                       osl_comsn_amt
                      ELSE
                       (osl_unit_qty / total_unit_qty_by_fsc_cd) * sct_unit_qty *
                       (osl_comsn_amt / osl_unit_qty)
                    END
                   ELSE
                    r_factor * bias * bi24_adj * osl_comsn_amt
                 END) comsn_amt,
             SUM(CASE
                   WHEN sct_unit_qty IS NOT NULL THEN
                    CASE
                      WHEN osl_unit_qty = 0 THEN
                       osl_tax_amt
                      ELSE
                       (osl_unit_qty / total_unit_qty_by_fsc_cd) * sct_unit_qty *
                       (osl_tax_amt / osl_unit_qty)
                    END
                   ELSE
                    r_factor * bias * bi24_adj * osl_tax_amt
                 END) tax_amt
        BULK COLLECT
        INTO l_table_reproc_trnd
        FROM (WITH dbt AS (SELECT dly_bilng_trnd.dly_bilng_id,
                                  dly_bilng_trnd.unit_qty,
                                  dly_bilng_trnd.fsc_cd,
                                  dly_bilng_trnd.sku_id,
                                  tc_fc_dbt.offst_lbl_id,
                                  tc_fc_dbt.sls_typ_lbl_id,
                                  tc_fc_dbt.src_sls_typ_id,
                                  tc_fc_dbt.trgt_sls_perd_id,
                                  tc_fc_dbt.trgt_offr_perd_id,
                                  mpp.perd_part
                             FROM dly_bilng_trnd,
                                  (SELECT src_sls_typ_id,
                                          x_src_sls_typ_id,
                                          offst_lbl_id,
                                          sls_typ_lbl_id,
                                          x_sls_typ_lbl_id,
                                          pa_maps_public.perd_plus(p_mrkt_id,
                                                                   p_sls_perd_id,
                                                                   offst_val_src_sls) src_sls_perd_id,
                                          pa_maps_public.perd_plus(p_mrkt_id,
                                                                   p_sls_perd_id,
                                                                   offst_val_trgt_sls) trgt_sls_perd_id,
                                          pa_maps_public.perd_plus(p_mrkt_id,
                                                                   p_sls_perd_id,
                                                                   offst_val_src_offr) src_offr_perd_id,
                                          pa_maps_public.perd_plus(p_mrkt_id,
                                                                   p_sls_perd_id,
                                                                   offst_val_trgt_offr) trgt_offr_perd_id,
                                          r_factor
                                     FROM (SELECT src_sls_typ_id,
                                                  est_src_sls_typ_id AS x_src_sls_typ_id,
                                                  offst_lbl_id,
                                                  sls_typ_lbl_id,
                                                  x_sls_typ_lbl_id,
                                                  offst_val_src_sls,
                                                  offst_val_trgt_sls,
                                                  offst_val_src_offr,
                                                  offst_val_trgt_offr,
                                                  r_factor,
                                                  eff_sls_perd_id,
                                                  lead(eff_sls_perd_id,
                                                       1) over(PARTITION BY offst_lbl_id, sls_typ_grp_nm ORDER BY eff_sls_perd_id) AS next_eff_sls_perd_id
                                             FROM ta_config
                                            WHERE mrkt_id =
                                                  p_mrkt_id
                                              AND trgt_sls_typ_id =
                                                  p_sls_typ_id
                                              AND upper(REPLACE(TRIM(sls_typ_grp_nm),
                                                                '  ',
                                                                ' ')) IN
                                                  (SELECT TRIM(regexp_substr(col,
                                                                             '[^,]+',
                                                                             1,
                                                                             LEVEL)) RESULT
                                                     FROM (SELECT c_sls_typ_grp_nm_fc_dbt col
                                                             FROM dual)
                                                   CONNECT BY LEVEL <=
                                                              length(regexp_replace(col,
                                                                                    '[^,]+')) + 1)
                                              AND eff_sls_perd_id <=
                                                  p_sls_perd_id)
                                    WHERE p_sls_perd_id BETWEEN
                                          eff_sls_perd_id AND
                                          nvl(next_eff_sls_perd_id,
                                              p_sls_perd_id)) tc_fc_dbt,
                                  (SELECT trnd_bilng_days.prcsng_dt,
                                          CASE
                                            WHEN trnd_bilng_days.day_num <
                                                 p.lte_day_num THEN
                                             1
                                            WHEN trnd_bilng_days.day_num >
                                                 p.gte_day_num THEN
                                             3
                                            ELSE
                                             2
                                          END AS perd_part
                                     FROM trnd_bilng_days,
                                          (SELECT lte_day_num,
                                                  gte_day_num
                                             FROM (SELECT mrkt_id,
                                                          eff_campgn_perd_id,
                                                          lte_day_num,
                                                          gte_day_num,
                                                          lead(eff_campgn_perd_id) over(ORDER BY eff_campgn_perd_id) next_eff_campgn_perd_id
                                                     FROM mrkt_perd_parts
                                                    WHERE mrkt_perd_parts.mrkt_id =
                                                          p_mrkt_id
                                                      AND mrkt_perd_parts.eff_campgn_perd_id <=
                                                          p_sls_perd_id)
                                            WHERE p_sls_perd_id BETWEEN
                                                  eff_campgn_perd_id AND
                                                  nvl(next_eff_campgn_perd_id,
                                                      p_sls_perd_id)) p
                                    WHERE trnd_bilng_days.mrkt_id =
                                          p_mrkt_id
                                      AND trnd_bilng_days.sls_perd_id =
                                          p_sls_perd_id
                                      AND trnd_bilng_days.prcsng_dt <=
                                          p_bilng_day) mpp
                            WHERE dly_bilng_trnd.mrkt_id =
                                  p_mrkt_id
                              AND dly_bilng_trnd.trnd_sls_perd_id =
                                  tc_fc_dbt.src_sls_perd_id
                              AND dly_bilng_trnd.offr_perd_id =
                                  tc_fc_dbt.src_offr_perd_id
                              AND trunc(dly_bilng_trnd.prcsng_dt) <=
                                  p_bilng_day
                              AND dly_bilng_trnd.trnd_aloctn_auto_stus_id IN
                                  (auto_matched,
                                   auto_suggested_single,
                                   auto_suggested_multi)
                              AND trunc(dly_bilng_trnd.prcsng_dt) =
                                  mpp.prcsng_dt(+))
               SELECT /*+ INDEX(DLY_BILNG_TRND_OFFR_SKU_LINE PK_DB_TRND_OFFR_SKU_LINE) */
                dly_bilng_trnd_offr_sku_line.offr_sku_line_id,
                dbt.offst_lbl_id,
                prfl.catgry_id,
                offr_prfl_prc_point.sls_cls_cd,
                offr.veh_id,
                dbt.perd_part,
                dbt.sku_id,
                dbt.sls_typ_lbl_id,
                nvl(dly_bilng_trnd_offr_sku_line.unit_qty, 0) osl_unit_qty,
                nvl(dly_bilng_trnd_offr_sku_line.comsn_amt, 0) osl_comsn_amt,
                nvl(dly_bilng_trnd_offr_sku_line.tax_amt, 0) osl_tax_amt,
                nvl(offr_prfl_prc_point.sls_prc_amt, 0) opp_sls_prc_amt,
                decode(nvl(offr_prfl_prc_point.nr_for_qty, 0),
                       0,
                       1,
                       offr_prfl_prc_point.nr_for_qty) opp_nr_for_qty,
                decode(nvl(offr_prfl_prc_point.net_to_avon_fct,
                           0),
                       0,
                       1,
                       offr_prfl_prc_point.net_to_avon_fct) opp_net_to_avon_fct,
                SUM(dly_bilng_trnd_offr_sku_line.unit_qty) over(PARTITION BY dbt.fsc_cd, dbt.offst_lbl_id) total_unit_qty_by_fsc_cd,
                COUNT(1) over(PARTITION BY dbt.fsc_cd, dbt.offst_lbl_id) total_cnt_by_fsc_cd,
                -- BIAS
                nvl(mrkt_perd_sku_bias.bias_pct / 100, 1) bias,
                -- BI24_ADJ
                nvl(dly_bilng_adjstmnt.unit_qty /
                    nvl(dbt.unit_qty, 1),
                    1) bi24_adj,
                -- FSC
                sct_fsc_ovrrd.sct_unit_qty,
                get_rule_nm(p_mrkt_id,
                            p_sls_perd_id,
                            p_sls_typ_id,
                            dbt.offst_lbl_id,
                            prfl.catgry_id,
                            offr_prfl_prc_point.sls_cls_cd,
                            offr.veh_id,
                            dbt.perd_part,
                            dbt.sku_id) rul_nm,
                 get_r_factor(p_mrkt_id,
                              p_sls_perd_id,
                              p_sls_typ_id,
                              dbt.offst_lbl_id,
                              prfl.catgry_id,
                              offr_prfl_prc_point.sls_cls_cd,
                              offr.veh_id,
                              dbt.perd_part,
                              dbt.sku_id)  r_factor
                 FROM dbt,
                      sct_fsc_ovrrd,
                      dly_bilng_trnd_offr_sku_line,
                      offr_sku_line,
                      offr_prfl_prc_point,
                      offr,
                      sku,
                      prfl,
                      mrkt_perd_sku_bias,
                      dly_bilng_adjstmnt
                WHERE p_mrkt_id = sct_fsc_ovrrd.mrkt_id(+)
                  AND dbt.trgt_sls_perd_id =
                      sct_fsc_ovrrd.sls_perd_id(+)
                  AND p_sls_typ_id =
                      sct_fsc_ovrrd.sls_typ_id(+)
                  AND to_number(dbt.fsc_cd) =
                      to_number(sct_fsc_ovrrd.fsc_cd(+))
                  AND dbt.offst_lbl_id =
                      sct_fsc_ovrrd.offst_lbl_id(+)
                  AND dbt.dly_bilng_id =
                      dly_bilng_trnd_offr_sku_line.dly_bilng_id
                  AND dly_bilng_trnd_offr_sku_line.sls_typ_id =
                      dbt.src_sls_typ_id
                  AND dly_bilng_trnd_offr_sku_line.offr_sku_line_id =
                      offr_sku_line.offr_sku_line_id
                  AND offr_sku_line.dltd_ind <> 'Y'
                  AND offr_sku_line.offr_prfl_prcpt_id =
                      offr_prfl_prc_point.offr_prfl_prcpt_id
                  AND offr_prfl_prc_point.offr_id =
                      offr.offr_id
                  AND offr.offr_typ = 'CMP'
                  AND offr.ver_id = 0
                  AND dbt.sku_id = sku.sku_id(+)
                  AND sku.prfl_cd = prfl.prfl_cd(+)
                  AND p_mrkt_id =
                      mrkt_perd_sku_bias.mrkt_id(+)
                  AND dbt.trgt_sls_perd_id =
                      mrkt_perd_sku_bias.sls_perd_id(+)
                  AND dbt.sku_id =
                      mrkt_perd_sku_bias.sku_id(+)
                  AND p_sls_typ_id =
                      mrkt_perd_sku_bias.sls_typ_id(+)
                  AND dbt.dly_bilng_id =
                      dly_bilng_adjstmnt.dly_bilng_id(+))
       GROUP BY offr_sku_line_id,
                rul_nm,
                offst_lbl_id,
                sls_typ_lbl_id,
                veh_id;
    EXCEPTION
      WHEN OTHERS THEN
        app_plsql_log.error(l_module_name || ' error code:' || SQLCODE ||
                            ' error message: ' || SQLERRM ||
                            l_parameter_list);
        RAISE e_get_reproc_trnd;
    END;
    --
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' end' || l_parameter_list);
    --
    RETURN l_table_reproc_trnd;
  END get_reproc_trnd;

  -- get_reproc_trnd2
  FUNCTION get_reproc_trnd2(p_mrkt_id              IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                            p_sls_perd_id          IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                            p_sls_typ_id           IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                            p_bilng_day            IN dly_bilng_trnd.prcsng_dt%TYPE,
                            p_user_id              IN VARCHAR2 DEFAULT NULL,
                            p_run_id               IN NUMBER DEFAULT NULL)
    RETURN t_reproc IS
    -- local variables
    l_table_reproc_trnd2   t_reproc;
    l_trg_perd_id          dstrbtd_mrkt_sls.sls_perd_id%TYPE;
    l_src_sls_typ_id       ta_config.src_sls_typ_id%TYPE;
    -- for LOG
    l_run_id         NUMBER := nvl(p_run_id,
                                   app_plsql_output.generate_new_run_id);
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'GET_REPROC_TRND2';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       to_char(p_mrkt_id) || ', ' ||
                                       'p_sls_perd_id: ' ||
                                       to_char(p_sls_perd_id) || ', ' ||
                                       'p_sls_typ_id: ' ||
                                       to_char(p_sls_typ_id) || ', ' ||
                                       'p_bilng_day: ' ||
                                       to_char(p_bilng_day, 'yyyymmdd') || ', ' ||
                                       'p_user_id: ' || l_user_id || ', ' ||
                                       'p_run_id: ' || to_char(l_run_id) || ')';
    -- exception
    e_get_reproc_trnd2 EXCEPTION;
    --
  BEGIN
    --
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start' || l_parameter_list);
    -- get TARGET period
    BEGIN
      SELECT MAX(trgt_sls_perd_id), MAX(src_sls_typ_id)
        INTO l_trg_perd_id, l_src_sls_typ_id
        FROM TABLE(get_ta_config(p_mrkt_id        => p_mrkt_id,
                                 p_sls_perd_id    => p_sls_perd_id,
                                 p_sls_typ_id     => p_sls_typ_id,
                                 p_sls_typ_grp_nm => c_sls_typ_grp_nm_fc_dbt));
    EXCEPTION
      WHEN OTHERS THEN
        app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
        app_plsql_log.info(l_module_name ||
                           ' warning: RECORD not found in CONFIG table: ta_config where mrkt_id=' ||
                           to_char(p_mrkt_id) || ' and trgt_sls_typ_id = ' ||
                           to_char(p_sls_typ_id) ||
                           ' and sls_typ_grp_nm in (' ||
                           c_sls_typ_grp_nm_fc_dbt || ')' ||
                           ' and eff_sls_perd_id <= ' ||
                           to_char(p_sls_perd_id) || l_parameter_list);
        l_trg_perd_id := NULL;
    END;
    l_parameter_list := ' (p_mrkt_id: ' ||
                           to_char(p_mrkt_id) || ', ' ||
                           'p_sls_perd_id: ' ||
                           to_char(p_sls_perd_id) || ', ' ||
                           'l_trg_perd_id: ' ||
                           to_char(l_trg_perd_id) || ', ' ||
                           'p_sls_typ_id: ' ||
                           to_char(p_sls_typ_id) || ', ' ||
                           'p_bilng_day: ' ||
                           to_char(p_bilng_day, 'yyyymmdd') || ', ' ||
                           'p_user_id: ' || l_user_id || ', ' ||
                           'p_run_id: ' || to_char(l_run_id) || ')';
    -- forecasted - TRND2
    BEGIN
      SELECT x.offr_sku_line_id,
             x.rul_nm,
             x.offst_lbl_id,
             x.sls_typ_lbl_id,
             round(SUM(nvl(x.unit_qty, 0))) units,
             round(SUM(nvl(x.unit_qty, 0) *
                       (nvl(x.sls_prc_amt, 0) /
                        decode(nvl(x.nr_for_qty, 0), 0, 1, x.nr_for_qty) *
                        decode(nvl(x.net_to_avon_fct, 0),
                               0,
                               1,
                               x.net_to_avon_fct) /
                        decode(nvl(x.unit_qty, 0), 0, 1, x.unit_qty)))) sales,
             x.veh_id,
             NULL comsn_amt,
             NULL tax_amt
        BULK COLLECT
        INTO l_table_reproc_trnd2
        FROM (SELECT dstrbtd_mrkt_sls.offr_sku_line_id,
                     tc_fc_dbt.offst_lbl_id,
                     prfl.catgry_id,
                     offr_prfl_prc_point.sls_cls_cd,
                     offr.veh_id,
                     NULL AS perd_part,
                     offr_sku_line.sku_id,
                     tc_fc_dbt.sls_typ_lbl_id,
                     tc_fc_dbt.lbl_desc,
                     sfo.sct_unit_qty as unit_qty,
                     offr_prfl_prc_point.sls_prc_amt,
                     offr_prfl_prc_point.nr_for_qty,
                     offr_prfl_prc_point.net_to_avon_fct,
                     sfo.fsc_cd,
                     get_rule_nm(p_mrkt_id,
                                 p_sls_perd_id,
                                 p_sls_typ_id,
                                 tc_fc_dbt.offst_lbl_id,
                                 prfl.catgry_id,
                                 offr_prfl_prc_point.sls_cls_cd,
                                 offr.veh_id,
                                 NULL,
                                 offr_sku_line.sku_id) rul_nm,
                     get_r_factor(p_mrkt_id,
                                  p_sls_perd_id,
                                  p_sls_typ_id,
                                  tc_fc_dbt.offst_lbl_id,
                                  prfl.catgry_id,
                                  offr_prfl_prc_point.sls_cls_cd,
                                  offr.veh_id,
                                  NULL,
                                  offr_sku_line.sku_id)  r_factor
                FROM dstrbtd_mrkt_sls,
                     (SELECT sct_fsc_ovrrd.mrkt_id,
                             sct_fsc_ovrrd.sls_perd_id,
                             sct_fsc_ovrrd.sls_typ_id,
                             sct_fsc_ovrrd.offst_lbl_id,
                             sct_fsc_ovrrd.fsc_cd,
                             nvl(mrkt_tmp_fsc_master.sku_id, mrkt_tmp_fsc.sku_id) sku_id,
                             sct_fsc_ovrrd.sct_unit_qty
                        FROM sct_fsc_ovrrd,
                             (SELECT mrkt_id, sku_id, fsc_cd
                                    FROM (SELECT mrkt_id,
                                                 sku_id,
                                                 to_number(mstr_fsc_cd) fsc_cd,
                                                 strt_perd_id from_strt_perd_id,
                                                 nvl(lead(strt_perd_id, 1) over(PARTITION BY mrkt_id, sku_id ORDER BY strt_perd_id), 99999999) to_strt_perd_id
                                            FROM mstr_fsc_asgnmt
                                           WHERE p_mrkt_id = mrkt_id
                                             AND l_trg_perd_id >= strt_perd_id)
                                   WHERE l_trg_perd_id >= from_strt_perd_id
                                     AND l_trg_perd_id < to_strt_perd_id) mrkt_tmp_fsc_master,
                                 (SELECT mrkt_id,
                                         MAX(sku_id) sku_id,
                                         fsc_cd
                                    FROM (SELECT mrkt_id,
                                                 sku_id,
                                                 to_number(fsc_cd) fsc_cd,
                                                 strt_perd_id from_strt_perd_id,
                                                 nvl(lead(strt_perd_id, 1) over(PARTITION BY mrkt_id, fsc_cd ORDER BY strt_perd_id), 99999999) to_strt_perd_id
                                            FROM mrkt_fsc
                                           WHERE p_mrkt_id = mrkt_id
                                             AND l_trg_perd_id >= strt_perd_id
                                             AND 'N' = dltd_ind)
                                   WHERE l_trg_perd_id >= from_strt_perd_id
                                     AND l_trg_perd_id < to_strt_perd_id
                                   GROUP BY mrkt_id, fsc_cd) mrkt_tmp_fsc
                      WHERE sct_fsc_ovrrd.mrkt_id  = p_mrkt_id
                        AND sct_fsc_ovrrd.sls_perd_id = l_trg_perd_id
                        AND sct_fsc_ovrrd.sls_typ_id = p_sls_typ_id
                        -- mrkt_tmp_fsc_master
                        AND p_mrkt_id  = mrkt_tmp_fsc_master.mrkt_id(+)
                        AND sct_fsc_ovrrd.fsc_cd  = mrkt_tmp_fsc_master.fsc_cd(+)
                        -- mrkt_tmp_fsc
                        AND p_mrkt_id  = mrkt_tmp_fsc.mrkt_id(+)
                        AND sct_fsc_ovrrd.fsc_cd  = mrkt_tmp_fsc.fsc_cd(+)) sfo,
                     (WITH dbt AS (SELECT /*+ INDEX(DLY_BILNG_TRND INDEX_DLY_BILNG_TRND_M_S) */
                                          dly_bilng_trnd.dly_bilng_id
                                     FROM dly_bilng_trnd,
                                          (SELECT src_sls_typ_id,
                                                  x_src_sls_typ_id,
                                                  offst_lbl_id,
                                                  sls_typ_lbl_id,
                                                  x_sls_typ_lbl_id,
                                                  pa_maps_public.perd_plus(p_mrkt_id,
                                                                           p_sls_perd_id,
                                                                           offst_val_src_sls) src_sls_perd_id,
                                                  pa_maps_public.perd_plus(p_mrkt_id,
                                                                           p_sls_perd_id,
                                                                           offst_val_trgt_sls) trgt_sls_perd_id,
                                                  pa_maps_public.perd_plus(p_mrkt_id,
                                                                           p_sls_perd_id,
                                                                           offst_val_src_offr) src_offr_perd_id,
                                                  pa_maps_public.perd_plus(p_mrkt_id,
                                                                           p_sls_perd_id,
                                                                           offst_val_trgt_offr) trgt_offr_perd_id,
                                                  r_factor
                                             FROM (SELECT src_sls_typ_id,
                                                          est_src_sls_typ_id AS x_src_sls_typ_id,
                                                          offst_lbl_id,
                                                          sls_typ_lbl_id,
                                                          x_sls_typ_lbl_id,
                                                          offst_val_src_sls,
                                                          offst_val_trgt_sls,
                                                          offst_val_src_offr,
                                                          offst_val_trgt_offr,
                                                          r_factor,
                                                          eff_sls_perd_id,
                                                          lead(eff_sls_perd_id,
                                                               1) over(PARTITION BY offst_lbl_id, sls_typ_grp_nm ORDER BY eff_sls_perd_id) AS next_eff_sls_perd_id
                                                     FROM ta_config
                                                    WHERE mrkt_id =
                                                          p_mrkt_id
                                                      AND trgt_sls_typ_id =
                                                          p_sls_typ_id
                                                      AND upper(REPLACE(TRIM(sls_typ_grp_nm),
                                                                        '  ',
                                                                        ' ')) IN
                                                          (SELECT TRIM(regexp_substr(col,
                                                                                     '[^,]+',
                                                                                     1,
                                                                                     LEVEL)) RESULT
                                                             FROM (SELECT c_sls_typ_grp_nm_fc_dbt col
                                                                     FROM dual)
                                                           CONNECT BY LEVEL <=
                                                                      length(regexp_replace(col,
                                                                                            '[^,]+')) + 1)
                                                      AND eff_sls_perd_id <=
                                                          p_sls_perd_id)
                                            WHERE p_sls_perd_id BETWEEN
                                                  eff_sls_perd_id AND
                                                  nvl(next_eff_sls_perd_id,
                                                      p_sls_perd_id)) tc_fc_dbt
                                    WHERE dly_bilng_trnd.mrkt_id = p_mrkt_id
                                      AND dly_bilng_trnd.trnd_sls_perd_id = tc_fc_dbt.src_sls_perd_id
                                      AND dly_bilng_trnd.offr_perd_id = tc_fc_dbt.src_offr_perd_id
                                      AND trunc(dly_bilng_trnd.prcsng_dt) <= p_bilng_day
                                      AND dly_bilng_trnd.trnd_aloctn_auto_stus_id IN
                                          (auto_matched,
                                           auto_suggested_single,
                                           auto_suggested_multi))
                      SELECT /*+ INDEX(DLY_BILNG_TRND_OFFR_SKU_LINE PK_DB_TRND_OFFR_SKU_LINE) */
                             DISTINCT osl.offr_sku_line_id
                        FROM dbt,
                             dly_bilng_trnd_offr_sku_line osl
                       WHERE dbt.dly_bilng_id = osl.dly_bilng_id
                         AND osl.sls_typ_id = l_src_sls_typ_id) dbtosl,
                     offr_sku_line,
                     offr_prfl_prc_point,
                     offr,
                     sku,
                     prfl,
                     (SELECT tc.src_sls_typ_id,
                             tc.x_src_sls_typ_id,
                             tc.offst_lbl_id,
                             tc.sls_typ_lbl_id,
                             tc.x_sls_typ_lbl_id,
                             tc.src_sls_perd_id,
                             tc.trgt_sls_perd_id,
                             tc.src_offr_perd_id,
                             tc.trgt_offr_perd_id,
                             tc.r_factor,
                             upper(td.lbl_desc) AS lbl_desc
                        FROM (SELECT src_sls_typ_id,
                                     x_src_sls_typ_id,
                                     offst_lbl_id,
                                     sls_typ_lbl_id,
                                     x_sls_typ_lbl_id,
                                     pa_maps_public.perd_plus(p_mrkt_id   => p_mrkt_id,
                                                              p_perd1     => p_sls_perd_id,
                                                              p_perd_diff => offst_val_src_sls) src_sls_perd_id,
                                     pa_maps_public.perd_plus(p_mrkt_id   => p_mrkt_id,
                                                              p_perd1     => p_sls_perd_id,
                                                              p_perd_diff => offst_val_trgt_sls) trgt_sls_perd_id,
                                     pa_maps_public.perd_plus(p_mrkt_id   => p_mrkt_id,
                                                              p_perd1     => p_sls_perd_id,
                                                              p_perd_diff => offst_val_src_offr) src_offr_perd_id,
                                     pa_maps_public.perd_plus(p_mrkt_id   => p_mrkt_id,
                                                              p_perd1     => p_sls_perd_id,
                                                              p_perd_diff => offst_val_trgt_offr) trgt_offr_perd_id,
                                     r_factor
                                FROM (SELECT src_sls_typ_id,
                                             est_src_sls_typ_id AS x_src_sls_typ_id,
                                             offst_lbl_id,
                                             sls_typ_lbl_id,
                                             x_sls_typ_lbl_id,
                                             offst_val_src_sls,
                                             offst_val_trgt_sls,
                                             offst_val_src_offr,
                                             offst_val_trgt_offr,
                                             r_factor,
                                             eff_sls_perd_id,
                                             lead(eff_sls_perd_id,
                                                  1) over(PARTITION BY offst_lbl_id, sls_typ_grp_nm ORDER BY eff_sls_perd_id) AS next_eff_sls_perd_id
                                        FROM ta_config
                                       WHERE mrkt_id = p_mrkt_id
                                         AND trgt_sls_typ_id =
                                             p_sls_typ_id
                                         AND upper(REPLACE(TRIM(sls_typ_grp_nm),
                                                           '  ',
                                                           ' ')) IN
                                             (SELECT TRIM(regexp_substr(col,
                                                                        '[^,]+',
                                                                        1,
                                                                        LEVEL)) RESULT
                                                FROM (SELECT c_sls_typ_grp_nm_fc_dbt col
                                                        FROM dual)
                                              CONNECT BY LEVEL <=
                                                         length(regexp_replace(col,
                                                                               '[^,]+')) + 1)
                                         AND eff_sls_perd_id <=
                                             p_sls_perd_id)
                               WHERE p_sls_perd_id BETWEEN
                                     eff_sls_perd_id AND
                                     nvl(next_eff_sls_perd_id,
                                         p_sls_perd_id)) tc,
                             ta_dict td
                       WHERE td.lbl_id = tc.offst_lbl_id) tc_fc_dbt
               WHERE dstrbtd_mrkt_sls.mrkt_id = p_mrkt_id
                 AND dstrbtd_mrkt_sls.sls_perd_id = tc_fc_dbt.trgt_sls_perd_id
                 AND dstrbtd_mrkt_sls.offr_perd_id = tc_fc_dbt.trgt_offr_perd_id
                 /* ORIGINAL where condition in
                      get_reproc_est 
                 AND dstrbtd_mrkt_sls.sls_typ_id = tc_fc_dbt.x_src_sls_typ_id
                 ------------------------------------------------------------
                 but we need here the sls_typ_id = 6 (demand actual)
                 OR 
                 input paramter: p_sls_typ_id
                 ------------------------------------------------------------
                 */
                 AND dstrbtd_mrkt_sls.sls_typ_id = tc_fc_dbt.src_sls_typ_id /* p_sls_typ_id */
                 AND dstrbtd_mrkt_sls.offr_sku_line_id = offr_sku_line.offr_sku_line_id
                 AND offr_sku_line.dltd_ind <> 'Y'
                 --------------------------------------------------
                 -- dms -> sfo
                 AND dstrbtd_mrkt_sls.mrkt_id = sfo.mrkt_id
                 AND dstrbtd_mrkt_sls.sls_perd_id = sfo.sls_perd_id
                 /* AND dstrbtd_mrkt_sls.sls_typ_id = sfo.sls_typ_id */
                 -- sfo -> tc_fc_dbt
                 AND sfo.offst_lbl_id = tc_fc_dbt.offst_lbl_id
                 -- sfo -> offr_sku_line
                 AND sfo.sku_id = offr_sku_line.sku_id
                 --------------------------------------------------
                 AND offr_sku_line.offr_prfl_prcpt_id = offr_prfl_prc_point.offr_prfl_prcpt_id
                 AND offr_prfl_prc_point.offr_id = offr.offr_id
                 AND offr.offr_typ = 'CMP'
                 AND offr.ver_id = 0
                 AND offr_sku_line.sku_id = sku.sku_id(+)
                 AND sku.prfl_cd = prfl.prfl_cd(+)
                 AND offr_sku_line.offr_sku_line_id = dbtosl.offr_sku_line_id(+)
                 AND dbtosl.offr_sku_line_id IS NULL) x
       GROUP BY x.offr_sku_line_id,
                x.rul_nm,
                x.offst_lbl_id,
                x.sls_typ_lbl_id,
                x.veh_id;
    EXCEPTION
      WHEN OTHERS THEN
        app_plsql_log.error(l_module_name || ' error code:' || SQLCODE ||
                            ' error message: ' || SQLERRM ||
                            l_parameter_list);
        RAISE e_get_reproc_trnd2;
    END;
    --
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' end' || l_parameter_list);
    --
    RETURN l_table_reproc_trnd2;
  END get_reproc_trnd2;

  -- get_reproc_est
  FUNCTION get_reproc_est(p_mrkt_id              IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                          p_sls_perd_id          IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                          p_sls_typ_id           IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                          p_bilng_day            IN dly_bilng_trnd.prcsng_dt%TYPE,
                          p_use_offers_on_sched  IN CHAR DEFAULT 'N',
                          p_use_offers_off_sched IN CHAR DEFAULT 'N',
                          p_user_id              IN VARCHAR2 DEFAULT NULL,
                          p_run_id               IN NUMBER DEFAULT NULL)
    RETURN t_reproc IS
    -- local variables
    l_table_reproc_est     t_reproc;
    l_trg_perd_id          dstrbtd_mrkt_sls.sls_perd_id%TYPE;
    l_use_offers_on_sched  CHAR(1) := upper(nvl(p_use_offers_on_sched, 'N'));
    l_use_offers_off_sched CHAR(1) := upper(nvl(p_use_offers_off_sched, 'N'));
    l_src_sls_typ_id       ta_config.src_sls_typ_id%TYPE;
    -- for LOG
    l_run_id         NUMBER := nvl(p_run_id,
                                   app_plsql_output.generate_new_run_id);
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'GET_REPROC_EST';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       to_char(p_mrkt_id) || ', ' ||
                                       'p_sls_perd_id: ' ||
                                       to_char(p_sls_perd_id) || ', ' ||
                                       'l_trg_perd_id: ' ||
                                       to_char(l_trg_perd_id) || ', ' ||
                                       'p_sls_typ_id: ' ||
                                       to_char(p_sls_typ_id) || ', ' ||
                                       'p_bilng_day: ' ||
                                       to_char(p_bilng_day, 'yyyymmdd') || ', ' ||
                                       'p_user_id: ' || l_user_id || ', ' ||
                                       'p_run_id: ' || to_char(l_run_id) || ')';
    -- exception
    e_get_reproc_est EXCEPTION;
    --
  BEGIN
    --
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start' || l_parameter_list);
    -- get TARGET period
    BEGIN
      SELECT MAX(trgt_sls_perd_id), MAX(src_sls_typ_id)
        INTO l_trg_perd_id, l_src_sls_typ_id
        FROM TABLE(get_ta_config(p_mrkt_id        => p_mrkt_id,
                                 p_sls_perd_id    => p_sls_perd_id,
                                 p_sls_typ_id     => p_sls_typ_id,
                                 p_sls_typ_grp_nm => c_sls_typ_grp_nm_fc_dbt));
    EXCEPTION
      WHEN OTHERS THEN
        app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
        app_plsql_log.info(l_module_name ||
                           ' warning: RECORD not found in CONFIG table: ta_config where mrkt_id=' ||
                           to_char(p_mrkt_id) || ' and trgt_sls_typ_id = ' ||
                           to_char(p_sls_typ_id) ||
                           ' and sls_typ_grp_nm in (' ||
                           c_sls_typ_grp_nm_fc_dbt || ')' ||
                           ' and eff_sls_perd_id <= ' ||
                           to_char(p_sls_perd_id) || l_parameter_list);
        l_trg_perd_id := NULL;
    END;
    -- forecasted - EST
    BEGIN
      SELECT x.offr_sku_line_id,
             x.rul_nm,
             x.offst_lbl_id,
             x.sls_typ_lbl_id,
             round(SUM(nvl(sct_fsc_ovrrd.sct_unit_qty,
                           CASE
                             WHEN x.use_estimate = 'Y' THEN nvl(x.unit_qty, 0)
                             ELSE 0
                           END))) units,
             round(SUM(nvl(sct_fsc_ovrrd.sct_unit_qty,
                           CASE
                             WHEN x.use_estimate = 'Y' THEN nvl(x.unit_qty, 0)
                             ELSE 0
                           END) *
                       (nvl(x.sls_prc_amt, 0) /
                        decode(nvl(x.nr_for_qty, 0), 0, 1, x.nr_for_qty) *
                        decode(nvl(x.net_to_avon_fct, 0),
                               0,
                               1,
                               x.net_to_avon_fct) /
                        decode(nvl(x.unit_qty, 0), 0, 1, x.unit_qty)))) sales,
             x.veh_id,
             NULL comsn_amt,
             NULL tax_amt
        BULK COLLECT
        INTO l_table_reproc_est
        FROM (SELECT dstrbtd_mrkt_sls.offr_sku_line_id,
                     tc_fc_dbt.offst_lbl_id,
                     prfl.catgry_id,
                     offr_prfl_prc_point.sls_cls_cd,
                     offr.veh_id,
                     NULL AS perd_part,
                     offr_sku_line.sku_id,
                     tc_fc_dbt.sls_typ_lbl_id,
                     tc_fc_dbt.lbl_desc,
                     dstrbtd_mrkt_sls.unit_qty,
                     offr_prfl_prc_point.sls_prc_amt,
                     offr_prfl_prc_point.nr_for_qty,
                     offr_prfl_prc_point.net_to_avon_fct,
                     nvl(mrkt_tmp_fsc_master.fsc_cd, mrkt_tmp_fsc.fsc_cd) fsc_cd,
                     get_rule_nm(p_mrkt_id,
                                 p_sls_perd_id,
                                 p_sls_typ_id,
                                 tc_fc_dbt.offst_lbl_id,
                                 prfl.catgry_id,
                                 offr_prfl_prc_point.sls_cls_cd,
                                 offr.veh_id,
                                 NULL,
                                 offr_sku_line.sku_id) rul_nm,
                     get_r_factor(p_mrkt_id,
                                  p_sls_perd_id,
                                  p_sls_typ_id,
                                  tc_fc_dbt.offst_lbl_id,
                                  prfl.catgry_id,
                                  offr_prfl_prc_point.sls_cls_cd,
                                  offr.veh_id,
                                  NULL,
                                  offr_sku_line.sku_id) r_factor,
                     get_use_estimate(p_mrkt_id,
                                      p_sls_perd_id,
                                      p_sls_typ_id,
                                      tc_fc_dbt.offst_lbl_id,
                                      prfl.catgry_id,
                                      offr_prfl_prc_point.sls_cls_cd,
                                      offr.veh_id,
                                      NULL,
                                      offr_sku_line.sku_id) use_estimate
                FROM dstrbtd_mrkt_sls,
                     (WITH dbt AS (SELECT /*+ INDEX(DLY_BILNG_TRND INDEX_DLY_BILNG_TRND_M_S) */
                                          dly_bilng_trnd.dly_bilng_id
                                     FROM dly_bilng_trnd,
                                          (SELECT src_sls_typ_id,
                                                  x_src_sls_typ_id,
                                                  offst_lbl_id,
                                                  sls_typ_lbl_id,
                                                  x_sls_typ_lbl_id,
                                                  pa_maps_public.perd_plus(p_mrkt_id,
                                                                           p_sls_perd_id,
                                                                           offst_val_src_sls) src_sls_perd_id,
                                                  pa_maps_public.perd_plus(p_mrkt_id,
                                                                           p_sls_perd_id,
                                                                           offst_val_trgt_sls) trgt_sls_perd_id,
                                                  pa_maps_public.perd_plus(p_mrkt_id,
                                                                           p_sls_perd_id,
                                                                           offst_val_src_offr) src_offr_perd_id,
                                                  pa_maps_public.perd_plus(p_mrkt_id,
                                                                           p_sls_perd_id,
                                                                           offst_val_trgt_offr) trgt_offr_perd_id,
                                                  r_factor
                                             FROM (SELECT src_sls_typ_id,
                                                          est_src_sls_typ_id AS x_src_sls_typ_id,
                                                          offst_lbl_id,
                                                          sls_typ_lbl_id,
                                                          x_sls_typ_lbl_id,
                                                          offst_val_src_sls,
                                                          offst_val_trgt_sls,
                                                          offst_val_src_offr,
                                                          offst_val_trgt_offr,
                                                          r_factor,
                                                          eff_sls_perd_id,
                                                          lead(eff_sls_perd_id,
                                                               1) over(PARTITION BY offst_lbl_id, sls_typ_grp_nm ORDER BY eff_sls_perd_id) AS next_eff_sls_perd_id
                                                     FROM ta_config
                                                    WHERE mrkt_id =
                                                          p_mrkt_id
                                                      AND trgt_sls_typ_id =
                                                          p_sls_typ_id
                                                      AND upper(REPLACE(TRIM(sls_typ_grp_nm),
                                                                        '  ',
                                                                        ' ')) IN
                                                          (SELECT TRIM(regexp_substr(col,
                                                                                     '[^,]+',
                                                                                     1,
                                                                                     LEVEL)) RESULT
                                                             FROM (SELECT c_sls_typ_grp_nm_fc_dbt col
                                                                     FROM dual)
                                                           CONNECT BY LEVEL <=
                                                                      length(regexp_replace(col,
                                                                                            '[^,]+')) + 1)
                                                      AND eff_sls_perd_id <=
                                                          p_sls_perd_id)
                                            WHERE p_sls_perd_id BETWEEN
                                                  eff_sls_perd_id AND
                                                  nvl(next_eff_sls_perd_id,
                                                      p_sls_perd_id)) tc_fc_dbt
                                    WHERE dly_bilng_trnd.mrkt_id = p_mrkt_id
                                      AND dly_bilng_trnd.trnd_sls_perd_id = tc_fc_dbt.src_sls_perd_id
                                      AND dly_bilng_trnd.offr_perd_id = tc_fc_dbt.src_offr_perd_id
                                      AND trunc(dly_bilng_trnd.prcsng_dt) <= p_bilng_day
                                      AND dly_bilng_trnd.trnd_aloctn_auto_stus_id IN
                                          (auto_matched,
                                           auto_suggested_single,
                                           auto_suggested_multi))
                      SELECT /*+ INDEX(DLY_BILNG_TRND_OFFR_SKU_LINE PK_DB_TRND_OFFR_SKU_LINE) */
                             DISTINCT osl.offr_sku_line_id
                        FROM dbt,
                             dly_bilng_trnd_offr_sku_line osl
                       WHERE dbt.dly_bilng_id = osl.dly_bilng_id
                         AND osl.sls_typ_id = l_src_sls_typ_id) dbtosl,
                     offr_sku_line,
                     offr_prfl_prc_point,
                     offr,
                     sku,
                     prfl,
                     (SELECT tc.src_sls_typ_id,
                             tc.x_src_sls_typ_id,
                             tc.offst_lbl_id,
                             tc.sls_typ_lbl_id,
                             tc.x_sls_typ_lbl_id,
                             tc.src_sls_perd_id,
                             tc.trgt_sls_perd_id,
                             tc.src_offr_perd_id,
                             tc.trgt_offr_perd_id,
                             tc.r_factor,
                             upper(td.lbl_desc) AS lbl_desc
                        FROM (SELECT src_sls_typ_id,
                                     x_src_sls_typ_id,
                                     offst_lbl_id,
                                     sls_typ_lbl_id,
                                     x_sls_typ_lbl_id,
                                     pa_maps_public.perd_plus(p_mrkt_id   => p_mrkt_id,
                                                              p_perd1     => p_sls_perd_id,
                                                              p_perd_diff => offst_val_src_sls) src_sls_perd_id,
                                     pa_maps_public.perd_plus(p_mrkt_id   => p_mrkt_id,
                                                              p_perd1     => p_sls_perd_id,
                                                              p_perd_diff => offst_val_trgt_sls) trgt_sls_perd_id,
                                     pa_maps_public.perd_plus(p_mrkt_id   => p_mrkt_id,
                                                              p_perd1     => p_sls_perd_id,
                                                              p_perd_diff => offst_val_src_offr) src_offr_perd_id,
                                     pa_maps_public.perd_plus(p_mrkt_id   => p_mrkt_id,
                                                              p_perd1     => p_sls_perd_id,
                                                              p_perd_diff => offst_val_trgt_offr) trgt_offr_perd_id,
                                     r_factor
                                FROM (SELECT src_sls_typ_id,
                                             est_src_sls_typ_id AS x_src_sls_typ_id,
                                             offst_lbl_id,
                                             sls_typ_lbl_id,
                                             x_sls_typ_lbl_id,
                                             offst_val_src_sls,
                                             offst_val_trgt_sls,
                                             offst_val_src_offr,
                                             offst_val_trgt_offr,
                                             r_factor,
                                             eff_sls_perd_id,
                                             lead(eff_sls_perd_id,
                                                  1) over(PARTITION BY offst_lbl_id, sls_typ_grp_nm ORDER BY eff_sls_perd_id) AS next_eff_sls_perd_id
                                        FROM ta_config
                                       WHERE mrkt_id = p_mrkt_id
                                         AND trgt_sls_typ_id =
                                             p_sls_typ_id
                                         AND upper(REPLACE(TRIM(sls_typ_grp_nm),
                                                           '  ',
                                                           ' ')) IN
                                             (SELECT TRIM(regexp_substr(col,
                                                                        '[^,]+',
                                                                        1,
                                                                        LEVEL)) RESULT
                                                FROM (SELECT c_sls_typ_grp_nm_fc_dbt col
                                                        FROM dual)
                                              CONNECT BY LEVEL <=
                                                         length(regexp_replace(col,
                                                                               '[^,]+')) + 1)
                                         AND eff_sls_perd_id <=
                                             p_sls_perd_id)
                               WHERE p_sls_perd_id BETWEEN
                                     eff_sls_perd_id AND
                                     nvl(next_eff_sls_perd_id,
                                         p_sls_perd_id)) tc,
                             ta_dict td
                       WHERE td.lbl_id = tc.offst_lbl_id) tc_fc_dbt,
                     (SELECT mrkt_id, sku_id, fsc_cd
                        FROM (SELECT mrkt_id,
                                     sku_id,
                                     to_number(mstr_fsc_cd) fsc_cd,
                                     strt_perd_id from_strt_perd_id,
                                     nvl(lead(strt_perd_id, 1)
                                         over(PARTITION BY
                                              mrkt_id,
                                              sku_id ORDER BY
                                              strt_perd_id),
                                         99999999) to_strt_perd_id
                                FROM mstr_fsc_asgnmt
                               WHERE p_mrkt_id = mrkt_id
                                 AND l_trg_perd_id >=
                                     strt_perd_id)
                       WHERE l_trg_perd_id >= from_strt_perd_id
                         AND l_trg_perd_id < to_strt_perd_id) mrkt_tmp_fsc_master,
                     (SELECT mrkt_id,
                             sku_id,
                             MAX(fsc_cd) fsc_cd
                        FROM (SELECT mrkt_id,
                                     sku_id,
                                     to_number(fsc_cd) fsc_cd,
                                     strt_perd_id from_strt_perd_id,
                                     nvl(lead(strt_perd_id, 1)
                                         over(PARTITION BY
                                              mrkt_id,
                                              fsc_cd ORDER BY
                                              strt_perd_id),
                                         99999999) to_strt_perd_id
                                FROM mrkt_fsc
                               WHERE p_mrkt_id = mrkt_id
                                 AND l_trg_perd_id >=
                                     strt_perd_id
                                 AND 'N' = dltd_ind)
                       WHERE l_trg_perd_id >= from_strt_perd_id
                         AND l_trg_perd_id < to_strt_perd_id
                       GROUP BY mrkt_id, sku_id) mrkt_tmp_fsc
               WHERE dstrbtd_mrkt_sls.mrkt_id = p_mrkt_id
                 AND dstrbtd_mrkt_sls.sls_perd_id = tc_fc_dbt.trgt_sls_perd_id
                 AND dstrbtd_mrkt_sls.offr_perd_id = tc_fc_dbt.trgt_offr_perd_id
                 AND dstrbtd_mrkt_sls.sls_typ_id = tc_fc_dbt.x_src_sls_typ_id
                 AND dstrbtd_mrkt_sls.offr_sku_line_id = offr_sku_line.offr_sku_line_id
                 AND offr_sku_line.dltd_ind <> 'Y'
                 AND offr_sku_line.offr_prfl_prcpt_id = offr_prfl_prc_point.offr_prfl_prcpt_id
                 AND offr_prfl_prc_point.offr_id = offr.offr_id
                 AND offr.offr_typ = 'CMP'
                 AND offr.ver_id = 0
                 AND offr_sku_line.sku_id = sku.sku_id(+)
                 AND sku.prfl_cd = prfl.prfl_cd(+)
                 AND offr_sku_line.offr_sku_line_id = dbtosl.offr_sku_line_id(+)
                 AND dbtosl.offr_sku_line_id IS NULL
                    -- mrkt_tmp_fsc_master
                 AND p_mrkt_id = mrkt_tmp_fsc_master.mrkt_id(+)
                 AND offr_sku_line.sku_id = mrkt_tmp_fsc_master.sku_id(+)
                    -- mrkt_tmp_fsc
                 AND p_mrkt_id = mrkt_tmp_fsc.mrkt_id(+)
                 AND offr_sku_line.sku_id = mrkt_tmp_fsc.sku_id(+)) x,
             sct_fsc_ovrrd
       WHERE p_mrkt_id = sct_fsc_ovrrd.mrkt_id(+)
         AND l_trg_perd_id = sct_fsc_ovrrd.sls_perd_id(+)
         AND p_sls_typ_id = sct_fsc_ovrrd.sls_typ_id(+)
         AND x.offst_lbl_id = sct_fsc_ovrrd.offst_lbl_id(+)
         AND x.fsc_cd = to_number(sct_fsc_ovrrd.fsc_cd(+))
       GROUP BY x.offr_sku_line_id,
                x.rul_nm,
                x.offst_lbl_id,
                x.sls_typ_lbl_id,
                x.veh_id;
    EXCEPTION
      WHEN OTHERS THEN
        app_plsql_log.error(l_module_name || ' error code:' || SQLCODE ||
                            ' error message: ' || SQLERRM ||
                            l_parameter_list);
        RAISE e_get_reproc_est;
    END;
    --
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' end' || l_parameter_list);
    --
    RETURN l_table_reproc_est;
  END get_reproc_est;

  -- get_reproc_est2
  FUNCTION get_reproc_est2(p_mrkt_id     IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                           p_sls_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                           p_sls_typ_id  IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                           p_bilng_day   IN dly_bilng_trnd.prcsng_dt%TYPE,
                           p_user_id     IN VARCHAR2 DEFAULT NULL,
                           p_run_id      IN NUMBER DEFAULT NULL)
    RETURN t_reproc_est2 IS
    -- local variables
    l_table_reproc_est2 t_reproc_est2;
    l_trg_perd_id       dstrbtd_mrkt_sls.sls_perd_id%TYPE;
    -- for LOG
    l_run_id         NUMBER := nvl(p_run_id,
                                   app_plsql_output.generate_new_run_id);
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'GET_REPROC_EST2';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       to_char(p_mrkt_id) || ', ' ||
                                       'p_sls_perd_id: ' ||
                                       to_char(p_sls_perd_id) || ', ' ||
                                       'l_trg_perd_id: ' ||
                                       to_char(l_trg_perd_id) || ', ' ||
                                       'p_sls_typ_id: ' ||
                                       to_char(p_sls_typ_id) || ', ' ||
                                       'p_bilng_day: ' ||
                                       to_char(p_bilng_day, 'yyyymmdd') || ', ' ||
                                       'p_user_id: ' || l_user_id || ', ' ||
                                       'p_run_id: ' || to_char(l_run_id) || ')';
    -- exception
    e_get_reproc_est2 EXCEPTION;
    --
  BEGIN
    --
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start' || l_parameter_list);
    -- forecasted - EST2
    BEGIN
      SELECT offr_sku_line_id,
             rul_nm,
             offst_lbl_id,
             x_sls_typ_lbl_id,
             round(SUM(unit_qty)) units_bi24,
             round(SUM(unit_qty * sls_prc_amt /
                       decode(nr_for_qty, 0, 1, nr_for_qty) *
                       decode(net_to_avon_fct, 0, 1, net_to_avon_fct))) sales_bi24,
             sls_typ_lbl_id,
             round(SUM(nvl(unit_qty * nvl(r_factor, tc_fc_dms_r_factor), 0))) units_forecasted,
             round(SUM(nvl(unit_qty * nvl(r_factor, tc_fc_dms_r_factor), 0) * sls_prc_amt /
                       decode(nr_for_qty, 0, 1, nr_for_qty) *
                       decode(net_to_avon_fct, 0, 1, net_to_avon_fct))) sales_forecasted,
             veh_id,
             NULL comsn_amt,
             NULL tax_amt
        BULK COLLECT
        INTO l_table_reproc_est2
        FROM (SELECT dstrbtd_mrkt_sls.offr_sku_line_id,
                     tc_fc_dms.offst_lbl_id,
                     prfl.catgry_id,
                     offr_prfl_prc_point.sls_cls_cd,
                     offr.veh_id,
                     NULL AS perd_part,
                     offr_sku_line.sku_id,
                     tc_fc_dms.x_sls_typ_lbl_id,
                     nvl(dstrbtd_mrkt_sls.unit_qty, 0) AS unit_qty,
                     nvl(offr_prfl_prc_point.sls_prc_amt, 0) AS sls_prc_amt,
                     nvl(offr_prfl_prc_point.nr_for_qty, 0) AS nr_for_qty,
                     nvl(offr_prfl_prc_point.net_to_avon_fct, 0) AS net_to_avon_fct,
                     tc_fc_dms.sls_typ_lbl_id,
                     nvl(tc_fc_dms.r_factor, 1) AS tc_fc_dms_r_factor,
                     get_rule_nm(p_mrkt_id,
                                 p_sls_perd_id,
                                 p_sls_typ_id,
                                 tc_fc_dms.offst_lbl_id,
                                 prfl.catgry_id,
                                 offr_prfl_prc_point.sls_cls_cd,
                                 offr.veh_id,
                                 NULL,
                                 offr_sku_line.sku_id) rul_nm,
                     get_r_factor(p_mrkt_id,
                                  p_sls_perd_id,
                                  p_sls_typ_id,
                                  tc_fc_dms.offst_lbl_id,
                                  prfl.catgry_id,
                                  offr_prfl_prc_point.sls_cls_cd,
                                  offr.veh_id,
                                  NULL,
                                  offr_sku_line.sku_id) r_factor
                FROM dstrbtd_mrkt_sls,
                     offr_sku_line,
                     offr_prfl_prc_point,
                     offr,
                     sku,
                     prfl,
                     (SELECT src_sls_typ_id,
                             x_src_sls_typ_id,
                             offst_lbl_id,
                             sls_typ_lbl_id,
                             x_sls_typ_lbl_id,
                             pa_maps_public.perd_plus(p_mrkt_id,
                                                      p_sls_perd_id,
                                                      offst_val_src_sls) src_sls_perd_id,
                             pa_maps_public.perd_plus(p_mrkt_id,
                                                      p_sls_perd_id,
                                                      offst_val_trgt_sls) trgt_sls_perd_id,
                             pa_maps_public.perd_plus(p_mrkt_id,
                                                      p_sls_perd_id,
                                                      offst_val_src_offr) src_offr_perd_id,
                             pa_maps_public.perd_plus(p_mrkt_id,
                                                      p_sls_perd_id,
                                                      offst_val_trgt_offr) trgt_offr_perd_id,
                             r_factor
                        FROM (SELECT src_sls_typ_id,
                                     est_src_sls_typ_id AS x_src_sls_typ_id,
                                     offst_lbl_id,
                                     sls_typ_lbl_id,
                                     x_sls_typ_lbl_id,
                                     offst_val_src_sls,
                                     offst_val_trgt_sls,
                                     offst_val_src_offr,
                                     offst_val_trgt_offr,
                                     r_factor,
                                     eff_sls_perd_id,
                                     lead(eff_sls_perd_id, 1) over(PARTITION BY offst_lbl_id, sls_typ_grp_nm ORDER BY eff_sls_perd_id) AS next_eff_sls_perd_id
                                FROM ta_config
                               WHERE mrkt_id = p_mrkt_id
                                 AND trgt_sls_typ_id =
                                     p_sls_typ_id
                                 AND upper(REPLACE(TRIM(sls_typ_grp_nm),
                                                   '  ',
                                                   ' ')) IN
                                     (SELECT TRIM(regexp_substr(col,
                                                                '[^,]+',
                                                                1,
                                                                LEVEL)) RESULT
                                        FROM (SELECT c_sls_typ_grp_nm_fc_dms col
                                                FROM dual)
                                      CONNECT BY LEVEL <=
                                                 length(regexp_replace(col,
                                                                       '[^,]+')) + 1)
                                 AND eff_sls_perd_id <=
                                     p_sls_perd_id)
                       WHERE p_sls_perd_id BETWEEN
                             eff_sls_perd_id AND
                             nvl(next_eff_sls_perd_id,
                                 p_sls_perd_id)) tc_fc_dms
               WHERE dstrbtd_mrkt_sls.mrkt_id = p_mrkt_id
                 AND dstrbtd_mrkt_sls.sls_perd_id =
                     tc_fc_dms.src_sls_perd_id
                 AND dstrbtd_mrkt_sls.offr_perd_id =
                     tc_fc_dms.src_offr_perd_id
                 AND dstrbtd_mrkt_sls.sls_typ_id =
                     tc_fc_dms.src_sls_typ_id
                 AND dstrbtd_mrkt_sls.offr_sku_line_id =
                     offr_sku_line.offr_sku_line_id
                 AND offr_sku_line.dltd_ind <> 'Y'
                 AND offr_sku_line.offr_prfl_prcpt_id =
                     offr_prfl_prc_point.offr_prfl_prcpt_id
                 AND offr_prfl_prc_point.offr_id =
                     offr.offr_id
                 AND offr.offr_typ = 'CMP'
                 AND offr.ver_id = 0
                 AND offr_sku_line.sku_id = sku.sku_id(+)
                 AND sku.prfl_cd = prfl.prfl_cd(+))
       GROUP BY offr_sku_line_id,
                rul_nm,
                offst_lbl_id,
                x_sls_typ_lbl_id,
                sls_typ_lbl_id,
                veh_id;
    EXCEPTION
      WHEN OTHERS THEN
        app_plsql_log.error(l_module_name || ' error code:' || SQLCODE ||
                            ' error message: ' || SQLERRM ||
                            l_parameter_list);
        RAISE e_get_reproc_est2;
    END;
    --
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' end' || l_parameter_list);
    --
    RETURN l_table_reproc_est2;
  END get_reproc_est2;

  -- get_trend_alloc_re_proc_rule
  FUNCTION get_trend_alloc_re_proc_rule(p_mrkt_id              IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                                        p_campgn_perd_id       IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                        p_sls_typ_id           IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                                        p_bilng_day            IN dly_bilng_trnd.prcsng_dt%TYPE,
                                        p_cash_value           IN NUMBER DEFAULT NULL,
                                        p_r_factor             IN NUMBER DEFAULT NULL,
                                        p_use_offers_on_sched  IN CHAR DEFAULT 'N',
                                        p_use_offers_off_sched IN CHAR DEFAULT 'N',
                                        p_user_id              IN VARCHAR2 DEFAULT NULL)
    RETURN obj_pa_trend_alloc_sgmnt_table
    PIPELINED AS
    PRAGMA AUTONOMOUS_TRANSACTION;
    -- local variables
    l_tbl_bi24         t_hist_detail := t_hist_detail();
    l_tbl_reproc_trnd  t_reproc := t_reproc();
    l_tbl_reproc_trnd2 t_reproc := t_reproc();
    l_tbl_reproc_est   t_reproc := t_reproc();
    l_tbl_reproc_est2  t_reproc_est2 := t_reproc_est2();
    l_tbl_reproc_rule  t_reproc_rule;
    l_offst_lbl_id_on  NUMBER;
    l_offst_lbl_id_off NUMBER;
    c_key              VARCHAR2(512);
    -- for LOG
    l_run_id         NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'GET_TREND_ALLOC_RE_PROC_RULE';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       to_char(p_mrkt_id) || ', ' ||
                                       'p_campgn_perd_id: ' ||
                                       to_char(p_campgn_perd_id) || ', ' ||
                                       'p_sls_typ_id: ' ||
                                       to_char(p_sls_typ_id) || ', ' ||
                                       'p_bilng_day: ' ||
                                       to_char(p_bilng_day, 'yyyymmdd') || ', ' ||
                                       'p_cash_value: ' ||
                                       to_char(p_cash_value) || ', ' ||
                                       'p_r_factor: ' ||
                                       to_char(p_r_factor) || ', ' ||
                                       'p_use_offers_on_sched: ' ||
                                       p_use_offers_on_sched || ', ' ||
                                       'p_use_offers_off_sched: ' ||
                                       p_use_offers_off_sched || ', ' ||
                                       'p_user_id: ' || l_user_id || ', ' ||
                                       'p_run_id: ' || to_char(l_run_id) || ')';
    --
  BEGIN
    --
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start' || l_parameter_list);
    -- BI24
    l_tbl_bi24 := get_bi24(p_mrkt_id              => p_mrkt_id,
                           p_sls_perd_id          => p_campgn_perd_id,
                           p_sls_typ_id           => p_sls_typ_id,
                           p_bilng_day            => p_bilng_day,
                           p_offst_lbl_id         => NULL,
                           p_cash_value           => NULL,
                           p_r_factor             => NULL,
                           p_x_sls_typ_lbl_id_flg => 'Y',
                           p_perd_part_flg        => 'Y',
                           p_user_id              => l_user_id,
                           p_run_id               => l_run_id);
    BEGIN
      SELECT offst_lbl_id_on, offst_lbl_id_off
        INTO l_offst_lbl_id_on, l_offst_lbl_id_off
        FROM (SELECT tc_bi24.offst_lbl_id AS offst_lbl_id_on,
                     lead(tc_bi24.offst_lbl_id) over(ORDER BY upper(td.lbl_desc) DESC) AS offst_lbl_id_off,
                     row_number() over(ORDER BY upper(td.lbl_desc) DESC) AS rn
                FROM (SELECT src_sls_typ_id,
                             x_src_sls_typ_id,
                             offst_lbl_id,
                             sls_typ_lbl_id,
                             x_sls_typ_lbl_id r_factor
                        FROM (SELECT src_sls_typ_id,
                                     est_src_sls_typ_id AS x_src_sls_typ_id,
                                     offst_lbl_id,
                                     sls_typ_lbl_id,
                                     x_sls_typ_lbl_id,
                                     offst_val_src_sls,
                                     offst_val_trgt_sls,
                                     offst_val_src_offr,
                                     offst_val_trgt_offr,
                                     r_factor,
                                     eff_sls_perd_id,
                                     lead(eff_sls_perd_id, 1) over(PARTITION BY offst_lbl_id, sls_typ_grp_nm ORDER BY eff_sls_perd_id) AS next_eff_sls_perd_id
                                FROM ta_config
                               WHERE mrkt_id = p_mrkt_id
                                 AND trgt_sls_typ_id = p_sls_typ_id
                                 AND upper(REPLACE(TRIM(sls_typ_grp_nm),
                                                   '  ',
                                                   ' ')) =
                                     c_sls_typ_grp_nm_bi24
                                 AND eff_sls_perd_id <= p_campgn_perd_id)
                       WHERE p_campgn_perd_id BETWEEN eff_sls_perd_id AND
                             nvl(next_eff_sls_perd_id, p_campgn_perd_id)) tc_bi24,
                     ta_dict td
               WHERE td.lbl_id = tc_bi24.offst_lbl_id
                 AND upper(td.int_lbl_desc) IN ('ON-SCHEDULE', 'OFF-SCHEDULE'))
       WHERE rn = 1;
    EXCEPTION
      WHEN OTHERS THEN
        l_offst_lbl_id_on  := NULL;
        l_offst_lbl_id_off := NULL;

    END;
    --
    IF l_tbl_bi24.count > 0 THEN
      -- collect BI24 data by RULES
      FOR i IN (SELECT rul_nm,
                       sls_perd_id,
                       bilng_day,
                       isselected,
                       SUM(units) units,
                       SUM(sales) sales
                  FROM (SELECT r.rul_nm,
                               p_campgn_perd_id sls_perd_id,
                               p_bilng_day bilng_day,
                               CASE
                                 WHEN instr(r.period_list, p_campgn_perd_id) > 0 THEN
                                  'Y'
                                 ELSE
                                  'N'
                               END isselected,
                               d.units,
                               d.sales,
                               row_number() over(PARTITION BY nvl(d.offst_lbl_id, -1), nvl(d.catgry_id, -1), nvl(d.sls_cls_cd, '-1'), nvl(d.veh_id, -1), nvl(d.perd_part, -1), nvl(d.sku_id, -1) ORDER BY r.prirty) primary_rule
                          FROM ( -- BI24 only
                                SELECT dtls.offst_lbl_id,
                                        prfl.catgry_id,
                                        dtls.sls_cls_cd,
                                        dtls.veh_id,
                                        dtls.perd_part,
                                        dtls.sku_id,
                                        SUM(dtls.units) AS units,
                                        SUM(dtls.sales) AS sales
                                  FROM trend_alloc_hist_dtls dtls,
                                        ta_dict               td,
                                        sku,
                                        prfl
                                 WHERE dtls.mrkt_id = p_mrkt_id
                                   AND dtls.sls_perd_id = p_campgn_perd_id
                                   AND dtls.sls_typ_id = p_sls_typ_id
                                   AND dtls.sls_typ_grp_nm =
                                       c_sls_typ_grp_nm_bi24
                                   AND dtls.bilng_day = p_bilng_day
                                   AND dtls.sls_typ_lbl_id = td.lbl_id
                                   AND td.int_lbl_desc = 'BI24'
                                   AND dtls.sku_id = sku.sku_id(+)
                                   AND sku.prfl_cd = prfl.prfl_cd(+)
                                 GROUP BY dtls.offst_lbl_id,
                                           prfl.catgry_id,
                                           dtls.sls_cls_cd,
                                           dtls.veh_id,
                                           dtls.perd_part,
                                           dtls.sku_id) d,
                               (SELECT rm.rul_nm,
                                       CAST(NULL AS NUMBER) offst_lbl_id,
                                       CAST(NULL AS NUMBER) catgry_id,
                                       CAST(NULL AS NUMBER) sls_cls_cd,
                                       CAST(NULL AS NUMBER) veh_id,
                                       CAST(NULL AS NUMBER) perd_part,
                                       listagg(rs.sku_id, ',') within GROUP(ORDER BY rs.sku_id) sku_list,
                                       0 prirty,
                                       r.period_list,
                                       r.r_factor,
                                       r.r_factor_manual
                                  FROM custm_rul_mstr     rm,
                                       custm_rul_perd     r,
                                       custm_rul_sku_list rs
                                 WHERE rm.mrkt_id = p_mrkt_id
                                   AND r.campgn_perd_id = p_campgn_perd_id
                                   AND rm.rul_id = rs.rul_id
                                   AND rm.rul_id = r.rul_id
                                 GROUP BY rm.rul_nm,
                                          r.period_list,
                                          r.r_factor,
                                          r.r_factor_manual
                                UNION
                                SELECT ms.rul_nm,
                                       ms.offst_lbl_id,
                                       ms.catgry_id,
                                       ms.sls_cls_cd,
                                       ms.veh_id,
                                       ms.perd_part,
                                       CAST(NULL AS VARCHAR(2048)) sku_list,
                                       s.prirty,
                                       s.period_list,
                                       s.r_factor,
                                       s.r_factor_manual
                                  FROM custm_seg_mstr ms, custm_seg_perd s
                                 WHERE ms.mrkt_id = p_mrkt_id
                                   AND ms.rul_id = s.rul_id
                                   AND p_campgn_perd_id = s.campgn_perd_id
                                   AND p_sls_typ_id = s.sls_typ_id) r
                         WHERE nvl(d.offst_lbl_id, -1) =
                               nvl(r.offst_lbl_id(+), nvl(d.offst_lbl_id, -1))
                           AND nvl(d.catgry_id, -1) =
                               nvl(r.catgry_id(+), nvl(d.catgry_id, -1))
                           AND nvl(d.sls_cls_cd, '-1') =
                               nvl(r.sls_cls_cd(+), nvl(d.sls_cls_cd, '-1'))
                           AND nvl(d.veh_id, -1) =
                               nvl(r.veh_id(+), nvl(d.veh_id, -1))
                           AND nvl(d.perd_part, -1) =
                               nvl(r.perd_part(+), nvl(d.perd_part, -1))
                           AND instr(nvl(r.sku_list(+), nvl(d.sku_id, -1)),
                                     nvl(d.sku_id, -1)) > 0)
                 WHERE primary_rule = 1
                 GROUP BY rul_nm, sls_perd_id, bilng_day, isselected) LOOP
        c_key := i.rul_nm;
        l_tbl_reproc_rule(c_key).rul_nm := i.rul_nm;
        l_tbl_reproc_rule(c_key).units_bi24 := nvl(l_tbl_reproc_rule(c_key).units_bi24, 0) + i.units;
        l_tbl_reproc_rule(c_key).sales_bi24 := nvl(l_tbl_reproc_rule(c_key).sales_bi24, 0) + i.sales;
      END LOOP;
    END IF;
    ----------------
    -- FORECASTED --
    ----------------
    -- TRND
    l_tbl_reproc_trnd := get_reproc_trnd(p_mrkt_id     => p_mrkt_id,
                                         p_sls_perd_id => p_campgn_perd_id,
                                         p_sls_typ_id  => p_sls_typ_id,
                                         p_bilng_day   => p_bilng_day,
                                         p_user_id     => l_user_id,
                                         p_run_id      => l_run_id);
    -- SUM (TRND)
    IF l_tbl_reproc_trnd.count > 0 THEN
      FOR i IN l_tbl_reproc_trnd.first .. l_tbl_reproc_trnd.last LOOP
        c_key := l_tbl_reproc_trnd(i).rul_nm;
        l_tbl_reproc_rule(c_key).rul_nm := l_tbl_reproc_trnd(i).rul_nm;
        l_tbl_reproc_rule(c_key).units_actual := nvl(l_tbl_reproc_rule(c_key).units_actual, 0) + l_tbl_reproc_trnd(i).units;
        l_tbl_reproc_rule(c_key).sales_actual := nvl(l_tbl_reproc_rule(c_key).sales_actual, 0) + l_tbl_reproc_trnd(i).sales;
      END LOOP;
      l_tbl_reproc_trnd.delete;
    END IF;
    -- TRND2
    l_tbl_reproc_trnd2 := get_reproc_trnd2(p_mrkt_id              => p_mrkt_id,
                                           p_sls_perd_id          => p_campgn_perd_id,
                                           p_sls_typ_id           => p_sls_typ_id,
                                           p_bilng_day            => p_bilng_day,
                                           p_user_id              => l_user_id,
                                           p_run_id               => l_run_id);
    -- SUM (TRND2)
    IF l_tbl_reproc_trnd2.count > 0 THEN
      FOR i IN l_tbl_reproc_trnd2.first .. l_tbl_reproc_trnd2.last LOOP
        c_key := l_tbl_reproc_trnd2(i).rul_nm;
        l_tbl_reproc_rule(c_key).rul_nm := l_tbl_reproc_trnd2(i).rul_nm;
        l_tbl_reproc_rule(c_key).units_actual := nvl(l_tbl_reproc_rule(c_key).units_actual, 0) + l_tbl_reproc_trnd2(i).units;
        l_tbl_reproc_rule(c_key).sales_actual := nvl(l_tbl_reproc_rule(c_key).sales_actual, 0) + l_tbl_reproc_trnd2(i).sales;
      END LOOP;
      l_tbl_reproc_trnd2.delete;
    END IF;
    -- EST
    l_tbl_reproc_est := get_reproc_est(p_mrkt_id              => p_mrkt_id,
                                       p_sls_perd_id          => p_campgn_perd_id,
                                       p_sls_typ_id           => p_sls_typ_id,
                                       p_bilng_day            => p_bilng_day,
                                       p_use_offers_on_sched  => p_use_offers_on_sched,
                                       p_use_offers_off_sched => p_use_offers_off_sched,
                                       p_user_id              => l_user_id,
                                       p_run_id               => l_run_id);
    -- SUM (EST)
    IF l_tbl_reproc_est.count > 0 THEN
      FOR i IN l_tbl_reproc_est.first .. l_tbl_reproc_est.last LOOP
        c_key := l_tbl_reproc_est(i).rul_nm;
        l_tbl_reproc_rule(c_key).rul_nm := l_tbl_reproc_est(i).rul_nm;
        l_tbl_reproc_rule(c_key).units_actual := nvl(l_tbl_reproc_rule(c_key).units_actual, 0) + l_tbl_reproc_est(i).units;
        l_tbl_reproc_rule(c_key).sales_actual := nvl(l_tbl_reproc_rule(c_key).sales_actual, 0) + l_tbl_reproc_est(i).sales;
      END LOOP;
      l_tbl_reproc_est.delete;
    END IF;
    -- EST2
    l_tbl_reproc_est2 := get_reproc_est2(p_mrkt_id     => p_mrkt_id,
                                         p_sls_perd_id => p_campgn_perd_id,
                                         p_sls_typ_id  => p_sls_typ_id,
                                         p_bilng_day   => p_bilng_day,
                                         p_user_id     => l_user_id,
                                         p_run_id      => l_run_id);
    -- SUM (EST2)
    IF l_tbl_reproc_est2.count > 0 THEN
      FOR i IN l_tbl_reproc_est2.first .. l_tbl_reproc_est2.last LOOP
        c_key := l_tbl_reproc_est2(i).rul_nm;
        l_tbl_reproc_rule(c_key).rul_nm := l_tbl_reproc_est2(i).rul_nm;
        -- BI24
        l_tbl_reproc_rule(c_key).units_bi24 := nvl(l_tbl_reproc_rule(c_key).units_bi24, 0) + l_tbl_reproc_est2(i).units_bi24;
        l_tbl_reproc_rule(c_key).sales_bi24 := nvl(l_tbl_reproc_rule(c_key).sales_bi24, 0) + l_tbl_reproc_est2(i).sales_bi24;
        -- FORECASTED
        l_tbl_reproc_rule(c_key).units_actual := nvl(l_tbl_reproc_rule(c_key).units_actual, 0) + l_tbl_reproc_est2(i).units_forecasted;
        l_tbl_reproc_rule(c_key).sales_actual := nvl(l_tbl_reproc_rule(c_key).sales_actual, 0) + l_tbl_reproc_est2(i).sales_forecasted;
      END LOOP;
      l_tbl_reproc_est2.delete;
    END IF;
    -- PIPE (HEAD)
    IF l_tbl_reproc_rule.count > 0 THEN
      c_key := l_tbl_reproc_rule.first;
      WHILE c_key IS NOT NULL LOOP
        PIPE ROW(obj_pa_trend_alloc_sgmnt_line(l_tbl_reproc_rule(c_key).rul_nm,
                                               p_campgn_perd_id,
                                               p_bilng_day,
                                               NULL,
                                               round(l_tbl_reproc_rule(c_key).units_bi24),
                                               round(l_tbl_reproc_rule(c_key).sales_bi24),
                                               round(l_tbl_reproc_rule(c_key).units_actual),
                                               round(l_tbl_reproc_rule(c_key).sales_actual)));
        c_key := l_tbl_reproc_rule.next(c_key);
      END LOOP;
      l_tbl_reproc_rule.delete;
    END IF;
    --
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' end' || l_parameter_list);
  END get_trend_alloc_re_proc_rule;

  -- get_trend_alloc_hist_dtls
  FUNCTION get_trend_alloc_hist_dtls(p_mrkt_id      IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                                     p_sls_perd_id  IN dly_bilng_trnd.trnd_sls_perd_id%TYPE,
                                     p_trg_perd_id  IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                     p_sls_typ_id   IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                                     p_bilng_day    IN dly_bilng.prcsng_dt%TYPE,
                                     p_offst_lbl_id IN ta_dict.lbl_id%TYPE DEFAULT NULL,
                                     p_user_id      IN VARCHAR2 DEFAULT NULL)
    RETURN obj_pa_trend_alloc_hist_dt_tbl
    PIPELINED IS
    -- local variables
    l_tbl_hist_detail t_hist_detail := t_hist_detail();
    l_tbl_hist_dtl2   t_hist_dtl2;
    l_catgry_id       prfl.catgry_id%TYPE;
    l_brnd_id         prfl.brnd_id%TYPE;
    l_sgmt_id         prfl.sgmt_id%TYPE;
    l_form_id         prfl.form_id%TYPE;
    l_prfl_cd         sku.prfl_cd%TYPE;
    l_fsc_cd          mrkt_fsc.fsc_cd%TYPE;
    c_key             VARCHAR2(256);
    -- for LOG
    l_run_id         NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'GET_TREND_ALLOC_HIST_DTLS';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       to_char(p_mrkt_id) || ', ' ||
                                       'p_sls_perd_id: ' ||
                                       to_char(p_sls_perd_id) || ', ' ||
                                       'p_trg_perd_id: ' ||
                                       to_char(p_trg_perd_id) || ', ' ||
                                       'p_sls_typ_id: ' ||
                                       to_char(p_sls_typ_id) || ', ' ||
                                       'p_bilng_day: ' ||
                                       to_char(p_bilng_day, 'yyyy-mm-dd') || ', ' ||
                                       'p_offst_lbl_id: ' ||
                                       to_char(p_offst_lbl_id) || ', ' ||
                                       'p_user_id: ' || l_user_id || ', ' ||
                                       'p_run_id: ' || to_char(l_run_id) || ')';
    --
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start' || l_parameter_list);
    -- get_head_details
    l_tbl_hist_detail := get_head_details(p_mrkt_id       => p_mrkt_id,
                                          p_sls_perd_id   => p_sls_perd_id,
                                          p_sls_typ_id    => p_sls_typ_id,
                                          p_bilng_day     => p_bilng_day,
                                          p_offst_lbl_id  => NULL,
                                          p_cash_value    => NULL,
                                          p_r_factor      => NULL,
                                          p_perd_part_flg => 'N',
                                          p_user_id       => l_user_id,
                                          p_run_id        => l_run_id);
    --
    IF l_tbl_hist_detail.count > 0 THEN
      FOR i IN l_tbl_hist_detail.first .. l_tbl_hist_detail.last LOOP
        IF l_tbl_hist_detail(i).offst_lbl_id = nvl(p_offst_lbl_id, l_tbl_hist_detail(i).offst_lbl_id) THEN
          -- GET MISSING ID'S and sum by sku_id
          BEGIN
            SELECT prfl.catgry_id,
                   prfl.brnd_id,
                   prfl.sgmt_id,
                   prfl.form_id,
                   sku.prfl_cd,
                   pa_maps_public.get_mstr_fsc_cd(p_mrkt_id,
                                                  sku.sku_id,
                                                  p_sls_perd_id)
              INTO l_catgry_id,
                   l_brnd_id,
                   l_sgmt_id,
                   l_form_id,
                   l_prfl_cd,
                   l_fsc_cd
              FROM sku, prfl
             WHERE l_tbl_hist_detail(i).sku_id = sku.sku_id
                AND sku.prfl_cd = prfl.prfl_cd;
          EXCEPTION
            WHEN OTHERS THEN
              l_catgry_id := NULL;
              l_brnd_id   := NULL;
              l_sgmt_id   := NULL;
              l_form_id   := NULL;
              l_prfl_cd   := NULL;
              l_fsc_cd    := NULL;
          END;
          c_key := to_char(l_catgry_id) || '_' || to_char(l_brnd_id) || '_' ||
                   to_char(l_sgmt_id) || '_' || to_char(l_form_id) || '_' ||
                   to_char(l_tbl_hist_detail(i).sku_id) || '_' ||
                   to_char(l_tbl_hist_detail(i).offst_lbl_id) || '_' ||
                   to_char(l_tbl_hist_detail(i).sls_typ_lbl_id);
          l_tbl_hist_dtl2(c_key).dtl_id := i;
          l_tbl_hist_dtl2(c_key).catgry_id := l_catgry_id;
          l_tbl_hist_dtl2(c_key).brnd_id := l_brnd_id;
          l_tbl_hist_dtl2(c_key).sgmt_id := l_sgmt_id;
          l_tbl_hist_dtl2(c_key).form_id := l_form_id;
          l_tbl_hist_dtl2(c_key).prfl_cd := l_prfl_cd;
          l_tbl_hist_dtl2(c_key).fsc_cd := l_fsc_cd;
          l_tbl_hist_dtl2(c_key).promtn_id := l_tbl_hist_detail(i).promtn_id;
          l_tbl_hist_dtl2(c_key).promtn_clm_id := l_tbl_hist_detail(i)
                                                  .promtn_clm_id;
          l_tbl_hist_dtl2(c_key).sls_cls_cd := l_tbl_hist_detail(i)
                                               .sls_cls_cd;
          l_tbl_hist_dtl2(c_key).veh_id := l_tbl_hist_detail(i).veh_id;
          l_tbl_hist_dtl2(c_key).sku_id := l_tbl_hist_detail(i).sku_id;
          l_tbl_hist_dtl2(c_key).offr_id := l_tbl_hist_detail(i).offr_id;
          l_tbl_hist_dtl2(c_key).offst_lbl_id := l_tbl_hist_detail(i)
                                                 .offst_lbl_id;
          l_tbl_hist_dtl2(c_key).cash_value := l_tbl_hist_detail(i)
                                               .cash_value;
          l_tbl_hist_dtl2(c_key).r_factor := l_tbl_hist_detail(i).r_factor;
          l_tbl_hist_dtl2(c_key).units := nvl(l_tbl_hist_dtl2(c_key).units,
                                              0) + l_tbl_hist_detail(i)
                                         .units;
          l_tbl_hist_dtl2(c_key).sales := nvl(l_tbl_hist_dtl2(c_key).sales,
                                              0) + l_tbl_hist_detail(i)
                                         .sales;
          l_tbl_hist_dtl2(c_key).sls_typ_lbl_id := l_tbl_hist_detail(i)
                                                   .sls_typ_lbl_id;
        END IF;
      END LOOP;
      IF l_tbl_hist_dtl2.count > 0 THEN
        c_key := l_tbl_hist_dtl2.first;
        WHILE c_key IS NOT NULL LOOP
          PIPE ROW(obj_pa_trend_alloc_hist_dt_ln(l_tbl_hist_dtl2(c_key).sku_id,
                                                 l_tbl_hist_dtl2(c_key).catgry_id,
                                                 l_tbl_hist_dtl2(c_key).brnd_id,
                                                 l_tbl_hist_dtl2(c_key).sgmt_id,
                                                 l_tbl_hist_dtl2(c_key).form_id,
                                                 l_tbl_hist_dtl2(c_key).prfl_cd,
                                                 l_tbl_hist_dtl2(c_key).promtn_id,
                                                 l_tbl_hist_dtl2(c_key).promtn_clm_id,
                                                 l_tbl_hist_dtl2(c_key).sls_cls_cd,
                                                 l_tbl_hist_dtl2(c_key).veh_id,
                                                 l_tbl_hist_dtl2(c_key).fsc_cd,
                                                 l_tbl_hist_dtl2(c_key).offr_id,
                                                 l_tbl_hist_dtl2(c_key).offst_lbl_id,
                                                 l_tbl_hist_dtl2(c_key).cash_value,
                                                 l_tbl_hist_dtl2(c_key).r_factor,
                                                 l_tbl_hist_dtl2(c_key).sls_typ_lbl_id,
                                                 l_tbl_hist_dtl2(c_key).units,
                                                 l_tbl_hist_dtl2(c_key).sales));
          c_key := l_tbl_hist_dtl2.next(c_key);
        END LOOP;
      END IF;
      l_tbl_hist_dtl2.delete;
      l_tbl_hist_detail.delete;
    END IF;
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' end' || l_parameter_list);
  END get_trend_alloc_hist_dtls;

  -- get_trend_alloc_resegment
  FUNCTION get_trend_alloc_resegment(p_mrkt_id        IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                                     p_campgn_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                     p_sls_typ_id     IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                                     p_bilng_day      IN dly_bilng_trnd.prcsng_dt%TYPE,
                                     p_perd_from      IN NUMBER,
                                     p_perd_to        IN NUMBER,
                                     p_rules          IN obj_pa_trend_alloc_rules_table,
                                     p_user_id        IN VARCHAR2 DEFAULT NULL)
    RETURN obj_pa_trend_alloc_sgmnt_table
    PIPELINED IS
    -- local variables
    l_periods   r_periods;
    --
    l_multplyr  NUMBER;
    l_perd_list mrkt_config_item.mrkt_config_item_val_txt%TYPE;
    -- for LOG
    l_run_id         NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'GET_TREND_ALLOC_RESEGMENT';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       to_char(p_mrkt_id) || ', ' ||
                                       'p_campgn_perd_id: ' ||
                                       to_char(p_campgn_perd_id) || ', ' ||
                                       'p_sls_typ_id: ' ||
                                       to_char(p_sls_typ_id) || ', ' ||
                                       'p_bilng_day: ' ||
                                       to_char(p_bilng_day, 'yyyy-mm-dd') || ', ' ||
                                       'p_perd_from: ' ||
                                       to_char(p_perd_from) || ', ' ||
                                       'p_perd_to: ' || to_char(p_perd_to) || ', ' ||
                                       'p_user_id: ' || l_user_id || ', ' ||
                                       'p_run_id: ' || to_char(l_run_id) || ')';
    --
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start' || l_parameter_list);
    --
    l_multplyr := to_number(substr(to_char(p_campgn_perd_id), 1, 4)) - p_perd_to;
    BEGIN
      SELECT mrkt_config_item_val_txt
        INTO l_perd_list
        FROM mrkt_config_item
       WHERE mrkt_id = p_mrkt_id
         AND mrkt_config_item_desc_txt = 'PA_TREND_ALLOCATION period list';
    EXCEPTION
      WHEN OTHERS THEN
        l_perd_list := '0';
    END;
    -- iterations for (FROM periods to TO periods) + periods of p_period_list
    FOR i_prd IN (SELECT pa_maps_public.perd_plus(p_mrkt_id,
                                                  p_campgn_perd_id - (10000 * dummy),
                                                  period) perd_id
                    FROM (SELECT LEVEL dummy
                            FROM dual
                          CONNECT BY LEVEL <= l_multplyr),
                         (SELECT DISTINCT TRIM(regexp_substr(col, '[^,]+', 1, LEVEL)) *
                                          CASE
                                            WHEN l_multplyr = 0 THEN 0
                                            ELSE 1
                                          END AS period
                              FROM (SELECT l_perd_list col
                                      FROM dual)
                            CONNECT BY LEVEL <=
                                       length(regexp_replace(col, '[^,]+')) + 1)
                  UNION
                  SELECT to_number(TRIM(regexp_substr(col, '[^,]+', 1, LEVEL))) perd_id
                    FROM (SELECT period_list col FROM TABLE(p_rules))
                   WHERE col IS NOT NULL
                  CONNECT BY LEVEL <=
                             length(regexp_replace(col, '[^,]+')) + 1
                   ORDER BY perd_id) LOOP
      -- get CURRENT periods
      l_periods := get_periods(p_mrkt_id       => p_mrkt_id,
                               p_orig_perd_id  => p_campgn_perd_id,
                               p_bilng_perd_id => i_prd.perd_id,
                               p_sls_typ_id    => p_sls_typ_id,
                               p_bilng_day     => p_bilng_day,
                               p_user_id       => l_user_id,
                               p_run_id        => l_run_id);
      IF (l_periods.bilng_day IS NOT NULL) THEN

        -- SUM (units, sales by RULE / SEGMENT)
        FOR c_seg IN (SELECT rul_nm,
                             sls_perd_id,
                             bilng_day,
                             isselected,
                             SUM(units_bi24) units_bi24,
                             SUM(sales_bi24) sales_bi24,
                             round(SUM(units_actual), 0) units_actual,
                             round(SUM(sales_actual), 0) sales_actual
                        FROM (SELECT r.rul_nm,
                                     i_prd.perd_id sls_perd_id,
                                     l_periods.bilng_day bilng_day,
                                     CASE
                                       WHEN instr(r.period_list, i_prd.perd_id) > 0 THEN
                                        'Y'
                                       ELSE
                                        'N'
                                     END isselected,
                                     d.units_bi24,
                                     d.sales_bi24,
                                     d.units_actual,
                                     d.sales_actual,
                                     row_number() over(PARTITION BY nvl(d.offst_lbl_id, -1), nvl(d.catgry_id, -1), nvl(d.sls_cls_cd, '-1'), nvl(d.veh_id, -1), nvl(d.perd_part, -1), nvl(d.sku_id, -1) ORDER BY r.prirty) primary_rule
                                FROM ( -- BI24 match ACTUAL
                                      SELECT *
                                        FROM (SELECT t_bi24.offst_lbl_id,
                                                      t_bi24.catgry_id,
                                                      t_bi24.sls_cls_cd,
                                                      t_bi24.veh_id,
                                                      t_bi24.perd_part,
                                                      t_bi24.sku_id,
                                                      t_bi24.units AS units_bi24,
                                                      t_bi24.sales AS sales_bi24,
                                                      CASE
                                                        WHEN t_bi24.units_sum = 0 THEN
                                                         t_actual.units /
                                                         t_bi24.units_cnt
                                                        ELSE
                                                         t_actual.units *
                                                         (t_bi24.units /
                                                         t_bi24.units_sum)
                                                      END AS units_actual,
                                                      CASE
                                                        WHEN t_bi24.sales_sum = 0 THEN
                                                         t_actual.sales /
                                                         t_bi24.units_cnt
                                                        ELSE
                                                         t_actual.sales *
                                                         (t_bi24.sales /
                                                         t_bi24.sales_sum)
                                                      END AS sales_actual
                                                 FROM (SELECT offst_lbl_id,
                                                              catgry_id,
                                                              sls_cls_cd,
                                                              veh_id,
                                                              perd_part,
                                                              sku_id,
                                                              units,
                                                              sales,
                                                              SUM(units) over(PARTITION BY offst_lbl_id, catgry_id, sls_cls_cd, veh_id, sku_id) AS units_sum,
                                                              SUM(sales) over(PARTITION BY offst_lbl_id, catgry_id, sls_cls_cd, veh_id, sku_id) AS sales_sum,
                                                              COUNT(1) over(PARTITION BY offst_lbl_id, catgry_id, sls_cls_cd, veh_id, sku_id) AS units_cnt
                                                         FROM (SELECT dtls.offst_lbl_id,
                                                                      prfl.catgry_id,
                                                                      dtls.sls_cls_cd,
                                                                      dtls.veh_id,
                                                                      dtls.perd_part,
                                                                      dtls.sku_id,
                                                                      SUM(dtls.units) AS units,
                                                                      SUM(dtls.sales) AS sales
                                                                 FROM trend_alloc_hist_dtls dtls,
                                                                      ta_dict               td,
                                                                      sku,
                                                                      prfl
                                                                WHERE dtls.mrkt_id =
                                                                      p_mrkt_id
                                                                  AND dtls.sls_perd_id =
                                                                      i_prd.perd_id
                                                                  AND dtls.sls_typ_id =
                                                                      p_sls_typ_id
                                                                  AND dtls.sls_typ_grp_nm =
                                                                      c_sls_typ_grp_nm_bi24
                                                                  AND dtls.bilng_day =
                                                                      l_periods.bilng_day
                                                                  AND dtls.sls_typ_lbl_id =
                                                                      td.lbl_id
                                                                  AND td.int_lbl_desc =
                                                                      'BI24'
                                                                  AND dtls.sku_id =
                                                                      sku.sku_id(+)
                                                                  AND sku.prfl_cd =
                                                                      prfl.prfl_cd(+)
                                                                GROUP BY dtls.offst_lbl_id,
                                                                         prfl.catgry_id,
                                                                         dtls.sls_cls_cd,
                                                                         dtls.veh_id,
                                                                         dtls.perd_part,
                                                                         dtls.sku_id)) t_bi24,
                                                      (SELECT dtls.offst_lbl_id,
                                                              prfl.catgry_id,
                                                              dtls.sls_cls_cd,
                                                              dtls.veh_id,
                                                              dtls.perd_part,
                                                              dtls.sku_id,
                                                              SUM(dtls.units) AS units,
                                                              SUM(dtls.sales) AS sales
                                                         FROM trend_alloc_hist_dtls dtls,
                                                              ta_dict               td,
                                                              sku,
                                                              prfl
                                                        WHERE dtls.mrkt_id =
                                                              p_mrkt_id
                                                          AND dtls.sls_perd_id =
                                                              i_prd.perd_id
                                                          AND dtls.sls_typ_id =
                                                              p_sls_typ_id
                                                          AND dtls.sls_typ_grp_nm =
                                                              c_sls_typ_grp_nm_dms
                                                          AND dtls.bilng_day =
                                                              l_periods.bilng_day
                                                          AND dtls.sls_typ_lbl_id =
                                                              td.lbl_id
                                                          AND td.int_lbl_desc =
                                                              'ACTUAL'
                                                          AND dtls.sku_id =
                                                              sku.sku_id(+)
                                                          AND sku.prfl_cd =
                                                              prfl.prfl_cd(+)
                                                        GROUP BY dtls.offst_lbl_id,
                                                                 prfl.catgry_id,
                                                                 dtls.sls_cls_cd,
                                                                 dtls.veh_id,
                                                                 dtls.perd_part,
                                                                 dtls.sku_id) t_actual
                                                WHERE nvl(t_bi24.offst_lbl_id, -1) =
                                                      nvl(t_actual.offst_lbl_id,
                                                          -1)
                                                  AND nvl(t_bi24.catgry_id, -1) =
                                                      nvl(t_actual.catgry_id, -1)
                                                  AND nvl(t_bi24.sls_cls_cd, -1) =
                                                      nvl(t_actual.sls_cls_cd, -1)
                                                  AND nvl(t_bi24.veh_id, -1) =
                                                      nvl(t_actual.veh_id, -1)
                                                  AND nvl(t_bi24.sku_id, -1) =
                                                      nvl(t_actual.sku_id, -1))
                                      UNION ALL
                                      -- ACTUAL only
                                      SELECT *
                                        FROM (SELECT t_actual.offst_lbl_id,
                                                      t_actual.catgry_id,
                                                      t_actual.sls_cls_cd,
                                                      t_actual.veh_id,
                                                      t_actual.perd_part,
                                                      t_actual.sku_id,
                                                      0                     AS units_bi24,
                                                      0                     AS sales_bi24,
                                                      t_actual.units        AS units_actual,
                                                      t_actual.sales        AS sales_actual
                                                 FROM (SELECT dtls.offst_lbl_id,
                                                              prfl.catgry_id,
                                                              dtls.sls_cls_cd,
                                                              dtls.veh_id,
                                                              dtls.perd_part,
                                                              dtls.sku_id,
                                                              SUM(dtls.units) AS units,
                                                              SUM(dtls.sales) AS sales
                                                         FROM trend_alloc_hist_dtls dtls,
                                                              ta_dict               td,
                                                              sku,
                                                              prfl
                                                        WHERE dtls.mrkt_id =
                                                              p_mrkt_id
                                                          AND dtls.sls_perd_id =
                                                              i_prd.perd_id
                                                          AND dtls.sls_typ_id =
                                                              p_sls_typ_id
                                                          AND dtls.sls_typ_grp_nm =
                                                              c_sls_typ_grp_nm_dms
                                                          AND dtls.bilng_day =
                                                              l_periods.bilng_day
                                                          AND dtls.sls_typ_lbl_id =
                                                              td.lbl_id
                                                          AND td.int_lbl_desc =
                                                              'ACTUAL'
                                                          AND dtls.sku_id =
                                                              sku.sku_id(+)
                                                          AND sku.prfl_cd =
                                                              prfl.prfl_cd(+)
                                                        GROUP BY dtls.offst_lbl_id,
                                                                 prfl.catgry_id,
                                                                 dtls.sls_cls_cd,
                                                                 dtls.veh_id,
                                                                 dtls.perd_part,
                                                                 dtls.sku_id) t_actual,
                                                      (SELECT dtls.offst_lbl_id,
                                                              prfl.catgry_id,
                                                              dtls.sls_cls_cd,
                                                              dtls.veh_id,
                                                              dtls.perd_part,
                                                              dtls.sku_id,
                                                              SUM(dtls.units) AS units,
                                                              SUM(dtls.sales) AS sales
                                                         FROM trend_alloc_hist_dtls dtls,
                                                              ta_dict               td,
                                                              sku,
                                                              prfl
                                                        WHERE dtls.mrkt_id =
                                                              p_mrkt_id
                                                          AND dtls.sls_perd_id =
                                                              i_prd.perd_id
                                                          AND dtls.sls_typ_id =
                                                              p_sls_typ_id
                                                          AND dtls.sls_typ_grp_nm =
                                                              c_sls_typ_grp_nm_bi24
                                                          AND dtls.bilng_day =
                                                              l_periods.bilng_day
                                                          AND dtls.sls_typ_lbl_id =
                                                              td.lbl_id
                                                          AND td.int_lbl_desc =
                                                              'BI24'
                                                          AND dtls.sku_id =
                                                              sku.sku_id(+)
                                                          AND sku.prfl_cd =
                                                              prfl.prfl_cd(+)
                                                        GROUP BY dtls.offst_lbl_id,
                                                                 prfl.catgry_id,
                                                                 dtls.sls_cls_cd,
                                                                 dtls.veh_id,
                                                                 dtls.perd_part,
                                                                 dtls.sku_id) t_bi24
                                                WHERE nvl(t_actual.offst_lbl_id,
                                                          -1) = nvl(t_bi24.offst_lbl_id(+),
                                                                    -1)
                                                  AND nvl(t_actual.catgry_id, -1) =
                                                      nvl(t_bi24.catgry_id(+), -1)
                                                  AND nvl(t_actual.sls_cls_cd, -1) =
                                                      nvl(t_bi24.sls_cls_cd(+),
                                                          -1)
                                                  AND nvl(t_actual.veh_id, -1) =
                                                      nvl(t_bi24.veh_id(+), -1)
                                                  AND nvl(t_actual.sku_id, -1) =
                                                      nvl(t_bi24.sku_id(+), -1)
                                                  AND t_bi24.sku_id IS NULL)
                                      UNION ALL
                                      -- BI24 only
                                      SELECT *
                                        FROM (SELECT t_bi24.offst_lbl_id,
                                                      t_bi24.catgry_id,
                                                      t_bi24.sls_cls_cd,
                                                      t_bi24.veh_id,
                                                      t_bi24.perd_part,
                                                      t_bi24.sku_id,
                                                      t_bi24.units        AS units_bi24,
                                                      t_bi24.sales        AS sales_bi24,
                                                      0                   AS units_actual,
                                                      0                   AS sales_actual
                                                 FROM (SELECT dtls.offst_lbl_id,
                                                              prfl.catgry_id,
                                                              dtls.sls_cls_cd,
                                                              dtls.veh_id,
                                                              dtls.perd_part,
                                                              dtls.sku_id,
                                                              SUM(dtls.units) AS units,
                                                              SUM(dtls.sales) AS sales
                                                         FROM trend_alloc_hist_dtls dtls,
                                                              ta_dict               td,
                                                              sku,
                                                              prfl
                                                        WHERE dtls.mrkt_id =
                                                              p_mrkt_id
                                                          AND dtls.sls_perd_id =
                                                              i_prd.perd_id
                                                          AND dtls.sls_typ_id =
                                                              p_sls_typ_id
                                                          AND dtls.sls_typ_grp_nm =
                                                              c_sls_typ_grp_nm_bi24
                                                          AND dtls.bilng_day =
                                                              l_periods.bilng_day
                                                          AND dtls.sls_typ_lbl_id =
                                                              td.lbl_id
                                                          AND td.int_lbl_desc =
                                                              'BI24'
                                                          AND dtls.sku_id =
                                                              sku.sku_id(+)
                                                          AND sku.prfl_cd =
                                                              prfl.prfl_cd(+)
                                                        GROUP BY dtls.offst_lbl_id,
                                                                 prfl.catgry_id,
                                                                 dtls.sls_cls_cd,
                                                                 dtls.veh_id,
                                                                 dtls.perd_part,
                                                                 dtls.sku_id) t_bi24,
                                                      (SELECT dtls.offst_lbl_id,
                                                              prfl.catgry_id,
                                                              dtls.sls_cls_cd,
                                                              dtls.veh_id,
                                                              dtls.perd_part,
                                                              dtls.sku_id,
                                                              SUM(dtls.units) AS units,
                                                              SUM(dtls.sales) AS sales
                                                         FROM trend_alloc_hist_dtls dtls,
                                                              ta_dict               td,
                                                              sku,
                                                              prfl
                                                        WHERE dtls.mrkt_id =
                                                              p_mrkt_id
                                                          AND dtls.sls_perd_id =
                                                              i_prd.perd_id
                                                          AND dtls.sls_typ_id =
                                                              p_sls_typ_id
                                                          AND dtls.sls_typ_grp_nm =
                                                              c_sls_typ_grp_nm_dms
                                                          AND dtls.bilng_day =
                                                              l_periods.bilng_day
                                                          AND dtls.sls_typ_lbl_id =
                                                              td.lbl_id
                                                          AND td.int_lbl_desc =
                                                              'ACTUAL'
                                                          AND dtls.sku_id =
                                                              sku.sku_id(+)
                                                          AND sku.prfl_cd =
                                                              prfl.prfl_cd(+)
                                                        GROUP BY dtls.offst_lbl_id,
                                                                 prfl.catgry_id,
                                                                 dtls.sls_cls_cd,
                                                                 dtls.veh_id,
                                                                 dtls.perd_part,
                                                                 dtls.sku_id) t_actual
                                                WHERE nvl(t_bi24.offst_lbl_id, -1) =
                                                      nvl(t_actual.offst_lbl_id(+),
                                                          -1)
                                                  AND nvl(t_bi24.catgry_id, -1) =
                                                      nvl(t_actual.catgry_id(+),
                                                          -1)
                                                  AND nvl(t_bi24.sls_cls_cd, -1) =
                                                      nvl(t_actual.sls_cls_cd(+),
                                                          -1)
                                                  AND nvl(t_bi24.veh_id, -1) =
                                                      nvl(t_actual.veh_id(+), -1)
                                                  AND nvl(t_bi24.sku_id, -1) =
                                                      nvl(t_actual.sku_id(+), -1)
                                                  AND t_actual.sku_id IS NULL)) d,
                                     TABLE(p_rules) r
                               WHERE nvl(d.offst_lbl_id, -1) =
                                     nvl(r.offst_lbl_id(+),
                                         nvl(d.offst_lbl_id, -1))
                                 AND nvl(d.catgry_id, -1) =
                                     nvl(r.catgry_id(+), nvl(d.catgry_id, -1))
                                 AND nvl(d.sls_cls_cd, '-1') =
                                     nvl(r.sls_cls_cd(+),
                                         nvl(d.sls_cls_cd, '-1'))
                                 AND nvl(d.veh_id, -1) =
                                     nvl(r.veh_id(+), nvl(d.veh_id, -1))
                                 AND nvl(d.perd_part, -1) =
                                     nvl(r.perd_part(+), nvl(d.perd_part, -1))
                                 AND instr(nvl(r.sku_list(+),
                                               nvl(d.sku_id, -1)),
                                           nvl(d.sku_id, -1)) > 0)
                       WHERE primary_rule = 1
                       GROUP BY rul_nm, sls_perd_id, bilng_day, isselected) LOOP
          PIPE ROW(obj_pa_trend_alloc_sgmnt_line(c_seg.rul_nm,
                                                 c_seg.sls_perd_id,
                                                 c_seg.bilng_day,
                                                 c_seg.isselected,
                                                 c_seg.units_bi24,
                                                 c_seg.sales_bi24,
                                                 c_seg.units_actual,
                                                 c_seg.sales_actual));
        END LOOP;
      END IF;
    END LOOP;
    --
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' end' || l_parameter_list);
  END get_trend_alloc_resegment;

  -- get_trend_alloc_current
  FUNCTION get_trend_alloc_current(p_mrkt_id        IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                                   p_campgn_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                   p_sls_typ_id     IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                                   p_bilng_day      IN dly_bilng_trnd.prcsng_dt%TYPE,
                                   p_user_id        IN VARCHAR2 DEFAULT NULL)
    RETURN obj_pa_trend_alloc_crrnt_table
    PIPELINED IS
    -- local variable
    l_tbl_bi24        t_hist_detail := t_hist_detail();
    l_catgry_id       prfl.catgry_id%TYPE;
    l_tbl_est2        t_current_est2 := t_current_est2();
    l_tbl_current     t_current;
    c_key             PLS_INTEGER := 0;
    -- for LOG
    l_run_id         NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'GET_TREND_ALLOC_CURRENT';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       to_char(p_mrkt_id) || ', ' ||
                                       'p_campgn_perd_id: ' ||
                                       to_char(p_campgn_perd_id) || ', ' ||
                                       'p_sls_typ_id: ' ||
                                       to_char(p_sls_typ_id) || ', ' ||
                                       'p_bilng_day: ' ||
                                       to_char(p_bilng_day, 'yyyy-mm-dd') || ', ' ||
                                       'p_user_id: ' || l_user_id || ')';
    --
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start' || l_parameter_list);
    -- BI24
    l_tbl_bi24 := get_bi24(p_mrkt_id              => p_mrkt_id,
                           p_sls_perd_id          => p_campgn_perd_id,
                           p_sls_typ_id           => p_sls_typ_id,
                           p_bilng_day            => p_bilng_day,
                           p_offst_lbl_id         => NULL,
                           p_cash_value           => NULL,
                           p_r_factor             => NULL,
                           p_x_sls_typ_lbl_id_flg => 'N',
                           p_perd_part_flg        => 'Y',
                           p_user_id              => l_user_id,
                           p_run_id               => l_run_id);
    IF l_tbl_bi24.count > 0 THEN
      FOR i IN l_tbl_bi24.first .. l_tbl_bi24.last LOOP
        BEGIN
          SELECT prfl.catgry_id
            INTO l_catgry_id
            FROM sku, prfl
           WHERE l_tbl_bi24(i).sku_id = sku.sku_id
              AND sku.prfl_cd = prfl.prfl_cd;
        EXCEPTION
          WHEN OTHERS THEN
            l_catgry_id := NULL;
        END;
        c_key := c_key + 1;
        l_tbl_current(c_key).offst_lbl_id := l_tbl_bi24 (i).offst_lbl_id;
        l_tbl_current(c_key).catgry_id := l_catgry_id;
        l_tbl_current(c_key).sls_cls_cd := l_tbl_bi24 (i).sls_cls_cd;
        l_tbl_current(c_key).veh_id := l_tbl_bi24 (i).veh_id;
        l_tbl_current(c_key).perd_part := l_tbl_bi24 (i).perd_part;
        l_tbl_current(c_key).sku_id := l_tbl_bi24 (i).sku_id;
        l_tbl_current(c_key).units := l_tbl_bi24 (i).units;
        l_tbl_current(c_key).sales := l_tbl_bi24 (i).sales;
      END LOOP;
      l_tbl_bi24.delete;
    END IF;
    -- EST2
    SELECT offst_lbl_id,
           catgry_id,
           sls_cls_cd,
           veh_id,
           perd_part,
           sku_id,
           round(SUM(unit_qty)),
           round(SUM(unit_qty * sls_prc_amt /
                     decode(nr_for_qty, 0, 1, nr_for_qty) *
                     decode(net_to_avon_fct, 0, 1, net_to_avon_fct)))
      BULK COLLECT
      INTO l_tbl_est2
      FROM (SELECT tc_fc_dms.offst_lbl_id,
                   prfl.catgry_id,
                   offr_prfl_prc_point.sls_cls_cd,
                   offr.veh_id,
                   NULL AS perd_part,
                   offr_sku_line.sku_id,
                   tc_fc_dms.x_sls_typ_lbl_id,
                   nvl(dstrbtd_mrkt_sls.unit_qty, 0) AS unit_qty,
                   nvl(offr_prfl_prc_point.sls_prc_amt, 0) AS sls_prc_amt,
                   nvl(offr_prfl_prc_point.nr_for_qty, 0) AS nr_for_qty,
                   nvl(offr_prfl_prc_point.net_to_avon_fct, 0) AS net_to_avon_fct
              FROM dstrbtd_mrkt_sls,
                   offr_sku_line,
                   offr_prfl_prc_point,
                   offr,
                   sku,
                   prfl,
                   (SELECT src_sls_typ_id,
                           x_src_sls_typ_id,
                           offst_lbl_id,
                           sls_typ_lbl_id,
                           x_sls_typ_lbl_id,
                           pa_maps_public.perd_plus(p_mrkt_id,
                                                    p_campgn_perd_id,
                                                    offst_val_src_sls) src_sls_perd_id,
                           pa_maps_public.perd_plus(p_mrkt_id,
                                                    p_campgn_perd_id,
                                                    offst_val_trgt_sls) trgt_sls_perd_id,
                           pa_maps_public.perd_plus(p_mrkt_id,
                                                    p_campgn_perd_id,
                                                    offst_val_src_offr) src_offr_perd_id,
                           pa_maps_public.perd_plus(p_mrkt_id,
                                                    p_campgn_perd_id,
                                                    offst_val_trgt_offr) trgt_offr_perd_id,
                           r_factor
                      FROM (SELECT src_sls_typ_id,
                                   est_src_sls_typ_id AS x_src_sls_typ_id,
                                   offst_lbl_id,
                                   sls_typ_lbl_id,
                                   x_sls_typ_lbl_id,
                                   offst_val_src_sls,
                                   offst_val_trgt_sls,
                                   offst_val_src_offr,
                                   offst_val_trgt_offr,
                                   r_factor,
                                   eff_sls_perd_id,
                                   lead(eff_sls_perd_id, 1) over(PARTITION BY offst_lbl_id, sls_typ_grp_nm ORDER BY eff_sls_perd_id) AS next_eff_sls_perd_id
                              FROM ta_config
                             WHERE mrkt_id = p_mrkt_id
                               AND trgt_sls_typ_id =
                                   p_sls_typ_id
                               AND upper(REPLACE(TRIM(sls_typ_grp_nm),
                                                 '  ',
                                                 ' ')) IN
                                   (SELECT TRIM(regexp_substr(col,
                                                              '[^,]+',
                                                              1,
                                                              LEVEL)) RESULT
                                      FROM (SELECT c_sls_typ_grp_nm_fc_dms col
                                              FROM dual)
                                    CONNECT BY LEVEL <=
                                               length(regexp_replace(col,
                                                                     '[^,]+')) + 1)
                               AND eff_sls_perd_id <=
                                   p_campgn_perd_id)
                     WHERE p_campgn_perd_id BETWEEN
                           eff_sls_perd_id AND
                           nvl(next_eff_sls_perd_id,
                               p_campgn_perd_id)) tc_fc_dms
             WHERE dstrbtd_mrkt_sls.mrkt_id = p_mrkt_id
               AND dstrbtd_mrkt_sls.sls_perd_id =
                   tc_fc_dms.src_sls_perd_id
               AND dstrbtd_mrkt_sls.offr_perd_id =
                   tc_fc_dms.src_offr_perd_id
               AND dstrbtd_mrkt_sls.sls_typ_id =
                   tc_fc_dms.src_sls_typ_id
               AND dstrbtd_mrkt_sls.offr_sku_line_id =
                   offr_sku_line.offr_sku_line_id
               AND offr_sku_line.dltd_ind <> 'Y'
               AND offr_sku_line.offr_prfl_prcpt_id =
                   offr_prfl_prc_point.offr_prfl_prcpt_id
               AND offr_prfl_prc_point.offr_id =
                   offr.offr_id
               AND offr.offr_typ = 'CMP'
               AND offr.ver_id = 0
               AND offr_sku_line.sku_id = sku.sku_id(+)
               AND sku.prfl_cd = prfl.prfl_cd(+))
  GROUP BY offst_lbl_id,
           catgry_id,
           sls_cls_cd,
           veh_id,
           perd_part,
           sku_id;
    IF l_tbl_est2.count > 0 THEN
      FOR i IN l_tbl_est2.first .. l_tbl_est2.last LOOP
        c_key := c_key + 1;
        l_tbl_current(c_key).offst_lbl_id := l_tbl_est2 (i).offst_lbl_id;
        l_tbl_current(c_key).catgry_id := l_tbl_est2 (i).catgry_id;
        l_tbl_current(c_key).sls_cls_cd := l_tbl_est2 (i).sls_cls_cd;
        l_tbl_current(c_key).veh_id := l_tbl_est2 (i).veh_id;
        l_tbl_current(c_key).perd_part := l_tbl_est2 (i).perd_part;
        l_tbl_current(c_key).sku_id := l_tbl_est2 (i).sku_id;
        l_tbl_current(c_key).units := l_tbl_est2 (i).units;
        l_tbl_current(c_key).sales := l_tbl_est2 (i).sales;
      END LOOP;
      l_tbl_est2.delete;
    END IF;
    -- PIPE (CURRENT)
    IF l_tbl_current.count > 0 THEN
      c_key := l_tbl_current.first;
      WHILE c_key IS NOT NULL LOOP
        PIPE ROW(obj_pa_trend_alloc_crrnt_line(l_tbl_current (c_key).offst_lbl_id,
                                               l_tbl_current (c_key).catgry_id,
                                               l_tbl_current (c_key).sls_cls_cd,
                                               l_tbl_current (c_key).veh_id,
                                               l_tbl_current (c_key).perd_part,
                                               l_tbl_current (c_key).sku_id,
                                               l_tbl_current (c_key).units,
                                               l_tbl_current (c_key).sales));
        c_key := l_tbl_current.next(c_key);
      END LOOP;
      l_tbl_current.delete;
    END IF;
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' end' || l_parameter_list);
  END get_trend_alloc_current;

  -- get_trend_alloc_prod_dtls
  FUNCTION get_trend_alloc_prod_dtls(p_mrkt_id        IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                                     p_campgn_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                     p_sls_typ_id     IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                                     p_bilng_day      IN dly_bilng_trnd.prcsng_dt%TYPE,
                                     p_fsc_cd_list    IN fsc_cd_list_array,
                                     p_user_id        IN VARCHAR2 DEFAULT NULL)
    RETURN obj_pa_trend_alloc_prd_dtl_tbl
    PIPELINED IS
    -- local variables
    l_tbl_bi24         t_hist_detail := t_hist_detail();
    l_tbl_hist_prd_dtl t_hist_prd_dtl;
    l_periods          r_periods;
    l_trgt_perd_id     dstrbtd_mrkt_sls.sls_perd_id%TYPE;
    l_intr_perd_id     dstrbtd_mrkt_sls.sls_perd_id%TYPE;
    l_disc_perd_id     dstrbtd_mrkt_sls.sls_perd_id%TYPE;
    l_sls_cls_cd       offr_prfl_prc_point.sls_cls_cd%TYPE;
    l_fsc_cd           mrkt_fsc.fsc_cd%TYPE;
    l_bias             mrkt_perd_sku_bias.bias_pct%TYPE;
    l_bi24_adj         dly_bilng_adjstmnt.unit_qty%TYPE;
    l_sls_prc_amt      dly_bilng_trnd.sls_prc_amt%TYPE;
    l_nr_for_qty       dly_bilng_trnd.nr_for_qty%TYPE;
    l_fsc_cd_exists    PLS_INTEGER;
    c_key              VARCHAR2(128);
    -- for LOG
    l_run_id         NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'GET_TREND_ALLOC_PROD_DTLS';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       to_char(p_mrkt_id) || ', ' ||
                                       'p_campgn_perd_id: ' ||
                                       to_char(p_campgn_perd_id) || ', ' ||
                                       'p_sls_typ_id: ' ||
                                       to_char(p_sls_typ_id) || ', ' ||
                                       'p_bilng_day: ' ||
                                       to_char(p_bilng_day, 'yyyy-mm-dd') || ', ' ||
                                       'p_user_id: ' || l_user_id || ')';
    --
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start' || l_parameter_list);
    -- get CURRENT periods
    l_periods := get_periods(p_mrkt_id       => p_mrkt_id,
                             p_orig_perd_id  => p_campgn_perd_id,
                             p_bilng_perd_id => p_campgn_perd_id,
                             p_sls_typ_id    => p_sls_typ_id,
                             p_bilng_day     => p_bilng_day,
                             p_user_id       => l_user_id,
                             p_run_id        => l_run_id);
    -- SHIPPED_TO_DATE - BI24 like data collection
    FOR i IN (WITH dbt AS
                 (SELECT dly_bilng_id,
                        fsc_cd,
                        sls_typ_lbl_id,
                        sls_prc_amt,
                        nr_for_qty,
                        unit_qty
                   FROM (SELECT CASE
                                  WHEN dly_bilng_trnd.offr_perd_id = l_periods.trg_perd_id THEN
                                   dly_bilng_trnd.dly_bilng_id
                                  ELSE
                                   NULL
                                END dly_bilng_id,
                                dly_bilng_trnd.fsc_cd,
                                50950 AS sls_typ_lbl_id,
                                MAX(dly_bilng_trnd.sls_prc_amt) over(PARTITION BY dly_bilng_trnd.fsc_cd) AS sls_prc_amt,
                                dly_bilng_trnd.nr_for_qty,
                                SUM(dly_bilng_trnd.unit_qty) over(PARTITION BY dly_bilng_trnd.fsc_cd) AS unit_qty
                           FROM dly_bilng_trnd, TABLE(p_fsc_cd_list) t
                          WHERE dly_bilng_trnd.mrkt_id = p_mrkt_id
                            AND dly_bilng_trnd.trnd_sls_perd_id =
                                p_campgn_perd_id
                            AND trunc(dly_bilng_trnd.prcsng_dt) <=
                                p_bilng_day
                            AND dly_bilng_trnd.trnd_aloctn_auto_stus_id IN
                                (auto_matched,
                                 auto_suggested_single,
                                 auto_suggested_multi)
                               -- shipped to date
                            AND to_number(dly_bilng_trnd.fsc_cd) =
                                to_number(t.column_value)
                            AND (dly_bilng_trnd.lcl_bilng_actn_cd NOT IN
                                ('2A', '7A') AND
                                NOT (dly_bilng_trnd.offr_perd_id <
                                 p_campgn_perd_id AND
                                 dly_bilng_trnd.lcl_bilng_actn_cd = '1' AND
                                 dly_bilng_trnd.lcl_bilng_tran_typ = '01')))
                  WHERE dly_bilng_id IS NOT NULL)
                SELECT l_periods.trg_perd_id AS trgt_perd_id,
                       mrkt_sku.intrdctn_perd_id AS intr_perd_id,
                       mrkt_sku.dspostn_perd_id AS disc_perd_id,
                       dbt.fsc_cd,
                       offr_prfl_prc_point.sls_cls_cd,
                       dbt.sls_typ_lbl_id,
                       dbt.sls_prc_amt,
                       dbt.nr_for_qty,
                       mrkt_perd_sku_bias.bias_pct AS bias,
                       dly_bilng_adjstmnt.unit_qty / nvl(dbt.unit_qty, 1) bi24_adj,
                       round(SUM(nvl(dbt.unit_qty, 0))) units,
                       round(SUM(nvl(dbt.unit_qty, 0) *
                                 nvl(dbt.sls_prc_amt, 0) /
                                 decode(nvl(dbt.nr_for_qty, 0),
                                        0,
                                        1,
                                        dbt.nr_for_qty) *
                                 decode(nvl(offr_prfl_prc_point.net_to_avon_fct,
                                            0),
                                        0,
                                        1,
                                        offr_prfl_prc_point.net_to_avon_fct))) sales
                  FROM dbt,
                       dly_bilng_trnd_offr_sku_line,
                       offr_sku_line,
                       offr_prfl_prc_point,
                       offr,
                       mrkt_sku,
                       mrkt_perd_sku_bias,
                       dly_bilng_adjstmnt
                 WHERE dbt.dly_bilng_id =
                       dly_bilng_trnd_offr_sku_line.dly_bilng_id
                   AND dly_bilng_trnd_offr_sku_line.sls_typ_id =
                       (SELECT MAX(src_sls_typ_id)
                          FROM TABLE(get_ta_config(p_mrkt_id        => p_mrkt_id,
                                                   p_sls_perd_id    => p_campgn_perd_id,
                                                   p_sls_typ_id     => p_sls_typ_id,
                                                   p_sls_typ_grp_nm => c_sls_typ_grp_nm_bi24)))
                   AND dly_bilng_trnd_offr_sku_line.offr_sku_line_id =
                       offr_sku_line.offr_sku_line_id
                   AND offr_sku_line.dltd_ind <> 'Y'
                   AND offr_sku_line.offr_prfl_prcpt_id =
                       offr_prfl_prc_point.offr_prfl_prcpt_id
                   AND offr_prfl_prc_point.offr_id = offr.offr_id
                   AND offr.offr_typ = 'CMP'
                   AND offr.ver_id = 0
                   AND offr_sku_line.sku_id = mrkt_sku.sku_id
                   AND p_mrkt_id = mrkt_sku.mrkt_id
                   AND p_mrkt_id = mrkt_perd_sku_bias.mrkt_id(+)
                   AND l_periods.trg_perd_id =
                       mrkt_perd_sku_bias.sls_perd_id(+)
                   AND offr_sku_line.sku_id = mrkt_perd_sku_bias.sku_id(+)
                   AND p_sls_typ_id = mrkt_perd_sku_bias.sls_typ_id(+)
                   AND dbt.dly_bilng_id = dly_bilng_adjstmnt.dly_bilng_id(+)
                 GROUP BY l_periods.trg_perd_id,
                          mrkt_sku.intrdctn_perd_id,
                          mrkt_sku.dspostn_perd_id,
                          dbt.fsc_cd,
                          offr_prfl_prc_point.sls_cls_cd,
                          dbt.sls_typ_lbl_id,
                          dbt.sls_prc_amt,
                          dbt.nr_for_qty,
                          mrkt_perd_sku_bias.bias_pct,
                          dly_bilng_adjstmnt.unit_qty / nvl(dbt.unit_qty, 1)
                 ORDER BY dbt.fsc_cd, dbt.sls_typ_lbl_id) LOOP
      c_key := i.fsc_cd || '_' || to_char(i.sls_typ_lbl_id);
      --
      l_tbl_hist_prd_dtl(c_key).trgt_perd_id := i.trgt_perd_id;
      l_tbl_hist_prd_dtl(c_key).intr_perd_id := i.intr_perd_id;
      l_tbl_hist_prd_dtl(c_key).disc_perd_id := i.disc_perd_id;
      l_tbl_hist_prd_dtl(c_key).sls_cls_cd := i.sls_cls_cd;
      l_tbl_hist_prd_dtl(c_key).fsc_cd := i.fsc_cd;
      l_tbl_hist_prd_dtl(c_key).bias := i.bias;
      l_tbl_hist_prd_dtl(c_key).bi24_adj := i.bi24_adj;
      l_tbl_hist_prd_dtl(c_key).sls_prc_amt := i.sls_prc_amt;
      l_tbl_hist_prd_dtl(c_key).nr_for_qty := i.nr_for_qty;
      l_tbl_hist_prd_dtl(c_key).offst_lbl_id := NULL;
      l_tbl_hist_prd_dtl(c_key).sls_typ_lbl_id := i.sls_typ_lbl_id;
      l_tbl_hist_prd_dtl(c_key).units := i.units;
      l_tbl_hist_prd_dtl(c_key).sales := i.sales;
    END LOOP;
    -- DMS like data collection
    FOR i IN (SELECT /*+ INDEX(OFFR_PRFL_PRC_POINT PK_OFFR_PRFL_PRC_POINT)
                         INDEX(OFFR PK_OFFR) */
               t.column_value AS fsc_cd,
               tc_dms.sls_typ_lbl_id,
               round(SUM(nvl(dstrbtd_mrkt_sls.unit_qty, 0))) units,
               round(SUM(nvl(dstrbtd_mrkt_sls.unit_qty, 0) *
                         nvl(offr_prfl_prc_point.sls_prc_amt, 0) /
                         decode(nvl(offr_prfl_prc_point.nr_for_qty, 0),
                                0,
                                1,
                                offr_prfl_prc_point.nr_for_qty) *
                         decode(nvl(offr_prfl_prc_point.net_to_avon_fct, 0),
                                0,
                                1,
                                offr_prfl_prc_point.net_to_avon_fct))) sales
                FROM dstrbtd_mrkt_sls,
                     offr_sku_line,
                     offr_prfl_prc_point,
                     offr,
                     TABLE(p_fsc_cd_list) t,
                     (SELECT estimate AS src_sls_typ_id,
                             501000 + estimate AS sls_typ_lbl_id,
                             l_periods.trg_perd_id AS trgt_sls_perd_id
                        FROM dual
                      UNION ALL
                      SELECT operational_estimate AS src_sls_typ_id,
                             501000 + operational_estimate AS sls_typ_lbl_id,
                             l_periods.trg_perd_id AS trgt_sls_perd_id
                        FROM dual
                      UNION ALL
                      SELECT p_sls_typ_id AS src_sls_typ_id,
                             501000 + p_sls_typ_id AS sls_typ_lbl_id,
                             l_periods.trg_perd_id AS trgt_sls_perd_id
                        FROM dual) tc_dms
               WHERE dstrbtd_mrkt_sls.mrkt_id = p_mrkt_id
                 AND dstrbtd_mrkt_sls.sls_perd_id = tc_dms.trgt_sls_perd_id
                 AND dstrbtd_mrkt_sls.sls_typ_id = tc_dms.src_sls_typ_id
                 AND dstrbtd_mrkt_sls.offr_sku_line_id =
                     offr_sku_line.offr_sku_line_id
                 AND offr_sku_line.dltd_ind <> 'Y'
                 AND offr_sku_line.offr_prfl_prcpt_id =
                     offr_prfl_prc_point.offr_prfl_prcpt_id
                 AND offr_prfl_prc_point.offr_id = offr.offr_id
                 AND offr.offr_typ = 'CMP'
                 AND offr.ver_id = 0
                 AND to_number(pa_maps_public.get_mstr_fsc_cd(p_mrkt_id,
                                                              offr_sku_line.sku_id,
                                                              p_campgn_perd_id)) =
                     to_number(t.column_value)
               GROUP BY t.column_value, tc_dms.sls_typ_lbl_id
               ORDER BY t.column_value, tc_dms.sls_typ_lbl_id) LOOP
      c_key := i.fsc_cd || '_' || to_char(i.sls_typ_lbl_id);
      --
      l_trgt_perd_id := l_tbl_hist_prd_dtl(i.fsc_cd ||'_50950').trgt_perd_id;
      l_intr_perd_id := l_tbl_hist_prd_dtl(i.fsc_cd ||'_50950').intr_perd_id;
      l_disc_perd_id := l_tbl_hist_prd_dtl(i.fsc_cd ||'_50950').disc_perd_id;
      l_sls_cls_cd   := l_tbl_hist_prd_dtl(i.fsc_cd ||'_50950').sls_cls_cd;
      l_bias         := l_tbl_hist_prd_dtl(i.fsc_cd ||'_50950').bias;
      l_bi24_adj     := l_tbl_hist_prd_dtl(i.fsc_cd ||'_50950').bi24_adj;
      l_sls_prc_amt  := l_tbl_hist_prd_dtl(i.fsc_cd ||'_50950').sls_prc_amt;
      l_nr_for_qty   := l_tbl_hist_prd_dtl(i.fsc_cd ||'_50950').nr_for_qty;
      --
      l_tbl_hist_prd_dtl(c_key).fsc_cd := i.fsc_cd;
      l_tbl_hist_prd_dtl(c_key).trgt_perd_id := l_trgt_perd_id;
      l_tbl_hist_prd_dtl(c_key).intr_perd_id := l_intr_perd_id;
      l_tbl_hist_prd_dtl(c_key).disc_perd_id := l_disc_perd_id;
      l_tbl_hist_prd_dtl(c_key).sls_cls_cd := l_sls_cls_cd;
      l_tbl_hist_prd_dtl(c_key).bias := l_bias;
      l_tbl_hist_prd_dtl(c_key).bi24_adj := l_bi24_adj;
      l_tbl_hist_prd_dtl(c_key).sls_prc_amt := l_sls_prc_amt;
      l_tbl_hist_prd_dtl(c_key).nr_for_qty := l_nr_for_qty;
      l_tbl_hist_prd_dtl(c_key).offst_lbl_id := NULL;
      l_tbl_hist_prd_dtl(c_key).sls_typ_lbl_id := i.sls_typ_lbl_id;
      l_tbl_hist_prd_dtl(c_key).units := i.units;
      l_tbl_hist_prd_dtl(c_key).sales := i.sales;
    END LOOP;
    -- BI24
    l_tbl_bi24 := get_bi24(p_mrkt_id              => p_mrkt_id,
                           p_sls_perd_id          => p_campgn_perd_id,
                           p_sls_typ_id           => p_sls_typ_id,
                           p_bilng_day            => p_bilng_day,
                           p_offst_lbl_id         => NULL,
                           p_cash_value           => NULL,
                           p_r_factor             => NULL,
                           p_x_sls_typ_lbl_id_flg => 'N',
                           p_perd_part_flg        => 'N',
                           p_user_id              => l_user_id,
                           p_run_id               => l_run_id);
    -- filtering (BI24) by FSC_CD_LIST
    IF l_tbl_bi24.count > 0 THEN
      FOR i IN l_tbl_bi24.first .. l_tbl_bi24.last LOOP
        l_fsc_cd := nvl(pa_maps_public.get_mstr_fsc_cd(p_mrkt_id,
                                                       l_tbl_bi24(i).sku_id,
                                                       p_campgn_perd_id),
                        '-1');
        SELECT COUNT(1)
          INTO l_fsc_cd_exists
          FROM TABLE(p_fsc_cd_list) t
         WHERE t.column_value = l_fsc_cd;
        IF l_fsc_cd_exists = 1 THEN
          c_key := l_fsc_cd || '_' ||
                   to_char(50000 + l_tbl_bi24(i).sls_typ_lbl_id);
          --
          l_trgt_perd_id := l_tbl_hist_prd_dtl(l_fsc_cd ||'_50950')
                            .trgt_perd_id;
          l_intr_perd_id := l_tbl_hist_prd_dtl(l_fsc_cd ||'_50950')
                            .intr_perd_id;
          l_disc_perd_id := l_tbl_hist_prd_dtl(l_fsc_cd ||'_50950')
                            .disc_perd_id;
          l_sls_cls_cd   := l_tbl_hist_prd_dtl(l_fsc_cd ||'_50950')
                            .sls_cls_cd;
          l_bias         := l_tbl_hist_prd_dtl(l_fsc_cd ||'_50950').bias;
          l_bi24_adj     := l_tbl_hist_prd_dtl(l_fsc_cd ||'_50950').bi24_adj;
          l_sls_prc_amt  := l_tbl_hist_prd_dtl(l_fsc_cd ||'_50950')
                            .sls_prc_amt;
          l_nr_for_qty   := l_tbl_hist_prd_dtl(l_fsc_cd ||'_50950')
                            .nr_for_qty;
          --
          l_tbl_hist_prd_dtl(c_key).fsc_cd := l_fsc_cd;
          l_tbl_hist_prd_dtl(c_key).trgt_perd_id := l_trgt_perd_id;
          l_tbl_hist_prd_dtl(c_key).intr_perd_id := l_intr_perd_id;
          l_tbl_hist_prd_dtl(c_key).disc_perd_id := l_disc_perd_id;
          l_tbl_hist_prd_dtl(c_key).sls_cls_cd := l_sls_cls_cd;
          l_tbl_hist_prd_dtl(c_key).bias := l_bias;
          l_tbl_hist_prd_dtl(c_key).bi24_adj := l_bi24_adj;
          l_tbl_hist_prd_dtl(c_key).sls_prc_amt := l_sls_prc_amt;
          l_tbl_hist_prd_dtl(c_key).nr_for_qty := l_nr_for_qty;
          l_tbl_hist_prd_dtl(c_key).offst_lbl_id := NULL;
          l_tbl_hist_prd_dtl(c_key).sls_typ_lbl_id := 50000 + l_tbl_bi24(i)
                                                     .sls_typ_lbl_id;
          l_tbl_hist_prd_dtl(c_key).units := nvl(l_tbl_hist_prd_dtl(c_key)
                                                 .units,
                                                 0) + l_tbl_bi24(i).units;
          l_tbl_hist_prd_dtl(c_key).sales := nvl(l_tbl_hist_prd_dtl(c_key)
                                                 .sales,
                                                 0) + l_tbl_bi24(i).sales;
        END IF;
      END LOOP;
    END IF;
    -- PIPE - result
    IF l_tbl_hist_prd_dtl.count > 0 THEN
      c_key := l_tbl_hist_prd_dtl.first;
      WHILE c_key IS NOT NULL LOOP
        PIPE ROW(obj_pa_trend_alloc_prd_dtl_ln(l_tbl_hist_prd_dtl(c_key)
                                               .trgt_perd_id,
                                               l_tbl_hist_prd_dtl(c_key)
                                               .intr_perd_id,
                                               l_tbl_hist_prd_dtl(c_key)
                                               .disc_perd_id,
                                               l_tbl_hist_prd_dtl(c_key)
                                               .sls_cls_cd,
                                               l_tbl_hist_prd_dtl(c_key)
                                               .fsc_cd,
                                               l_tbl_hist_prd_dtl(c_key).bias,
                                               l_tbl_hist_prd_dtl(c_key)
                                               .bi24_adj,
                                               l_tbl_hist_prd_dtl(c_key)
                                               .sls_prc_amt,
                                               l_tbl_hist_prd_dtl(c_key)
                                               .nr_for_qty,
                                               l_tbl_hist_prd_dtl(c_key)
                                               .offst_lbl_id,
                                               l_tbl_hist_prd_dtl(c_key)
                                               .sls_typ_lbl_id,
                                               l_tbl_hist_prd_dtl(c_key)
                                               .units,
                                               l_tbl_hist_prd_dtl(c_key)
                                               .sales));
        c_key := l_tbl_hist_prd_dtl.next(c_key);
      END LOOP;
    END IF;
    l_tbl_hist_prd_dtl.delete;
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' end' || l_parameter_list);
  END get_trend_alloc_prod_dtls;

  -- save_rules
  PROCEDURE save_rules(p_mrkt_id        IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                       p_campgn_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                       p_sls_typ_id     IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                       p_bilng_day      IN dly_bilng_trnd.prcsng_dt%TYPE,
                       p_rules          IN obj_pa_trend_alloc_rules_table,
                       p_user_id        IN VARCHAR2 DEFAULT NULL,
                       p_stus           OUT NUMBER) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    -- local variables
    CURSOR c_p_rules IS
      SELECT * FROM TABLE(p_rules);
    l_cnt_mstr    NUMBER;
    l_mstr_exists NUMBER;
    l_rul_id      NUMBER;
    -- for LOG
    l_run_id         NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id        VARCHAR(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'SAVE_RULES';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       to_char(p_mrkt_id) || ', ' ||
                                       'p_campgn_perd_id: ' ||
                                       to_char(p_campgn_perd_id) || ', ' ||
                                       'p_sls_typ_id: ' ||
                                       to_char(p_sls_typ_id) || ', ' ||
                                       'p_bilng_day: ' ||
                                       to_char(p_bilng_day, 'yyyymmdd') || ', ' ||
                                       'p_user_id: ' || l_user_id || ')';
    --
  BEGIN
    --
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start' || l_parameter_list);
    --
    p_stus := 0;
    BEGIN
      --
      DELETE FROM custm_seg_perd
       WHERE campgn_perd_id = p_campgn_perd_id
         AND sls_typ_id = p_sls_typ_id;
      --
      FOR p_rule IN c_p_rules LOOP
        IF p_rule.prirty > 0 THEN
          -- for custom groups nothing can be changed at master record level.
          IF p_rule.rul_id IS NOT NULL THEN
            SELECT COUNT(1)
              INTO l_cnt_mstr
              FROM custm_seg_mstr sm
             WHERE sm.rul_id = p_rule.rul_id;
            IF l_cnt_mstr = 1 THEN
              -- Rule exists
              l_rul_id := p_rule.rul_id;
            ELSE
              p_stus := 1; -- Non-existing rule ID passed
              app_plsql_log.error(l_module_name ||
                                  ' Non-existing rule ID passed - ' ||
                                  'rul_id: ' || to_char(p_rule.rul_id) ||
                                  ', offst_lbl_id: ' ||
                                  to_char(p_rule.offst_lbl_id) ||
                                  ', catgry_id: ' ||
                                  to_char(p_rule.catgry_id) ||
                                  ', sls_cls_cd: ' || p_rule.sls_cls_cd ||
                                  ', veh_id: ' || to_char(p_rule.veh_id) ||
                                  ', perd_part: ' ||
                                  to_char(p_rule.perd_part) ||
                                  l_parameter_list);
              EXIT;
            END IF;
          ELSE
            SELECT COUNT(1)
              INTO l_mstr_exists
              FROM custm_seg_mstr
             WHERE mrkt_id = p_mrkt_id
               AND nvl(offst_lbl_id, -1) = nvl(p_rule.offst_lbl_id, -1)
               AND nvl(catgry_id, -1) = nvl(p_rule.catgry_id, -1)
               AND nvl(sls_cls_cd, '-1') = nvl(p_rule.sls_cls_cd, '-1')
               AND nvl(veh_id, -1) = nvl(p_rule.veh_id, -1)
               AND nvl(perd_part, -1) = nvl(p_rule.perd_part, -1);
            IF l_mstr_exists <> 0 THEN
              p_stus := 2; -- Master record with the same ruleset exists
              app_plsql_log.error(l_module_name ||
                                  ' Master record with the same ruleset exists - ' ||
                                  'rul_id: ' || to_char(p_rule.rul_id) ||
                                  ', offst_lbl_id: ' ||
                                  to_char(p_rule.offst_lbl_id) ||
                                  ', catgry_id: ' ||
                                  to_char(p_rule.catgry_id) ||
                                  ', sls_cls_cd: ' || p_rule.sls_cls_cd ||
                                  ', veh_id: ' || to_char(p_rule.veh_id) ||
                                  ', perd_part: ' ||
                                  to_char(p_rule.perd_part) ||
                                  l_parameter_list);
              EXIT;
            ELSE
              INSERT INTO custm_seg_mstr
                (mrkt_id,
                 rul_nm,
                 offst_lbl_id,
                 catgry_id,
                 sls_cls_cd,
                 veh_id,
                 perd_part)
              VALUES
                (p_mrkt_id,
                 p_rule.rul_nm,
                 p_rule.offst_lbl_id,
                 p_rule.catgry_id,
                 p_rule.sls_cls_cd,
                 p_rule.veh_id,
                 p_rule.perd_part);
              BEGIN
                SELECT rul_id
                  INTO l_rul_id
                  FROM custm_seg_mstr
                 WHERE mrkt_id = p_mrkt_id
                   AND nvl(offst_lbl_id, -1) = nvl(p_rule.offst_lbl_id, -1)
                   AND nvl(catgry_id, -1) = nvl(p_rule.catgry_id, -1)
                   AND nvl(sls_cls_cd, '-1') = nvl(p_rule.sls_cls_cd, '-1')
                   AND nvl(veh_id, -1) = nvl(p_rule.veh_id, -1)
                   AND nvl(perd_part, -1) = nvl(p_rule.perd_part, -1);
              EXCEPTION
                WHEN OTHERS THEN
                  l_rul_id := NULL;
              END;
            END IF;
          END IF;
        END IF;
        -- Master rules are handled, it's time to take care of the rules themselves.
        IF p_stus = 0 THEN
          IF p_rule.prirty > 0 THEN
            IF p_rule.r_factor IS NULL AND p_rule.r_factor_manual IS NULL THEN
              p_stus := 4; -- Got NULL value for both r_factor AND r_factor_manual
              app_plsql_log.error(l_module_name ||
                                  ' Got NULL value as input for both R_factor and Manual_R_factor - ' ||
                                  'rul_id: ' || to_char(p_rule.rul_id) ||
                                  ', offst_lbl_id: ' ||
                                  to_char(p_rule.offst_lbl_id) ||
                                  ', catgry_id: ' ||
                                  to_char(p_rule.catgry_id) ||
                                  ', sls_cls_cd: ' || p_rule.sls_cls_cd ||
                                  ', veh_id: ' || to_char(p_rule.veh_id) ||
                                  ', R_factor: ' ||
                                  to_char(p_rule.r_factor) ||
                                  ', Manual_R_factor: ' ||
                                  to_char(p_rule.r_factor_manual) ||
                                  l_parameter_list);
              EXIT;
            ELSE
              -- custom groups have their own rule table.
              INSERT INTO custm_seg_perd
                (campgn_perd_id,
                 rul_id,
                 sls_typ_id,
                 prirty,
                 period_list,
                 r_factor,
                 r_factor_manual,
                 cash_value,
                 use_estimate)
              VALUES
                (p_campgn_perd_id,
                 l_rul_id,
                 p_sls_typ_id,
                 p_rule.prirty,
                 p_rule.period_list,
                 p_rule.r_factor,
                 p_rule.r_factor_manual,
                 p_rule.cash_value,
                 CASE
                   WHEN upper(p_rule.use_estimate) = 'Y' THEN 'Y'
                   ELSE 'N'
                 END);
            END IF;
          ELSE
            -- for custom groups only update is possible
            SELECT COUNT(1)
              INTO l_mstr_exists
              FROM custm_rul_perd
             WHERE campgn_perd_id = p_campgn_perd_id
               AND rul_id = nvl(p_rule.rul_id, -1);
            IF l_mstr_exists = 0 THEN
              p_stus := 3; -- custom group ruleset does NOT exists
              app_plsql_log.error(l_module_name ||
                                  ' Custom Group ruleset does NOT exists - ' ||
                                  'rul_id: ' || to_char(p_rule.rul_id) ||
                                  ', offst_lbl_id: ' ||
                                  to_char(p_rule.offst_lbl_id) ||
                                  ', catgry_id: ' ||
                                  to_char(p_rule.catgry_id) ||
                                  ', sls_cls_cd: ' || p_rule.sls_cls_cd ||
                                  ', veh_id: ' || to_char(p_rule.veh_id) ||
                                  ', perd_part: ' ||
                                  to_char(p_rule.perd_part) ||
                                  l_parameter_list);
              EXIT;
            ELSE
              UPDATE custm_rul_perd
                 SET period_list     = p_rule.period_list,
                     r_factor        = p_rule.r_factor,
                     r_factor_manual = p_rule.r_factor_manual,
                     cash_value      = p_rule.cash_value,
                     use_estimate    = CASE
                                         WHEN upper(p_rule.use_estimate) = 'Y' THEN 'Y'
                                         ELSE 'N'
                                       END
               WHERE campgn_perd_id = p_campgn_perd_id
                 AND rul_id = p_rule.rul_id;
            END IF;
          END IF;
        END IF;
      END LOOP;
    EXCEPTION
      WHEN OTHERS THEN
        ROLLBACK;
        p_stus := 5;
        app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
        app_plsql_log.info(l_module_name || ' FAILED, error code: ' ||
                           SQLCODE || ' error message: ' || SQLERRM ||
                           l_parameter_list);
    END;
    IF p_stus = 0 THEN
      COMMIT;
      app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
      app_plsql_log.info(l_module_name || ' COMMIT, status_code: ' ||
                         to_char(p_stus) || l_parameter_list);
    ELSE
      ROLLBACK;
      app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
      app_plsql_log.info(l_module_name || ' ROLLBACK, status_code: ' ||
                         to_char(p_stus) || l_parameter_list);
    END IF;
    --
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' end, status_code: ' ||
                       to_char(p_stus) || l_parameter_list);
  END save_rules;

  -- save_trend_alloctn
  PROCEDURE save_trend_alloctn(p_mrkt_id              IN mrkt_trnd_sls_perd_sls_typ.mrkt_id%TYPE,
                               p_campgn_perd_id       IN mrkt_trnd_sls_perd_sls_typ.trnd_sls_perd_id%TYPE,
                               p_sls_typ_id           IN mrkt_trnd_sls_perd_sls_typ.sls_typ_id%TYPE,
                               p_bilng_day            IN mrkt_trnd_sls_perd_sls_typ.sct_prcsng_dt%TYPE,
                               p_cash_value           IN NUMBER,
                               p_r_factor             IN NUMBER,
                               p_use_offers_on_sched  IN CHAR,
                               p_use_offers_off_sched IN CHAR,
                               p_user_id              IN VARCHAR2 DEFAULT NULL,
                               p_run_id               IN NUMBER DEFAULT NULL,
                               p_stus                 OUT NUMBER) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    -- local variables
    l_ins_cnt                    NUMBER;
    l_cycl_cnt                   NUMBER;
    l_deleted_rows               NUMBER;
    l_tbl_reproc_trnd            t_reproc := t_reproc();
    l_tbl_reproc_trnd2           t_reproc := t_reproc();
    l_tbl_reproc_est             t_reproc := t_reproc();
    l_tbl_reproc_est2            t_reproc_est2 := t_reproc_est2();
    l_tbl_reproc                 t_reproc_save;
    l_tbl_save                   t_save;
    c_key                        VARCHAR2(32);
    l_sls_perd_id_mrkttrndslsprd NUMBER;
    l_sct_cash_value_on          mrkt_trnd_sls_perd_sls_typ.sct_cash_value_on%TYPE;
    l_sct_cash_value_off         mrkt_trnd_sls_perd_sls_typ.sct_cash_value_off%TYPE;
    l_sct_prcsng_dt              mrkt_trnd_sls_perd_sls_typ.sct_prcsng_dt%TYPE;
    l_sct_bilng_dt               mrkt_trnd_sls_perd_sls_typ.sct_bilng_dt%TYPE;
    l_max_prcsng_dt              mrkt_trnd_sls_perd_sls_typ.sct_prcsng_dt%TYPE;
    l_p_cash_value_on            mrkt_trnd_sls_perd_sls_typ.sct_cash_value_on%TYPE;
    l_p_cash_value_off           mrkt_trnd_sls_perd_sls_typ.sct_cash_value_off%TYPE;
    --
    l_spec_sls_typ_id           mrkt_trnd_sls_perd_sls_typ.sls_typ_id%TYPE;
    -- for LOG
    l_run_id         NUMBER := nvl(p_run_id,
                                   app_plsql_output.generate_new_run_id);
    l_user_id        VARCHAR(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'SAVE_TREND_ALLOCTN';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       to_char(p_mrkt_id) || ', ' ||
                                       'p_campgn_perd_id: ' ||
                                       to_char(p_campgn_perd_id) || ', ' ||
                                       'p_sls_typ_id: ' ||
                                       to_char(p_sls_typ_id) || ', ' ||
                                       'p_bilng_day: ' ||
                                       to_char(p_bilng_day, 'yyyymmdd') || ', ' ||
                                       'p_cash_value: ' ||
                                       to_char(p_cash_value) || ', ' ||
                                       'p_r_factor: ' ||
                                       to_char(p_r_factor) || ', ' ||
                                       'p_use_offers_on_sched: ' ||
                                       p_use_offers_on_sched || ', ' ||
                                       'p_use_offers_off_sched: ' ||
                                       p_use_offers_off_sched || ', ' ||
                                       'p_user_id: ' || l_user_id || ', ' ||
                                       'p_run_id: ' || to_char(l_run_id) || ')';
    --
  BEGIN
    --
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start' || l_parameter_list);
    --
    p_stus := 0;
    -- create NEW record into MRKT_TRND_SLS_PERD
    BEGIN
      MERGE INTO mrkt_trnd_sls_perd m
      USING (SELECT p_mrkt_id AS mrkt_id,
                    (SELECT MAX(tc_bi24.trgt_sls_perd_id)
                       FROM TABLE(get_ta_config(p_mrkt_id        => p_mrkt_id,
                                                p_sls_perd_id    => p_campgn_perd_id,
                                                p_sls_typ_id     => p_sls_typ_id,
                                                p_sls_typ_grp_nm => c_sls_typ_grp_nm_bi24)) tc_bi24,
                            ta_dict td
                      WHERE td.lbl_id = tc_bi24.offst_lbl_id
                        AND upper(td.int_lbl_desc) = 'ON-SCHEDULE') AS trnd_sls_perd_id
               FROM dual) d
      ON (m.mrkt_id = d.mrkt_id AND m.trnd_sls_perd_id = d.trnd_sls_perd_id)
      WHEN NOT MATCHED THEN
        INSERT
          (mrkt_id, trnd_sls_perd_id, creat_user_id)
        VALUES
          (d.mrkt_id, d.trnd_sls_perd_id, l_user_id);
    EXCEPTION
      WHEN OTHERS THEN
        app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
        app_plsql_log.info(l_module_name ||
                           ' create NEW record into MRKT_TRND_SLS_PERD for mrkt_id=' ||
                           to_char(p_mrkt_id) ||
                           ' and trnd_sls_perd_id=MAX(trgt_sls_perd_id) >>> ' ||
                           to_char(p_campgn_perd_id) ||
                           ' <<< FAILED, error code: ' || SQLCODE ||
                           ' error message: ' || SQLERRM ||
                           l_parameter_list);
        p_stus := 2;
    END;
    -- create NEW record into MRKT_TRND_SLS_PERD_SLS_TYP
    BEGIN
      MERGE INTO mrkt_trnd_sls_perd_sls_typ m
      USING (SELECT p_mrkt_id AS mrkt_id,
                    (SELECT MAX(tc_bi24.trgt_sls_perd_id)
                       FROM TABLE(get_ta_config(p_mrkt_id        => p_mrkt_id,
                                                p_sls_perd_id    => p_campgn_perd_id,
                                                p_sls_typ_id     => p_sls_typ_id,
                                                p_sls_typ_grp_nm => c_sls_typ_grp_nm_bi24)) tc_bi24,
                            ta_dict td
                      WHERE td.lbl_id = tc_bi24.offst_lbl_id
                        AND upper(td.int_lbl_desc) = 'ON-SCHEDULE') AS trnd_sls_perd_id,
                    p_sls_typ_id AS sls_typ_id
               FROM dual) d
      ON (m.mrkt_id = d.mrkt_id AND m.trnd_sls_perd_id = d.trnd_sls_perd_id AND m.sls_typ_id = d.sls_typ_id)
      WHEN NOT MATCHED THEN
        INSERT
          (mrkt_id, trnd_sls_perd_id, sls_typ_id, creat_user_id)
        VALUES
          (d.mrkt_id, d.trnd_sls_perd_id, d.sls_typ_id, l_user_id);
    EXCEPTION
      WHEN OTHERS THEN
        app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
        app_plsql_log.info(l_module_name ||
                           ' create NEW record into MRKT_TRND_SLS_PERD_SLS_TYP for mrkt_id=' ||
                           to_char(p_mrkt_id) ||
                           ' and trnd_sls_perd_id=MAX(trgt_sls_perd_id) >>> ' ||
                           to_char(p_campgn_perd_id) ||
                           ' <<< and sls_typ_id=' || to_char(p_sls_typ_id) ||
                           ' FAILED, error code: ' || SQLCODE ||
                           ' error message: ' || SQLERRM ||
                           l_parameter_list);
        p_stus := 2;
    END;
    --
    IF p_stus = 0 THEN
      -- set values last used for calculation into MRKT_TRND_SLS_PERD_SLS_TYP table, if changed
      BEGIN
        SELECT trnd_sls_perd_id,
               sct_cash_value_on,
               sct_cash_value_off,
               sct_prcsng_dt,
               sct_bilng_dt
          INTO l_sls_perd_id_mrkttrndslsprd,
               l_sct_cash_value_on,
               l_sct_cash_value_off,
               l_sct_prcsng_dt,
               l_sct_bilng_dt
          FROM mrkt_trnd_sls_perd_sls_typ
         WHERE mrkt_id = p_mrkt_id
           AND trnd_sls_perd_id =
               (SELECT MAX(tc_bi24.trgt_sls_perd_id)
                  FROM TABLE(get_ta_config(p_mrkt_id        => p_mrkt_id,
                                           p_sls_perd_id    => p_campgn_perd_id,
                                           p_sls_typ_id     => p_sls_typ_id,
                                           p_sls_typ_grp_nm => c_sls_typ_grp_nm_bi24)) tc_bi24,
                       ta_dict td
                 WHERE td.lbl_id = tc_bi24.offst_lbl_id
                   AND upper(td.int_lbl_desc) = 'ON-SCHEDULE')
           AND sls_typ_id = p_sls_typ_id;
      EXCEPTION
        WHEN OTHERS THEN
          app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
          app_plsql_log.info(l_module_name ||
                             ' PROCESSING of table MRKT_TRND_SLS_PERD_SLS_TYP for mrkt_id=' ||
                             to_char(p_mrkt_id) ||
                             ' and trnd_sls_perd_id=MAX(trgt_sls_perd_id) >>> ' ||
                             to_char(p_campgn_perd_id) ||
                             ' <<< and sls_typ_id=' ||
                             to_char(p_sls_typ_id) ||
                             ' FAILED, error code: ' || SQLCODE ||
                             ' error message: ' || SQLERRM ||
                             l_parameter_list);
          l_sls_perd_id_mrkttrndslsprd := NULL;
          p_stus                       := 2;
      END;
    END IF;
    IF l_sls_perd_id_mrkttrndslsprd IS NOT NULL THEN
      BEGIN
        -- protecting delete and insert process
        p_stus := 2;
-- SzA - 20171206 - by ON-schedule / OFF-schedule
--                  we have to collect (from rule and segment tables) 
--                  the SUMMA of cash_value and an average(r_factor)              
--                  INTO         l_p_cash_value,   l_p_r_factor
-- BEFORE this CHECK and INSERT statement
/*
        IF NOT (nvl(l_sct_cash_value, -1) = nvl(l_p_cash_value, -2) AND
                nvl(l_sct_r_factor, -1) = nvl(l_p_r_factor, -2) AND
                nvl(l_sct_onsch_est_bi24_ind, 'X') = nvl(p_use_offers_on_sched, 'Z') AND
                nvl(l_sct_offsch_est_bi24_ind, 'X') = nvl(p_use_offers_off_sched, 'Z')) THEN
          -- create historic record 
          INSERT INTO cash_val_rf_hist
            (mrkt_id,
             sls_perd_id,
             cash_val,
             r_factor,
             onsch_est_bi24_ind,
             offsch_est_bi24_ind,
             sls_typ_id,
             last_updt_user_id)
          VALUES
            (p_mrkt_id,
             l_sls_perd_id_mrkttrndslsprd,
             l_p_cash_value,
             l_p_r_factor,
             p_use_offers_on_sched,
             p_use_offers_off_sched,
             p_sls_typ_id,
             l_user_id);
        END IF;
*/
        -- set actual MAX(prcsng_dt) from dly_bilng into appropriate columns
        BEGIN
          SELECT MAX(prcsng_dt)
            INTO l_max_prcsng_dt
            FROM dly_bilng_trnd
           WHERE mrkt_id = p_mrkt_id
             AND trnd_sls_perd_id = p_campgn_perd_id
             AND prcsng_dt <= p_bilng_day
             AND trnd_aloctn_auto_stus_id IN
                 (auto_matched, auto_suggested_single, auto_suggested_multi);
        EXCEPTION
          WHEN OTHERS THEN
            l_max_prcsng_dt := NULL;
        END;
--IF nvl(l_sct_cash_value, -1) <> nvl(l_p_cash_value, -2) THEN
          l_sct_prcsng_dt := l_max_prcsng_dt;
--END IF;
        IF p_sls_typ_id IN (marketing_est_id, supply_est_id) THEN
          l_sct_bilng_dt := trunc(l_max_prcsng_dt);
        END IF;
        IF p_sls_typ_id IN (marketing_bst_id, supply_bst_id) THEN
          l_sct_bilng_dt := trunc(l_max_prcsng_dt);
        END IF;
        UPDATE mrkt_trnd_sls_perd_sls_typ
           SET sct_aloctn_user_id      = l_user_id,
               sct_aloctn_strt_ts      = SYSDATE,
               sct_aloctn_end_ts       = NULL,
               sct_prcsng_dt           = l_sct_prcsng_dt,
               sct_bilng_dt            = l_sct_bilng_dt,
               last_updt_user_id       = l_user_id
         WHERE mrkt_id = p_mrkt_id
           AND trnd_sls_perd_id = l_sls_perd_id_mrkttrndslsprd
           AND sls_typ_id = p_sls_typ_id;
        -- drop all previously existing records from DMS - p_sls_typ_id
        DELETE FROM dstrbtd_mrkt_sls
         WHERE (mrkt_id, sls_perd_id, offr_perd_id, sls_typ_id) IN
               (SELECT DISTINCT p_mrkt_id,
                                pa_maps_public.perd_plus(p_mrkt_id,
                                                         p_campgn_perd_id,
                                                         offst_val_trgt_sls) trgt_sls_perd_id,
                                pa_maps_public.perd_plus(p_mrkt_id,
                                                         p_campgn_perd_id,
                                                         offst_val_trgt_offr) trgt_offr_perd_id,
                                p_sls_typ_id
                  FROM (SELECT src_sls_typ_id,
                               est_src_sls_typ_id AS x_src_sls_typ_id,
                               offst_lbl_id,
                               sls_typ_lbl_id,
                               x_sls_typ_lbl_id,
                               offst_val_src_sls,
                               offst_val_trgt_sls,
                               offst_val_src_offr,
                               offst_val_trgt_offr,
                               r_factor,
                               eff_sls_perd_id,
                               lead(eff_sls_perd_id, 1) over(PARTITION BY offst_lbl_id, sls_typ_grp_nm ORDER BY eff_sls_perd_id) AS next_eff_sls_perd_id
                          FROM ta_config
                         WHERE mrkt_id = p_mrkt_id
                           AND trgt_sls_typ_id = p_sls_typ_id
                           AND upper(REPLACE(TRIM(sls_typ_grp_nm), '  ', ' ')) IN
                               (SELECT TRIM(regexp_substr(col,
                                                          '[^,]+',
                                                          1,
                                                          LEVEL)) RESULT
                                  FROM (SELECT c_sls_typ_grp_nm_fc_dbt || ', ' ||
                                               c_sls_typ_grp_nm_fc_dms col
                                          FROM dual)
                                CONNECT BY LEVEL <=
                                           length(regexp_replace(col, '[^,]+')) + 1)
                           AND eff_sls_perd_id <= p_campgn_perd_id)
                 WHERE p_campgn_perd_id BETWEEN eff_sls_perd_id AND
                       nvl(next_eff_sls_perd_id, p_campgn_perd_id));
        l_deleted_rows := SQL%ROWCOUNT;
        app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
        app_plsql_log.info(l_module_name || ' ' || l_deleted_rows || ' records DELETED from dstrbtd_mrkt_sls' || l_parameter_list);
        -- FORECASTED
        FOR i IN (SELECT DISTINCT offst_lbl_id,
                                  pa_maps_public.perd_plus(p_mrkt_id,
                                                           p_campgn_perd_id,
                                                           offst_val_trgt_sls) trgt_sls_perd_id,
                                  pa_maps_public.perd_plus(p_mrkt_id,
                                                           p_campgn_perd_id,
                                                           offst_val_trgt_offr) trgt_offr_perd_id
                    FROM (SELECT src_sls_typ_id,
                                 est_src_sls_typ_id AS x_src_sls_typ_id,
                                 offst_lbl_id,
                                 sls_typ_lbl_id,
                                 x_sls_typ_lbl_id,
                                 offst_val_src_sls,
                                 offst_val_trgt_sls,
                                 offst_val_src_offr,
                                 offst_val_trgt_offr,
                                 r_factor,
                                 eff_sls_perd_id,
                                 lead(eff_sls_perd_id, 1) over(PARTITION BY offst_lbl_id, sls_typ_grp_nm ORDER BY eff_sls_perd_id) AS next_eff_sls_perd_id
                            FROM ta_config
                           WHERE mrkt_id = p_mrkt_id
                             AND trgt_sls_typ_id = p_sls_typ_id
                             AND upper(REPLACE(TRIM(sls_typ_grp_nm),
                                               '  ',
                                               ' ')) IN
                                 (SELECT TRIM(regexp_substr(col,
                                                            '[^,]+',
                                                            1,
                                                            LEVEL)) RESULT
                                    FROM (SELECT c_sls_typ_grp_nm_fc_dbt || ', ' ||
                                                 c_sls_typ_grp_nm_fc_dms col
                                            FROM dual)
                                  CONNECT BY LEVEL <=
                                             length(regexp_replace(col,
                                                                   '[^,]+')) + 1)
                             AND eff_sls_perd_id <= p_campgn_perd_id)
                   WHERE p_campgn_perd_id BETWEEN eff_sls_perd_id AND
                         nvl(next_eff_sls_perd_id, p_campgn_perd_id)) LOOP
          l_tbl_save(i.offst_lbl_id).offst_lbl_id := i.offst_lbl_id;
          l_tbl_save(i.offst_lbl_id).trgt_sls_perd_id := i.trgt_sls_perd_id;
          l_tbl_save(i.offst_lbl_id).trgt_offr_perd_id := i.trgt_offr_perd_id;
        END LOOP;
        -- TRND
        l_tbl_reproc_trnd := get_reproc_trnd(p_mrkt_id     => p_mrkt_id,
                                             p_sls_perd_id => p_campgn_perd_id,
                                             p_sls_typ_id  => p_sls_typ_id,
                                             p_bilng_day   => p_bilng_day,
                                             p_user_id     => l_user_id,
                                             p_run_id      => l_run_id);
        -- SUM (TRND)
        IF l_tbl_reproc_trnd.count > 0 THEN
          FOR i IN l_tbl_reproc_trnd.first .. l_tbl_reproc_trnd.last LOOP
            c_key := l_tbl_reproc_trnd(i).offr_sku_line_id;
            l_tbl_reproc(c_key).offr_sku_line_id := l_tbl_reproc_trnd(i)
                                                    .offr_sku_line_id;
            l_tbl_reproc(c_key).offst_lbl_id := l_tbl_reproc_trnd(i)
                                                .offst_lbl_id;
            l_tbl_reproc(c_key).sls_typ_lbl_id := l_tbl_reproc_trnd(i)
                                                  .sls_typ_lbl_id;
            l_tbl_reproc(c_key).units := nvl(l_tbl_reproc(c_key).units, 0) + l_tbl_reproc_trnd(i)
                                        .units;
            l_tbl_reproc(c_key).sales := nvl(l_tbl_reproc(c_key).sales, 0) + l_tbl_reproc_trnd(i)
                                        .sales;
            l_tbl_reproc(c_key).veh_id := l_tbl_reproc_trnd(i).veh_id;
            l_tbl_reproc(c_key).comsn_amt := nvl(l_tbl_reproc(c_key)
                                                 .comsn_amt,
                                                 0) + l_tbl_reproc_trnd(i)
                                            .comsn_amt;
            l_tbl_reproc(c_key).tax_amt := nvl(l_tbl_reproc(c_key).tax_amt,
                                               0) + l_tbl_reproc_trnd(i)
                                          .tax_amt;
          END LOOP;
          l_tbl_reproc_trnd.delete;
        END IF;
        IF l_tbl_reproc.count > 0 THEN
          l_cycl_cnt := 0;
          l_ins_cnt  := 0;
          c_key      := l_tbl_reproc.first;
          WHILE c_key IS NOT NULL LOOP
            l_cycl_cnt := l_cycl_cnt + 1;
            p_stus     := 2;
            BEGIN
              INSERT INTO dstrbtd_mrkt_sls
                (mrkt_id,
                 sls_perd_id,
                 offr_sku_line_id,
                 sls_typ_id,
                 sls_srce_id,
                 offr_perd_id,
                 sls_stus_cd,
                 veh_id,
                 unit_qty,
                 comsn_amt,
                 tax_amt,
                 cost_amt,
                 net_to_avon_fct,
                 creat_user_id,
                 last_updt_user_id)
              VALUES
                (p_mrkt_id,
                 l_tbl_save(l_tbl_reproc(c_key).offst_lbl_id).trgt_sls_perd_id,
                 l_tbl_reproc(c_key).offr_sku_line_id,
                 p_sls_typ_id,
                 billing_sls_srce_id,
                 l_tbl_save(l_tbl_reproc(c_key).offst_lbl_id).trgt_offr_perd_id,
                 final_sls_stus_cd,
                 l_tbl_reproc(c_key).veh_id,
                 l_tbl_reproc(c_key).units,
                 l_tbl_reproc(c_key).comsn_amt,
                 l_tbl_reproc(c_key).tax_amt,
                 default_cost_amt,
                 (SELECT CASE
                           WHEN nvl(opp.sls_prc_amt, 0) = 0 THEN
                            0
                           WHEN nvl(opp.nr_for_qty, 0) = 0 THEN
                            0
                           WHEN nvl(l_tbl_reproc(c_key).units, 0) = 0 THEN
                            0
                           ELSE
                            ((opp.sls_prc_amt / opp.nr_for_qty * l_tbl_reproc(c_key).units) - l_tbl_reproc(c_key).comsn_amt - l_tbl_reproc(c_key).tax_amt)
                            / (opp.sls_prc_amt / opp.nr_for_qty * l_tbl_reproc(c_key).units)
                         END
                    FROM offr_prfl_prc_point opp, offr_sku_line osl
                   WHERE osl.offr_sku_line_id = l_tbl_reproc(c_key).offr_sku_line_id
                     AND opp.offr_prfl_prcpt_id = osl.offr_prfl_prcpt_id),
                 l_user_id,
                 l_user_id);
              l_ins_cnt := l_ins_cnt + 1;
              p_stus    := 0;
            EXCEPTION
              WHEN OTHERS THEN
                app_plsql_log.error('ERROR at INSERT INTO dstrbtd_mrkt_sls (T) ' ||
                                    p_mrkt_id || ', ' || l_tbl_save(l_tbl_reproc(c_key).offst_lbl_id)
                                    .trgt_sls_perd_id || ', ' || l_tbl_reproc(c_key)
                                    .offr_sku_line_id || ', ' ||
                                    p_sls_typ_id || ', ' ||
                                    billing_sls_srce_id || ', ' || l_tbl_save(l_tbl_reproc(c_key).offst_lbl_id)
                                    .trgt_offr_perd_id || ', ' ||
                                    final_sls_stus_cd || ', ' || l_tbl_reproc(c_key)
                                    .veh_id || ', ' || l_tbl_reproc(c_key)
                                    .units || ', ' || l_tbl_reproc(c_key)
                                    .comsn_amt || ', ' || l_tbl_reproc(c_key)
                                    .tax_amt || ', ' || default_cost_amt);
                RAISE;
            END;
            c_key := l_tbl_reproc.next(c_key);
          END LOOP;
          app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
          app_plsql_log.info(l_module_name || ' trnd (cycle): ' ||
                             l_cycl_cnt || ' trnd (insert): ' || l_ins_cnt ||
                             l_parameter_list);
          l_tbl_reproc.delete;
        END IF;
        -- TRND2
        l_tbl_reproc_trnd2 := get_reproc_trnd2(p_mrkt_id     => p_mrkt_id,
                                               p_sls_perd_id => p_campgn_perd_id,
                                               p_sls_typ_id  => p_sls_typ_id,
                                               p_bilng_day   => p_bilng_day,
                                               p_user_id     => l_user_id,
                                               p_run_id      => l_run_id);
        -- SUM (TRND2)
        IF l_tbl_reproc_trnd2.count > 0 THEN
          FOR i IN l_tbl_reproc_trnd2.first .. l_tbl_reproc_trnd2.last LOOP
            c_key := l_tbl_reproc_trnd2(i).offr_sku_line_id;
            l_tbl_reproc(c_key).offr_sku_line_id := l_tbl_reproc_trnd2(i).offr_sku_line_id;
            l_tbl_reproc(c_key).offst_lbl_id := l_tbl_reproc_trnd2(i).offst_lbl_id;
            l_tbl_reproc(c_key).sls_typ_lbl_id := l_tbl_reproc_trnd2(i).sls_typ_lbl_id;
            l_tbl_reproc(c_key).units := nvl(l_tbl_reproc(c_key).units, 0) + l_tbl_reproc_trnd2(i).units;
            l_tbl_reproc(c_key).sales := nvl(l_tbl_reproc(c_key).sales, 0) + l_tbl_reproc_trnd2(i).sales;
            l_tbl_reproc(c_key).veh_id := l_tbl_reproc_trnd2(i).veh_id;
            l_tbl_reproc(c_key).comsn_amt := nvl(l_tbl_reproc(c_key).comsn_amt, 0) + l_tbl_reproc_trnd2(i).comsn_amt;
            l_tbl_reproc(c_key).tax_amt := nvl(l_tbl_reproc(c_key).tax_amt, 0) + l_tbl_reproc_trnd2(i).tax_amt;
          END LOOP;
          l_tbl_reproc_trnd2.delete;
        END IF;
        IF l_tbl_reproc.count > 0 THEN
          l_cycl_cnt := 0;
          l_ins_cnt  := 0;
          c_key      := l_tbl_reproc.first;
          WHILE c_key IS NOT NULL LOOP
            l_cycl_cnt := l_cycl_cnt + 1;
            p_stus     := 2;
            BEGIN
              INSERT INTO dstrbtd_mrkt_sls
                (mrkt_id,
                 sls_perd_id,
                 offr_sku_line_id,
                 sls_typ_id,
                 sls_srce_id,
                 offr_perd_id,
                 sls_stus_cd,
                 veh_id,
                 unit_qty,
                 comsn_amt,
                 tax_amt,
                 cost_amt,
                 net_to_avon_fct,
                 creat_user_id,
                 last_updt_user_id)
              VALUES
                (p_mrkt_id,
                 l_tbl_save(l_tbl_reproc(c_key).offst_lbl_id).trgt_sls_perd_id,
                 l_tbl_reproc(c_key).offr_sku_line_id,
                 p_sls_typ_id,
                 billing_sls_srce_id,
                 l_tbl_save(l_tbl_reproc(c_key).offst_lbl_id).trgt_offr_perd_id,
                 final_sls_stus_cd,
                 l_tbl_reproc(c_key).veh_id,
                 l_tbl_reproc(c_key).units,
                 l_tbl_reproc(c_key).comsn_amt,
                 l_tbl_reproc(c_key).tax_amt,
                 default_cost_amt,
                 (SELECT CASE
                           WHEN nvl(opp.sls_prc_amt, 0) = 0 THEN
                            0
                           WHEN nvl(opp.nr_for_qty, 0) = 0 THEN
                            0
                           WHEN nvl(l_tbl_reproc(c_key).units, 0) = 0 THEN
                            0
                           ELSE
                            ((opp.sls_prc_amt / opp.nr_for_qty * l_tbl_reproc(c_key).units) - l_tbl_reproc(c_key).comsn_amt - l_tbl_reproc(c_key).tax_amt)
                            / (opp.sls_prc_amt / opp.nr_for_qty * l_tbl_reproc(c_key).units)
                         END
                    FROM offr_prfl_prc_point opp, offr_sku_line osl
                   WHERE osl.offr_sku_line_id = l_tbl_reproc(c_key).offr_sku_line_id
                     AND opp.offr_prfl_prcpt_id = osl.offr_prfl_prcpt_id),
                 l_user_id,
                 l_user_id);
              l_ins_cnt := l_ins_cnt + 1;
              p_stus    := 0;
            EXCEPTION
              WHEN OTHERS THEN
                app_plsql_log.error('ERROR at INSERT INTO dstrbtd_mrkt_sls (T) ' ||
                                    p_mrkt_id || ', ' || l_tbl_save(l_tbl_reproc(c_key).offst_lbl_id)
                                    .trgt_sls_perd_id || ', ' || l_tbl_reproc(c_key)
                                    .offr_sku_line_id || ', ' ||
                                    p_sls_typ_id || ', ' ||
                                    billing_sls_srce_id || ', ' || l_tbl_save(l_tbl_reproc(c_key).offst_lbl_id)
                                    .trgt_offr_perd_id || ', ' ||
                                    final_sls_stus_cd || ', ' || l_tbl_reproc(c_key)
                                    .veh_id || ', ' || l_tbl_reproc(c_key)
                                    .units || ', ' || l_tbl_reproc(c_key)
                                    .comsn_amt || ', ' || l_tbl_reproc(c_key)
                                    .tax_amt || ', ' || default_cost_amt);
                RAISE;
            END;
            c_key := l_tbl_reproc.next(c_key);
          END LOOP;
          app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
          app_plsql_log.info(l_module_name || ' trnd (cycle): ' ||
                             l_cycl_cnt || ' trnd (insert): ' || l_ins_cnt ||
                             l_parameter_list);
          l_tbl_reproc.delete;
        END IF;
        -- EST
-- SzA - 20171207 - the following IF statement is necessary or not?
-- if upper(nvl(l_sct_onsch_est_bi24_ind, 'N')) = 'Y' or upper(nvl(l_sct_offsch_est_bi24_ind, 'N')) = 'Y' then
        l_tbl_reproc_est := get_reproc_est(p_mrkt_id              => p_mrkt_id,
                                           p_sls_perd_id          => p_campgn_perd_id,
                                           p_sls_typ_id           => p_sls_typ_id,
                                           p_bilng_day            => p_bilng_day,
                                           p_use_offers_on_sched  => p_use_offers_on_sched,
                                           p_use_offers_off_sched => p_use_offers_off_sched,
                                           p_user_id              => l_user_id,
                                           p_run_id               => l_run_id);
        -- SUM (EST)
        IF l_tbl_reproc_est.count > 0 THEN
          FOR i IN l_tbl_reproc_est.first .. l_tbl_reproc_est.last LOOP
            c_key := l_tbl_reproc_est(i).offr_sku_line_id;
            l_tbl_reproc(c_key).offr_sku_line_id := l_tbl_reproc_est(i)
                                                    .offr_sku_line_id;
            l_tbl_reproc(c_key).offst_lbl_id := l_tbl_reproc_est(i)
                                                .offst_lbl_id;
            l_tbl_reproc(c_key).sls_typ_lbl_id := l_tbl_reproc_est(i)
                                                  .sls_typ_lbl_id;
            l_tbl_reproc(c_key).units := nvl(l_tbl_reproc(c_key).units, 0) + l_tbl_reproc_est(i)
                                        .units;
            l_tbl_reproc(c_key).sales := nvl(l_tbl_reproc(c_key).sales, 0) + l_tbl_reproc_est(i)
                                        .sales;
            l_tbl_reproc(c_key).veh_id := l_tbl_reproc_est(i).veh_id;
            l_tbl_reproc(c_key).comsn_amt := nvl(l_tbl_reproc(c_key)
                                                 .comsn_amt,
                                                 0) + l_tbl_reproc_est(i)
                                            .comsn_amt;
            l_tbl_reproc(c_key).tax_amt := nvl(l_tbl_reproc(c_key).tax_amt,
                                               0) + l_tbl_reproc_est(i)
                                          .tax_amt;
          END LOOP;
          l_tbl_reproc_est.delete;
        END IF;
        IF l_tbl_reproc.count > 0 THEN
          l_cycl_cnt := 0;
          l_ins_cnt  := 0;
          c_key      := l_tbl_reproc.first;
          WHILE c_key IS NOT NULL LOOP
            l_cycl_cnt := l_cycl_cnt + 1;
            p_stus     := 2;
            BEGIN
              INSERT INTO dstrbtd_mrkt_sls
                (mrkt_id,
                 sls_perd_id,
                 offr_sku_line_id,
                 sls_typ_id,
                 sls_srce_id,
                 offr_perd_id,
                 sls_stus_cd,
                 veh_id,
                 unit_qty,
                 comsn_amt,
                 tax_amt,
                 cost_amt,
                 net_to_avon_fct,
                 creat_user_id,
                 last_updt_user_id)
              VALUES
                (p_mrkt_id,
                 l_tbl_save(l_tbl_reproc(c_key).offst_lbl_id)
                 .trgt_sls_perd_id,
                 l_tbl_reproc(c_key).offr_sku_line_id,
                 p_sls_typ_id,
                 billing_sls_srce_id,
                 l_tbl_save(l_tbl_reproc(c_key).offst_lbl_id)
                 .trgt_offr_perd_id,
                 final_sls_stus_cd,
                 l_tbl_reproc(c_key).veh_id,
                 l_tbl_reproc(c_key).units,
                 l_tbl_reproc(c_key).comsn_amt,
                 l_tbl_reproc(c_key).tax_amt,
                 default_cost_amt,
                 (SELECT CASE
                           WHEN nvl(opp.sls_prc_amt, 0) = 0 THEN
                            0
                           WHEN nvl(opp.nr_for_qty, 0) = 0 THEN
                            0
                           WHEN nvl(l_tbl_reproc(c_key).units, 0) = 0 THEN
                            0
                           ELSE
                            ((opp.sls_prc_amt / opp.nr_for_qty * l_tbl_reproc(c_key).units) - l_tbl_reproc(c_key).comsn_amt - l_tbl_reproc(c_key).tax_amt)
                            / (opp.sls_prc_amt / opp.nr_for_qty * l_tbl_reproc(c_key).units)
                         END
                    FROM offr_prfl_prc_point opp, offr_sku_line osl
                   WHERE osl.offr_sku_line_id = l_tbl_reproc(c_key)
                        .offr_sku_line_id
                     AND opp.offr_prfl_prcpt_id = osl.offr_prfl_prcpt_id),
                 l_user_id,
                 l_user_id);
              l_ins_cnt := l_ins_cnt + 1;
              p_stus    := 0;
            EXCEPTION
              WHEN OTHERS THEN
                app_plsql_log.error('ERROR at INSERT INTO dstrbtd_mrkt_sls (E) ' ||
                                    p_mrkt_id || ', ' || l_tbl_save(l_tbl_reproc(c_key).offst_lbl_id)
                                    .trgt_sls_perd_id || ', ' || l_tbl_reproc(c_key)
                                    .offr_sku_line_id || ', ' ||
                                    p_sls_typ_id || ', ' ||
                                    billing_sls_srce_id || ', ' || l_tbl_save(l_tbl_reproc(c_key).offst_lbl_id)
                                    .trgt_offr_perd_id || ', ' ||
                                    final_sls_stus_cd || ', ' || l_tbl_reproc(c_key)
                                    .veh_id || ', ' || l_tbl_reproc(c_key)
                                    .units || ', ' || l_tbl_reproc(c_key)
                                    .comsn_amt || ', ' || l_tbl_reproc(c_key)
                                    .tax_amt || ', ' || default_cost_amt);
                RAISE;
            END;
            c_key := l_tbl_reproc.next(c_key);
          END LOOP;
          app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
          app_plsql_log.info(l_module_name || ' est (cycle): ' ||
                             l_cycl_cnt || ' est (insert): ' || l_ins_cnt ||
                             l_parameter_list);
          l_tbl_reproc.delete;
        END IF;
-- end if;
        -- EST2
        l_tbl_reproc_est2 := get_reproc_est2(p_mrkt_id     => p_mrkt_id,
                                             p_sls_perd_id => p_campgn_perd_id,
                                             p_sls_typ_id  => p_sls_typ_id,
                                             p_bilng_day   => p_bilng_day,
                                             p_user_id     => l_user_id,
                                             p_run_id      => l_run_id);
        -- SUM (EST2)
        IF l_tbl_reproc_est2.count > 0 THEN
          FOR i IN l_tbl_reproc_est2.first .. l_tbl_reproc_est2.last LOOP
            c_key := l_tbl_reproc_est2(i).offr_sku_line_id;
            l_tbl_reproc(c_key).offr_sku_line_id := l_tbl_reproc_est2(i)
                                                    .offr_sku_line_id;
            l_tbl_reproc(c_key).offst_lbl_id := l_tbl_reproc_est2(i)
                                                .offst_lbl_id;
            l_tbl_reproc(c_key).sls_typ_lbl_id := l_tbl_reproc_est2(i)
                                                  .sls_typ_lbl_id;
            l_tbl_reproc(c_key).units := nvl(l_tbl_reproc(c_key).units, 0) + l_tbl_reproc_est2(i)
                                        .units_forecasted;
            l_tbl_reproc(c_key).sales := nvl(l_tbl_reproc(c_key).sales, 0) + l_tbl_reproc_est2(i)
                                        .sales_forecasted;
            l_tbl_reproc(c_key).veh_id := l_tbl_reproc_est2(i).veh_id;
            l_tbl_reproc(c_key).comsn_amt := nvl(l_tbl_reproc(c_key)
                                                 .comsn_amt,
                                                 0) + l_tbl_reproc_est2(i)
                                            .comsn_amt;
            l_tbl_reproc(c_key).tax_amt := nvl(l_tbl_reproc(c_key).tax_amt,
                                               0) + l_tbl_reproc_est2(i)
                                          .tax_amt;
          END LOOP;
          l_tbl_reproc_est2.delete;
        END IF;
        IF l_tbl_reproc.count > 0 THEN
          l_cycl_cnt := 0;
          l_ins_cnt  := 0;
          c_key      := l_tbl_reproc.first;
          WHILE c_key IS NOT NULL LOOP
            l_cycl_cnt := l_cycl_cnt + 1;
            p_stus     := 2;
            BEGIN
              INSERT INTO dstrbtd_mrkt_sls
                (mrkt_id,
                 sls_perd_id,
                 offr_sku_line_id,
                 sls_typ_id,
                 sls_srce_id,
                 offr_perd_id,
                 sls_stus_cd,
                 veh_id,
                 unit_qty,
                 comsn_amt,
                 tax_amt,
                 cost_amt,
                 net_to_avon_fct,
                 creat_user_id,
                 last_updt_user_id)
              VALUES
                (p_mrkt_id,
                 l_tbl_save(l_tbl_reproc(c_key).offst_lbl_id)
                 .trgt_sls_perd_id,
                 l_tbl_reproc(c_key).offr_sku_line_id,
                 p_sls_typ_id,
                 billing_sls_srce_id,
                 l_tbl_save(l_tbl_reproc(c_key).offst_lbl_id)
                 .trgt_offr_perd_id,
                 final_sls_stus_cd,
                 l_tbl_reproc(c_key).veh_id,
                 l_tbl_reproc(c_key).units,
                 l_tbl_reproc(c_key).comsn_amt,
                 l_tbl_reproc(c_key).tax_amt,
                 default_cost_amt,
                 (SELECT CASE
                           WHEN nvl(opp.sls_prc_amt, 0) = 0 THEN
                            0
                           WHEN nvl(opp.nr_for_qty, 0) = 0 THEN
                            0
                           WHEN nvl(l_tbl_reproc(c_key).units, 0) = 0 THEN
                            0
                           ELSE
                            ((opp.sls_prc_amt / opp.nr_for_qty * l_tbl_reproc(c_key)
                            .units) - l_tbl_reproc(c_key).comsn_amt - l_tbl_reproc(c_key)
                            .tax_amt) / (opp.sls_prc_amt / opp.nr_for_qty * l_tbl_reproc(c_key)
                            .units)
                         END
                    FROM offr_prfl_prc_point opp, offr_sku_line osl
                   WHERE osl.offr_sku_line_id = l_tbl_reproc(c_key)
                        .offr_sku_line_id
                     AND opp.offr_prfl_prcpt_id = osl.offr_prfl_prcpt_id),
                 l_user_id,
                 l_user_id);
              l_ins_cnt := l_ins_cnt + 1;
              p_stus    := 0;
            EXCEPTION
              WHEN OTHERS THEN
                app_plsql_log.error('ERROR at INSERT INTO dstrbtd_mrkt_sls (E2) ' ||
                                    p_mrkt_id || ', ' || l_tbl_save(l_tbl_reproc(c_key).offst_lbl_id)
                                    .trgt_sls_perd_id || ', ' || l_tbl_reproc(c_key)
                                    .offr_sku_line_id || ', ' ||
                                    p_sls_typ_id || ', ' ||
                                    billing_sls_srce_id || ', ' || l_tbl_save(l_tbl_reproc(c_key).offst_lbl_id)
                                    .trgt_offr_perd_id || ', ' ||
                                    final_sls_stus_cd || ', ' || l_tbl_reproc(c_key)
                                    .veh_id || ', ' || l_tbl_reproc(c_key)
                                    .units || ', ' || l_tbl_reproc(c_key)
                                    .comsn_amt || ', ' || l_tbl_reproc(c_key)
                                    .tax_amt || ', ' || default_cost_amt);
                RAISE;
            END;
            c_key := l_tbl_reproc.next(c_key);
          END LOOP;
          app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
          app_plsql_log.info(l_module_name || ' est2 (cycle): ' || l_cycl_cnt || ' est2 (insert): ' || l_ins_cnt || l_parameter_list);
          l_tbl_reproc.delete;
        END IF;
        --
        -- SAVE with l_spec_sls_typ_id the same records (which was processed in (trnd, est, est2)
        --
        -- set l_spec_sls_typ_id (versioned special sales type ID) for SAVE
        SELECT CASE
                 WHEN p_sls_typ_id = supply_est_id THEN sc_est_day_0
                 WHEN p_sls_typ_id = supply_est_id THEN sc_bst_day_0
                 WHEN p_sls_typ_id = marketing_est_id THEN est_day_0
                 WHEN p_sls_typ_id = marketing_bst_id THEN bst_day_0
               END + trnd_bilng_days.day_num
          INTO l_spec_sls_typ_id
          FROM trnd_bilng_days
         WHERE trnd_bilng_days.mrkt_id = p_mrkt_id
           AND trnd_bilng_days.sls_perd_id = p_campgn_perd_id
           AND trnd_bilng_days.prcsng_dt = p_bilng_day;
        IF l_spec_sls_typ_id >= LEAST(supply_est_id, supply_est_id, marketing_est_id, marketing_bst_id) THEN
          -- drop all previously existing records from DMS - l_spec_sls_typ_id
          DELETE FROM dstrbtd_mrkt_sls
           WHERE (mrkt_id, sls_perd_id, offr_perd_id, sls_typ_id) IN
                 (SELECT DISTINCT p_mrkt_id,
                                  pa_maps_public.perd_plus(p_mrkt_id,
                                                           p_campgn_perd_id,
                                                           offst_val_trgt_sls) trgt_sls_perd_id,
                                  pa_maps_public.perd_plus(p_mrkt_id,
                                                           p_campgn_perd_id,
                                                           offst_val_trgt_offr) trgt_offr_perd_id,
                                  l_spec_sls_typ_id
                    FROM (SELECT src_sls_typ_id,
                                 est_src_sls_typ_id AS x_src_sls_typ_id,
                                 offst_lbl_id,
                                 sls_typ_lbl_id,
                                 x_sls_typ_lbl_id,
                                 offst_val_src_sls,
                                 offst_val_trgt_sls,
                                 offst_val_src_offr,
                                 offst_val_trgt_offr,
                                 r_factor,
                                 eff_sls_perd_id,
                                 lead(eff_sls_perd_id, 1) over(PARTITION BY offst_lbl_id, sls_typ_grp_nm ORDER BY eff_sls_perd_id) AS next_eff_sls_perd_id
                            FROM ta_config
                           WHERE mrkt_id = p_mrkt_id
                             AND trgt_sls_typ_id = p_sls_typ_id
                             AND upper(REPLACE(TRIM(sls_typ_grp_nm), '  ', ' ')) IN
                                 (SELECT TRIM(regexp_substr(col,
                                                            '[^,]+',
                                                            1,
                                                            LEVEL)) RESULT
                                    FROM (SELECT c_sls_typ_grp_nm_fc_dbt || ', ' ||
                                                 c_sls_typ_grp_nm_fc_dms col
                                            FROM dual)
                                  CONNECT BY LEVEL <=
                                             length(regexp_replace(col, '[^,]+')) + 1)
                             AND eff_sls_perd_id <= p_campgn_perd_id)
                   WHERE p_campgn_perd_id BETWEEN eff_sls_perd_id AND
                         nvl(next_eff_sls_perd_id, p_campgn_perd_id));
          l_deleted_rows := SQL%ROWCOUNT;
          app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
          app_plsql_log.info(l_module_name || ' ' || l_deleted_rows || ' records DELETED from dstrbtd_mrkt_sls (l_spec_sls_typ_id: ' || to_char(l_spec_sls_typ_id) || ') ' || l_parameter_list);
          -- INSERT "new" records from DMS - l_spec_sls_typ_id (based on p_sls_typ_id)
          BEGIN
            INSERT INTO dstrbtd_mrkt_sls (mrkt_id,
                                          sls_perd_id,
                                          offr_sku_line_id,
                                          sls_typ_id,
                                          sls_srce_id,
                                          offr_perd_id,
                                          sls_stus_cd,
                                          veh_id,
                                          unit_qty,
                                          creat_user_id,
                                          creat_ts,
                                          last_updt_user_id,
                                          last_updt_ts,
                                          comsn_amt,
                                          tax_amt,
                                          net_to_avon_fct,
                                          unit_ovrrd_ind,
                                          prev_unit_qty,
                                          prev_sls_srce_id,
                                          currnt_est_ind,
                                          sls_perd_ofs_nr,
                                          cost_amt,
                                          ver_id)
            SELECT mrkt_id,
                   sls_perd_id,
                   offr_sku_line_id,
                   l_spec_sls_typ_id,
                   sls_srce_id,
                   offr_perd_id,
                   sls_stus_cd,
                   veh_id,
                   unit_qty,
                   creat_user_id,
                   creat_ts,
                   last_updt_user_id,
                   last_updt_ts,
                   comsn_amt,
                   tax_amt,
                   net_to_avon_fct,
                   unit_ovrrd_ind,
                   prev_unit_qty,
                   prev_sls_srce_id,
                   currnt_est_ind,
                   sls_perd_ofs_nr,
                   cost_amt,
                   ver_id
              FROM dstrbtd_mrkt_sls
             WHERE (mrkt_id, sls_perd_id, offr_perd_id, sls_typ_id) IN
                   (SELECT DISTINCT p_mrkt_id,
                                    pa_maps_public.perd_plus(p_mrkt_id,
                                                             p_campgn_perd_id,
                                                             offst_val_trgt_sls) trgt_sls_perd_id,
                                    pa_maps_public.perd_plus(p_mrkt_id,
                                                             p_campgn_perd_id,
                                                             offst_val_trgt_offr) trgt_offr_perd_id,
                                    p_sls_typ_id
                      FROM (SELECT src_sls_typ_id,
                                   est_src_sls_typ_id AS x_src_sls_typ_id,
                                   offst_lbl_id,
                                   sls_typ_lbl_id,
                                   x_sls_typ_lbl_id,
                                   offst_val_src_sls,
                                   offst_val_trgt_sls,
                                   offst_val_src_offr,
                                   offst_val_trgt_offr,
                                   r_factor,
                                   eff_sls_perd_id,
                                   lead(eff_sls_perd_id, 1) over(PARTITION BY offst_lbl_id, sls_typ_grp_nm ORDER BY eff_sls_perd_id) AS next_eff_sls_perd_id
                              FROM ta_config
                             WHERE mrkt_id = p_mrkt_id
                               AND trgt_sls_typ_id = p_sls_typ_id
                               AND upper(REPLACE(TRIM(sls_typ_grp_nm), '  ', ' ')) IN
                                   (SELECT TRIM(regexp_substr(col,
                                                              '[^,]+',
                                                              1,
                                                              LEVEL)) RESULT
                                      FROM (SELECT c_sls_typ_grp_nm_fc_dbt || ', ' ||
                                                   c_sls_typ_grp_nm_fc_dms col
                                              FROM dual)
                                    CONNECT BY LEVEL <=
                                               length(regexp_replace(col, '[^,]+')) + 1)
                               AND eff_sls_perd_id <= p_campgn_perd_id)
                     WHERE p_campgn_perd_id BETWEEN eff_sls_perd_id AND
                           nvl(next_eff_sls_perd_id, p_campgn_perd_id));
            l_ins_cnt := SQL%ROWCOUNT;
            app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
            app_plsql_log.info(l_module_name || ' (l_spec_sls_typ_id: ' || to_char(l_spec_sls_typ_id) || ', based on p_sls_typ_id: ' || to_char(p_sls_typ_id) || ') spec_sls_typ_ID (insert): ' || l_ins_cnt || l_parameter_list);
          EXCEPTION
            WHEN OTHERS THEN
              app_plsql_log.error('ERROR at INSERT INTO dstrbtd_mrkt_sls (S) l_spec_sls_typ_id: ' || to_char(l_spec_sls_typ_id) || ', ' ||
                                       'p_mrkt_id: ' || to_char(p_mrkt_id) || ', ' ||
                                       'p_campgn_perd_id: ' || to_char(p_campgn_perd_id) || ', ' ||
                                       'p_sls_typ_id: ' || to_char(p_sls_typ_id) || ', ' ||
                                       'p_bilng_day: ' || to_char(p_bilng_day, 'yyyymmdd') || ', ' ||
                                       'p_cash_value: ' || to_char(p_cash_value) || ', ' ||
                                       'p_r_factor: ' || to_char(p_r_factor) || ', ' ||
                                       'p_use_offers_on_sched: ' || p_use_offers_on_sched || ', ' ||
                                       'p_use_offers_off_sched: ' || p_use_offers_off_sched || ', ' ||
                                       'p_user_id: ' || l_user_id || ', ' ||
                                       'p_run_id: ' || to_char(l_run_id) || ')');
              RAISE;
          END;
        END IF;
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK;
          p_stus := 2;
          app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
          app_plsql_log.info(l_module_name || ' FAILED, error code: ' || SQLCODE || ' error message: ' || SQLERRM || l_parameter_list);
      END;
      IF p_stus = 0 THEN
        UPDATE mrkt_trnd_sls_perd_sls_typ
           SET sct_aloctn_end_ts = SYSDATE, last_updt_user_id = l_user_id
         WHERE mrkt_id = p_mrkt_id
           AND trnd_sls_perd_id = l_sls_perd_id_mrkttrndslsprd
           AND sls_typ_id = p_sls_typ_id;
        COMMIT;
        app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
        app_plsql_log.info(l_module_name || ' COMMIT, status_code: ' || to_char(p_stus) || l_parameter_list);
      ELSE
        ROLLBACK;
        app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
        app_plsql_log.info(l_module_name || ' ROLLBACK, status_code: ' || to_char(p_stus) || l_parameter_list);
      END IF;
    END IF;
    --
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' end, status_code: ' ||
                       to_char(p_stus) || l_parameter_list);
  END save_trend_alloctn;

  -- set_sct_autclc
  PROCEDURE set_sct_autclc(p_mrkt_id        IN mrkt_trnd_sls_perd_sls_typ.mrkt_id%TYPE,
                           p_campgn_perd_id IN mrkt_trnd_sls_perd_sls_typ.trnd_sls_perd_id%TYPE,
                           p_sls_typ_id     IN mrkt_trnd_sls_perd_sls_typ.sls_typ_id%TYPE,
                           p_autclc_ind     IN mrkt_trnd_sls_perd_sls_typ.sct_autclc_ind%TYPE,
                           p_user_id        IN VARCHAR2 DEFAULT NULL,
                           p_stus           OUT NUMBER) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    -- local variables
    l_autclc_ind                 mrkt_trnd_sls_perd_sls_typ.sct_autclc_ind%TYPE;
    l_sls_perd_id_mrkttrndslsprd mrkt_trnd_sls_perd_sls_typ.trnd_sls_perd_id%TYPE;
    -- for LOG
    l_run_id         NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'SET_SCT_AUTCLC';
    l_parameter_list VARCHAR2(2048);
  BEGIN
    l_parameter_list := ' (mrkt_id: ' || to_char(p_mrkt_id) || ', ' ||
                        'p_campgn_perd_id: ' || to_char(p_campgn_perd_id) || ', ' ||
                        'sls_typ_id: ' || to_char(p_sls_typ_id) || ', ' ||
                        'autclc_ind: ' || to_char(p_autclc_ind) || ', ' ||
                        'p_user_id: ' || l_user_id || ', ' || 'p_run_id: ' ||
                        to_char(l_run_id) || ')';
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start ' || l_parameter_list);
    p_stus := 0;
    -- create NEW record into MRKT_TRND_SLS_PERD
    BEGIN
      MERGE INTO mrkt_trnd_sls_perd m
      USING (SELECT p_mrkt_id AS mrkt_id,
                    (SELECT MAX(tc_bi24.trgt_sls_perd_id)
                       FROM TABLE(get_ta_config(p_mrkt_id        => p_mrkt_id,
                                                p_sls_perd_id    => p_campgn_perd_id,
                                                p_sls_typ_id     => p_sls_typ_id,
                                                p_sls_typ_grp_nm => c_sls_typ_grp_nm_bi24)) tc_bi24,
                            ta_dict td
                      WHERE td.lbl_id = tc_bi24.offst_lbl_id
                        AND upper(td.int_lbl_desc) = 'ON-SCHEDULE') AS trnd_sls_perd_id
               FROM dual) d
      ON (m.mrkt_id = d.mrkt_id AND m.trnd_sls_perd_id = d.trnd_sls_perd_id)
      WHEN NOT MATCHED THEN
        INSERT
          (mrkt_id, trnd_sls_perd_id, creat_user_id)
        VALUES
          (d.mrkt_id, d.trnd_sls_perd_id, l_user_id);
    EXCEPTION
      WHEN OTHERS THEN
        app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
        app_plsql_log.info(l_module_name ||
                           ' create NEW record into MRKT_TRND_SLS_PERD for mrkt_id=' ||
                           to_char(p_mrkt_id) ||
                           ' and trnd_sls_perd_id=MAX(trgt_sls_perd_id) >>> ' ||
                           to_char(p_campgn_perd_id) ||
                           ' <<< FAILED, error code: ' || SQLCODE ||
                           ' error message: ' || SQLERRM ||
                           l_parameter_list);
        p_stus := 2;
    END;
    -- create NEW record into MRKT_TRND_SLS_PERD_SLS_TYP
    BEGIN
      MERGE INTO mrkt_trnd_sls_perd_sls_typ m
      USING (SELECT p_mrkt_id AS mrkt_id,
                    (SELECT MAX(tc_bi24.trgt_sls_perd_id)
                       FROM TABLE(get_ta_config(p_mrkt_id        => p_mrkt_id,
                                                p_sls_perd_id    => p_campgn_perd_id,
                                                p_sls_typ_id     => p_sls_typ_id,
                                                p_sls_typ_grp_nm => c_sls_typ_grp_nm_bi24)) tc_bi24,
                            ta_dict td
                      WHERE td.lbl_id = tc_bi24.offst_lbl_id
                        AND upper(td.int_lbl_desc) = 'ON-SCHEDULE') AS trnd_sls_perd_id,
                    p_sls_typ_id AS sls_typ_id
               FROM dual) d
      ON (m.mrkt_id = d.mrkt_id AND m.trnd_sls_perd_id = d.trnd_sls_perd_id AND m.sls_typ_id = d.sls_typ_id)
      WHEN NOT MATCHED THEN
        INSERT
          (mrkt_id, trnd_sls_perd_id, sls_typ_id, creat_user_id)
        VALUES
          (d.mrkt_id, d.trnd_sls_perd_id, d.sls_typ_id, l_user_id);
    EXCEPTION
      WHEN OTHERS THEN
        app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
        app_plsql_log.info(l_module_name ||
                           ' create NEW record into MRKT_TRND_SLS_PERD_SLS_TYP for mrkt_id=' ||
                           to_char(p_mrkt_id) ||
                           ' and trnd_sls_perd_id=MAX(trgt_sls_perd_id) >>> ' ||
                           to_char(p_campgn_perd_id) ||
                           ' <<< and sls_typ_id=' || to_char(p_sls_typ_id) ||
                           ' FAILED, error code: ' || SQLCODE ||
                           ' error message: ' || SQLERRM ||
                           l_parameter_list);
        p_stus := 2;
    END;
    IF p_stus = 0 THEN
      -- set VALUE of l_autclc_ind, l_sls_perd_id_mrkttrndslsprd
      BEGIN
        SELECT sct_autclc_ind, trnd_sls_perd_id
          INTO l_autclc_ind,
               l_sls_perd_id_mrkttrndslsprd
          FROM mrkt_trnd_sls_perd_sls_typ
         WHERE mrkt_id = p_mrkt_id
           AND trnd_sls_perd_id =
               (SELECT MAX(tc_bi24.trgt_sls_perd_id)
                  FROM TABLE(get_ta_config(p_mrkt_id        => p_mrkt_id,
                                           p_sls_perd_id    => p_campgn_perd_id,
                                           p_sls_typ_id     => p_sls_typ_id,
                                           p_sls_typ_grp_nm => c_sls_typ_grp_nm_bi24)) tc_bi24,
                       ta_dict td
                 WHERE td.lbl_id = tc_bi24.offst_lbl_id
                   AND upper(td.int_lbl_desc) = 'ON-SCHEDULE')
           AND sls_typ_id = p_sls_typ_id;
        --
        CASE
          WHEN p_sls_typ_id IN (marketing_est_id, supply_est_id) THEN
            l_autclc_ind := upper(p_autclc_ind);
          WHEN p_sls_typ_id IN (marketing_bst_id, supply_bst_id) THEN
            l_autclc_ind := upper(p_autclc_ind);
          WHEN p_sls_typ_id IN (marketing_fst_id, supply_fst_id) THEN
            l_autclc_ind := 'N';
          ELSE
            p_stus := 3;
        END CASE;
        IF p_stus = 0 THEN
          UPDATE mrkt_trnd_sls_perd_sls_typ
             SET sct_autclc_ind = l_autclc_ind
           WHERE mrkt_id = p_mrkt_id
             AND trnd_sls_perd_id = l_sls_perd_id_mrkttrndslsprd
             AND sls_typ_id = p_sls_typ_id;
          --
          COMMIT;
        ELSE
          ROLLBACK;
        END IF;
      EXCEPTION
        WHEN no_data_found THEN
          p_stus := 1;
        WHEN OTHERS THEN
          p_stus := 2;
      END;
    END IF;
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' end, status_code: ' ||
                       to_char(p_stus) || l_parameter_list);
  END set_sct_autclc;

  ---------------------------------------------
  -- inherited from TRND_ALOCTN package -
  ---------------------------------------------
  -- --------------------------------------------------------------------------
  --
  -- Procedure: TRND_ALOCTN_START (internal)
  --
  --
  --
  -- Records start time of Trend Allocation process for specified MRKT/PERD.
  --
  -- No commit is issued, effectively preventing any other session from running
  --
  -- Sales Allocation for the same MRKT/PERD until this run is complete.
  --
  --
  --
  --
  -- --------------------------------------------------------------------------
  PROCEDURE trnd_aloctn_start(p_mrkt_id          IN mrkt.mrkt_id%TYPE,
                              p_trnd_sls_perd_id IN mrkt_trnd_sls_perd.trnd_sls_perd_id%TYPE,
                              p_user_nm          IN VARCHAR,
                              p_strt_ts          IN DATE) IS
  BEGIN
    MERGE INTO mrkt_trnd_sls_perd mtsp
    USING (SELECT p_mrkt_id          AS mrkt_id,
                  p_trnd_sls_perd_id AS trnd_sls_perd_id
             FROM dual) d
    ON (mtsp.mrkt_id = d.mrkt_id AND mtsp.trnd_sls_perd_id = d.trnd_sls_perd_id)
    WHEN MATCHED THEN
      UPDATE
         SET trnd_aloctn_auto_user_id = p_user_nm,
             trnd_aloctn_auto_strt_ts = p_strt_ts,
             trnd_aloctn_auto_end_ts  = NULL
    WHEN NOT MATCHED THEN
      INSERT
        (mrkt_id,
         trnd_sls_perd_id,
         trnd_aloctn_auto_user_id,
         trnd_aloctn_auto_strt_ts,
         trnd_aloctn_auto_end_ts)
      VALUES
        (p_mrkt_id, p_trnd_sls_perd_id, p_user_nm, p_strt_ts, NULL);
  END trnd_aloctn_start;

  -- crct_gta
  PROCEDURE crct_gta(p_mrkt_id               IN mrkt.mrkt_id%TYPE,
                     p_sls_perd_id           IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                     p_sls_aloc_auto_stus_id IN CHAR,
                     p_sls_aloc_manl_stus_id IN CHAR,
                     p_forc_mtch_ind         IN CHAR,
                     p_user_id               IN VARCHAR2 DEFAULT NULL,
                     p_run_id                IN NUMBER DEFAULT NULL) IS
    -- local variables
    v_gta_ovr_rid dly_bilng_offr_sku_line.gta_ovrrd_ind%TYPE;
    -- GTA FIX
    CURSOR cra_gta_ovride IS
      SELECT dly_bilng_id,
             new_dly_comsn_amt,
             new_dly_tax_amt,
             dms_update,
             offr_sku_line_id,
             sls_perd_id,
             sls_typ_id,
             dms_comsn_amt,
             dms_tax_amt,
             CASE
               WHEN dms_unit_qty * estimate_sls_prc_amt /
                    estimate_nr_for_qty = 0 THEN
                0
               ELSE
                (dms_unit_qty * estimate_sls_prc_amt / estimate_nr_for_qty -
                dms_comsn_amt - dms_tax_amt) /
                (dms_unit_qty * estimate_sls_prc_amt / estimate_nr_for_qty)
             END new_dms_gta,
             dbosl_id,
             dms_id
        FROM (SELECT dly_bilng_id,
                     new_dly_comsn_amt,
                     new_dly_tax_amt,
                     CASE
                       WHEN first_dly_bilng_id = dly_bilng_id THEN
                        'Y'
                       ELSE
                        'N'
                     END dms_update,
                     offr_sku_line_id,
                     sls_perd_id,
                     sls_typ_id,
                     old_dms_comsn_amt - dly_minus_comsn_amt +
                     dly_plus_comsn_amt dms_comsn_amt,
                     old_dms_tax_amt - dly_minus_tax_amt + dly_plus_tax_amt dms_tax_amt,
                     dms_unit_qty,
                     estimate_sls_prc_amt,
                     estimate_nr_for_qty,
                     dbosl_id,
                     dms_id
                FROM (SELECT offr_sku_line_id,
                             sls_perd_id,
                             sls_typ_id,
                             new_dly_comsn_amt,
                             new_dly_tax_amt,
                             old_dly_tax_amt,
                             old_dly_comsn_amt,
                             old_dms_comsn_amt,
                             old_dms_tax_amt,
                             COUNT(*) over(PARTITION BY offr_sku_line_id, sls_typ_id) db,
                             SUM(old_dly_comsn_amt) over(PARTITION BY offr_sku_line_id, sls_typ_id) dly_minus_comsn_amt,
                             SUM(old_dly_tax_amt) over(PARTITION BY offr_sku_line_id, sls_typ_id) dly_minus_tax_amt,
                             SUM(new_dly_comsn_amt) over(PARTITION BY offr_sku_line_id, sls_typ_id) dly_plus_comsn_amt,
                             0 dly_plus_tax_amt,
                             MIN(dly_bilng_id) over(PARTITION BY offr_sku_line_id, sls_typ_id) first_dly_bilng_id,
                             dly_bilng_id,
                             dms_unit_qty,
                             estimate_sls_prc_amt,
                             estimate_nr_for_qty,
                             dbosl_id,
                             dms_id
                        FROM (SELECT offr_sku_line_id,
                                     sls_perd_id,
                                     sls_typ_id,
                                     dly_bilng_id,
                                     dly_unit_qty * estimate_sls_prc_amt /
                                     estimate_nr_for_qty -
                                     dly_unit_qty * estimate_sls_prc_amt /
                                     estimate_nr_for_qty * estimate_gta new_dly_comsn_amt,
                                     0 new_dly_tax_amt,
                                     old_dly_tax_amt,
                                     old_dly_comsn_amt,
                                     old_dms_comsn_amt,
                                     old_dms_tax_amt,
                                     dms_unit_qty,
                                     estimate_sls_prc_amt,
                                     estimate_nr_for_qty,
                                     dbosl_id,
                                     dms_id
                                FROM (SELECT dstrbtd_mrkt_sls.offr_sku_line_id offr_sku_line_id,
                                             dstrbtd_mrkt_sls.sls_perd_id sls_perd_id,
                                             dstrbtd_mrkt_sls.sls_typ_id sls_typ_id,
                                             offr_prfl_prc_point.nr_for_qty estimate_nr_for_qty,
                                             offr_prfl_prc_point.sls_prc_amt estimate_sls_prc_amt,
                                             nvl(offr_prfl_prc_point.net_to_avon_fct,
                                                 0) estimate_gta,
                                             dly_bilng_trnd.dly_bilng_id dly_bilng_id,
                                             dly_bilng_trnd_offr_sku_line.unit_qty dly_unit_qty,
                                             dly_bilng_trnd_offr_sku_line.tax_amt old_dly_tax_amt,
                                             dly_bilng_trnd_offr_sku_line.comsn_amt old_dly_comsn_amt,
                                             dstrbtd_mrkt_sls.comsn_amt old_dms_comsn_amt,
                                             dstrbtd_mrkt_sls.tax_amt old_dms_tax_amt,
                                             dstrbtd_mrkt_sls.unit_qty dms_unit_qty,
                                             dstrbtd_mrkt_sls.rowid dms_id,
                                             dly_bilng_trnd_offr_sku_line.rowid dbosl_id
                                        FROM dly_bilng_trnd,
                                             dly_bilng_trnd_offr_sku_line,
                                             dstrbtd_mrkt_sls,
                                             offr_sku_line,
                                             offr_prfl_prc_point,
                                             mrkt_sls_aloctn_gta_ovrrd
                                       WHERE dly_bilng_trnd.mrkt_id = p_mrkt_id
                                         AND dly_bilng_trnd.trnd_sls_perd_id =
                                             p_sls_perd_id
                                         AND dly_bilng_trnd.dly_bilng_id =
                                             dly_bilng_trnd_offr_sku_line.dly_bilng_id
                                         AND dly_bilng_trnd_offr_sku_line.offr_sku_line_id =
                                             dstrbtd_mrkt_sls.offr_sku_line_id
                                         AND dstrbtd_mrkt_sls.sls_perd_id =
                                             p_sls_perd_id
                                         AND dstrbtd_mrkt_sls.mrkt_id =
                                             p_mrkt_id
                                         AND dstrbtd_mrkt_sls.offr_sku_line_id =
                                             offr_sku_line.offr_sku_line_id
                                         AND offr_sku_line.offr_prfl_prcpt_id =
                                             offr_prfl_prc_point.offr_prfl_prcpt_id
                                         AND dstrbtd_mrkt_sls.sls_typ_id IN
                                             (demand_actuals, billed_actuals)
                                         AND dstrbtd_mrkt_sls.sls_typ_id =
                                             dly_bilng_trnd_offr_sku_line.sls_typ_id
                                            -------------
                                         AND dly_bilng_trnd.trnd_aloctn_auto_stus_id =
                                             decode(p_sls_aloc_auto_stus_id,
                                                    -1,
                                                    dly_bilng_trnd.trnd_aloctn_auto_stus_id,
                                                    p_sls_aloc_auto_stus_id)
                                            -- paramater of the procedure
                                            -- SLS_ALOC_AUTO_STUS_ID
                                         AND dly_bilng_trnd.trnd_aloctn_manul_stus_id =
                                             decode(p_sls_aloc_manl_stus_id,
                                                    -1,
                                                    dly_bilng_trnd.trnd_aloctn_manul_stus_id,
                                                    p_sls_aloc_manl_stus_id)
                                            -- paramater of the procedure
                                            -- SLS_ALOC_MANL_STUS_ID
                                         AND dly_bilng_trnd_offr_sku_line.frc_mtch_ind =
                                             p_forc_mtch_ind
                                         AND dly_bilng_trnd.mrkt_id =
                                             mrkt_sls_aloctn_gta_ovrrd.mrkt_id
                                         AND dly_bilng_trnd.trnd_sls_perd_id BETWEEN
                                             mrkt_sls_aloctn_gta_ovrrd.strt_sls_perd_id AND
                                             nvl(mrkt_sls_aloctn_gta_ovrrd.end_sls_perd_id,
                                                 99990399)
                                         AND nvl(mrkt_sls_aloctn_gta_ovrrd.sls_typ_id,
                                                 dly_bilng_trnd_offr_sku_line.sls_typ_id) =
                                             dly_bilng_trnd_offr_sku_line.sls_typ_id
                                         AND nvl(mrkt_sls_aloctn_gta_ovrrd.lcl_bilng_actn_cd,
                                                 dly_bilng_trnd.lcl_bilng_actn_cd) =
                                             dly_bilng_trnd.lcl_bilng_actn_cd
                                         AND dly_bilng_trnd_offr_sku_line.gta_ovrrd_ind =
                                             v_gta_ovr_rid
                                      -------------
                                      ))
                       ORDER BY sls_typ_id, offr_sku_line_id));
    -- for LOG
    l_run_id         NUMBER := nvl(p_run_id,
                                   app_plsql_output.generate_new_run_id);
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'CRCT_GTA';
    l_parameter_list VARCHAR2(2048);
  BEGIN
    l_parameter_list := ' (p_mrkt_id:' || to_char(p_mrkt_id) || ', ' ||
                        'p_sls_perd_id: ' || to_char(p_sls_perd_id) || ', ' ||
                        'p_sls_aloc_auto_stus_id: ' ||
                        p_sls_aloc_auto_stus_id || ', ' ||
                        'p_sls_aloc_manl_stus_id: ' ||
                        p_sls_aloc_manl_stus_id || ', ' ||
                        'p_forc_mtch_ind: ' || p_forc_mtch_ind || ', ' ||
                        'p_user_id: ' || l_user_id || ', ' || 'p_run_id: ' ||
                        to_char(l_run_id) || ')';
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' GTA OVERRIDE start' ||
                       l_parameter_list);
    v_gta_ovr_rid := 'N';
    FOR cra_gta_ovr IN cra_gta_ovride LOOP
      UPDATE dly_bilng_trnd_offr_sku_line
         SET dly_bilng_trnd_offr_sku_line.comsn_amt     = cra_gta_ovr.new_dly_comsn_amt,
             dly_bilng_trnd_offr_sku_line.tax_amt       = cra_gta_ovr.new_dly_tax_amt,
             dly_bilng_trnd_offr_sku_line.gta_ovrrd_ind = 'Y'
       WHERE ROWID = cra_gta_ovr.dbosl_id;
    END LOOP;
  END crct_gta;

  -- --------------------------------------------------------------------------
  --
  -- Procedure: CHECK_VEH_SLS_CHNL (internal)
  --
  --
  --
  -- Checks whether a billing Sales Channel is valid for an offer Vehicle.
  --
  -- A vehicle/sales channel counts as a valid match if:
  --
  -- (a) there is a record in mrkt_veh_perd_lcl_chnl mapping the offer vehicle
  --
  --     to the billing sales channel OR
  --
  -- (b) there are no records in mrkt_veh_perd_lcl_chnl for the offer vehicle,
  --
  --     and no records mapping the billing sales channel to any other vehicle
  --
  --     of the same type ie planned/unplanned
  --
  --
  --
  --
  -- --------------------------------------------------------------------------
  --
  PROCEDURE check_veh_sls_chnl(p_mrkt_id             IN mrkt.mrkt_id%TYPE,
                               p_sls_perd_id         IN mrkt_trnd_sls_perd.trnd_sls_perd_id%TYPE,
                               p_veh_id              IN veh.veh_id%TYPE,
                               p_plnd_veh_ind        IN mrkt_veh_perd.plnd_veh_ind%TYPE,
                               p_sls_chnl_cd         IN dly_bilng_trnd.sls_chnl_cd%TYPE,
                               p_offr_perd_id        IN offr.offr_perd_id%TYPE,
                               p_sales_channel_match IN OUT dly_bilng_trnd_osl_audit.trnd_chnl_mtch_ind%TYPE,
                               p_sales_channel_used  IN OUT dly_bilng_trnd_osl_audit.trnd_chnl_used_ind%TYPE) IS
    l_plnd_veh_ind mrkt_veh_perd.plnd_veh_ind%TYPE := p_plnd_veh_ind;
  BEGIN
    IF l_plnd_veh_ind IS NULL THEN
      SELECT plnd_veh_ind
        INTO l_plnd_veh_ind
        FROM mrkt_veh_perd
       WHERE mrkt_id = p_mrkt_id
         AND veh_id = p_veh_id
         AND offr_perd_id = p_offr_perd_id;
    END IF;
    SELECT decode(COUNT(*), 0, 'N', 'Y')
      INTO p_sales_channel_match
      FROM mrkt_veh_perd_lcl_chnl
     WHERE mrkt_id = p_mrkt_id
       AND veh_id = p_veh_id
       AND lcl_chnl_id = p_sls_chnl_cd
       AND strt_sls_perd_id <= p_sls_perd_id
       AND (endg_sls_perd_id IS NULL OR endg_sls_perd_id >= p_sls_perd_id);
    p_sales_channel_used := 'N';
    IF p_sales_channel_match = 'N' THEN
      SELECT decode(COUNT(*), 0, 'N', 'Y')
        INTO p_sales_channel_used
        FROM mrkt_veh_perd_lcl_chnl lsc, mrkt_veh_perd mvp
       WHERE lsc.mrkt_id = p_mrkt_id
         AND (lsc.veh_id = p_veh_id OR (lcl_chnl_id = p_sls_chnl_cd AND
             mvp.plnd_veh_ind = l_plnd_veh_ind))
         AND mvp.mrkt_id = p_mrkt_id
         AND mvp.veh_id = lsc.veh_id
         AND mvp.offr_perd_id = p_offr_perd_id
         AND strt_sls_perd_id <= p_sls_perd_id
         AND (endg_sls_perd_id IS NULL OR endg_sls_perd_id >= p_sls_perd_id);
    END IF;
  END check_veh_sls_chnl;

  -- process_jobs
  PROCEDURE process_jobs(p_user_id IN VARCHAR2 DEFAULT NULL,
                         p_run_id  IN NUMBER DEFAULT NULL) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    -- local variables
    l_tbl_bi24             t_hist_detail := t_hist_detail();
    l_offst_lbl_id_on      NUMBER;
    l_offst_lbl_id_off     NUMBER;
    l_periods              r_periods;
    l_stus                 NUMBER;
    l_campaign_sls_perd_id mrkt_trnd_sls_perd.trnd_sls_perd_id%TYPE;
    -- for LOG
    l_run_id         NUMBER := nvl(p_run_id,
                                   app_plsql_output.generate_new_run_id);
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'PROCESS_JOBS';
    l_parameter_list VARCHAR2(2048);
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    ---------------------------------------
    -- sales camapaign / target campaign --
    ---------------------------------------
    FOR rec IN (SELECT mrkt_trnd_sls_perd_sls_typ.mrkt_id,
                       mrkt_trnd_sls_perd_sls_typ.sls_typ_id,
                       mrkt_trnd_sls_perd_sls_typ.trnd_sls_perd_id trg_perd_id,
                       mrkt_trnd_sls_perd_sls_typ.sct_prcsng_dt bilng_day,
                       mrkt_trnd_sls_perd_sls_typ.sct_cash_value_on,
                       mrkt_trnd_sls_perd_sls_typ.sct_cash_value_off
                  FROM mrkt_config_item,
                       mrkt_trnd_sls_perd_sls_typ
                 WHERE mrkt_config_item.mrkt_config_item_desc_txt = 'PA_TREND_ALLOCATION ENABLED'
                   AND mrkt_config_item.mrkt_config_item_val_txt = 'Y'
                   AND mrkt_config_item.mrkt_id = mrkt_trnd_sls_perd_sls_typ.mrkt_id
                   AND mrkt_trnd_sls_perd_sls_typ.sct_autclc_ind = 'Y'
                   AND mrkt_trnd_sls_perd_sls_typ.sls_typ_id IS NOT NULL
                 ORDER BY mrkt_trnd_sls_perd_sls_typ.mrkt_id,
                          mrkt_trnd_sls_perd_sls_typ.trnd_sls_perd_id,
                          mrkt_trnd_sls_perd_sls_typ.sls_typ_id) LOOP
      l_parameter_list := ' (rec: mrkt_id:' || to_char(rec.mrkt_id) || ', ' ||
                          'trg_perd_id: ' || to_char(rec.trg_perd_id) || ', ' ||
                          'sls_typ_id: ' || to_char(rec.sls_typ_id) || ', ' ||
                          'bilng_day: ' || to_char(rec.bilng_day) || ', ' ||
                          'p_user_id: ' || l_user_id || ', ' ||
                          'p_run_id: ' || to_char(l_run_id) || ')';
      app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
      app_plsql_log.info(l_module_name || ' - TREND SALES CALCULATE start' ||
                         l_parameter_list);
      UPDATE mrkt_trnd_sls_perd_sls_typ
         SET sct_aloctn_user_id = l_user_id,
             sct_aloctn_strt_ts = SYSDATE,
             sct_aloctn_end_ts  = NULL
       WHERE mrkt_id = rec.mrkt_id
         AND trnd_sls_perd_id = rec.trg_perd_id
         AND sls_typ_id = rec.sls_typ_id;
      COMMIT;
      -- get CAMPAIGN sls_perd_id
      BEGIN
        SELECT DISTINCT campaign_sls_perd_id
          INTO l_campaign_sls_perd_id
          FROM (SELECT mrkt_id,
                       sls_typ_id,
                       trg_perd_id,
                       campaign_sls_perd_id,
                       offst_lbl_id,
                       sls_typ_grp_nm,
                       eff_sls_perd_id,
                       lead(eff_sls_perd_id, 1) over(PARTITION BY offst_lbl_id, sls_typ_grp_nm ORDER BY eff_sls_perd_id) AS next_eff_sls_perd_id
                  FROM (SELECT DISTINCT rec.mrkt_id mrkt_id,
                                        rec.sls_typ_id sls_typ_id,
                                        rec.trg_perd_id trg_perd_id,
                                        pa_maps_public.perd_plus(rec.mrkt_id,
                                                                 rec.trg_perd_id,
                                                                 (-1) *
                                                                 offst_val_trgt_sls) campaign_sls_perd_id,
                                        offst_lbl_id,
                                        sls_typ_grp_nm,
                                        eff_sls_perd_id
                          FROM ta_config
                         WHERE mrkt_id = rec.mrkt_id
                           AND trgt_sls_typ_id = rec.sls_typ_id
                           AND upper(REPLACE(TRIM(sls_typ_grp_nm), '  ', ' ')) =
                               c_sls_typ_grp_nm_bi24
                           AND eff_sls_perd_id <=
                               pa_maps_public.perd_plus(rec.mrkt_id,
                                                        rec.trg_perd_id,
                                                        (-1) *
                                                        offst_val_trgt_sls)))
         WHERE campaign_sls_perd_id BETWEEN eff_sls_perd_id AND
               nvl(next_eff_sls_perd_id, campaign_sls_perd_id);
      EXCEPTION
        WHEN OTHERS THEN
          l_campaign_sls_perd_id := NULL;
      END;
      -- SAVE
      l_stus := 0;
      save_trend_alloctn(p_mrkt_id              => rec.mrkt_id,
                         p_campgn_perd_id       => l_campaign_sls_perd_id,
                         p_sls_typ_id           => rec.sls_typ_id,
                         p_bilng_day            => rec.bilng_day,
                         p_cash_value           => NULL,
                         p_r_factor             => NULL,
                         p_use_offers_on_sched  => NULL,
                         p_use_offers_off_sched => NULL,
                         p_user_id              => l_user_id,
                         p_run_id               => l_run_id,
                         p_stus                 => l_stus);
      IF l_stus = 0 THEN
        UPDATE mrkt_trnd_sls_perd_sls_typ
           SET sct_aloctn_end_ts = SYSDATE
         WHERE mrkt_id = rec.mrkt_id
           AND trnd_sls_perd_id = rec.trg_perd_id
           AND sls_typ_id = rec.sls_typ_id;
        COMMIT;
      ELSE
        ROLLBACK;
      END IF;
      app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
      app_plsql_log.info(l_module_name ||
                         ' - TREND SALES CALCULATE end, status_code: ' ||
                         to_char(l_stus) || l_parameter_list);
    END LOOP;
  END process_jobs;

  -- ----------------------------------------------------------------------- --
  -- Procedure: CLC_DLY_BILNG_DAYS                                           --
  --                                                                         --
  -- Collecting Daily billing days for the given sales period and the given --
  -- number of previous sales period                                         --
  -- ----------------------------------------------------------------------- --
  PROCEDURE clc_catgry_sls(p_mrkt_id     IN mrkt.mrkt_id%TYPE,
                           p_sls_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                           p_user_id     IN VARCHAR2 DEFAULT NULL,
                           p_run_id      IN NUMBER DEFAULT NULL) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    -- for LOG
    l_run_id         NUMBER := nvl(p_run_id,
                                   app_plsql_output.generate_new_run_id);
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'CLC_CATGRY_SLS';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id:' || to_char(p_mrkt_id) || ', ' ||
                                       'p_sls_perd_id: ' ||
                                       to_char(p_sls_perd_id) || ', ' ||
                                       'p_user_id: ' || l_user_id || ', ' ||
                                       'p_run_id: ' || to_char(l_run_id) || ')';
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start' || l_parameter_list);
    BEGIN
      DELETE trnd_bilng_days
       WHERE mrkt_id = p_mrkt_id
         AND sls_perd_id = p_sls_perd_id;
      INSERT INTO trnd_bilng_days
        (mrkt_id, sls_perd_id, day_num, prcsng_dt)
        SELECT mrkt_id,
               sls_perd_id,
               row_number() over(PARTITION BY mrkt_id, sls_perd_id ORDER BY prcsng_dt) day_num,
               prcsng_dt
          FROM (SELECT DISTINCT mrkt_id,
                                trnd_sls_perd_id sls_perd_id,
                                trunc(prcsng_dt) prcsng_dt
                  FROM dly_bilng_trnd)
         WHERE mrkt_id = p_mrkt_id
           AND sls_perd_id = p_sls_perd_id;
      --
      COMMIT;
    EXCEPTION
      WHEN OTHERS THEN
        ROLLBACK;
        app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
        app_plsql_log.info(l_module_name || ' FAILED, error code: ' ||
                           SQLCODE || ' error message: ' || SQLERRM ||
                           l_parameter_list);
    END;
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' end' || l_parameter_list);
    --
  END;

  -- ----------------------------------------------------------------------- --
  -- Procedure: PROCS_MULTMTCH (internal)                                     --
  --                                                                         --
  -- Updates the DLY_BILNG_OFFR_SKU_LINE unit, tax, comsn values on          --
  -- MULTIMATCH records. In OSL  level the quantities are recalculated
  --
  -- by the ratio stored in DMS between the given OSL                        --
  -- The first rows on DLY_BILNG_OFFR_SKU_LINE records contains all unit,    --
  -- the others became 0.                                                    --
  -- ----------------------------------------------------------------------- --
  PROCEDURE procs_multmtch(p_mrkt_id     IN mrkt.mrkt_id%TYPE,
                           p_sls_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                           p_sls_typ_id  IN dly_bilng_offr_sku_line.sls_typ_id%TYPE) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    FOR rec IN (SELECT dly_bilng_trnd.dly_bilng_id,
                       dly_bilng_trnd.unit_qty,
                       dly_bilng_trnd.comsn_amt,
                       dly_bilng_trnd.tax_amt,
                       dly_bilng_trnd_offr_sku_line.unit_qty osl_unit_qty,
                       COUNT(*) over(PARTITION BY dly_bilng_trnd_offr_sku_line.dly_bilng_id) cc,
                       SUM(dly_bilng_trnd_offr_sku_line.unit_qty) over(PARTITION BY dly_bilng_trnd_offr_sku_line.dly_bilng_id) uu,
                       dly_bilng_trnd_offr_sku_line.offr_sku_line_id
                  FROM dly_bilng_trnd, dly_bilng_trnd_offr_sku_line
                 WHERE dly_bilng_trnd.mrkt_id = p_mrkt_id
                   AND dly_bilng_trnd.trnd_sls_perd_id = p_sls_perd_id
                   AND dly_bilng_trnd_offr_sku_line.sls_typ_id =
                       p_sls_typ_id
                   AND dly_bilng_trnd.trnd_aloctn_manul_stus_id =
                       manual_not_processed
                   AND dly_bilng_trnd.dly_bilng_id =
                       dly_bilng_trnd_offr_sku_line.dly_bilng_id) LOOP
      IF rec.uu > 0 THEN
        UPDATE dly_bilng_trnd_offr_sku_line
           SET unit_qty  = rec.unit_qty * (rec.osl_unit_qty / rec.uu),
               comsn_amt = rec.comsn_amt * (rec.osl_unit_qty / rec.uu),
               tax_amt   = rec.tax_amt * (rec.osl_unit_qty / rec.uu)
         WHERE dly_bilng_id = rec.dly_bilng_id
           AND sls_typ_id = p_sls_typ_id
           AND offr_sku_line_id = rec.offr_sku_line_id;
      ELSE
        UPDATE dly_bilng_trnd_offr_sku_line
           SET unit_qty  = rec.unit_qty / rec.cc,
               comsn_amt = rec.comsn_amt / rec.cc,
               tax_amt   = rec.tax_amt / rec.cc
         WHERE dly_bilng_id = rec.dly_bilng_id
           AND sls_typ_id = p_sls_typ_id
           AND offr_sku_line_id = rec.offr_sku_line_id;
      END IF;
      --
      COMMIT;
      --
    END LOOP;
  END procs_multmtch;

  -- ----------------------------------------------------------------------- --
  -- Procedure: AUTO_PROCS (external)                                  --
  --                                                                         --
  -- Trend Allocation auto-match process and retrieval of control totals     --
  -- ----------------------------------------------------------------------- --
  PROCEDURE auto_procs(p_mrkt_id     IN mrkt.mrkt_id%TYPE,
                       p_sls_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                       p_user_nm     IN VARCHAR,
                       p_user_id     IN VARCHAR2 DEFAULT NULL,
                       p_run_id      IN NUMBER DEFAULT NULL) IS
    TYPE t_dly_bilng_id IS TABLE OF dly_bilng_trnd.dly_bilng_id%TYPE;
    TYPE t_sku_id IS TABLE OF sku.sku_id%TYPE INDEX BY BINARY_INTEGER;
    TYPE t_bilng_line_nr IS TABLE OF CHAR(1) INDEX BY dly_bilng_trnd.bilng_line_nr%TYPE;
    l_use_clstr_lvl_fsc_sku_ind    mrkt_eff_trnd_sls_perd.use_clstr_lvl_fsc_sku_ind%TYPE;
    l_dly_bilng_mtch_id            mrkt_eff_trnd_sls_perd.dly_bilng_mtch_id%TYPE;
    l_unit_prc_mtch_ind            mrkt_eff_trnd_sls_perd.unit_prc_mtch_ind%TYPE;
    l_unit_prc_auto_mtch_tolr_amt  mrkt_eff_trnd_sls_perd.unit_prc_auto_mtch_tolr_amt%TYPE;
    l_unit_prc_manul_mtch_tolr_amt mrkt_eff_trnd_sls_perd.unit_prc_manul_mtch_tolr_amt%TYPE;
    l_line_nr_used_ind             mrkt_eff_trnd_sls_perd.line_nr_used_ind%TYPE;
    l_trnd_aloctn_auto_strt_ts     mrkt_trnd_sls_perd.trnd_aloctn_auto_strt_ts%TYPE := SYSDATE;
    control_parameters_ok          BOOLEAN := TRUE;
    max_tolerance                  NUMBER;
    match_method                   NUMBER;
    sku_list                       t_sku_id;
    sku_list_temp                  t_sku_id;
    i                              NUMBER;
    sales_channel_match            CHAR(1);
    sales_channel_used             CHAR(1);
    planned_matches                NUMBER;
    num_direct_matches             NUMBER;
    num_suggested_matches          NUMBER;
    first_direct_sku               sku.sku_id%TYPE;
    last_direct_sku                sku.sku_id%TYPE;
    first_suggested_sku            sku.sku_id%TYPE;
    last_suggested_sku             sku.sku_id%TYPE;
    num_direct_lnm_matches         NUMBER;
    num_suggested_lnm_matches      NUMBER;
    direct_lnm_sku                 sku.sku_id%TYPE;
    matched_sku                    sku.sku_id%TYPE;
    match_type                     CHAR(1);
    delete_reason                  dly_bilng_trnd_osl_temp.del_resn_cd%TYPE;
    auto_status                    dly_bilng_trnd.trnd_aloctn_auto_stus_id%TYPE;
    l_veh_id                       dstrbtd_mrkt_sls.veh_id%TYPE;
    reset_records                  t_dly_bilng_id;
    reset_records_temp             t_dly_bilng_id;
    line_nr_duplicates             t_bilng_line_nr;
    duplicate_line_nr              NUMBER;
    last_loaded_dly_bilng_id       NUMBER := 0;
    -- for LOG
    l_run_id         NUMBER := nvl(p_run_id,
                                   app_plsql_output.generate_new_run_id);
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'AUTO_PROCS';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id:' || to_char(p_mrkt_id) || ', ' ||
                                       'p_sls_perd_id: ' ||
                                       to_char(p_sls_perd_id) || ', ' ||
                                       'p_user_nm: ' || p_user_nm || ', ' ||
                                       'p_user_id: ' || l_user_id || ', ' ||
                                       'p_run_id: ' || to_char(l_run_id) || ')';
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    -- AUTO_PROCS Procedure
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start' || l_parameter_list);
    -- read process control parameters
    -- For example the tolerance level
    BEGIN
      -- PREPARE DATA IN THE DLY_BILNG_TRND TABLE START
      SELECT nvl(MAX(dly_bilng_id), -1)
        INTO last_loaded_dly_bilng_id
        FROM dly_bilng_trnd
       WHERE mrkt_id = p_mrkt_id
         AND trnd_sls_perd_id = p_sls_perd_id;
      --
      INSERT INTO dly_bilng_trnd
        (dly_bilng_id,
         mrkt_id,
         sls_chnl_cd,
         trnd_sls_perd_id,
         offr_perd_id,
         lcl_bilng_actn_cd,
         lcl_bilng_tran_typ,
         fsc_cd,
         sls_prc_amt,
         nr_for_qty,
         prcsng_dt,
         crncy_cd,
         lcl_bilng_offr_typ,
         unit_qty,
         comsn_amt,
         tax_amt,
         bilng_line_nr,
         reg_prc_amt,
         mlpln_cd,
         lcl_bilng_defrd_cd,
         lcl_bilng_shpng_cd,
         sbsttd_fsc_cd,
         sbsttd_bilng_line_nr,
         bilng_plnd_ind,
         trnd_aloctn_auto_stus_id,
         trnd_aloctn_manul_stus_id,
         trnd_cls_vld_ind,
         creat_user_id,
         creat_ts,
         last_updt_user_id,
         last_updt_ts,
         sku_id,
         offr_sku_line_id,
         finshd_gds_cd,
         sku_mtch_mthd_id)
        SELECT dly_bilng_id,
               mrkt_id,
               sls_chnl_cd,
               sls_perd_id,
               offr_perd_id,
               lcl_bilng_actn_cd,
               lcl_bilng_tran_typ,
               fsc_cd,
               sls_prc_amt,
               nr_for_qty,
               prcsng_dt,
               crncy_cd,
               lcl_bilng_offr_typ,
               unit_qty,
               comsn_amt,
               tax_amt,
               bilng_line_nr,
               reg_prc_amt,
               mlpln_cd,
               lcl_bilng_defrd_cd,
               lcl_bilng_shpng_cd,
               sbsttd_fsc_cd,
               sbsttd_bilng_line_nr,
               bilng_plnd_ind,
               0,
               0,
               NULL,
               creat_user_id,
               creat_ts,
               last_updt_user_id,
               last_updt_ts,
               NULL,
               NULL,
               NULL,
               NULL
          FROM dly_bilng
         WHERE mrkt_id = p_mrkt_id
           AND sls_perd_id = p_sls_perd_id
           AND (last_loaded_dly_bilng_id IS NULL OR
               dly_bilng_id > last_loaded_dly_bilng_id);
      COMMIT;
      --
      app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
      app_plsql_log.info(l_module_name ||
                         ' TREND DLY_BILNG DATA LOAD FINISHED FROM_ID: ' ||
                         last_loaded_dly_bilng_id || l_parameter_list);
      -- PREPARE DATA IN THE DLY_BILNG_TRND TABLE END
      trnd_aloctn_start(p_mrkt_id,
                        p_sls_perd_id,
                        p_user_nm,
                        l_trnd_aloctn_auto_strt_ts);
      SELECT use_clstr_lvl_fsc_sku_ind,
             dly_bilng_mtch_id,
             unit_prc_mtch_ind,
             unit_prc_auto_mtch_tolr_amt,
             unit_prc_manul_mtch_tolr_amt,
             line_nr_used_ind
        INTO l_use_clstr_lvl_fsc_sku_ind,
             l_dly_bilng_mtch_id,
             l_unit_prc_mtch_ind,
             l_unit_prc_auto_mtch_tolr_amt,
             l_unit_prc_manul_mtch_tolr_amt,
             l_line_nr_used_ind
        FROM mrkt_eff_trnd_sls_perd
       WHERE mrkt_id = p_mrkt_id
         AND eff_trnd_sls_perd_id =
             (SELECT MAX(eff_trnd_sls_perd_id)
                FROM mrkt_eff_trnd_sls_perd
               WHERE mrkt_id = p_mrkt_id
                 AND eff_trnd_sls_perd_id <= p_sls_perd_id);
      max_tolerance := greatest(l_unit_prc_auto_mtch_tolr_amt,
                                l_unit_prc_manul_mtch_tolr_amt);
      app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
      app_plsql_log.info(l_module_name || ' Cluster Level: ' ||
                         l_use_clstr_lvl_fsc_sku_ind || l_parameter_list);
      app_plsql_log.info(l_module_name || ' Billing Match: ' ||
                         l_dly_bilng_mtch_id || l_parameter_list);
      app_plsql_log.info(l_module_name || ' Unit Price Match: ' ||
                         l_unit_prc_mtch_ind || l_parameter_list);
      app_plsql_log.info(l_module_name || ' Auto Tolerance: ' ||
                         l_unit_prc_auto_mtch_tolr_amt || l_parameter_list);
      app_plsql_log.info(l_module_name || ' Manual Tolerance: ' ||
                         l_unit_prc_manul_mtch_tolr_amt ||
                         l_parameter_list);
      app_plsql_log.info(l_module_name || ' Line Number Check: ' ||
                         l_line_nr_used_ind || l_parameter_list);
    END;
    -- RESET OF DAILY BILLING TABLE STATUSES STARTED
    -- OSL = Offer Sku Line
    -- look for existing suggested matches where OSL has since been deleted
    SELECT DISTINCT db.dly_bilng_id
      BULK COLLECT
      INTO reset_records_temp
      FROM dly_bilng_trnd db, dly_bilng_trnd_offr_sku_line dsl
     WHERE db.mrkt_id = p_mrkt_id
       AND db.trnd_sls_perd_id = p_sls_perd_id
       AND db.trnd_aloctn_auto_stus_id IN
           (auto_suggested_single, auto_suggested_multi)
       AND db.trnd_aloctn_manul_stus_id = manual_not_processed
       AND dsl.dly_bilng_id = db.dly_bilng_id
       AND NOT EXISTS
     (SELECT offr_sku_line_id
              FROM offr_sku_line osl, offr o
             WHERE osl.offr_sku_line_id = dsl.offr_sku_line_id
               AND osl.dltd_ind <> 'Y'
               AND o.offr_id = osl.offr_id
               AND o.mrkt_id = db.mrkt_id
               AND o.offr_perd_id = db.offr_perd_id
               AND o.ver_id = 0
               AND o.offr_typ = 'CMP');
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' find deleted OSLs: ' ||
                       reset_records_temp.count || l_parameter_list);
    reset_records := reset_records_temp;
    -- look for existing suggested matches where the Offer price has since been
    -- changed beyond tolerance
    SELECT DISTINCT db.dly_bilng_id
      BULK COLLECT
      INTO reset_records_temp
      FROM dly_bilng_trnd               db,
           dly_bilng_trnd_offr_sku_line dsl,
           offr_sku_line                osl,
           offr_prfl_prc_point          opp
     WHERE db.mrkt_id = p_mrkt_id
       AND db.trnd_sls_perd_id = p_sls_perd_id
       AND db.trnd_aloctn_auto_stus_id IN
           (auto_suggested_single, auto_suggested_multi)
       AND db.trnd_aloctn_manul_stus_id = manual_not_processed
       AND dsl.dly_bilng_id = db.dly_bilng_id
       AND osl.offr_sku_line_id = dsl.offr_sku_line_id
       AND opp.offr_prfl_prcpt_id = osl.offr_prfl_prcpt_id
       AND NOT ((db.nr_for_qty = opp.nr_for_qty AND
            abs(db.sls_prc_amt - opp.sls_prc_amt) <=
            l_unit_prc_manul_mtch_tolr_amt) OR
            (l_unit_prc_mtch_ind = 'Y' AND
            abs((db.sls_prc_amt / db.nr_for_qty) -
                     (opp.sls_prc_amt / opp.nr_for_qty)) <=
            l_unit_prc_manul_mtch_tolr_amt));
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' find price-change OSLs: ' ||
                       reset_records_temp.count || l_parameter_list);
    reset_records := reset_records MULTISET UNION DISTINCT
                     reset_records_temp;
    IF control_parameters_ok THEN
      app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
      app_plsql_log.info(l_module_name || ' running auto-match for ' ||
                         p_mrkt_id || '/' || p_sls_perd_id ||
                         l_parameter_list);
      -- record start of Sales Allocation in mrkt_trnd_perd table
      -- identify MANUAL_NOT_SUGGESTED and MANUAL_EXCLUDED records
      -- if re-processing requested
      SELECT DISTINCT dly_bilng_id
        BULK COLLECT
        INTO reset_records_temp
        FROM dly_bilng_trnd
       WHERE mrkt_id = p_mrkt_id
         AND trnd_sls_perd_id = p_sls_perd_id
         AND ((trnd_aloctn_manul_stus_id = manual_not_suggested) OR
             (trnd_aloctn_manul_stus_id = manual_excluded));
      app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
      app_plsql_log.info(l_module_name ||
                         ' reset manual exluded/not_suggested: ' ||
                         reset_records_temp.count || l_parameter_list);
      reset_records := reset_records MULTISET UNION DISTINCT
                       reset_records_temp;
      -- record start of Sales Allocation in mrkt_trnd_perd table
      -- identify MANUAL_MATCHED (UNPLANNED) records
      -- if re-processing requested
      SELECT DISTINCT dly_bilng_id
        BULK COLLECT
        INTO reset_records_temp
        FROM dly_bilng_trnd
       WHERE mrkt_id = p_mrkt_id
         AND trnd_sls_perd_id = p_sls_perd_id
         AND trnd_aloctn_auto_stus_id = auto_not_processed
         AND trnd_aloctn_manul_stus_id = manual_matched;
      app_plsql_log.info(l_module_name ||
                         ' reset manual_matched (UNPLANNED): ' ||
                         reset_records_temp.count || l_parameter_list);
      reset_records := reset_records MULTISET UNION DISTINCT
                       reset_records_temp;
      -- reset all identified daily billing records for re-processing
      app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
      app_plsql_log.info(l_module_name || ' update reset records' ||
                         l_parameter_list);
      FOR i IN 1 .. reset_records.count LOOP
        UPDATE dly_bilng_trnd
           SET trnd_aloctn_auto_stus_id  = auto_not_processed,
               trnd_aloctn_manul_stus_id = manual_not_processed
         WHERE dly_bilng_id = reset_records(i);
        DELETE dly_bilng_trnd_offr_sku_line
         WHERE dly_bilng_id = reset_records(i);
      END LOOP;
      reset_records_temp.delete;
      reset_records.delete;
      -- RESET OF DAILY BILLING TABLE STATUSES ENDED
      -- exclude any unplanned daily billing records
      UPDATE dly_bilng_trnd
         SET trnd_aloctn_auto_stus_id = auto_excluded,
             last_updt_user_id        = p_user_nm,
             last_updt_ts             = SYSDATE
       WHERE mrkt_id = p_mrkt_id
         AND trnd_sls_perd_id = p_sls_perd_id
         AND trnd_aloctn_auto_stus_id = auto_not_processed
         AND trnd_aloctn_manul_stus_id = manual_not_processed
         AND bilng_plnd_ind = 'N';
      app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
      app_plsql_log.info(l_module_name || ' exclude unplanned: ' ||
                         SQL%ROWCOUNT || l_parameter_list);
      -- exclude any records which do not have demand/billed sales types on
      -- control table
      UPDATE dly_bilng_trnd db
         SET trnd_aloctn_auto_stus_id = auto_excluded,
             last_updt_user_id        = p_user_nm,
             last_updt_ts             = SYSDATE
       WHERE mrkt_id = p_mrkt_id
         AND trnd_sls_perd_id = p_sls_perd_id
         AND trnd_aloctn_auto_stus_id = auto_not_processed
         AND trnd_aloctn_manul_stus_id = manual_not_processed
         AND NOT EXISTS
       (SELECT *
                FROM dly_bilng_trnd_cntrl
               WHERE dly_bilng_mtch_id = l_dly_bilng_mtch_id
                 AND sls_typ_id IN (demand_actuals, billed_actuals)
                 AND nvl(lcl_bilng_actn_cd, db.lcl_bilng_actn_cd) =
                     db.lcl_bilng_actn_cd
                 AND nvl(lcl_bilng_tran_typ, db.lcl_bilng_tran_typ) =
                     db.lcl_bilng_tran_typ
                 AND nvl(lcl_bilng_offr_typ, db.lcl_bilng_offr_typ) =
                     db.lcl_bilng_offr_typ
                 AND nvl(lcl_bilng_defrd_cd, db.lcl_bilng_defrd_cd) =
                     db.lcl_bilng_defrd_cd
                 AND nvl(lcl_bilng_shpng_cd, db.lcl_bilng_shpng_cd) =
                     db.lcl_bilng_shpng_cd);
      app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
      app_plsql_log.info(l_module_name || ' exclude no demand/billed: ' ||
                         SQL%ROWCOUNT || l_parameter_list);
      -- Set status for auto-process override records
      UPDATE dly_bilng_trnd db
         SET (trnd_aloctn_auto_stus_id, trnd_aloctn_manul_stus_id) =
             (SELECT trnd_aloctn_auto_stus_id, trnd_aloctn_manul_stus_id
                FROM trnd_aloctn_auto_prcs_ovrrd
               WHERE mrkt_id = db.mrkt_id
                 AND sls_chnl_cd = db.sls_chnl_cd
                 AND lcl_bilng_offr_typ = db.lcl_bilng_offr_typ
                 AND strt_trnd_sls_perd_id <= db.trnd_sls_perd_id
                 AND (end_trnd_sls_perd_id IS NULL OR
                     end_trnd_sls_perd_id >= db.trnd_sls_perd_id))
       WHERE mrkt_id = p_mrkt_id
         AND trnd_sls_perd_id = p_sls_perd_id
         AND trnd_aloctn_auto_stus_id = auto_not_processed
         AND trnd_aloctn_manul_stus_id = manual_not_processed
         AND EXISTS
       (SELECT *
                FROM trnd_aloctn_auto_prcs_ovrrd
               WHERE mrkt_id = db.mrkt_id
                 AND sls_chnl_cd = db.sls_chnl_cd
                 AND lcl_bilng_offr_typ = db.lcl_bilng_offr_typ
                 AND strt_trnd_sls_perd_id <= db.trnd_sls_perd_id
                 AND (end_trnd_sls_perd_id IS NULL OR
                     end_trnd_sls_perd_id >= db.trnd_sls_perd_id));
      app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
      app_plsql_log.info(l_module_name || ' exclude overrides: ' ||
                         SQL%ROWCOUNT || l_parameter_list);
      -- initialise auto-status and null out sku_id/sls_cls flag for records
      -- which are unprocessed,
      -- not suggested or where no sku id was found last time
      UPDATE dly_bilng_trnd
         SET sku_id                   = NULL,
             trnd_cls_vld_ind         = NULL,
             trnd_aloctn_auto_stus_id = auto_not_processed
       WHERE mrkt_id = p_mrkt_id
         AND trnd_sls_perd_id = p_sls_perd_id
         AND (trnd_aloctn_auto_stus_id = auto_no_fsc_to_item OR
             (trnd_aloctn_manul_stus_id = manual_not_processed AND
             trnd_aloctn_auto_stus_id IN
             (auto_not_processed, auto_no_suggested) AND
             sku_id IS NOT NULL));
      app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
      app_plsql_log.info(l_module_name || ' initialise unprocessed: ' ||
                         SQL%ROWCOUNT || l_parameter_list);
      -- record list of duplicate line numbers so they can be excluded during
      -- line number checking later on
      IF l_line_nr_used_ind = 'Y' THEN
        FOR rec IN (SELECT DISTINCT bln.bilng_line_nr
                      FROM offr               o,
                           offr_sku_line      osl,
                           bilng_line_nr_data bln,
                           dstrbtd_mrkt_sls   dms
                     WHERE dms.mrkt_id = p_mrkt_id
                       AND dms.sls_perd_id = p_sls_perd_id
                       AND bln.mrkt_id = dms.mrkt_id
                       AND bln.offr_sku_id = dms.offr_sku_line_id
                       AND bln.bilng_line_nr IS NOT NULL
                       AND osl.offr_sku_line_id = dms.offr_sku_line_id
                       AND osl.dltd_ind <> 'Y'
                       AND o.offr_id = osl.offr_id
                       AND o.offr_typ = 'CMP'
                       AND o.ver_id = 0
                     GROUP BY bln.bilng_line_nr,
                              bln.offr_perd_id,
                              dms.sls_typ_id
                    HAVING COUNT(*) > 1) LOOP
          line_nr_duplicates(rec.bilng_line_nr) := 'Y';
        END LOOP;
        app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
        app_plsql_log.info(l_module_name || ' duplicate line numbers: ' ||
                           line_nr_duplicates.count || l_parameter_list);
      END IF;
      DELETE dly_bilng_trnd_temp;
      DELETE dly_bilng_trnd_osl_temp;
      -- AUTOMATCH PROCESS STARTS
      -- auto-match process starts here
      app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
      app_plsql_log.info(l_module_name || ' auto-match start' ||
                         l_parameter_list);
      FOR db_rec IN (SELECT dly_bilng_id,
                            mrkt_id,
                            trnd_sls_perd_id,
                            offr_perd_id,
                            offr_sku_line_id,
                            finshd_gds_cd,
                            fsc_cd,
                            sls_chnl_cd,
                            sls_prc_amt,
                            nr_for_qty,
                            bilng_line_nr,
                            unit_qty,
                            comsn_amt,
                            tax_amt,
                            (SELECT decode(MIN(ROWID), NULL, 'N', 'Y')
                               FROM dly_bilng_trnd_cntrl
                              WHERE dly_bilng_mtch_id = l_dly_bilng_mtch_id
                                AND sls_typ_id = demand_actuals
                                AND nvl(lcl_bilng_actn_cd,
                                        db.lcl_bilng_actn_cd) =
                                    db.lcl_bilng_actn_cd
                                AND nvl(lcl_bilng_tran_typ,
                                        db.lcl_bilng_tran_typ) =
                                    db.lcl_bilng_tran_typ
                                AND nvl(lcl_bilng_offr_typ,
                                        db.lcl_bilng_offr_typ) =
                                    db.lcl_bilng_offr_typ
                                AND nvl(lcl_bilng_defrd_cd,
                                        db.lcl_bilng_defrd_cd) =
                                    db.lcl_bilng_defrd_cd
                                AND nvl(lcl_bilng_shpng_cd,
                                        db.lcl_bilng_shpng_cd) =
                                    db.lcl_bilng_shpng_cd) demand_ind,
                            (SELECT decode(MIN(ROWID), NULL, 'N', 'Y')
                               FROM dly_bilng_trnd_cntrl
                              WHERE dly_bilng_mtch_id = l_dly_bilng_mtch_id
                                AND sls_typ_id = billed_actuals
                                AND nvl(lcl_bilng_actn_cd,
                                        db.lcl_bilng_actn_cd) =
                                    db.lcl_bilng_actn_cd
                                AND nvl(lcl_bilng_tran_typ,
                                        db.lcl_bilng_tran_typ) =
                                    db.lcl_bilng_tran_typ
                                AND nvl(lcl_bilng_offr_typ,
                                        db.lcl_bilng_offr_typ) =
                                    db.lcl_bilng_offr_typ
                                AND nvl(lcl_bilng_defrd_cd,
                                        db.lcl_bilng_defrd_cd) =
                                    db.lcl_bilng_defrd_cd
                                AND nvl(lcl_bilng_shpng_cd,
                                        db.lcl_bilng_shpng_cd) =
                                    db.lcl_bilng_shpng_cd) billed_ind
                       FROM dly_bilng_trnd db
                      WHERE db.mrkt_id = p_mrkt_id
                        AND db.trnd_sls_perd_id = p_sls_perd_id
                        AND db.trnd_aloctn_auto_stus_id = auto_not_processed) LOOP
        sku_list.delete;
        planned_matches := 0;
        auto_status     := auto_not_processed;
        matched_sku     := NULL;
        match_method    := NULL;
        -- first try and match by osl_id if it exists
        IF db_rec.offr_sku_line_id IS NOT NULL THEN
          BEGIN
            SELECT sku_id, veh_id
              INTO matched_sku, l_veh_id
              FROM offr_sku_line
             WHERE offr_sku_line_id = db_rec.offr_sku_line_id
               AND mrkt_id = p_mrkt_id
               AND offr_perd_id = db_rec.offr_perd_id
               AND dltd_ind != 'Y';
          EXCEPTION
            WHEN no_data_found THEN
              NULL;
          END;
          IF matched_sku IS NOT NULL THEN
            -- check vehicle/sales channel mapping
            check_veh_sls_chnl(p_mrkt_id,
                               p_sls_perd_id,
                               l_veh_id,
                               NULL,
                               db_rec.sls_chnl_cd,
                               db_rec.offr_perd_id,
                               sales_channel_match,
                               sales_channel_used);
            IF sales_channel_match = 'Y'
               OR sales_channel_used = 'N' THEN
              match_method := osl_match;
              auto_status  := auto_matched;
              INSERT INTO dly_bilng_trnd_osl_temp
                (dly_bilng_id,
                 demand_ind,
                 billed_ind,
                 unit_qty,
                 comsn_amt,
                 tax_amt,
                 sku_id,
                 offr_sku_line_id,
                 veh_id,
                 sls_chnl_mtch_ind,
                 sls_chnl_used_ind,
                 auto_mtch_ind)
              VALUES
                (db_rec.dly_bilng_id,
                 db_rec.demand_ind,
                 db_rec.billed_ind,
                 db_rec.unit_qty,
                 db_rec.comsn_amt,
                 db_rec.tax_amt,
                 matched_sku,
                 db_rec.offr_sku_line_id,
                 l_veh_id,
                 sales_channel_match,
                 sales_channel_used,
                 'Y');
            ELSE
              matched_sku := NULL;
            END IF; -- vehicle/sales channel check
          END IF; -- found OSL record
        END IF;
        -- match sku by OSL
        -- if no match was found using OSL then try FGC then FSC
        WHILE auto_status = auto_not_processed LOOP
          -- if we have a FINSHD_GDS_CD and haven't tried matching with it yet     
          IF db_rec.finshd_gds_cd IS NOT NULL
             AND match_method IS NULL THEN
            match_method := fgc_match;
            -- find all possible skus for the FINISHD_GDS_CD used by the billing
            -- record
            FOR sku_rec IN (SELECT DISTINCT sku_id
                              FROM mrkt_fsc mf
                             WHERE finshd_gds_cd = db_rec.finshd_gds_cd
                               AND (l_use_clstr_lvl_fsc_sku_ind = 'Y' OR
                                   mrkt_id = db_rec.mrkt_id)
                               AND strt_perd_id =
                                   (SELECT MAX(strt_perd_id)
                                      FROM mrkt_fsc
                                     WHERE mrkt_id = mf.mrkt_id
                                       AND finshd_gds_cd = mf.finshd_gds_cd
                                       AND strt_perd_id <=
                                           db_rec.offr_perd_id)
                               AND dltd_ind != 'Y'
                               AND EXISTS
                             (SELECT *
                                      FROM mrkt_sku
                                     WHERE mrkt_id = db_rec.mrkt_id
                                       AND sku_id = mf.sku_id
                                       AND dltd_ind != 'Y')) LOOP
              sku_list(sku_list.count + 1) := sku_rec.sku_id;
            END LOOP;
          ELSE
            -- no matches found via FGC or no FGC given
            sku_list_temp.delete;
            match_method := fsc_match;
            -- find all possible skus for the FSC used by the billing record
            FOR sku_rec IN (SELECT DISTINCT sku_id,
                                            decode(mrkt_id, p_mrkt_id, 0, 1) this_market
                              FROM mrkt_fsc mf
                             WHERE fsc_cd = db_rec.fsc_cd
                               AND (l_use_clstr_lvl_fsc_sku_ind = 'Y' OR
                                   mrkt_id = db_rec.mrkt_id)
                               AND strt_perd_id =
                                   (SELECT MAX(strt_perd_id)
                                      FROM mrkt_fsc
                                     WHERE mrkt_id = mf.mrkt_id
                                       AND fsc_cd = mf.fsc_cd
                                       AND strt_perd_id <=
                                           db_rec.offr_perd_id)
                               AND dltd_ind != 'Y'
                               AND EXISTS
                             (SELECT *
                                      FROM mrkt_sku
                                     WHERE mrkt_id = db_rec.mrkt_id
                                       AND sku_id = mf.sku_id
                                       AND dltd_ind != 'Y')
                             ORDER BY this_market) LOOP
              IF NOT sku_list_temp.exists(sku_rec.sku_id) THEN
                sku_list_temp(sku_rec.sku_id) := sku_rec.sku_id;
                sku_list(sku_list.count + 1) := sku_rec.sku_id;
              END IF;
            END LOOP;
          END IF;
          -- match sku by FGC/FSC
          duplicate_line_nr := 0;
          IF line_nr_duplicates.exists(db_rec.bilng_line_nr) THEN
            duplicate_line_nr := 1;
          END IF;
          i := sku_list.first;
          WHILE i IS NOT NULL LOOP
            FOR osl_rec IN (SELECT offr_sku_line_id,
                                   sls_prc_amt,
                                   nr_for_qty,
                                   qty_diff,
                                   prc_diff,
                                   unit_prc_diff,
                                   veh_id,
                                   plnd_veh_ind,
                                   osl_line_nr,
                                   CASE
                                     WHEN duplicate_line_nr = 1 THEN
                                      'N'
                                     WHEN nvl(osl_line_nr, -1) =
                                          nvl(db_rec.bilng_line_nr, -1) THEN
                                      'Y'
                                     ELSE
                                      'N'
                                   END line_nr_match,
                                   dms_unit_qty
                              FROM (SELECT osl.offr_sku_line_id,
                                           opp.sls_prc_amt,
                                           opp.nr_for_qty,
                                           abs(db_rec.nr_for_qty -
                                               opp.nr_for_qty) qty_diff,
                                           abs(db_rec.sls_prc_amt -
                                               opp.sls_prc_amt) prc_diff,
                                           abs((db_rec.sls_prc_amt /
                                               db_rec.nr_for_qty) -
                                               (opp.sls_prc_amt /
                                               opp.nr_for_qty)) unit_prc_diff,
                                           o.veh_id,
                                           mvp.plnd_veh_ind,
                                           (SELECT bilng_line_nr
                                              FROM bilng_line_nr_data
                                             WHERE offr_sku_id =
                                                   osl.offr_sku_line_id) osl_line_nr,
                                           dms.unit_qty dms_unit_qty
                                      FROM dstrbtd_mrkt_sls    dms,
                                           offr_sku_line       osl,
                                           offr_prfl_prc_point opp,
                                           offr                o,
                                           mrkt_veh_perd       mvp
                                     WHERE osl.mrkt_id = db_rec.mrkt_id
                                       AND osl.offr_perd_id =
                                           db_rec.offr_perd_id
                                       AND osl.sku_id = sku_list(i)
                                       AND osl.dltd_ind != 'Y'
                                       AND dms.offr_sku_line_id =
                                           osl.offr_sku_line_id
                                       AND dms.sls_typ_id = 1
                                       AND dms.offr_perd_id = dms.sls_perd_id
                                       AND opp.offr_prfl_prcpt_id =
                                           osl.offr_prfl_prcpt_id
                                       AND o.offr_id = opp.offr_id
                                       AND o.ver_id = 0
                                       AND o.offr_typ = 'CMP'
                                       AND mvp.mrkt_id = o.mrkt_id
                                       AND mvp.veh_id = o.veh_id
                                       AND mvp.offr_perd_id = o.offr_perd_id)
                             WHERE (qty_diff = 0 AND
                                   prc_diff <= max_tolerance)
                                OR (l_unit_prc_mtch_ind = 'Y' AND
                                   unit_prc_diff <= max_tolerance)) LOOP
              delete_reason := NULL;
              match_type    := NULL;
              -- check vehicle/sales channel mapping
              check_veh_sls_chnl(p_mrkt_id,
                                 p_sls_perd_id,
                                 osl_rec.veh_id,
                                 osl_rec.plnd_veh_ind,
                                 db_rec.sls_chnl_cd,
                                 db_rec.offr_perd_id,
                                 sales_channel_match,
                                 sales_channel_used);
              -- work out if this is a valid match, and if it is 'direct' or '        
              -- suggested' based on tolerance values          
              IF sales_channel_match = 'Y'
                 OR sales_channel_used = 'N' THEN
                IF osl_rec.plnd_veh_ind = 'Y' THEN
                  planned_matches := planned_matches + 1;
                END IF;
                --match_type := NO_MATCH;
                match_type := direct;
                IF (osl_rec.qty_diff = 0 AND
                   osl_rec.prc_diff <= l_unit_prc_auto_mtch_tolr_amt)
                   OR
                   (l_unit_prc_mtch_ind = 'Y' AND
                   osl_rec.unit_prc_diff <= l_unit_prc_auto_mtch_tolr_amt) THEN
                  match_type := direct;
                ELSIF (osl_rec.qty_diff = 0 AND
                      osl_rec.prc_diff <= l_unit_prc_manul_mtch_tolr_amt)
                      OR (l_unit_prc_mtch_ind = 'Y' AND
                      osl_rec.unit_prc_diff <=
                      l_unit_prc_manul_mtch_tolr_amt) THEN
                  match_type := suggested;
                END IF;
              ELSE
                delete_reason := invalid_veh_sls_chnl;
              END IF;
              -- valid match
              -- record details of possible matching offer/sku
              INSERT INTO dly_bilng_trnd_osl_temp
                (dly_bilng_id,
                 demand_ind,
                 billed_ind,
                 unit_qty,
                 comsn_amt,
                 tax_amt,
                 sku_id,
                 offr_sku_line_id,
                 osl_line_nr,
                 line_nr_mtch_ind,
                 qty_diff,
                 prc_diff,
                 unit_prc_diff,
                 veh_id,
                 plnd_veh_ind,
                 sls_chnl_mtch_ind,
                 sls_chnl_used_ind,
                 mtch_typ_id,
                 del_resn_cd)
              VALUES
                (db_rec.dly_bilng_id,
                 db_rec.demand_ind,
                 db_rec.billed_ind, 
                 db_rec.unit_qty,  --osl_rec.dms_unit_qty,
                 db_rec.comsn_amt,
                 db_rec.tax_amt,
                 sku_list(i),
                 osl_rec.offr_sku_line_id,
                 osl_rec.osl_line_nr,
                 osl_rec.line_nr_match,
                 osl_rec.qty_diff,
                 osl_rec.prc_diff,
                 osl_rec.unit_prc_diff,
                 osl_rec.veh_id,
                 osl_rec.plnd_veh_ind,
                 sales_channel_match,
                 sales_channel_used,
                 match_type,
                 delete_reason);
            END LOOP;
            -- possible matching offers
            i := sku_list.next(i);
          END LOOP;
          -- possible skus for a dly_bilng_trnd id
          -- if we found no skus, set the status accordingly
          IF sku_list.count = 0 THEN
            -- only set NO_FSC status if matching by FSC, for FGC leave it as
            -- NOT_PROCESSED so we go round the loop again
            IF match_method = fsc_match THEN
              auto_status  := auto_no_fsc_to_item;
              match_method := NULL;
            END IF;
          ELSE
            -- if the billing id matches offers from planned vehicles, we can
            -- ignore any unplanned ones (all others)
            IF planned_matches > 0 THEN
              UPDATE dly_bilng_trnd_osl_temp
                 SET del_resn_cd = unplanned_offer
               WHERE dly_bilng_id = db_rec.dly_bilng_id
                 AND plnd_veh_ind = 'N'
                 AND del_resn_cd IS NULL;
            END IF;
            -- work out how many direct/suggested matches we've found + record          -- first/last skus used
            SELECT MIN(decode(mtch_typ_id, direct, sku_id, NULL)),
                   MAX(decode(mtch_typ_id, direct, sku_id, NULL)),
                   nvl(SUM(decode(mtch_typ_id, direct, 1, 0)), 0),
                   MIN(decode(mtch_typ_id, suggested, sku_id, NULL)),
                   MAX(decode(mtch_typ_id, suggested, sku_id, NULL)),
                   nvl(SUM(decode(mtch_typ_id, suggested, 1, 0)), 0)
              INTO first_direct_sku,
                   last_direct_sku,
                   num_direct_matches,
                   first_suggested_sku,
                   last_suggested_sku,
                   num_suggested_matches
              FROM dly_bilng_trnd_osl_temp
             WHERE dly_bilng_id = db_rec.dly_bilng_id
               AND del_resn_cd IS NULL;
            delete_reason := NULL;
            -- if we have one direct match then status is MATCHED and we don't          -- need to check anything else
            IF num_direct_matches = 1 THEN
              auto_status := auto_matched;
              matched_sku := first_direct_sku;
              UPDATE dly_bilng_trnd_osl_temp
                 SET auto_mtch_ind = 'Y'
               WHERE dly_bilng_id = db_rec.dly_bilng_id
                 AND mtch_typ_id = direct
                 AND del_resn_cd IS NULL;
              IF num_suggested_matches > 0 THEN
                delete_reason := suggested_match;
              END IF;
              -- if we have no direct matches then we need to look at the
              -- suggested ones
            ELSIF num_direct_matches = 0 THEN
              IF num_suggested_matches = 0 THEN
                -- only set NO_SUGGESTED status if matching by FSC, for FGC leave
                -- it as NOT_PROCESSED so we go round the loop again
                -- set matched_sku for no_suggested records as a null value
                -- causes problems for Exclude Screen processing
                IF match_method = fsc_match THEN
                  auto_status := auto_no_suggested;
                  matched_sku := sku_list(sku_list.first);
                END IF;
              ELSIF num_suggested_matches = 1 THEN
                auto_status := auto_suggested_single;
                matched_sku := first_suggested_sku;
              ELSE
                auto_status := auto_suggested_multi;
                IF first_suggested_sku = last_suggested_sku THEN
                  matched_sku := first_suggested_sku;
                END IF;
              END IF;
              UPDATE dly_bilng_trnd_osl_temp
                 SET auto_mtch_ind = 'Y'
               WHERE dly_bilng_id = db_rec.dly_bilng_id
                 AND mtch_typ_id = suggested
                 AND del_resn_cd IS NULL;
              -- multiple direct matches
            ELSE
              -- see if we can use line number checking to try and convert to a
              -- single match
              IF l_line_nr_used_ind = 'Y' THEN
                -- see how many direct and suggested matches have the same line
                -- number as the billing record
                SELECT MIN(decode(mtch_typ_id,
                                  direct,
                                  decode(line_nr_mtch_ind, 'Y', sku_id, NULL),
                                  NULL)),
                       SUM(decode(mtch_typ_id,
                                  direct,
                                  decode(line_nr_mtch_ind, 'Y', 1, 0),
                                  0)),
                       SUM(decode(mtch_typ_id,
                                  suggested,
                                  decode(line_nr_mtch_ind, 'Y', 1, 0),
                                  0))
                  INTO direct_lnm_sku,
                       num_direct_lnm_matches,
                       num_suggested_lnm_matches
                  FROM dly_bilng_trnd_osl_temp
                 WHERE dly_bilng_id = db_rec.dly_bilng_id
                   AND del_resn_cd IS NULL;
                -- if one direct match has correct line number then status is
                -- MATCHED
                -- if no direct but one or more suggested have correct line
                -- number then MULTI_MATCH (direct + suggested)
                -- if several direct or no matches have correct line number then
                -- MULTI_MATCH (direct only)
                IF num_direct_lnm_matches = 1 THEN
                  auto_status := auto_matched;
                  matched_sku := direct_lnm_sku;
                  UPDATE dly_bilng_trnd_osl_temp
                     SET auto_mtch_ind = 'Y'
                   WHERE dly_bilng_id = db_rec.dly_bilng_id
                     AND mtch_typ_id = direct
                     AND line_nr_mtch_ind = 'Y'
                     AND del_resn_cd IS NULL;
                  delete_reason := line_number_direct;
                ELSIF num_direct_lnm_matches = 0
                      AND num_suggested_lnm_matches > 0 THEN
                  auto_status := auto_suggested_multi;
                  UPDATE dly_bilng_trnd_osl_temp
                     SET auto_mtch_ind = 'Y'
                   WHERE dly_bilng_id = db_rec.dly_bilng_id
                     AND del_resn_cd IS NULL;
                  IF first_direct_sku = last_suggested_sku THEN
                    matched_sku := first_direct_sku;
                  END IF;
                ELSE
                  -- more than one direct or none at all
                  auto_status := auto_suggested_multi;
                  UPDATE dly_bilng_trnd_osl_temp
                     SET auto_mtch_ind = 'Y'
                   WHERE dly_bilng_id = db_rec.dly_bilng_id
                     AND mtch_typ_id = direct
                     AND del_resn_cd IS NULL;
                  IF first_direct_sku = last_direct_sku THEN
                    matched_sku := first_direct_sku;
                  END IF;
                  delete_reason := line_number_suggested;
                END IF;
                -- line number checking
              ELSE
                -- line number checking not carried out so all direct matches
                -- become MULTI
                auto_status := auto_suggested_multi;
                UPDATE dly_bilng_trnd_osl_temp
                   SET auto_mtch_ind = 'Y'
                 WHERE dly_bilng_id = db_rec.dly_bilng_id
                   AND mtch_typ_id = direct
                   AND del_resn_cd IS NULL;
                IF first_direct_sku = last_direct_sku THEN
                  matched_sku := first_direct_sku;
                END IF;
                delete_reason := no_line_number_suggested;
              END IF;
              -- line number checking allowed
            END IF;
            -- checking direct/suggested matches
            -- set delete reason for any rejected matches
            IF delete_reason IS NOT NULL THEN
              UPDATE dly_bilng_trnd_osl_temp
                 SET del_resn_cd = delete_reason
               WHERE dly_bilng_id = db_rec.dly_bilng_id
                 AND del_resn_cd IS NULL
                 AND auto_mtch_ind != 'Y';
            END IF;
            sku_list.delete;
          END IF;
          -- found matching skus
        END LOOP;
        -- FGC/FSC matching
        -- if we found any kind of match, store derived status/sku for updating
        -- parent table after auto-match complete
        IF auto_status != auto_not_processed THEN
          INSERT INTO dly_bilng_trnd_temp
            (dly_bilng_id,
             trnd_aloctn_auto_stus_id,
             sku_id,
             sku_mtch_mthd_id)
          VALUES
            (db_rec.dly_bilng_id, auto_status, matched_sku, match_method);
        END IF;
        app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
        app_plsql_log.info(l_module_name || ' Processing daily billing: ' ||
                           db_rec.dly_bilng_id || ' end' ||
                           l_parameter_list);
      END LOOP;
      -- end of auto_match process
      app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
      app_plsql_log.info(l_module_name || ' auto-match end' ||
                         l_parameter_list);
      -- AUTOMATCH PROCESS ENDS
      -- record derived status/sku values back on to dly_bilng_trnd table and
      -- validate sales class
      app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
      app_plsql_log.info(l_module_name || ' update daily billing' ||
                         l_parameter_list);
      UPDATE dly_bilng_trnd db
         SET (trnd_aloctn_auto_stus_id, sku_id, sku_mtch_mthd_id) =
             (SELECT trnd_aloctn_auto_stus_id, sku_id, sku_mtch_mthd_id
                FROM dly_bilng_trnd_temp
               WHERE dly_bilng_id = db.dly_bilng_id),
             trnd_cls_vld_ind =
             (SELECT decode(sign(pa_maps_public.get_sls_cls_cd(db.offr_perd_id,
                                                               p_mrkt_id,
                                                               ms.avlbl_perd_id,
                                                               ms.intrdctn_perd_id,
                                                               ms.demo_ofs_nr,
                                                               ms.demo_durtn_nr,
                                                               ms.new_durtn_nr,
                                                               ms.stus_perd_id,
                                                               ms.dspostn_perd_id,
                                                               ms.on_stus_perd_id)),
                            1,
                            'Y',
                            0,
                            'N',
                            -1,
                            'N')
                FROM mrkt_sku ms, dly_bilng_trnd_temp dt
               WHERE dt.dly_bilng_id = db.dly_bilng_id
                 AND dt.sku_id IS NOT NULL
                 AND ms.mrkt_id = p_mrkt_id
                 AND ms.sku_id = dt.sku_id)
       WHERE mrkt_id = p_mrkt_id
         AND trnd_sls_perd_id = p_sls_perd_id
         AND trnd_aloctn_auto_stus_id = auto_not_processed;
      -- multimatch fix
      app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
      app_plsql_log.info(l_module_name || ' Processing multi match fix(UNIT_QTY = dbt.unit_qty / osl count): ' || l_parameter_list);
      FOR mm_fix IN (SELECT dly_bilng_trnd.dly_bilng_id,
                            COUNT(*) c,
                            MIN(dly_bilng_trnd.unit_qty) unit_qty,
                            MAX(dly_bilng_trnd_osl_temp.offr_sku_line_id) max_sku_id,
                            MIN(dly_bilng_trnd.comsn_amt) comsn_amt,
                            MIN(dly_bilng_trnd.tax_amt) tax_amt
                       FROM dly_bilng_trnd, dly_bilng_trnd_osl_temp
                      WHERE dly_bilng_trnd.dly_bilng_id =
                            dly_bilng_trnd_osl_temp.dly_bilng_id
                        AND dly_bilng_trnd_osl_temp.demand_ind = 'Y'
                        AND dly_bilng_trnd.trnd_aloctn_auto_stus_id =
                            auto_suggested_multi
                      GROUP BY dly_bilng_trnd.dly_bilng_id
                     HAVING MIN(dly_bilng_trnd.unit_qty) <> SUM(dly_bilng_trnd_osl_temp.unit_qty) AND MIN(dly_bilng_trnd.unit_qty) = SUM(dly_bilng_trnd_osl_temp.unit_qty) / COUNT(1)) LOOP
        UPDATE dly_bilng_trnd_osl_temp
           SET unit_qty =
               (mm_fix.unit_qty - MOD(mm_fix.unit_qty, mm_fix.c)) / mm_fix.c
         WHERE dly_bilng_id = mm_fix.dly_bilng_id
           AND demand_ind = 'Y';
        IF MOD(mm_fix.unit_qty, mm_fix.c) > 0 THEN
          UPDATE dly_bilng_trnd_osl_temp
             SET unit_qty = unit_qty + MOD(mm_fix.unit_qty, mm_fix.c)
           WHERE dly_bilng_id = mm_fix.dly_bilng_id
             AND dly_bilng_trnd_osl_temp.offr_sku_line_id = mm_fix.max_sku_id;
        END IF;
        UPDATE dly_bilng_trnd_osl_temp
           SET comsn_amt = mm_fix.comsn_amt * unit_qty / mm_fix.unit_qty,
               tax_amt   = mm_fix.tax_amt * unit_qty / mm_fix.unit_qty
         WHERE dly_bilng_id = mm_fix.dly_bilng_id;
      END LOOP;      
      -- create dly_bilng_trnd_OFFR_SKU_LINE records for all identified matches
      app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
      app_plsql_log.info(l_module_name || ' insert DSL records' ||
                         l_parameter_list);
      INSERT INTO dly_bilng_trnd_offr_sku_line
        (dly_bilng_id,
         sls_typ_id,
         offr_sku_line_id,
         unit_qty,
         comsn_amt,
         tax_amt)
        SELECT dly_bilng_id,
               sls_typ_id,
               offr_sku_line_id,
               unit_qty,
               comsn_amt,
               tax_amt
          FROM dly_bilng_trnd_osl_temp, sls_typ
         WHERE auto_mtch_ind = 'Y'
           AND ((demand_ind = 'Y' AND sls_typ_id = demand_actuals) OR
               (billed_ind = 'Y' AND sls_typ_id = billed_actuals));
      -- record all considered matches in audit trail table
      app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
      app_plsql_log.info(l_module_name || ' save audit trail' ||
                         l_parameter_list);
      INSERT INTO dly_bilng_trnd_osl_audit
        (trnd_aloctn_auto_strt_ts,
         dly_bilng_id,
         demand_ind,
         billed_ind,
         sku_id,
         offr_sku_line_id,
         osl_line_nr,
         line_nr_mtch_ind,
         qty_diff,
         prc_diff,
         unit_prc_diff,
         veh_id,
         plnd_veh_ind,
         trnd_chnl_mtch_ind,
         trnd_chnl_used_ind,
         mtch_typ_id,
         del_resn_cd,
         auto_mtch_ind)
        SELECT l_trnd_aloctn_auto_strt_ts,
               dly_bilng_id,
               demand_ind,
               billed_ind,
               sku_id,
               offr_sku_line_id,
               osl_line_nr,
               line_nr_mtch_ind,
               qty_diff,
               prc_diff,
               unit_prc_diff,
               veh_id,
               plnd_veh_ind,
               sls_chnl_mtch_ind,
               sls_chnl_used_ind,
               mtch_typ_id,
               del_resn_cd,
               auto_mtch_ind
          FROM dly_bilng_trnd_osl_temp;
      -- apply changes to DSTRBTD_MRKT_SLS for each OSL which was auto-matched to
      -- any billing data
      app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
      app_plsql_log.info(l_module_name || ' apply DMS changes' ||
                         l_parameter_list);
      -- free up remaining collections
      line_nr_duplicates.delete;
      -- dms_changes.DELETE;
      procs_multmtch(p_mrkt_id, p_sls_perd_id, demand_actuals);
      procs_multmtch(p_mrkt_id, p_sls_perd_id, billed_actuals);
      app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
      app_plsql_log.info(l_module_name || ' CONTRL_TOTL_SCRN: correct GTA' ||
                         l_parameter_list);
      crct_gta(p_mrkt_id,
               p_sls_perd_id,
               auto_matched,
               manual_not_processed,
               'N',
               l_user_id,
               l_run_id);
      crct_gta(p_mrkt_id,
               p_sls_perd_id,
               auto_suggested_single,
               manual_not_processed,
               'N',
               l_user_id,
               l_run_id);
      crct_gta(p_mrkt_id,
               p_sls_perd_id,
               auto_suggested_multi,
               manual_not_processed,
               'N',
               l_user_id,
               l_run_id);
      UPDATE dly_bilng_trnd
         SET dly_bilng_trnd.trnd_aloctn_manul_stus_id = manual_matched
       WHERE mrkt_id = p_mrkt_id
         AND trnd_sls_perd_id = p_sls_perd_id
         AND dly_bilng_trnd.trnd_aloctn_auto_stus_id IN
             (auto_matched, auto_suggested_single, auto_suggested_multi);
      clc_catgry_sls(p_mrkt_id, p_sls_perd_id, l_user_id, l_run_id);
      UPDATE mrkt_trnd_sls_perd
         SET trnd_aloctn_auto_user_id = p_user_nm,
             trnd_aloctn_auto_end_ts  = SYSDATE
       WHERE mrkt_id = p_mrkt_id
         AND trnd_sls_perd_id = p_sls_perd_id;
      COMMIT;
      --
    END IF;
    -- end auto process
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' end' || l_parameter_list);
  EXCEPTION
    -- for any error log the error message and re-raise the exception
    WHEN OTHERS THEN
      ROLLBACK;
      app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
      app_plsql_log.info(l_module_name || ' FAILED, error code: ' ||
                         SQLCODE || ' error message: ' || SQLERRM ||
                         l_parameter_list);
      RAISE;
  END auto_procs;

  -- process_jobs_new_periods
  PROCEDURE process_jobs_new_periods IS
    -- for LOG
    l_run_id         NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id        VARCHAR2(35) := USER();
    l_module_name    VARCHAR2(30) := 'PROCESS_JOBS_NEW_PERIODS';
    l_parameter_list VARCHAR2(2048);
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start' || l_parameter_list);
    -- check records for auto_procs
    FOR rec IN (WITH db AS
                   (SELECT mrkt_id,
                          sls_perd_id,
                          MAX(dly_bilng_id) AS max_dly_bilng_id
                     FROM dly_bilng
                    WHERE mrkt_id IN
                          (SELECT mrkt_id
                             FROM mrkt_config_item
                            WHERE mrkt_config_item_desc_txt = 'PA_TREND_ALLOCATION ENABLED'
                              AND mrkt_config_item_val_txt = 'Y')
                    GROUP BY mrkt_id, sls_perd_id)
                  SELECT db.mrkt_id, db.sls_perd_id, db.max_dly_bilng_id
                    FROM db, dly_bilng_trnd
                   WHERE db.max_dly_bilng_id = dly_bilng_trnd.dly_bilng_id(+)
                     AND dly_bilng_trnd.dly_bilng_id IS NULL
                   ORDER BY db.mrkt_id, db.sls_perd_id) LOOP
      -- auto_procs
      l_parameter_list := ' (sysdate: ' ||
                          to_char(SYSDATE, 'yyyy-mm-dd hh24:mi:ss') || ', ' ||
                          'rec: mrkt_id: ' || to_char(rec.mrkt_id) || ', ' ||
                          'sls_perd_id: ' || to_char(rec.sls_perd_id) || ', ' ||
                          'max_dly_bilng_id: ' ||
                          to_char(rec.max_dly_bilng_id) || ', ' ||
                          'p_user_id: ' || l_user_id || ', ' ||
                          'p_run_id: ' || to_char(l_run_id) || ')';
      app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
      app_plsql_log.info(l_module_name || ' TREND AUTO JOB START' ||
                         l_parameter_list);
      auto_procs(p_mrkt_id     => rec.mrkt_id,
                 p_sls_perd_id => rec.sls_perd_id,
                 p_user_nm     => 'TREND JOB',
                 p_user_id     => l_user_id,
                 p_run_id      => l_run_id);
      app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
      app_plsql_log.info(l_module_name || ' TREND AUTO JOB END' ||
                         l_parameter_list);
    END LOOP;
    -- process_jobs
    process_jobs(p_user_id => l_user_id, p_run_id => l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' end' || l_parameter_list);
  END process_jobs_new_periods;

  ---------------------------------------------
  -- inherited from TRND_ALOCTN package -
  ---------------------------------------------
  PROCEDURE unplnd_offr_brchmt_plcmt(p_mrkt_id      mrkt.mrkt_id%TYPE,
                                     p_offr_perd_id dstrbtd_mrkt_sls.offr_perd_id%TYPE,
                                     p_veh_id       mrkt_veh.veh_id%TYPE,
                                     p_user_id      IN VARCHAR2 DEFAULT NULL,
                                     p_run_id       IN NUMBER DEFAULT NULL) IS
    -- for LOG
    l_run_id         NUMBER := nvl(p_run_id,
                                   app_plsql_output.generate_new_run_id);
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'UNPLND_OFFR_BRCHMT_PLCMT';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       to_char(p_mrkt_id) || ', ' ||
                                       'p_offr_perd_id: ' ||
                                       to_char(p_offr_perd_id) || ', ' ||
                                       'p_veh_id: ' || to_char(p_veh_id) || ', ' ||
                                       'p_user_id: ' || l_user_id || ', ' ||
                                       'p_run_id: ' || to_char(l_run_id) || ')';
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start' || l_parameter_list);
    FOR rec IN (SELECT p_mrkt_id mrkt_id,
                       p_offr_perd_id offr_perd_id,
                       0 ver_id,
                       seq.nextval mrkt_veh_perd_sctn_id,
                       rownum sctn_seq_nr,
                       p_veh_id veh_id,
                       brchr_plcmt.brchr_plcmt_id brchr_plcmt_id,
                       2 pg_cnt,
                       'Unplanned - ' || brchr_plcmt.brchr_plcmt_nm sctn_nm,
                       rownum * 2 strtg_page_nr,
                       0 strtg_page_side_nr
                  FROM (SELECT brchr_plcmt.*
                          FROM brchr_plcmt
                         ORDER BY brchr_plcmt.brchr_plcmt_nm) brchr_plcmt
                 WHERE (brchr_plcmt.brchr_plcmt_id,
                        'Unplanned - ' || brchr_plcmt.brchr_plcmt_nm) NOT IN
                       (SELECT mrkt_veh_perd_sctn.brchr_plcmt_id,
                               mrkt_veh_perd_sctn.sctn_nm
                          FROM mrkt_veh_perd_sctn
                         WHERE mrkt_veh_perd_sctn.mrkt_id = p_mrkt_id
                           AND mrkt_veh_perd_sctn.veh_id = p_veh_id
                           AND mrkt_veh_perd_sctn.offr_perd_id =
                               p_offr_perd_id
                           AND ver_id = 0)) LOOP
      BEGIN
        INSERT INTO mrkt_veh_perd_sctn
          (mrkt_id,
           offr_perd_id,
           ver_id,
           mrkt_veh_perd_sctn_id,
           sctn_seq_nr,
           veh_id,
           brchr_plcmt_id,
           pg_cnt,
           sctn_nm,
           strtg_page_nr,
           strtg_page_side_nr)
        VALUES
          (rec.mrkt_id,
           rec.offr_perd_id,
           rec.ver_id,
           rec.mrkt_veh_perd_sctn_id,
           rec.sctn_seq_nr,
           rec.veh_id,
           rec.brchr_plcmt_id,
           rec.pg_cnt,
           rec.sctn_nm,
           rec.strtg_page_nr,
           rec.strtg_page_side_nr);
        COMMIT;
      EXCEPTION
        WHEN OTHERS THEN
          app_plsql_log.info('CREATE UNPLANNED OFFERS: MRKT_VEH_PERD_SCTN already exists' || '|' ||
                             rec.mrkt_id || '|' || rec.offr_perd_id || '|' ||
                             rec.ver_id || '|' ||
                             rec.mrkt_veh_perd_sctn_id || '|' ||
                             rec.sctn_seq_nr || '|' || rec.veh_id || '|' ||
                             rec.brchr_plcmt_id || '|' || rec.pg_cnt || '|' ||
                             rec.sctn_nm || '|' || rec.strtg_page_nr || '|' ||
                             rec.strtg_page_side_nr);
      END;
    END LOOP;
    app_plsql_log.info(l_module_name || ' end' || l_parameter_list);
  END;

  PROCEDURE offr_perds(p_mrkt_id     IN mrkt.mrkt_id%TYPE,
                       p_sls_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                       p_result      OUT tbl_perd_id) AS
    -- local variables
    r NUMBER;
    i NUMBER;
  BEGIN
    p_result := tbl_perd_id();
    i        := 0;
    FOR r IN (SELECT mrkt_perd.perd_id
                FROM mrkt_perd
               WHERE mrkt_id = p_mrkt_id
                 AND perd_id BETWEEN
                     (SELECT MIN(dly_bilng_trnd.offr_perd_id)
                        FROM dly_bilng_trnd
                       WHERE dly_bilng_trnd.mrkt_id = p_mrkt_id
                         AND dly_bilng_trnd.trnd_sls_perd_id = p_sls_perd_id)
                 AND (SELECT MAX(dly_bilng_trnd.offr_perd_id)
                        FROM dly_bilng_trnd
                       WHERE dly_bilng_trnd.mrkt_id = p_mrkt_id
                         AND dly_bilng_trnd.trnd_sls_perd_id = p_sls_perd_id)
                 AND mrkt_perd.perd_typ = 'SC') LOOP
      i := i + 1;
      p_result.extend;
      p_result(i) := r.perd_id;
    END LOOP;
  END;

  PROCEDURE unplnd_offr_recrds(p_mrkt_id     IN mrkt.mrkt_id%TYPE,
                               p_sls_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                               p_user_id     IN VARCHAR2 DEFAULT NULL,
                               p_run_id      IN NUMBER DEFAULT NULL,
                               p_result      OUT tbl_sa_unplnd_offr_vw) AS
    -- local variables
    mrkt_id_used                  CHAR;
    v_unplnd_offr_avg_sls_prc_ind CHAR;
    bilng_mtch_id                 NUMBER;
    v_posbl_offr_perd_ids         tbl_perd_id;
    period_index                  NUMBER;
    v_cur_prfl_cd                 NUMBER;
    --offer level
    v_offr_id_prfl_cd      tbl_key_value;
    v_offr_ids_for_prfl_cd tbl_key_value;
    v_offr_key             VARCHAR(1000);
    v_cur_offr_id          NUMBER;
    --profile level
    --pricepoint level
    v_prcpt_ids_for_prflslsnf tbl_key_value;
    v_prcpt_key               VARCHAR(1000);
    v_cur_prcpt_id            NUMBER;
    --offer sku line level
    v_offr_sku_line_id     tbl_key_value;
    v_offr_sku_line_id_key VARCHAR(1000);
    v_cur_offr_sku_line_id NUMBER;
    v_key1                 VARCHAR(255);
    --v_used_offer_ids              varchar(4000);
    ok BOOLEAN;
    -- config_item_id for UNPLND_VEH_ID
    l_config_item_id mrkt_config_item.config_item_id%TYPE := 13000;
    l_veh_id         mrkt_veh.veh_id%TYPE;
    -- for LOG
    l_run_id         NUMBER := nvl(p_run_id,
                                   app_plsql_output.generate_new_run_id);
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'UNPLND_OFFR_RECRDS';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       to_char(p_mrkt_id) || ', ' ||
                                       'p_sls_perd_id: ' ||
                                       to_char(p_sls_perd_id) || ', ' ||
                                       'p_user_id: ' || l_user_id || ', ' ||
                                       'p_run_id: ' || to_char(l_run_id) || ')';
  BEGIN
    ok       := TRUE;
    p_result := tbl_sa_unplnd_offr_vw();
    --v_used_offer_ids := '-1';
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start' || l_parameter_list);
    --create brochure sections start
    app_plsql_log.info(l_module_name || ' CREATE BROCHURE SECTIONS STARTS' ||
                       l_parameter_list);
    offr_perds(p_mrkt_id     => p_mrkt_id,
               p_sls_perd_id => p_sls_perd_id,
               p_result      => v_posbl_offr_perd_ids);
    l_veh_id := get_value_from_config(p_mrkt_id        => p_mrkt_id,
                                      p_config_item_id => l_config_item_id,
                                      p_user_id        => l_user_id,
                                      p_run_id         => l_run_id);
    FOR period_index IN v_posbl_offr_perd_ids.first .. v_posbl_offr_perd_ids.last LOOP
      app_plsql_log.info(l_module_name ||
                         ' UNPLND_OFFR_RECRDS CREATE BROCHURE SECTIONS STARTS FOR OFFR_PERD_ID:' ||
                         v_posbl_offr_perd_ids(period_index) ||
                         l_parameter_list);
      unplnd_offr_brchmt_plcmt(p_mrkt_id,
                               v_posbl_offr_perd_ids(period_index),
                               l_veh_id);
      app_plsql_log.info(l_module_name ||
                         ' UNPLND_OFFR_RECRDS CREATE BROCHURE SECTIONS ENDS FOR OFFR_PERD_ID:' ||
                         v_posbl_offr_perd_ids(period_index) ||
                         l_parameter_list);
    END LOOP;
    app_plsql_log.info(l_module_name ||
                       ' UNPLND_OFFR_RECRDS CREATE BROCHURE SECTIONS ENDS' ||
                       l_parameter_list);
    --create brochure sections end
    SELECT mrkt_eff_sls_perd.use_clstr_lvl_fsc_sku_ind,
           dly_bilng_mtch_id,
           mrkt_eff_sls_perd.unplnd_offr_avg_sls_prc_ind
      INTO mrkt_id_used, bilng_mtch_id, v_unplnd_offr_avg_sls_prc_ind
      FROM mrkt_eff_sls_perd
     WHERE mrkt_id = p_mrkt_id
       AND eff_sls_perd_id =
           (SELECT MAX(mesp.eff_sls_perd_id)
              FROM mrkt_eff_sls_perd mesp
             WHERE mesp.mrkt_id = p_mrkt_id
               AND mesp.eff_sls_perd_id <= p_sls_perd_id);
    FOR period_index IN v_posbl_offr_perd_ids.first .. v_posbl_offr_perd_ids.last LOOP
      v_offr_key    := '-1';
      v_cur_prfl_cd := -1;
      FOR rec IN (SELECT db.mrkt_id,
                         db.veh_id,
                         0 ver_id,
                         db.offr_perd_id,
                         (SELECT MAX(mrkt_veh_perd_sctn.mrkt_veh_perd_sctn_id)
                            FROM mrkt_veh_perd_sctn
                           WHERE mrkt_veh_perd_sctn.mrkt_id = db.mrkt_id
                             AND mrkt_veh_perd_sctn.offr_perd_id =
                                 db.offr_perd_id
                             AND mrkt_veh_perd_sctn.veh_id = db.veh_id
                             AND mrkt_veh_perd_sctn.brchr_plcmt_id =
                                 db.catgry_id
                             AND mrkt_veh_perd_sctn.ver_id = 0) mrkt_veh_perd_sctn_id,
                         db.catgry_id,
                         db.prfl_cd,
                         db.prfl_nm,
                         sls_prc_amt,
                         nr_for_qty,
                         db.sku_id,
                         db.sls_perd_id,
                         --SKU_COST.WGHTD_AVG_COST_AMT,----552----
                         NULL wghtd_avg_cost_amt, ---QC3288
                         pa_maps_gta.get_commission_type(db.mrkt_id,
                                                         db.veh_id,
                                                         db.offr_perd_id,
                                                         db.prfl_cd,
                                                         'N',
                                                         NULL,
                                                         NULL,
                                                         NULL,
                                                         NULL,
                                                         NULL) comsn_typ_id,
                         pa_maps_gta.get_commission_percentage(db.mrkt_id,
                                                               db.offr_perd_id,
                                                               pa_maps_gta.get_commission_type(db.mrkt_id,
                                                                                               db.veh_id,
                                                                                               db.offr_perd_id,
                                                                                               db.prfl_cd,
                                                                                               'N',
                                                                                               NULL,
                                                                                               NULL,
                                                                                               NULL,
                                                                                               NULL,
                                                                                               NULL)) comsn_amt,
                         pa_maps_gta.get_default_tax_type_id(db.mrkt_id,
                                                             db.prfl_cd,
                                                             sls_cls_cd,
                                                             db.offr_perd_id,
                                                             db.veh_id) tax_typ_id,
                         pa_maps_gta.pri_get_tax_amount(db.mrkt_id,
                                                        pa_maps_gta.get_default_tax_type_id(db.mrkt_id,
                                                                                            db.prfl_cd,
                                                                                            sls_cls_cd,
                                                                                            db.offr_perd_id,
                                                                                            db.veh_id),
                                                        db.offr_perd_id) tax_amt,
                         pa_maps_gta.pri_get_gta_method_id(db.mrkt_id,
                                                           db.offr_perd_id) gta_method_id,
                         round(pa_maps_gta.get_gta_without_price_point(pa_maps_gta.pri_get_gta_method_id(db.mrkt_id,
                                                                                                         db.offr_perd_id),
                                                                       nvl(db.sls_prc_amt,
                                                                           1),
                                                                       0,
                                                                       0,
                                                                       pa_maps_gta.get_commission_percentage(db.mrkt_id,
                                                                                                             db.offr_perd_id,
                                                                                                             pa_maps_gta.get_commission_type(db.mrkt_id,
                                                                                                                                             db.veh_id,
                                                                                                                                             db.offr_perd_id,
                                                                                                                                             db.prfl_cd,
                                                                                                                                             'N',
                                                                                                                                             NULL,
                                                                                                                                             NULL,
                                                                                                                                             NULL,
                                                                                                                                             NULL,
                                                                                                                                             NULL)),
                                                                       pa_maps_gta.pri_get_tax_amount(db.mrkt_id,
                                                                                                      pa_maps_gta.get_default_tax_type_id(db.mrkt_id,
                                                                                                                                          db.prfl_cd,
                                                                                                                                          pa_maps_public.get_sls_cls_cd(db.offr_perd_id,
                                                                                                                                                                        db.mrkt_id,
                                                                                                                                                                        mrkt_sku.avlbl_perd_id,
                                                                                                                                                                        mrkt_sku.intrdctn_perd_id,
                                                                                                                                                                        mrkt_sku.demo_ofs_nr,
                                                                                                                                                                        mrkt_sku.demo_durtn_nr,
                                                                                                                                                                        mrkt_sku.new_durtn_nr,
                                                                                                                                                                        mrkt_sku.stus_perd_id,
                                                                                                                                                                        mrkt_sku.dspostn_perd_id,
                                                                                                                                                                        mrkt_sku.on_stus_perd_id),
                                                                                                                                          db.offr_perd_id,
                                                                                                                                          db.veh_id),
                                                                                                      db.offr_perd_id),
                                                                       0),
                               4) gta_pct,
                         sls_cls_cd,
                         db.bilng_id,
                         db.demand_unit_qty,
                         db.billed_unit_qty,
                         db.demand_gta,
                         db.billed_gta,
                         db.demand_comsn_amt,
                         db.billed_comsn_amt,
                         db.demand_tax_amt,
                         db.billed_tax_amt,
                         sku_reg_prc.reg_prc_amt,
                         mrkt_perd.crncy_cd
                    FROM (SELECT mrkt_id,
                                 veh_id,
                                 offr_perd_id,
                                 catgry_id,
                                 prfl_cd,
                                 prfl_nm,
                                 sls_prc_amt,
                                 nr_for_qty,
                                 sku_id,
                                 sls_perd_id,
                                 bilng_id,
                                 demand_unit_qty,
                                 billed_unit_qty,
                                 demand_gta,
                                 billed_gta,
                                 demand_comsn_amt,
                                 billed_comsn_amt,
                                 demand_tax_amt,
                                 billed_tax_amt,
                                 sls_cls_cd
                            FROM (SELECT mrkt_id,
                                         veh_id,
                                         offr_perd_id,
                                         catgry_id,
                                         prfl_cd,
                                         MAX(prfl_nm) prfl_nm,
                                         sls_prc_amt,
                                         nr_for_qty,
                                         sku_id,
                                         sls_perd_id,
                                         bilng_id,
                                         SUM(demand_unit_qty) demand_unit_qty,
                                         SUM(billed_unit_qty) billed_unit_qty,
                                         SUM(demand_gta) demand_gta,
                                         SUM(billed_gta) billed_gta,
                                         SUM(demand_comsn_amt) demand_comsn_amt,
                                         SUM(billed_comsn_amt) billed_comsn_amt,
                                         SUM(demand_tax_amt) demand_tax_amt,
                                         SUM(billed_tax_amt) billed_tax_amt,
                                         sls_cls_cd
                                    FROM (SELECT p_mrkt_id mrkt_id,
                                                 dly_offr_perd_id offr_perd_id,
                                                 p_sls_perd_id sls_perd_id,
                                                 dly_fsc_cd fsc_cd,
                                                 mrkt_sku.sku_id sku_id,
                                                 dly_bilng_line_nr line_nr,
                                                 dly_sls_prc_amt sls_prc_amt,
                                                 dly_nr_for_qty nr_for_qty,
                                                 ospa,
                                                 onfq,
                                                 CASE
                                                   WHEN dly_sls_typ_id = 6 THEN
                                                    dly_unit_qty
                                                   ELSE
                                                    0
                                                 END demand_unit_qty,
                                                 CASE
                                                   WHEN dly_sls_typ_id = 7 THEN
                                                    dly_unit_qty
                                                   ELSE
                                                    0
                                                 END billed_unit_qty,
                                                 0 demand_gta,
                                                 0 billed_gta,
                                                 CASE
                                                   WHEN dly_sls_typ_id = 6 THEN
                                                    dly_comsn_amt
                                                   ELSE
                                                    0
                                                 END demand_comsn_amt,
                                                 CASE
                                                   WHEN dly_sls_typ_id = 7 THEN
                                                    dly_comsn_amt
                                                   ELSE
                                                    0
                                                 END billed_comsn_amt,
                                                 CASE
                                                   WHEN dly_sls_typ_id = 6 THEN
                                                    dly_tax_amt
                                                   ELSE
                                                    0
                                                 END demand_tax_amt,
                                                 CASE
                                                   WHEN dly_sls_typ_id = 7 THEN
                                                    dly_tax_amt
                                                   ELSE
                                                    0
                                                 END billed_tax_amt,
                                                 dly_bilng_id bilng_id,
                                                 sku.prfl_cd,
                                                 catgry.catgry_id,
                                                 prfl.prfl_nm,
                                                 veh_id,
                                                 sls_cls_cd
                                            FROM (SELECT db.dly_bilng_id dly_bilng_id,
                                                         db.mrkt_id dly_mrkt_id,
                                                         db.offr_perd_id dly_offr_perd_id,
                                                         db.sls_perd_id dly_sls_perd_id,
                                                         decode(v_unplnd_offr_avg_sls_prc_ind,
                                                                'N',
                                                                db.sls_prc_amt,
                                                                decode(SUM(unit_qty)
                                                                       over(PARTITION BY
                                                                            dly_sku_id,
                                                                            db.sls_cls_cd,
                                                                            db.veh_id,
                                                                            db.sls_typ_id),
                                                                       0,
                                                                       db.sls_prc_amt,
                                                                       round(SUM(db.sls_prc_amt /
                                                                                 decode(db.nr_for_qty,
                                                                                        0,
                                                                                        1,
                                                                                        db.nr_for_qty) *
                                                                                 unit_qty)
                                                                             over(PARTITION BY
                                                                                  db.dly_sku_id,
                                                                                  db.sls_cls_cd,
                                                                                  db.veh_id,
                                                                                  db.sls_typ_id) /
                                                                             SUM(unit_qty)
                                                                             over(PARTITION BY
                                                                                  dly_sku_id,
                                                                                  db.sls_cls_cd,
                                                                                  db.veh_id,
                                                                                  db.sls_typ_id),
                                                                             4))) dly_sls_prc_amt,
                                                         decode(v_unplnd_offr_avg_sls_prc_ind,
                                                                'N',
                                                                db.nr_for_qty,
                                                                1) dly_nr_for_qty,
                                                         sls_prc_amt ospa,
                                                         nr_for_qty onfq,
                                                         db.comsn_amt dly_comsn_amt,
                                                         db.tax_amt dly_tax_amt,
                                                         db.bilng_line_nr dly_bilng_line_nr,
                                                         db.sls_typ_id dly_sls_typ_id,
                                                         db.offr_typ dly_offr_typ,
                                                         db.fsc_cd dly_fsc_cd,
                                                         db.unit_qty dly_unit_qty,
                                                         db.sls_chnl dly_sls_chnl,
                                                         dly_sku_id,
                                                         veh_id,
                                                         sls_cls_cd
                                                    FROM (SELECT dly_bilng.dly_bilng_id,
                                                                 dly_bilng.mrkt_id,
                                                                 dly_bilng.sls_perd_id,
                                                                 dly_bilng.offr_perd_id,
                                                                 dly_bilng.fsc_cd,
                                                                 dly_bilng.sls_prc_amt,
                                                                 dly_bilng.nr_for_qty,
                                                                 dly_bilng.comsn_amt,
                                                                 dly_bilng.tax_amt,
                                                                 dly_bilng.unit_qty,
                                                                 dly_bilng.lcl_bilng_offr_typ offr_typ,
                                                                 dly_bilng.bilng_line_nr,
                                                                 dly_bilng.sls_typ_id,
                                                                 dly_bilng.sls_chnl_cd sls_chnl,
                                                                 dly_sku_id,
                                                                 veh_id,
                                                                 pa_maps_public.get_sls_cls_cd(offr_perd_id,
                                                                                               dly_bilng.mrkt_id,
                                                                                               mrkt_sku.avlbl_perd_id,
                                                                                               mrkt_sku.intrdctn_perd_id,
                                                                                               mrkt_sku.demo_ofs_nr,
                                                                                               mrkt_sku.demo_durtn_nr,
                                                                                               mrkt_sku.new_durtn_nr,
                                                                                               mrkt_sku.stus_perd_id,
                                                                                               mrkt_sku.dspostn_perd_id,
                                                                                               mrkt_sku.on_stus_perd_id) sls_cls_cd
                                                            FROM --weighted avg starts
                                                                 (SELECT dly_bilng_id,
                                                                         mrkt_id,
                                                                         sls_chnl_cd,
                                                                         sls_perd_id,
                                                                         offr_perd_id,
                                                                         lcl_bilng_actn_cd,
                                                                         lcl_bilng_tran_typ,
                                                                         lcl_bilng_offr_typ,
                                                                         mlpln_cd,
                                                                         lcl_bilng_defrd_cd,
                                                                         lcl_bilng_shpng_cd,
                                                                         sbsttd_fsc_cd,
                                                                         sbsttd_bilng_line_nr,
                                                                         bilng_plnd_ind,
                                                                         bilng_line_nr,
                                                                         sls_aloctn_auto_stus_id,
                                                                         sls_aloctn_manul_stus_id,
                                                                         sls_cls_vld_ind,
                                                                         fsc_cd,
                                                                         sls_prc_amt,
                                                                         nr_for_qty,
                                                                         unit_qty,
                                                                         comsn_amt,
                                                                         tax_amt,
                                                                         dly_sku_id,
                                                                         veh_id,
                                                                         sls_typ_id
                                                                    FROM (SELECT dly_bilng_trnd.dly_bilng_id,
                                                                                 dly_bilng_trnd.mrkt_id,
                                                                                 dly_bilng_trnd.sls_chnl_cd,
                                                                                 dly_bilng_trnd.trnd_sls_perd_id sls_perd_id,
                                                                                 dly_bilng_trnd.offr_perd_id,
                                                                                 dly_bilng_trnd.lcl_bilng_actn_cd,
                                                                                 dly_bilng_trnd.lcl_bilng_tran_typ,
                                                                                 dly_bilng_trnd.lcl_bilng_offr_typ,
                                                                                 dly_bilng_trnd.mlpln_cd,
                                                                                 dly_bilng_trnd.lcl_bilng_defrd_cd,
                                                                                 dly_bilng_trnd.lcl_bilng_shpng_cd,
                                                                                 dly_bilng_trnd.sbsttd_fsc_cd,
                                                                                 dly_bilng_trnd.sbsttd_bilng_line_nr,
                                                                                 dly_bilng_trnd.bilng_plnd_ind,
                                                                                 dly_bilng_trnd.trnd_aloctn_auto_stus_id sls_aloctn_auto_stus_id,
                                                                                 dly_bilng_trnd.trnd_aloctn_manul_stus_id sls_aloctn_manul_stus_id,
                                                                                 dly_bilng_trnd.trnd_cls_vld_ind sls_cls_vld_ind,
                                                                                 dly_bilng_trnd.fsc_cd,
                                                                                 dly_bilng_trnd.bilng_line_nr,
                                                                                 dly_bilng_trnd.sls_prc_amt,
                                                                                 dly_bilng_trnd.nr_for_qty,
                                                                                 dly_bilng_trnd.unit_qty,
                                                                                 dly_bilng_trnd.comsn_amt,
                                                                                 dly_bilng_trnd.tax_amt,
                                                                                 dly_bilng_trnd_cntrl.sls_typ_id,
                                                                                 dly_bilng_trnd.sku_id dly_sku_id,
                                                                                 nvl((SELECT MIN(unplnd_veh_id)
                                                                                       FROM mrkt_sls_chnl_offr_typ_slsperd
                                                                                      WHERE mrkt_id =
                                                                                            p_mrkt_id
                                                                                        AND nvl(strt_sls_perd_id,
                                                                                                0) <=
                                                                                            p_sls_perd_id
                                                                                        AND nvl(end_sls_perd_id,
                                                                                                99999999) >=
                                                                                            p_sls_perd_id
                                                                                        AND nvl(mrkt_sls_chnl_offr_typ_slsperd.lcl_bilng_offr_typ,
                                                                                                'XXX') =
                                                                                            dly_bilng_trnd.lcl_bilng_offr_typ
                                                                                        AND mrkt_sls_chnl_offr_typ_slsperd.sls_chnl_cd =
                                                                                            dly_bilng_trnd.sls_chnl_cd),
                                                                                     nvl((SELECT MIN(unplnd_veh_id)
                                                                                           FROM mrkt_sls_chnl_offr_typ_slsperd
                                                                                          WHERE mrkt_id =
                                                                                                p_mrkt_id
                                                                                            AND nvl(strt_sls_perd_id,
                                                                                                    0) <=
                                                                                                p_sls_perd_id
                                                                                            AND nvl(end_sls_perd_id,
                                                                                                    99999999) >=
                                                                                                p_sls_perd_id
                                                                                            AND mrkt_sls_chnl_offr_typ_slsperd.sls_chnl_cd =
                                                                                                dly_bilng_trnd.sls_chnl_cd
                                                                                            AND mrkt_sls_chnl_offr_typ_slsperd.lcl_bilng_offr_typ IS NULL),
                                                                                         l_veh_id)) veh_id
                                                                            FROM dly_bilng_trnd,
                                                                                 dly_bilng_trnd_cntrl
                                                                           WHERE dly_bilng_trnd.mrkt_id =
                                                                                 p_mrkt_id
                                                                             AND dly_bilng_trnd.trnd_sls_perd_id =
                                                                                 p_sls_perd_id
                                                                             AND dly_bilng_trnd.trnd_cls_vld_ind = 'Y'
                                                                             AND dly_bilng_trnd.bilng_plnd_ind = 'Y'
                                                                             AND dly_bilng_trnd.offr_perd_id =
                                                                                 v_posbl_offr_perd_ids(period_index)
                                                                             AND dly_bilng_trnd_cntrl.dly_bilng_mtch_id =
                                                                                 bilng_mtch_id
                                                                             AND dly_bilng_trnd_cntrl.sls_typ_id IN (6,
                                                                                                                     7)
                                                                             AND nvl(dly_bilng_trnd_cntrl.lcl_bilng_actn_cd,
                                                                                     dly_bilng_trnd.lcl_bilng_actn_cd) =
                                                                                 dly_bilng_trnd.lcl_bilng_actn_cd
                                                                             AND nvl(dly_bilng_trnd_cntrl.lcl_bilng_tran_typ,
                                                                                     dly_bilng_trnd.lcl_bilng_tran_typ) =
                                                                                 dly_bilng_trnd.lcl_bilng_tran_typ
                                                                             AND nvl(dly_bilng_trnd_cntrl.lcl_bilng_offr_typ,
                                                                                     dly_bilng_trnd.lcl_bilng_offr_typ) =
                                                                                 dly_bilng_trnd.lcl_bilng_offr_typ
                                                                             AND nvl(dly_bilng_trnd_cntrl.lcl_bilng_defrd_cd,
                                                                                     dly_bilng_trnd.lcl_bilng_defrd_cd) =
                                                                                 dly_bilng_trnd.lcl_bilng_defrd_cd
                                                                             AND nvl(dly_bilng_trnd_cntrl.lcl_bilng_shpng_cd,
                                                                                     dly_bilng_trnd.lcl_bilng_shpng_cd) =
                                                                                 dly_bilng_trnd.lcl_bilng_shpng_cd
                                                                             AND ((dly_bilng_trnd.trnd_aloctn_auto_stus_id =
                                                                                 auto_no_suggested AND
                                                                                 dly_bilng_trnd.trnd_aloctn_manul_stus_id =
                                                                                 manual_not_processed) OR
                                                                                 (dly_bilng_trnd.trnd_aloctn_auto_stus_id IN
                                                                                 (auto_suggested_single,
                                                                                    auto_suggested_multi) AND
                                                                                 dly_bilng_trnd.trnd_aloctn_manul_stus_id =
                                                                                 manual_not_suggested)))) dly_bilng,
                                                                 mrkt_sku
                                                           WHERE dly_bilng.dly_sku_id =
                                                                 mrkt_sku.sku_id
                                                             AND dly_bilng.mrkt_id =
                                                                 mrkt_sku.mrkt_id --weighted avg ends
                                                          ) db
                                                   ORDER BY dly_bilng_id) dly_bilng_db,
                                                 sku,
                                                 prfl,
                                                 catgry,
                                                 (SELECT *
                                                    FROM mrkt_sku
                                                   WHERE mrkt_id = p_mrkt_id) mrkt_sku
                                           WHERE mrkt_sku.sku_id = sku.sku_id
                                             AND sku.sku_id = dly_sku_id
                                             AND sku.prfl_cd = prfl.prfl_cd
                                             AND prfl.catgry_id =
                                                 catgry.catgry_id
                                           ORDER BY dly_offr_perd_id,
                                                    dly_fsc_cd,
                                                    dly_sku_id,
                                                    dly_sls_prc_amt,
                                                    dly_nr_for_qty)
                                   GROUP BY mrkt_id,
                                            bilng_id,
                                            offr_perd_id,
                                            sls_perd_id,
                                            sku_id,
                                            sls_cls_cd,
                                            prfl_cd,
                                            sls_prc_amt,
                                            nr_for_qty,
                                            veh_id,
                                            catgry_id)) db,
                         mrkt_sku,
                         sku_reg_prc,
                         mrkt_perd
                   WHERE db.sls_perd_id = p_sls_perd_id --QC32888
                     AND db.mrkt_id = p_mrkt_id
                     AND db.mrkt_id = mrkt_sku.mrkt_id
                     AND db.mrkt_id = mrkt_sku.mrkt_id
                     AND db.sku_id = mrkt_sku.sku_id
                     AND db.mrkt_id = sku_reg_prc.mrkt_id
                     AND v_posbl_offr_perd_ids(period_index) =
                         sku_reg_prc.offr_perd_id
                     AND db.sku_id = sku_reg_prc.sku_id
                     AND mrkt_perd.mrkt_id = p_mrkt_id
                     AND mrkt_perd.perd_id =
                         v_posbl_offr_perd_ids(period_index)
                   ORDER BY mrkt_id,
                            veh_id,
                            prfl_cd,
                            sls_prc_amt,
                            nr_for_qty,
                            sku_id,
                            sls_perd_id) LOOP
        IF rec.bilng_id IS NOT NULL THEN
          v_cur_prfl_cd := rec.prfl_cd;
          --create offer record start
          v_offr_key := rec.mrkt_id || '-' || rec.veh_id || '-' ||
                        rec.offr_perd_id || '-' || rec.prfl_cd;
          IF v_offr_ids_for_prfl_cd.exists(v_offr_key) THEN
            --old offer;
            v_cur_offr_id := v_offr_ids_for_prfl_cd(v_offr_key);
          ELSE
            --new offer;
            SELECT seq.nextval INTO v_cur_offr_id FROM dual;
            v_offr_ids_for_prfl_cd(v_offr_key) := v_cur_offr_id;
            v_offr_id_prfl_cd(v_cur_offr_id) := rec.prfl_cd;
            --v_used_offer_ids := v_used_offer_ids || ',' || v_cur_offr_id;
            --insert
            BEGIN
              INSERT INTO offr
                (offr_id,
                 mrkt_id,
                 offr_perd_id,
                 veh_id,
                 ver_id,
                 pg_wght_pct,
                 ssnl_evnt_id,
                 est_srce_id,
                 est_stus_cd,
                 offr_desc_txt,
                 mrkt_veh_perd_sctn_id,
                 offr_typ,
                 enrgy_chrt_postn_id,
                 enrgy_chrt_offr_desc_txt,
                 brchr_plcmt_id,
                 std_offr_id,
                 offr_link_ind,
                 offr_link_id,
                 offr_ntes_txt,
                 offr_lyot_cmnts_txt,
                 sctn_page_ofs_nr,
                 offr_prsntn_strnth_id,
                 prfl_offr_strgth_pct,
                 prfl_cnt,
                 sku_cnt,
                 featrd_side_cd,
                 flap_ind,
                 frnt_cvr_ind,
                 offr_stus_cd,
                 offr_stus_rsn_desc_txt,
                 bilng_perd_id,
                 shpng_perd_id,
                 brchr_postn_id,
                 flap_pg_wght_pct,
                 unit_rptg_lvl_id,
                 rpt_sbtl_typ_id,
                 micr_ncpsltn_ind,
                 micr_ncpsltn_desc_txt,
                 pg_typ_id,
                 offr_cls_id,
                 cust_pull_id)
              VALUES
                (v_cur_offr_id,
                 rec.mrkt_id,
                 rec.offr_perd_id,
                 rec.veh_id,
                 0,
                 200,
                 0,
                 NULL,
                 NULL,
                 'Unplanned - ' || rec.prfl_nm,
                 rec.mrkt_veh_perd_sctn_id,
                 'CMP',
                 NULL,
                 NULL,
                 rec.catgry_id,
                 NULL,
                 'N',
                 NULL,
                 NULL,
                 NULL,
                 0,
                 NULL,
                 NULL,
                 1,
                 0, --???????????????????????????????????  SKU_CNT
                 0,
                 'N',
                 'N',
                 4,
                 'FINAL',
                 rec.sls_perd_id,
                 rec.sls_perd_id,
                 0,
                 NULL,
                 2,
                 1,
                 'N',
                 NULL,
                 1,
                 1,
                 NULL);
              COMMIT;
            EXCEPTION
              WHEN OTHERS THEN
                app_plsql_log.info('Insert OFFR: ' || SQLERRM);
                app_plsql_log.info('ERROR - INSERT INTO OFFR (
OFFR_ID,
MRKT_ID,
OFFR_PERD_ID,
VEH_ID,
VER_ID,
PG_WGHT_PCT,
SSNL_EVNT_ID,
EST_SRCE_ID,
EST_STUS_CD,
OFFR_DESC_TXT,
MRKT_VEH_PERD_SCTN_ID,
OFFR_TYP,
ENRGY_CHRT_POSTN_ID,
ENRGY_CHRT_OFFR_DESC_TXT,
BRCHR_PLCMT_ID,
STD_OFFR_ID,
OFFR_LINK_IND,
OFFR_LINK_ID,
OFFR_NTES_TXT,
OFFR_LYOT_CMNTS_TXT,
SCTN_PAGE_OFS_NR,
OFFR_PRSNTN_STRNTH_ID,
PRFL_OFFR_STRGTH_PCT,
PRFL_CNT,
SKU_CNT,
FEATRD_SIDE_CD,
FLAP_IND,
FRNT_CVR_IND,
OFFR_STUS_CD,
OFFR_STUS_RSN_DESC_TXT,
BILNG_PERD_ID,
SHPNG_PERD_ID,
BRCHR_POSTN_ID,
FLAP_PG_WGHT_PCT,
UNIT_RPTG_LVL_ID,
RPT_SBTL_TYP_ID,
MICR_NCPSLTN_IND,
MICR_NCPSLTN_DESC_TXT,
PG_TYP_ID,
OFFR_CLS_ID,
CUST_PULL_ID
)
VALUES (
' || v_cur_offr_id || ',
' || rec.mrkt_id || ',
' || rec.offr_perd_id || ',
' || rec.veh_id || ',
0,
200,
0,
null,
null,
' || 'Unplanned - ' || rec.prfl_nm || ',
' || rec.mrkt_veh_perd_sctn_id || ',
' || 'CMP' || ',
null,
null,
' || rec.catgry_id || ',
null,
' || 'N' || ',
null,
null,
null,
0,
null,
null,
1,
0, --???????????????????????????????????  SKU_CNT
0,
' || 'N' || ',
' || 'N' || ',
4,
' || 'FINAL' || ',
' || rec.sls_perd_id || ',
' || rec.sls_perd_id || ',
0,
null,
2,
1,
' || 'N' || ',
null,
1,
1,
null
);');
            END;
          END IF;
          --create offer record end
          -- OFFR_PRFL_SLS_CLS_PLCMT record start
          BEGIN
            INSERT INTO offr_prfl_sls_cls_plcmt
              (offr_id,
               sls_cls_cd,
               prfl_cd,
               pg_ofs_nr,
               featrd_side_cd,
               mrkt_id,
               veh_id,
               offr_perd_id,
               sku_cnt,
               pg_wght_pct,
               sku_offr_strgth_pct,
               featrd_prfl_ind,
               use_instrctns_ind,
               prod_endrsmt_id,
               fxd_pg_wght_ind,
               pg_typ_id)
            VALUES
              (v_cur_offr_id,
               rec.sls_cls_cd,
               rec.prfl_cd,
               0,
               0,
               rec.mrkt_id,
               rec.veh_id,
               rec.offr_perd_id,
               0, --???????????????????????????????????// SKU_CNT
               100,
               NULL,
               'N',
               'N',
               NULL,
               'N',
               1);
            COMMIT;
          EXCEPTION
            WHEN OTHERS THEN
              app_plsql_log.info('Insert OFFR_PRFL_SLS_CLS_PLCMT: ' ||
                                 SQLERRM);
              app_plsql_log.info('ERROR - OFFR_PRFL_SLS_CLS_PLCMT (
INSERT INTO OFFR_PRFL_SLS_CLS_PLCMT (
OFFR_ID,
SLS_CLS_CD,
PRFL_CD,
PG_OFS_NR,
FEATRD_SIDE_CD,
MRKT_ID,
VEH_ID,
OFFR_PERD_ID,
SKU_CNT,
PG_WGHT_PCT,
SKU_OFFR_STRGTH_PCT,
FEATRD_PRFL_IND,
USE_INSTRCTNS_IND,
PROD_ENDRSMT_ID,
FXD_PG_WGHT_IND,
PG_TYP_ID
)
VALUES (
' || v_cur_offr_id || ',
' || rec.sls_cls_cd || ',
' || rec.prfl_cd || ',
0,
0,
' || rec.mrkt_id || ',
' || rec.veh_id || ',
' || rec.offr_perd_id || ',
0, --???????????????????????????????????// SKU_CNT
100,
null,
' || 'N' || ',
' || 'N' || ',
null,
' || 'N' || ',
1
);');
          END;
          -- OFFR_PRFL_SLS_CLS_PLCMT record end
          -- OFFR_SLS_CLS_SKU record start
          BEGIN
            INSERT INTO offr_sls_cls_sku
              (offr_id,
               sls_cls_cd,
               prfl_cd,
               pg_ofs_nr,
               featrd_side_cd,
               sku_id,
               mrkt_id,
               smplg_ind,
               hero_ind,
               micr_ncpsltn_ind,
               reg_prc_amt,
               incntv_id,
               cost_amt)
            VALUES
              (v_cur_offr_id,
               rec.sls_cls_cd,
               rec.prfl_cd,
               0,
               0,
               rec.sku_id,
               rec.mrkt_id,
               'N',
               'N',
               'N',
               rec.reg_prc_amt,
               NULL,
               rec.wghtd_avg_cost_amt);
            COMMIT;
          EXCEPTION
            WHEN OTHERS THEN
              app_plsql_log.info('Insert OFFR_SLS_CLS_SKU: ' || SQLERRM);
              app_plsql_log.info('ERROR - OFFR_SLS_CLS_SKU (
INSERT INTO OFFR_SLS_CLS_SKU (
OFFR_ID,
SLS_CLS_CD,
PRFL_CD,
PG_OFS_NR,
FEATRD_SIDE_CD,
SKU_ID,
MRKT_ID,
SMPLG_IND,
HERO_IND,
MICR_NCPSLTN_IND,
REG_PRC_AMT,
INCNTV_ID,
COST_AMT
)
VALUES (
' || v_cur_offr_id || ',
' || rec.sls_cls_cd || ',
' || rec.prfl_cd || ',
0,
0,
' || rec.sku_id || ',
' || rec.mrkt_id || ',
N,
N,
N,
' || rec.reg_prc_amt || ',
null,
' || rec.wghtd_avg_cost_amt || '
);');
          END;
          --create price point record start
          v_prcpt_key := rec.offr_perd_id || '-' || rec.veh_id || '-' ||
                         rec.prfl_cd || '-' || rec.sls_cls_cd || '-' ||
                         rec.sls_prc_amt || '-' || rec.nr_for_qty;
          IF v_prcpt_ids_for_prflslsnf.exists(v_prcpt_key) THEN
            --old pricepoint id;
            v_cur_prcpt_id := v_prcpt_ids_for_prflslsnf(v_prcpt_key);
          ELSE
            --new pricepoint id;
            SELECT seq.nextval INTO v_cur_prcpt_id FROM dual;
            v_prcpt_ids_for_prflslsnf(v_prcpt_key) := v_cur_prcpt_id;
            --insert
            BEGIN
              INSERT INTO offr_prfl_prc_point
                (offr_prfl_prcpt_id,
                 offr_id,
                 promtn_clm_id,
                 veh_id,
                 promtn_id,
                 mrkt_id,
                 sls_cls_cd,
                 prfl_cd,
                 ssnl_evnt_id,
                 offr_perd_id,
                 sls_stus_cd,
                 crncy_cd,
                 sku_cnt,
                 nr_for_qty,
                 sku_offr_strgth_pct,
                 est_unit_qty,
                 est_sls_amt,
                 est_cost_amt,
                 sls_srce_id,
                 prfl_stus_rsn_desc_txt,
                 prfl_stus_cd,
                 sls_prc_amt,
                 tax_amt,
                 pymt_typ,
                 comsn_amt,
                 comsn_typ,
                 net_to_avon_fct,
                 prc_point_desc_txt,
                 prmry_offr_ind,
                 impct_catgry_id,
                 pg_ofs_nr,
                 featrd_side_cd,
                 cnsmr_invstmt_bdgt_id,
                 offr_prfl_prcpt_link_id,
                 sls_promtn_ind,
                 impct_prfl_cd,
                 awrd_sls_prc_amt,
                 chrty_amt,
                 chrty_ovrrd_ind,
                 tax_type_id,
                 roylt_pct,
                 roylt_ovrrd_ind,
                 unit_calc_ind,
                 demo_discnt_id,
                 frc_mtch_mthd_id)
              VALUES
                (v_cur_prcpt_id,
                 v_cur_offr_id,
                 147,
                 rec.veh_id,
                 10,
                 rec.mrkt_id,
                 rec.sls_cls_cd,
                 rec.prfl_cd,
                 NULL,
                 rec.offr_perd_id,
                 2,
                 rec.crncy_cd,
                 1, --?????????????????????????????????????????????????????????????????????? SKU_CNT
                 rec.nr_for_qty,
                 NULL,
                 0,
                 rec.sls_prc_amt,
                 rec.wghtd_avg_cost_amt,
                 1,
                 NULL,
                 NULL,
                 rec.sls_prc_amt,
                 rec.tax_amt,
                 1,
                 rec.comsn_amt,
                 rec.comsn_typ_id,
                 rec.gta_pct,
                 NULL,
                 'N',
                 NULL,
                 0,
                 0,
                 NULL,
                 NULL,
                 'N',
                 NULL,
                 0,
                 0,
                 'N',
                 rec.tax_typ_id,
                 0,
                 'N',
                 'N',
                 NULL,
                 NULL);
            EXCEPTION
              WHEN OTHERS THEN
                app_plsql_log.info('Insert OFFR_PRLF_PRC_POINT: ' ||
                                   SQLERRM);
                app_plsql_log.info('ERROR - OFFR_PRFL_PRC_POINT (
INSERT INTO OFFR_PRFL_PRC_POINT (
OFFR_PRFL_PRCPT_ID,
OFFR_ID,
PROMTN_CLM_ID,
VEH_ID,
PROMTN_ID,
MRKT_ID,
SLS_CLS_CD,
PRFL_CD,
SSNL_EVNT_ID,
OFFR_PERD_ID,
SLS_STUS_CD,
CRNCY_CD,
SKU_CNT,
NR_FOR_QTY,
SKU_OFFR_STRGTH_PCT,
EST_UNIT_QTY,
EST_SLS_AMT,
EST_COST_AMT,
SLS_SRCE_ID,
PRFL_STUS_RSN_DESC_TXT,
PRFL_STUS_CD,
SLS_PRC_AMT,
TAX_AMT,
PYMT_TYP,
COMSN_AMT,
COMSN_TYP,
NET_TO_AVON_FCT,
PRC_POINT_DESC_TXT,
PRMRY_OFFR_IND,
IMPCT_CATGRY_ID,
PG_OFS_NR,
FEATRD_SIDE_CD,
CNSMR_INVSTMT_BDGT_ID,
OFFR_PRFL_PRCPT_LINK_ID,
SLS_PROMTN_IND,
IMPCT_PRFL_CD,
AWRD_SLS_PRC_AMT,
CHRTY_AMT,
CHRTY_OVRRD_IND,
TAX_TYPE_ID,
ROYLT_PCT,
ROYLT_OVRRD_IND,
UNIT_CALC_IND,
DEMO_DISCNT_ID,
FRC_MTCH_MTHD_ID
)
VALUES (
' || v_cur_prcpt_id || ',
' || v_cur_offr_id || ',
147,
' || rec.veh_id || ',
10,
' || rec.mrkt_id || ',
' || rec.sls_cls_cd || ',
' || rec.prfl_cd || ',
null,
' || rec.offr_perd_id || ',
2,
' || rec.crncy_cd || ',
1 , --?????????????????????????????????????????????????????????????????????? SKU_CNT
' || rec.nr_for_qty || ',
null,
0,
' || rec.sls_prc_amt || ',
' || rec.wghtd_avg_cost_amt || ',
1,
null,
null,
' || rec.sls_prc_amt || ',
' || rec.tax_amt || ',
1,
' || rec.comsn_amt || ',
' || rec.comsn_typ_id || ',
' || rec.gta_pct || ',
null,
' || 'N' || ',
null,
0,
0,
null,
null,
' || 'N' || ',
null,
0,
0,
' || 'N' || ',
' || rec.tax_typ_id || ',
0,
' || 'N' || ',
' || 'N' || ',
null,
null
);');
            END;
          END IF;
          --create price point record end
          --create offer sku line record start
          v_offr_sku_line_id_key := rec.offr_perd_id || '-' || rec.veh_id || '-' ||
                                    rec.prfl_cd || '-' || rec.sls_cls_cd || '-' ||
                                    rec.sls_prc_amt || '-' ||
                                    rec.nr_for_qty || '-' || rec.sku_id;
          IF v_offr_sku_line_id.exists(v_offr_sku_line_id_key) THEN
            --old offer sku line id;
            v_cur_offr_sku_line_id := v_offr_sku_line_id(v_offr_sku_line_id_key);
          ELSE
            --new offer sku line id;
            SELECT seq.nextval INTO v_cur_offr_sku_line_id FROM dual;
            v_offr_sku_line_id(v_offr_sku_line_id_key) := v_cur_offr_sku_line_id;
            --insert
            BEGIN
              INSERT INTO offr_sku_line
                (offr_sku_line_id,
                 offr_id,
                 veh_id,
                 featrd_side_cd,
                 offr_perd_id,
                 mrkt_id,
                 sku_id,
                 pg_ofs_nr,
                 prfl_cd,
                 crncy_cd,
                 prmry_sku_offr_ind,
                 sls_cls_cd,
                 offr_prfl_prcpt_id,
                 promtn_desc_txt,
                 demo_avlbl_ind,
                 dltd_ind,
                 unit_splt_pct,
                 sls_prc_amt,
                 cost_typ,
                 offr_sku_line_link_id,
                 set_cmpnt_ind,
                 set_cmpnt_qty,
                 offr_sku_set_id,
                 line_nr,
                 unit_prc_amt,
                 line_nr_typ_id)
              VALUES
                (v_cur_offr_sku_line_id,
                 v_cur_offr_id,
                 rec.veh_id,
                 0,
                 rec.offr_perd_id,
                 rec.mrkt_id,
                 rec.sku_id,
                 0,
                 rec.prfl_cd,
                 rec.crncy_cd,
                 'N',
                 rec.sls_cls_cd,
                 v_cur_prcpt_id,
                 NULL,
                 'N',
                 decode(rec.bilng_id, NULL, 'Y', 'N'),
                 0, --??????????????????????????????????????????????????????????? UNIT_SPLT_PCT
                 rec.sls_prc_amt,
                 --'A',----552----
                 'P', ---QC3288
                 NULL,
                 'N',
                 0,
                 NULL,
                 NULL,
                 NULL,
                 NULL);
            EXCEPTION
              WHEN OTHERS THEN
                app_plsql_log.info('Insert OFFR_SKU_LINE: ' || SQLERRM);
                ok := FALSE;
                app_plsql_log.info('ERROR - OFFR_SKU_LINE (
INSERT INTO OFFR_SKU_LINE (
OFFR_SKU_LINE_ID,
OFFR_ID,
VEH_ID,
FEATRD_SIDE_CD,
OFFR_PERD_ID,
MRKT_ID,
SKU_ID,
PG_OFS_NR,
PRFL_CD,
CRNCY_CD,
PRMRY_SKU_OFFR_IND,
SLS_CLS_CD,
OFFR_PRFL_PRCPT_ID,
PROMTN_DESC_TXT,
DEMO_AVLBL_IND,
DLTD_IND,
UNIT_SPLT_PCT,
SLS_PRC_AMT,
COST_TYP,
OFFR_SKU_LINE_LINK_ID,
SET_CMPNT_IND,
SET_CMPNT_QTY,
OFFR_SKU_SET_ID,
LINE_NR,
UNIT_PRC_AMT,
LINE_NR_TYP_ID
)
VALUES (
' || v_cur_offr_sku_line_id || ',
' || v_cur_offr_id || ',
' || rec.veh_id || ',
0,
' || rec.offr_perd_id || ',
' || rec.mrkt_id || ',
' || rec.sku_id || ',
0,
' || rec.prfl_cd || ',
' || rec.crncy_cd || ',
' || 'N' || ',
' || rec.sls_cls_cd || ',
' || v_cur_prcpt_id || ',
null,
' || 'N' || ',
DECODE(' || rec.bilng_id || ',null,' || 'Y' || ',' || 'N' || '),
0, --??????????????????????????????????????????????????????????? UNIT_SPLT_PCT
' || rec.sls_prc_amt || ',
' || 'A' || ',
null,
' || 'N' || ',
0,
null,
null,
null,
null
);
');
            END;
          END IF;
          --create offer sku line record end
          --create dstrbtd_mrkt_sls record start
          --estimate
          BEGIN
            INSERT INTO dstrbtd_mrkt_sls
              (mrkt_id,
               sls_perd_id,
               offr_sku_line_id,
               sls_typ_id,
               sls_srce_id,
               offr_perd_id,
               sls_stus_cd,
               veh_id,
               unit_qty,
               comsn_amt,
               tax_amt,
               net_to_avon_fct,
               cost_amt)
            VALUES
              (rec.mrkt_id,
               rec.sls_perd_id,
               v_cur_offr_sku_line_id,
               1,
               billing_sls_srce_id,
               rec.offr_perd_id,
               final_sls_stus_cd,
               rec.veh_id,
               0,
               0,
               0,
               0,
               rec.wghtd_avg_cost_amt);
            COMMIT;
          EXCEPTION
            WHEN OTHERS THEN
              NULL;
          END;
          --operational estimate
          BEGIN
            INSERT INTO dstrbtd_mrkt_sls
              (mrkt_id,
               sls_perd_id,
               offr_sku_line_id,
               sls_typ_id,
               sls_srce_id,
               offr_perd_id,
               sls_stus_cd,
               veh_id,
               unit_qty,
               comsn_amt,
               tax_amt,
               net_to_avon_fct,
               cost_amt)
            VALUES
              (rec.mrkt_id,
               rec.sls_perd_id,
               v_cur_offr_sku_line_id,
               2,
               billing_sls_srce_id,
               rec.offr_perd_id,
               final_sls_stus_cd,
               rec.veh_id,
               0,
               0,
               0,
               0,
               rec.wghtd_avg_cost_amt);
            COMMIT;
          EXCEPTION
            WHEN OTHERS THEN
              NULL;
          END;
          --demand
          --billed
          --GTA calc
          --create dstrbtd_mrkt_sls record end
          --daily billing update
          IF ok THEN
            BEGIN
              UPDATE dly_bilng_trnd
                 SET dly_bilng_trnd.trnd_aloctn_manul_stus_id = manual_matched,
                     dly_bilng_trnd.trnd_aloctn_auto_stus_id  = auto_not_processed
               WHERE dly_bilng_trnd.dly_bilng_id = rec.bilng_id;
              COMMIT;
            EXCEPTION
              WHEN OTHERS THEN
                app_plsql_log.info('Update DLY_BILNG_TRND: ' || SQLERRM);
                NULL;
            END;
          END IF;
          ok := TRUE;
          app_plsql_log.info('UNPLND_OFFR_RECRDS REC:' || ' | ' ||
                             rec.mrkt_id || ' | ' || rec.veh_id || ' | ' ||
                             rec.ver_id || ' | ' || rec.offr_perd_id ||
                             ' | ' || rec.mrkt_veh_perd_sctn_id || ' | ' ||
                             rec.catgry_id || ' | ' || rec.prfl_cd ||
                             ' | ' || rec.prfl_nm || ' | ' ||
                             rec.sls_prc_amt || ' | ' || rec.nr_for_qty ||
                             ' | ' || rec.sku_id || ' | ' ||
                             rec.sls_perd_id || ' | ' ||
                             rec.wghtd_avg_cost_amt || ' | ' ||
                             rec.comsn_typ_id || ' | ' || rec.comsn_amt ||
                             ' | ' || rec.tax_typ_id || ' | ' ||
                             rec.tax_amt || ' | ' || rec.gta_method_id ||
                             ' | ' || rec.gta_pct || ' | ' ||
                             rec.sls_cls_cd || ' | ' || rec.bilng_id ||
                             ' | ' || rec.demand_unit_qty || ' | ' ||
                             rec.billed_unit_qty || ' | ' ||
                             rec.demand_gta || ' | ' || rec.billed_gta ||
                             ' | ' || rec.demand_comsn_amt || ' | ' ||
                             rec.billed_comsn_amt || ' | ' ||
                             rec.demand_tax_amt || ' | ' ||
                             rec.billed_tax_amt);
        ELSE
          app_plsql_log.info('NULL rec.bilng_id:' || rec.bilng_id);
        END IF;
      END LOOP;
    END LOOP;
    --V_OFFR_IDS_FOR_PRFL_CD
    v_key1 := v_offr_id_prfl_cd.first;
    WHILE v_key1 <= v_offr_id_prfl_cd.last LOOP
      app_plsql_log.info('UNPLND_OFFR_RECRDS REC OFFER_ID/PRFL_CD:' ||
                         v_key1 || '/' || v_offr_id_prfl_cd(v_key1));
      --create soft deleted items start
      FOR rec IN (SELECT
                  -- dstrbtd_mrkt_sls
                   p_sls_perd_id                    sls_perd_id,
                   offr_prfl_prc_point.offr_perd_id,
                   0                                unit_qty,
                   0                                comsn_amt,
                   0                                tax_amt,
                   0                                net_to_avon_fct,
                   -- offr_sku_line
                   offr_prfl_prc_point.veh_id,
                   offr_prfl_prc_point.crncy_cd,
                   'N' prmry_sku_offr_ind,
                   offr_prfl_prc_point.offr_prfl_prcpt_id,
                   NULL promtn_desc_txt,
                   'N' demo_avlbl_ind,
                   'Y' dltd_ind,
                   0 unit_splt_pct,
                   offr_prfl_prc_point.sls_prc_amt,
                   --'A' COST_TYP,----552----
                   'P' cost_typ, --QC3288
                   NULL offr_sku_line_link_id,
                   'N' set_cmpnt_ind,
                   0 set_cmpnt_qty,
                   NULL offr_sku_set_id,
                   NULL line_nr,
                   NULL unit_prc_amt,
                   NULL line_nr_typ_id,
                   --sls cls sku
                   offr_prfl_prc_point.offr_id,
                   offr_prfl_prc_point.sls_cls_cd,
                   offr_prfl_prc_point.prfl_cd,
                   offr_prfl_prc_point.pg_ofs_nr,
                   offr_prfl_prc_point.featrd_side_cd,
                   not_e_db.sku_id,
                   offr_prfl_prc_point.mrkt_id,
                   'N' smplg_ind,
                   'N' hero_ind,
                   'N' micr_ncpsltn_ind,
                   sku_reg_prc.reg_prc_amt,
                   NULL incntv_id,
                   sku_cost.wghtd_avg_cost_amt
                  -----
                    FROM (SELECT db.offr_prfl_prcpt_id,
                                 db.prfl_cd,
                                 sku.sku_id
                            FROM (SELECT offr_sku_line.offr_prfl_prcpt_id,
                                         offr_sku_line.prfl_cd,
                                         offr_sku_line.sku_id,
                                         COUNT(*) over(PARTITION BY offr_sku_line.offr_prfl_prcpt_id) sku_count,
                                         sv.v_count
                                    FROM (SELECT *
                                            FROM offr_sku_line
                                           WHERE offr_sku_line.mrkt_id =
                                                 p_mrkt_id
                                             AND offr_sku_line.offr_id = v_key1) offr_sku_line,
                                         (SELECT prfl_cd, COUNT(*) v_count
                                            FROM mrkt_sku, sku
                                           WHERE sku.sku_id = mrkt_sku.sku_id
                                             AND sku.prfl_cd =
                                                 v_offr_id_prfl_cd(v_key1)
                                             AND mrkt_sku.mrkt_id = p_mrkt_id
                                             AND pa_maps_public.get_sls_cls_cd(p_sls_perd_id,
                                                                               mrkt_sku.mrkt_id,
                                                                               mrkt_sku.avlbl_perd_id,
                                                                               mrkt_sku.intrdctn_perd_id,
                                                                               mrkt_sku.demo_ofs_nr,
                                                                               mrkt_sku.demo_durtn_nr,
                                                                               mrkt_sku.new_durtn_nr,
                                                                               mrkt_sku.stus_perd_id,
                                                                               mrkt_sku.dspostn_perd_id,
                                                                               mrkt_sku.on_stus_perd_id) > -1
                                           GROUP BY prfl_cd) sv
                                   WHERE sv.prfl_cd = offr_sku_line.prfl_cd(+)) db,
                                 sku
                           WHERE offr_prfl_prcpt_id IS NOT NULL
                             AND sku_count < v_count
                             AND db.prfl_cd = sku.prfl_cd
                             AND db.sku_id <> sku.sku_id) not_e_db,
                         offr_prfl_prc_point,
                         sku_reg_prc,
                         sku_cost
                   WHERE not_e_db.offr_prfl_prcpt_id =
                         offr_prfl_prc_point.offr_prfl_prcpt_id
                     AND sku_reg_prc.mrkt_id = offr_prfl_prc_point.mrkt_id
                     AND sku_reg_prc.offr_perd_id =
                         offr_prfl_prc_point.offr_perd_id
                     AND sku_reg_prc.sku_id = not_e_db.sku_id
                     AND sku_cost.mrkt_id = offr_prfl_prc_point.mrkt_id
                     AND sku_cost.cost_typ = 'A'
                     AND sku_cost.offr_perd_id =
                         offr_prfl_prc_point.offr_perd_id
                     AND sku_cost.sku_id = not_e_db.sku_id
                   ORDER BY offr_prfl_prc_point.offr_prfl_prcpt_id, sku_id) LOOP
        -- OFFR_SLS_CLS_SKU record start
        BEGIN
          INSERT INTO offr_sls_cls_sku
            (offr_id,
             sls_cls_cd,
             prfl_cd,
             pg_ofs_nr,
             featrd_side_cd,
             sku_id,
             mrkt_id,
             smplg_ind,
             hero_ind,
             micr_ncpsltn_ind,
             reg_prc_amt,
             incntv_id,
             cost_amt)
          VALUES
            (rec.offr_id,
             rec.sls_cls_cd,
             rec.prfl_cd,
             0,
             0,
             rec.sku_id,
             rec.mrkt_id,
             'N',
             'N',
             'N',
             rec.reg_prc_amt,
             NULL,
             rec.wghtd_avg_cost_amt);
          COMMIT;
        EXCEPTION
          WHEN OTHERS THEN
            app_plsql_log.info('Insert OFFR_SLS_CLS_SKU 2: ' || SQLERRM);
            app_plsql_log.info('ERROR2 - OFFR_SLS_CLS_SKU (
INSERT INTO OFFR_SLS_CLS_SKU (
OFFR_ID,
SLS_CLS_CD,
PRFL_CD,
PG_OFS_NR,
FEATRD_SIDE_CD,
SKU_ID,
MRKT_ID,
SMPLG_IND,
HERO_IND,
MICR_NCPSLTN_IND,
REG_PRC_AMT,
INCNTV_ID,
COST_AMT
)
VALUES (
' || rec.offr_id || ',
' || rec.sls_cls_cd || ',
' || rec.prfl_cd || ',
0,
0,
' || rec.sku_id || ',
' || rec.mrkt_id || ',
N,
N,
N,
' || rec.reg_prc_amt || ',
null,
' || rec.wghtd_avg_cost_amt || '
);');
        END;
        --offr_sku_line
        SELECT seq.nextval INTO v_cur_offr_sku_line_id FROM dual;
        v_offr_sku_line_id(v_offr_sku_line_id_key) := v_cur_offr_sku_line_id;
        --insert
        BEGIN
          INSERT INTO offr_sku_line
            (offr_sku_line_id,
             offr_id,
             veh_id,
             featrd_side_cd,
             offr_perd_id,
             mrkt_id,
             sku_id,
             pg_ofs_nr,
             prfl_cd,
             crncy_cd,
             prmry_sku_offr_ind,
             sls_cls_cd,
             offr_prfl_prcpt_id,
             promtn_desc_txt,
             demo_avlbl_ind,
             dltd_ind,
             unit_splt_pct,
             sls_prc_amt,
             cost_typ,
             offr_sku_line_link_id,
             set_cmpnt_ind,
             set_cmpnt_qty,
             offr_sku_set_id,
             line_nr,
             unit_prc_amt,
             line_nr_typ_id)
          VALUES
            (v_cur_offr_sku_line_id,
             rec.offr_id,
             rec.veh_id,
             0,
             rec.offr_perd_id,
             rec.mrkt_id,
             rec.sku_id,
             0,
             rec.prfl_cd,
             rec.crncy_cd,
             'N',
             rec.sls_cls_cd,
             rec.offr_prfl_prcpt_id,
             NULL,
             'N',
             'Y',
             0, --??????????????????????????????????????????????????????????? UNIT_SPLT_PCT
             rec.sls_prc_amt,
             'A',
             NULL,
             'N',
             0,
             NULL,
             NULL,
             NULL,
             NULL);
        EXCEPTION
          WHEN OTHERS THEN
            app_plsql_log.info('Insert OFFR_SKU_LINE 2: ' || SQLERRM);
            app_plsql_log.info('ERROR2 - OFFR_SKU_LINE (
INSERT INTO OFFR_SKU_LINE (
OFFR_SKU_LINE_ID,
OFFR_ID,
VEH_ID,
FEATRD_SIDE_CD,
OFFR_PERD_ID,
MRKT_ID,
SKU_ID,
PG_OFS_NR,
PRFL_CD,
CRNCY_CD,
PRMRY_SKU_OFFR_IND,
SLS_CLS_CD,
OFFR_PRFL_PRCPT_ID,
PROMTN_DESC_TXT,
DEMO_AVLBL_IND,
DLTD_IND,
UNIT_SPLT_PCT,
SLS_PRC_AMT,
COST_TYP,
OFFR_SKU_LINE_LINK_ID,
SET_CMPNT_IND,
SET_CMPNT_QTY,
OFFR_SKU_SET_ID,
LINE_NR,
UNIT_PRC_AMT,
LINE_NR_TYP_ID
)
VALUES (
' || v_cur_offr_sku_line_id || ',
' || rec.offr_id || ',
' || rec.veh_id || ',
0,
' || rec.offr_perd_id || ',
' || rec.mrkt_id || ',
' || rec.sku_id || ',
0,
' || rec.prfl_cd || ',
' || rec.crncy_cd || ',
' || 'N' || ',
' || rec.sls_cls_cd || ',
' || rec.offr_prfl_prcpt_id || ',
null,
' || 'N' || ',
' || 'N' || ',
0, --??????????????????????????????????????????????????????????? UNIT_SPLT_PCT
' || rec.sls_prc_amt || ',
' || 'A' || ',
null,
' || 'N' || ',
0,
null,
null,
null,
null
);
');
        END;
        --estimate
        BEGIN
          INSERT INTO dstrbtd_mrkt_sls
            (mrkt_id,
             sls_perd_id,
             offr_sku_line_id,
             sls_typ_id,
             sls_srce_id,
             offr_perd_id,
             sls_stus_cd,
             veh_id,
             unit_qty,
             comsn_amt,
             tax_amt,
             net_to_avon_fct,
             cost_amt)
          VALUES
            (rec.mrkt_id,
             rec.sls_perd_id,
             v_cur_offr_sku_line_id,
             1,
             billing_sls_srce_id,
             rec.offr_perd_id,
             final_sls_stus_cd,
             rec.veh_id,
             0,
             0,
             0,
             0,
             rec.wghtd_avg_cost_amt);
          COMMIT;
        EXCEPTION
          WHEN OTHERS THEN
            app_plsql_log.info('Insert DSTRBTD_MRKT_SLS: ' || SQLERRM);
            NULL;
        END;
      END LOOP;
      DELETE FROM dstrbtd_mrkt_sls
       WHERE mrkt_id = p_mrkt_id
         AND sls_perd_id = p_sls_perd_id
         AND offr_sku_line_id IN
             (SELECT offr_sku_line.offr_sku_line_id
                FROM offr_sku_line, mrkt_sku
               WHERE offr_sku_line.mrkt_id = p_mrkt_id
                 AND offr_sku_line.offr_id = v_key1
                 AND offr_sku_line.mrkt_id = mrkt_sku.mrkt_id
                 AND offr_sku_line.sku_id = mrkt_sku.sku_id
                 AND offr_sku_line.sls_cls_cd <>
                     pa_maps_public.get_sls_cls_cd(offr_sku_line.offr_perd_id,
                                                   mrkt_sku.mrkt_id,
                                                   mrkt_sku.avlbl_perd_id,
                                                   mrkt_sku.intrdctn_perd_id,
                                                   mrkt_sku.demo_ofs_nr,
                                                   mrkt_sku.demo_durtn_nr,
                                                   mrkt_sku.new_durtn_nr,
                                                   mrkt_sku.stus_perd_id,
                                                   mrkt_sku.dspostn_perd_id,
                                                   mrkt_sku.on_stus_perd_id));
      COMMIT;
      DELETE FROM offr_sku_line
       WHERE offr_sku_line.mrkt_id = p_mrkt_id
         AND offr_sku_line.offr_id = v_key1
         AND offr_sku_line.sls_cls_cd <>
             (SELECT MAX(pa_maps_public.get_sls_cls_cd(offr_sku_line.offr_perd_id,
                                                       mrkt_sku.mrkt_id,
                                                       mrkt_sku.avlbl_perd_id,
                                                       mrkt_sku.intrdctn_perd_id,
                                                       mrkt_sku.demo_ofs_nr,
                                                       mrkt_sku.demo_durtn_nr,
                                                       mrkt_sku.new_durtn_nr,
                                                       mrkt_sku.stus_perd_id,
                                                       mrkt_sku.dspostn_perd_id,
                                                       mrkt_sku.on_stus_perd_id))
                FROM mrkt_sku
               WHERE mrkt_sku.mrkt_id = offr_sku_line.mrkt_id
                 AND mrkt_sku.sku_id = offr_sku_line.sku_id);
      COMMIT;
      --create soft deleted items end
      v_key1 := v_offr_id_prfl_cd.next(v_key1);
    END LOOP;
    --APP_PLSQL_LOG.info('UNPLND_OFFR_RECRDS CREATED OFFER IDS:' || v_used_offer_ids);
    COMMIT;
    app_plsql_log.info(l_module_name || ' end' || l_parameter_list);
  END unplnd_offr_recrds;

  PROCEDURE unplan_offr_creation(p_mrkt_id     IN NUMBER,
                                 p_sls_perd_id IN NUMBER,
                                 p_user_id     IN VARCHAR2 DEFAULT NULL,
                                 p_run_id      IN NUMBER DEFAULT NULL,
                                 p_stus        OUT NUMBER) IS
    -- local variables
    p_result tbl_sa_unplnd_offr_vw;
    -- for LOG
    l_run_id         NUMBER := nvl(p_run_id,
                                   app_plsql_output.generate_new_run_id);
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'UNPLAN_OFFR_CREATION';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       to_char(p_mrkt_id) || ', ' ||
                                       'p_sls_perd_id: ' ||
                                       to_char(p_sls_perd_id) || ', ' ||
                                       'p_user_id: ' || l_user_id || ', ' ||
                                       'p_run_id: ' || to_char(l_run_id) || ')';
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start' || l_parameter_list);
    BEGIN
      -- unplnd_offr_recrds
      unplnd_offr_recrds(p_mrkt_id     => p_mrkt_id,
                         p_sls_perd_id => p_sls_perd_id,
                         p_user_id     => l_user_id,
                         p_run_id      => l_run_id,
                         p_result      => p_result);
    EXCEPTION
      WHEN OTHERS THEN
        p_stus := 2;
    END;
    p_stus := 0;
    app_plsql_log.info(l_module_name || ' end' || l_parameter_list);
  END unplan_offr_creation;
  --
END pa_trend_alloc;