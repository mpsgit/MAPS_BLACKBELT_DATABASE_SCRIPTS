CREATE OR REPLACE PACKAGE pa_trend_alloc AS

  g_package_name CONSTANT VARCHAR2(30) := 'PA_TREND_ALLOC';

  TYPE r_periods IS RECORD(
    dbt_on_sls_perd_id   dly_bilng_trnd.trnd_sls_perd_id%TYPE,
    dbt_on_offr_perd_id  dly_bilng_trnd.offr_perd_id%TYPE,
    dbt_off_sls_perd_id  dly_bilng_trnd.trnd_sls_perd_id%TYPE,
    dbt_off_offr_perd_id dly_bilng_trnd.offr_perd_id%TYPE,
    dms_on_sls_perd_id   dstrbtd_mrkt_sls.sls_perd_id%TYPE,
    dms_on_offr_perd_id  dstrbtd_mrkt_sls.offr_perd_id%TYPE,
    dms_off_sls_perd_id  dstrbtd_mrkt_sls.sls_perd_id%TYPE,
    dms_off_offr_perd_id dstrbtd_mrkt_sls.offr_perd_id%TYPE,
    dbt_bilng_day        dly_bilng_trnd.prcsng_dt%TYPE,
    sct_cash_val         mrkt_sls_perd.sct_cash_val%TYPE,
    sct_r_factor         mrkt_sls_perd.sct_r_factor%TYPE);

  TYPE r_hist_head_aggr IS RECORD(
    p_sls_perd_id     dly_bilng_trnd.trnd_sls_perd_id%TYPE,
    p_sku_id          offr_sku_line.sku_id%TYPE,
    mrkt_id           offr.mrkt_id%TYPE,
    veh_id            offr.veh_id%TYPE,
    offr_desc_txt     offr.offr_desc_txt%TYPE,
    promtn_id         offr_prfl_prc_point.promtn_id%TYPE,
    promtn_clm_id     offr_prfl_prc_point.promtn_clm_id%TYPE,
    sls_cls_cd        offr_prfl_prc_point.sls_cls_cd%TYPE,
    bi24_unts_on      NUMBER,
    bi24_sls_on       NUMBER,
    bi24_unts_off     NUMBER,
    bi24_sls_off      NUMBER,
    estimate_unts_on  NUMBER,
    estimate_sls_on   NUMBER,
    estimate_unts_off NUMBER,
    estimate_sls_off  NUMBER,
    trend_unts_on     NUMBER,
    trend_sls_on      NUMBER,
    trend_unts_off    NUMBER,
    trend_sls_off     NUMBER,
    actual_unts_on    NUMBER,
    actual_sls_on     NUMBER,
    actual_unts_off   NUMBER,
    actual_sls_off    NUMBER);
  TYPE t_hist_head_aggr IS TABLE OF r_hist_head_aggr;

  TYPE r_reproc IS RECORD(
    mrkt_id              dstrbtd_mrkt_sls.mrkt_id%TYPE,
    sls_typ_id           dstrbtd_mrkt_sls.sls_typ_id%TYPE,
    offr_sku_line_id     dstrbtd_mrkt_sls.offr_sku_line_id%TYPE,
    dbt_on_sls_perd_id   dly_bilng_trnd.trnd_sls_perd_id%TYPE,
    dbt_on_offr_perd_id  dly_bilng_trnd.offr_perd_id%TYPE,
    dbt_off_sls_perd_id  dly_bilng_trnd.trnd_sls_perd_id%TYPE,
    dbt_off_offr_perd_id dly_bilng_trnd.offr_perd_id%TYPE,
    dms_on_sls_perd_id   dstrbtd_mrkt_sls.sls_perd_id%TYPE,
    dms_on_offr_perd_id  dstrbtd_mrkt_sls.offr_perd_id%TYPE,
    dms_off_sls_perd_id  dstrbtd_mrkt_sls.sls_perd_id%TYPE,
    dms_off_offr_perd_id dstrbtd_mrkt_sls.offr_perd_id%TYPE,
    frcstd_unts_on       NUMBER,
    frcstd_sls_on        NUMBER,
    frcstd_unts_off      NUMBER,
    frcstd_sls_off       NUMBER,
    veh_id               offr.veh_id%TYPE,
    comsn_amt            NUMBER,
    tax_amt              NUMBER,
    on_off_flag          CHAR(3));
  TYPE t_reproc IS TABLE OF r_reproc;

  TYPE r_bi24_r_factor IS RECORD(
    bi24_unts_on  NUMBER,
    bi24_sls_on   NUMBER,
    bi24_unts_off NUMBER,
    bi24_sls_off  NUMBER,
    r_factor      NUMBER);

  FUNCTION get_trend_alloc_head_view(p_mrkt_id     IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                                     p_sls_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                     p_trg_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                     p_sls_typ_id  IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                                     p_bilng_day   IN dly_bilng_trnd.prcsng_dt%TYPE,
                                     p_run_id      IN NUMBER DEFAULT NULL,
                                     p_user_id     IN VARCHAR2 DEFAULT NULL)
    RETURN obj_trend_alloc_view_table
    PIPELINED;

  FUNCTION get_trend_alloc_hist_head(p_mrkt_id     IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                                     p_sls_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                     p_trg_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                     p_sls_typ_id  IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                                     p_bilng_day   IN dly_bilng_trnd.prcsng_dt%TYPE,
                                     p_perd_from   IN NUMBER,
                                     p_perd_to     IN NUMBER,
                                     p_run_id      IN NUMBER DEFAULT NULL,
                                     p_user_id     IN VARCHAR2 DEFAULT NULL)
    RETURN obj_trend_alloc_hist_hd_table
    PIPELINED;

  FUNCTION get_trend_alloc(p_mrkt_id     IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                           p_sls_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                           p_trg_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                           p_sls_typ_id  IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                           p_bilng_day   IN dly_bilng_trnd.prcsng_dt%TYPE,
                           p_run_id      IN NUMBER DEFAULT NULL,
                           p_user_id     IN VARCHAR2 DEFAULT NULL)
    RETURN obj_trend_alloc_hist_hd_table
    PIPELINED;

  FUNCTION get_trend_alloc_re_proc(p_mrkt_id              IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                                   p_sls_perd_id          IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                   p_trg_perd_id          IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                   p_sls_typ_id           IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                                   p_bilng_day            IN dly_bilng_trnd.prcsng_dt%TYPE,
                                   p_cash_value           IN NUMBER,
                                   p_r_factor             IN NUMBER,
                                   p_use_offers_on_sched  IN CHAR DEFAULT 'N',
                                   p_use_offers_off_sched IN CHAR DEFAULT 'N',
                                   p_run_id               IN NUMBER DEFAULT NULL,
                                   p_user_id              IN VARCHAR2 DEFAULT NULL)
    RETURN obj_trend_alloc_hist_hd_table
    PIPELINED;

  FUNCTION get_trend_alloc_hist_dtls(p_mrkt_id     IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                                     p_sls_perd_id IN dly_bilng_trnd.trnd_sls_perd_id%TYPE,
                                     p_trg_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                     p_sls_typ_id  IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                                     p_bilng_day   IN dly_bilng.prcsng_dt%TYPE,
                                     p_run_id      IN NUMBER DEFAULT NULL,
                                     p_user_id     IN VARCHAR2 DEFAULT NULL)
    RETURN obj_trend_alloc_hist_dt_table
    PIPELINED;

  PROCEDURE save_trend_alloctn(p_mrkt_id              IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                               p_sls_perd_id          IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                               p_trg_perd_id          IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                               p_sls_typ_id           IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                               p_bilng_day            IN dly_bilng_trnd.prcsng_dt%TYPE,
                               p_cash_value           IN mrkt_sls_perd.sct_cash_val%TYPE,
                               p_r_factor             IN mrkt_sls_perd.sct_r_factor%TYPE,
                               p_use_offers_on_sched  IN CHAR,
                               p_use_offers_off_sched IN CHAR,
                               p_run_id               IN NUMBER DEFAULT NULL,
                               p_user_id              IN VARCHAR2 DEFAULT NULL,
                               p_stus                 OUT NUMBER);

  PROCEDURE set_sct_autclc(p_mrkt_id     IN mrkt_sls_perd.mrkt_id%TYPE,
                           p_trg_perd_id IN mrkt_sls_perd.sls_perd_id%TYPE,
                           p_sls_typ_id  IN mrkt_sls_perd.sct_sls_typ_id%TYPE,
                           p_autclc_ind  IN mrkt_sls_perd.sct_autclc_est_ind%TYPE,
                           p_run_id      IN NUMBER DEFAULT NULL,
                           p_user_id     IN VARCHAR2 DEFAULT NULL,
                           p_stus        OUT NUMBER);

  FUNCTION get_saved_trend_head(p_mrkt_id     IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                                p_sls_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                p_trg_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                p_sls_typ_id  IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                                p_bilng_day   IN dly_bilng_trnd.prcsng_dt%TYPE,
                                p_run_id      IN NUMBER DEFAULT NULL,
                                p_user_id     IN VARCHAR2 DEFAULT NULL)
    RETURN obj_trend_alloc_hist_hd_table
    PIPELINED;

  ---------------------------------------------
  -- inherited from WEDEV.TRND_ALOCTN package -
  ---------------------------------------------
  --MARKETS
  uk CONSTANT NUMBER := 73;

  --NUMBERS
  four  CONSTANT NUMBER := 4;
  three CONSTANT NUMBER := 3;

  --sales type ids
  marketing_bst_id CONSTANT NUMBER := 4;
  marketing_est_id CONSTANT NUMBER := 3;
  marketing_fst_id CONSTANT NUMBER := 5;

  supply_bst_id CONSTANT NUMBER := 104;
  supply_est_id CONSTANT NUMBER := 103;
  supply_fst_id CONSTANT NUMBER := 105;

  -- auto status values
  auto_not_processed    CONSTANT NUMBER := 0;
  auto_excluded         CONSTANT NUMBER := 1;
  auto_matched          CONSTANT NUMBER := 2;
  auto_no_suggested     CONSTANT NUMBER := 3;
  auto_no_fsc_to_item   CONSTANT NUMBER := 4;
  auto_suggested_single CONSTANT NUMBER := 5;
  auto_suggested_multi  CONSTANT NUMBER := 6;

  -- manual status values
  manual_any_status    CONSTANT NUMBER := -1;
  manual_not_processed CONSTANT NUMBER := 0;
  manual_excluded      CONSTANT NUMBER := 1;
  manual_matched       CONSTANT NUMBER := 2;
  manual_not_suggested CONSTANT NUMBER := 3;

  -- data exclusion types
  force_match_data_xclusn      CONSTANT NUMBER := 3;
  suggested_match_data_xclusn  CONSTANT NUMBER := 4;
  exclude_data_xclusn          CONSTANT NUMBER := 2;
  unplanned_offers_data_xclusn CONSTANT NUMBER := 5;
  control_total_data_xclusn    CONSTANT NUMBER := 1;

  -- sales source/status values for DMS
  billing_sls_srce_id CONSTANT NUMBER := 5;
  final_sls_stus_cd   CONSTANT NUMBER := 3;
  default_cost_amt    CONSTANT NUMBER := 0;

  -- sales types
  estimate       CONSTANT NUMBER := 1;
  demand_actuals CONSTANT NUMBER := 6;
  billed_actuals CONSTANT NUMBER := 7;

  -- SA match types ie via auto/manual tolerance levels
  no_match  CONSTANT CHAR(1) := 'N';
  direct    CONSTANT CHAR(1) := 'D';
  suggested CONSTANT CHAR(1) := 'S';
  -- Sku Match methods
  osl_match CONSTANT NUMBER := 1;
  fgc_match CONSTANT NUMBER := 2;
  fsc_match CONSTANT NUMBER := 3;

  -- delete reasons for possible SA matches which were subsequently rejected
  invalid_veh_sls_chnl CONSTANT NUMBER := 1;
  -- vehicle/sales channel did not form a valid combination
  unplanned_offer CONSTANT NUMBER := 2;
  -- planned matches were found so unplanned matches ignored
  suggested_match CONSTANT NUMBER := 3;
  -- a direct match was found so suggested ones ignored
  line_number_direct CONSTANT NUMBER := 4;
  -- line number check converted multi to direct match
  line_number_suggested CONSTANT NUMBER := 5;
  -- no line numbers matched so only direct matches used in multi
  no_line_number_suggested CONSTANT NUMBER := 6;
  -- no line number checking, directs become multi, suggested ignored

  PROCEDURE auto_procs(p_mrkt_id     IN mrkt.mrkt_id%TYPE,
                       p_sls_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                       p_user_nm     IN VARCHAR,
                       p_run_id      IN NUMBER DEFAULT NULL,
                       p_user_id     IN VARCHAR2 DEFAULT NULL);

  PROCEDURE procs_multmtch(p_mrkt_id     IN mrkt.mrkt_id%TYPE,
                           p_sls_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                           p_sls_typ_id  IN dly_bilng_offr_sku_line.sls_typ_id%TYPE);

  TYPE trec_ctgry_sls IS RECORD(
    mrkt_id               NUMBER,
    offst                 NUMBER,
    offr_perd_id          NUMBER,
    sls_perd_id           NUMBER,
    prcsng_dt             DATE,
    day_num               NUMBER,
    catgry_id             NUMBER,
    billed_units          NUMBER,
    billed_sls_amt        NUMBER,
    billed_comsn_amt      NUMBER,
    billed_tax_amt        NUMBER,
    demand_units          NUMBER,
    demand_sls_amt        NUMBER,
    demand_comsn_amt      NUMBER,
    demand_tax_amt        NUMBER,
    bi24_billed_units     NUMBER,
    bi24_billed_sls_amt   NUMBER,
    bi24_billed_comsn_amt NUMBER,
    bi24_billed_tax_amt   NUMBER,
    bi24_demand_units     NUMBER,
    bi24_demand_sls_amt   NUMBER,
    bi24_demand_comsn_amt NUMBER,
    bi24_demand_tax_amt   NUMBER,
    trend_units           NUMBER,
    trend_sls_amt         NUMBER,
    trend_comsn_amt       NUMBER,
    trend_tax_amt         NUMBER,
    early_trend_units     NUMBER,
    early_trend_sls_amt   NUMBER,
    early_trend_comsn_amt NUMBER,
    early_trend_tax_amt   NUMBER,
    final_trend_units     NUMBER,
    final_trend_sls_amt   NUMBER,
    final_trend_comsn_amt NUMBER,
    final_trend_tax_amt   NUMBER,
    selected_global_hist  NUMBER,
    selected_catgry_hist  NUMBER,
    global_pct_ovr        NUMBER,
    catgry_pct_ovr        NUMBER,
    selected_catgry       NUMBER);

  -- runs the jobs from the TRND_CTGRY_SLS_JOB table
  PROCEDURE process_jobs(p_run_id  IN NUMBER DEFAULT NULL,
                         p_user_id IN VARCHAR2 DEFAULT NULL);

  -- process new periods (load data from dly_bilng to dly_bilng_trnd)
  -- creates a new job record in TRND_CTGRY_SLS_JOB table
  PROCEDURE process_jobs_new_periods;
  --
END pa_trend_alloc;
/
CREATE OR REPLACE PACKAGE BODY pa_trend_alloc AS
  -- get_sls_typ_id_from_config
  FUNCTION get_sls_typ_id_from_config(p_mrkt_id IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                                      p_run_id  IN NUMBER DEFAULT NULL,
                                      p_user_id IN VARCHAR2 DEFAULT NULL)
    RETURN dly_bilng_trnd_offr_sku_line.sls_typ_id%TYPE IS
    l_sls_typ_id_from_config dly_bilng_trnd_offr_sku_line.sls_typ_id%TYPE;
    -- for LOG
    l_run_id         NUMBER := nvl(p_run_id,
                                   app_plsql_output.generate_new_run_id);
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'GET_SLS_TYP_ID_FROM_CONFIG';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       to_char(p_mrkt_id) || ', ' ||
                                       'p_run_id: ' || to_char(l_run_id) || ', ' ||
                                       'p_user_id: ' || l_user_id || ')';
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    -- sls_typ_id_from_config
    BEGIN
      SELECT mrkt_config_item_val_txt
        INTO l_sls_typ_id_from_config
        FROM mrkt_config_item
       WHERE mrkt_id = p_mrkt_id
         AND config_item_id = 10000;
    EXCEPTION
      WHEN OTHERS THEN
        app_plsql_log.info(l_module_name ||
                           ' warning: "MRKT_CONFIG_ITEM_VAL_TXT" not found in table: mrkt_config_item where mrkt_id=' ||
                           to_char(p_mrkt_id) ||
                           'and config_item_id = 10000' ||
                           l_parameter_list);
        l_sls_typ_id_from_config := -999;
    END;
    RETURN l_sls_typ_id_from_config;
  END get_sls_typ_id_from_config;

  -- get_periods
  FUNCTION get_periods(p_mrkt_id       IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                       p_orig_perd_id  IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                       p_bilng_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                       p_sls_typ_id    IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                       p_bilng_day     IN dly_bilng_trnd.prcsng_dt%TYPE,
                       p_run_id        IN NUMBER DEFAULT NULL,
                       p_user_id       IN VARCHAR2 DEFAULT NULL)
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
                                       'p_run_id: ' || to_char(l_run_id) || ', ' ||
                                       'p_user_id: ' || l_user_id || ')';
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    -- dbt
    l_periods.dbt_on_sls_perd_id   := p_bilng_perd_id;
    l_periods.dbt_on_offr_perd_id  := pa_manl_trend_adjstmnt.get_target_campaign(p_mrkt_id     => p_mrkt_id,
                                                                                 p_sls_perd_id => p_bilng_perd_id,
                                                                                 p_sls_typ_id  => p_sls_typ_id);
    l_periods.dbt_off_sls_perd_id  := p_bilng_perd_id;
    l_periods.dbt_off_offr_perd_id := pa_maps_public.perd_plus(p_mrkt_id,
                                                               pa_manl_trend_adjstmnt.get_target_campaign(p_mrkt_id     => p_mrkt_id,
                                                                                                          p_sls_perd_id => p_bilng_perd_id,
                                                                                                          p_sls_typ_id  => p_sls_typ_id),
                                                               -1);
    -- dms
    l_periods.dms_on_sls_perd_id   := pa_manl_trend_adjstmnt.get_target_campaign(p_mrkt_id     => p_mrkt_id,
                                                                                 p_sls_perd_id => p_bilng_perd_id,
                                                                                 p_sls_typ_id  => p_sls_typ_id);
    l_periods.dms_on_offr_perd_id  := pa_manl_trend_adjstmnt.get_target_campaign(p_mrkt_id     => p_mrkt_id,
                                                                                 p_sls_perd_id => p_bilng_perd_id,
                                                                                 p_sls_typ_id  => p_sls_typ_id);
    l_periods.dms_off_sls_perd_id  := pa_manl_trend_adjstmnt.get_target_campaign(p_mrkt_id     => p_mrkt_id,
                                                                                 p_sls_perd_id => p_bilng_perd_id,
                                                                                 p_sls_typ_id  => p_sls_typ_id);
    l_periods.dms_off_offr_perd_id := pa_maps_public.perd_plus(p_mrkt_id,
                                                               pa_manl_trend_adjstmnt.get_target_campaign(p_mrkt_id     => p_mrkt_id,
                                                                                                          p_sls_perd_id => p_bilng_perd_id,
                                                                                                          p_sls_typ_id  => p_sls_typ_id),
                                                               -1);
    -- billing day
    IF p_orig_perd_id = p_bilng_perd_id THEN
      l_periods.dbt_bilng_day := p_bilng_day;
    ELSE
      -- has to be recalculated for billing period 
      BEGIN
        SELECT MAX(trnd_bilng_days.prcsng_dt)
          INTO l_periods.dbt_bilng_day
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
          app_plsql_log.info(l_module_name ||
                             ' warning: RECORD not found in table: trnd_bilng_days where mrkt_id=' ||
                             to_char(p_mrkt_id) || ' and orig_perd_id = ' ||
                             to_char(p_orig_perd_id) ||
                             ' and bilng_perd_id = ' ||
                             to_char(p_bilng_perd_id) ||
                             ' and prcsng_dt = ' ||
                             to_char(p_bilng_day, 'yyyy-mm-dd') ||
                             l_parameter_list);
          l_periods.dbt_bilng_day := NULL;
      END;
    END IF;
    -- sct_cash_val, sct_r_factor
    BEGIN
      SELECT mrkt_sls_perd.sct_cash_val, mrkt_sls_perd.sct_r_factor
        INTO l_periods.sct_cash_val, l_periods.sct_r_factor
        FROM mrkt_sls_perd
       WHERE p_mrkt_id = mrkt_sls_perd.mrkt_id(+)
         AND pa_manl_trend_adjstmnt.get_target_campaign(p_mrkt_id     => p_mrkt_id,
                                                        p_sls_perd_id => p_bilng_perd_id,
                                                        p_sls_typ_id  => p_sls_typ_id) =
             mrkt_sls_perd.sls_perd_id(+);
    EXCEPTION
      WHEN OTHERS THEN
        app_plsql_log.info(l_module_name ||
                           ' warning: RECORD not found in table: mrkt_sls_perd where mrkt_id=' ||
                           to_char(p_mrkt_id) || ' and sls_perd_id = ' ||
                           to_char(p_bilng_perd_id) || l_parameter_list);
        l_periods.sct_cash_val := 0;
        l_periods.sct_r_factor := 0;
    END;
    RETURN l_periods;
  END get_periods;

  -- R-faktor
  FUNCTION get_bi24_r_factor(p_mrkt_id                IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                             p_dbt_bilng_day          IN dly_bilng_trnd.prcsng_dt%TYPE,
                             p_sls_typ_id_from_config IN mrkt_config_item.mrkt_config_item_val_txt%TYPE,
                             p_dbt_on_sls_perd_id     IN dly_bilng_trnd.trnd_sls_perd_id%TYPE,
                             p_dbt_on_offr_perd_id    IN dly_bilng_trnd.offr_perd_id%TYPE,
                             p_dbt_off_sls_perd_id    IN dly_bilng_trnd.trnd_sls_perd_id%TYPE,
                             p_dbt_off_offr_perd_id   IN dly_bilng_trnd.offr_perd_id%TYPE,
                             p_dms_on_sls_perd_id     IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                             p_dms_on_offr_perd_id    IN dstrbtd_mrkt_sls.offr_perd_id%TYPE,
                             p_dms_off_sls_perd_id    IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                             p_dms_off_offr_perd_id   IN dstrbtd_mrkt_sls.offr_perd_id%TYPE,
                             p_sct_cash_val           IN NUMBER,
                             p_r_factor               IN NUMBER,
                             p_sls_typ_id             IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                             p_run_id                 IN NUMBER DEFAULT NULL,
                             p_user_id                IN VARCHAR2 DEFAULT NULL)
    RETURN r_bi24_r_factor IS
    -- local variables
    l_bi24_r_factor r_bi24_r_factor;
    -- for LOG
    l_run_id         NUMBER := nvl(p_run_id,
                                   app_plsql_output.generate_new_run_id);
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'GET_BI24_R_FACTOR';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       to_char(p_mrkt_id) || ', ' ||
                                       ' p_dbt_bilng_day: ' ||
                                       to_char(p_dbt_bilng_day) || ', ' ||
                                       ' p_sls_typ_id_from_config: ' ||
                                       to_char(p_sls_typ_id_from_config) || ', ' ||
                                       ' p_dbt_on_sls_perd_id: ' ||
                                       to_char(p_dbt_on_sls_perd_id) || ', ' ||
                                       ' p_dbt_on_offr_perd_id: ' ||
                                       to_char(p_dbt_on_offr_perd_id) || ', ' ||
                                       ' p_dbt_off_sls_perd_id: ' ||
                                       to_char(p_dbt_off_sls_perd_id) || ', ' ||
                                       ' p_dbt_off_offr_perd_id: ' ||
                                       to_char(p_dbt_off_offr_perd_id) || ', ' ||
                                       ' p_sct_cash_val: ' ||
                                       to_char(p_sct_cash_val) || ', ' ||
                                       ' p_r_factor: ' ||
                                       to_char(p_r_factor) || ', ' ||
                                       'p_run_id: ' || to_char(l_run_id) || ', ' ||
                                       'p_user_id: ' || l_user_id || ')';
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start' || l_parameter_list);
    BEGIN
      SELECT SUM(bi24_unts_on) bi24_unts_on,
             SUM(bi24_sls_on) bi24_sls_on,
             SUM(bi24_unts_off) bi24_unts_off,
             SUM(bi24_sls_off) bi24_sls_off,
             nvl(p_r_factor, round((p_sct_cash_val / SUM(bi24_sls_on)), 4)) r_factor
        INTO l_bi24_r_factor.bi24_unts_on,
             l_bi24_r_factor.bi24_sls_on,
             l_bi24_r_factor.bi24_unts_off,
             l_bi24_r_factor.bi24_sls_off,
             l_bi24_r_factor.r_factor
        FROM (SELECT p_dbt_on_sls_perd_id sls_perd_id,
                     offr_sku_line.sku_id,
                     -- On-Schedule
                     -- bi24_unts_on
                     round(SUM(CASE
                                 WHEN dstrbtd_mrkt_sls.sls_typ_id = 1 AND
                                      dly_bilng_trnd.trnd_sls_perd_id =
                                      p_dbt_on_sls_perd_id AND dly_bilng_trnd.offr_perd_id =
                                      p_dbt_on_offr_perd_id THEN
                                  dly_bilng_trnd_offr_sku_line.unit_qty
                                 ELSE
                                  0
                               END)) bi24_unts_on,
                     -- bi24_sls_on
                     round(SUM(CASE
                                 WHEN dstrbtd_mrkt_sls.sls_typ_id = 1 AND
                                      dly_bilng_trnd.trnd_sls_perd_id =
                                      p_dbt_on_sls_perd_id AND dly_bilng_trnd.offr_perd_id =
                                      p_dbt_on_offr_perd_id THEN
                                  dly_bilng_trnd_offr_sku_line.unit_qty *
                                  dly_bilng_trnd.sls_prc_amt /
                                  decode(dly_bilng_trnd.nr_for_qty,
                                         0,
                                         1,
                                         dly_bilng_trnd.nr_for_qty) -
                                  dly_bilng_trnd_offr_sku_line.comsn_amt -
                                  dly_bilng_trnd_offr_sku_line.tax_amt
                                 ELSE
                                  0
                               END)) bi24_sls_on,
                     -- Off-Schedule
                     -- bi24_unts_off
                     round(SUM(CASE
                                 WHEN dstrbtd_mrkt_sls.sls_typ_id = 1 AND
                                      dly_bilng_trnd.trnd_sls_perd_id =
                                      p_dbt_off_sls_perd_id AND dly_bilng_trnd.offr_perd_id =
                                      p_dbt_off_offr_perd_id THEN
                                  dly_bilng_trnd_offr_sku_line.unit_qty
                                 ELSE
                                  0
                               END)) bi24_unts_off,
                     -- bi24_sls_off
                     round(SUM(CASE
                                 WHEN dstrbtd_mrkt_sls.sls_typ_id = 1 AND
                                      dly_bilng_trnd.trnd_sls_perd_id =
                                      p_dbt_off_sls_perd_id AND dly_bilng_trnd.offr_perd_id =
                                      p_dbt_off_offr_perd_id THEN
                                  dly_bilng_trnd_offr_sku_line.unit_qty *
                                  dly_bilng_trnd.sls_prc_amt /
                                  decode(dly_bilng_trnd.nr_for_qty,
                                         0,
                                         1,
                                         dly_bilng_trnd.nr_for_qty) -
                                  dly_bilng_trnd_offr_sku_line.comsn_amt -
                                  dly_bilng_trnd_offr_sku_line.tax_amt
                                 ELSE
                                  0
                               END)) bi24_sls_off
                FROM dly_bilng_trnd,
                     dly_bilng_trnd_offr_sku_line,
                     dstrbtd_mrkt_sls,
                     offr_sku_line,
                     offr_prfl_prc_point,
                     offr
               WHERE 1 = 1
                    -- filter: dly_bilng_trnd
                 AND dly_bilng_trnd.mrkt_id = p_mrkt_id
                 AND (( --on-schedule
                      dly_bilng_trnd.trnd_sls_perd_id = p_dbt_on_sls_perd_id AND
                      dly_bilng_trnd.offr_perd_id = p_dbt_on_offr_perd_id) OR
                     ( --off-schedule
                      dly_bilng_trnd.trnd_sls_perd_id =
                      p_dbt_off_sls_perd_id AND
                      dly_bilng_trnd.offr_perd_id = p_dbt_off_offr_perd_id))
                 AND trunc(dly_bilng_trnd.prcsng_dt) <= p_dbt_bilng_day
                    -- join: dly_bilng_trnd -> dly_bilng_trnd_offr_sku_line
                 AND dly_bilng_trnd.dly_bilng_id =
                     dly_bilng_trnd_offr_sku_line.dly_bilng_id
                    -- filter: p_sls_typ_id
                 AND dly_bilng_trnd_offr_sku_line.sls_typ_id =
                     p_sls_typ_id_from_config
                    -- join: dly_bilng_trnd_offr_sku_line -> dstrbtd_mrkt_sls
                 AND dly_bilng_trnd_offr_sku_line.offr_sku_line_id =
                     dstrbtd_mrkt_sls.offr_sku_line_id
                    -- filter: mrkt_id, 
                 AND dstrbtd_mrkt_sls.mrkt_id = p_mrkt_id
                 AND (( --on-schedule
                      dstrbtd_mrkt_sls.sls_perd_id = p_dms_on_sls_perd_id AND
                      dstrbtd_mrkt_sls.offr_perd_id = p_dms_on_offr_perd_id) OR
                     ( --off-schedule
                      dstrbtd_mrkt_sls.sls_perd_id = p_dms_off_sls_perd_id AND
                      dstrbtd_mrkt_sls.offr_perd_id = p_dms_off_offr_perd_id))
                 AND dstrbtd_mrkt_sls.sls_typ_id IN (1, p_sls_typ_id, 6)
                    -- additional filtering...
                    -- maybe, we will not need it
                 AND dly_bilng_trnd_offr_sku_line.offr_sku_line_id =
                     offr_sku_line.offr_sku_line_id
                 AND offr_sku_line.dltd_ind <> 'Y'
                 AND offr_sku_line.offr_prfl_prcpt_id =
                     offr_prfl_prc_point.offr_prfl_prcpt_id
                 AND offr_prfl_prc_point.offr_id = offr.offr_id
                 AND offr.offr_typ = 'CMP'
                 AND offr.ver_id = 0
               GROUP BY p_dbt_on_sls_perd_id, offr_sku_line.sku_id);
    EXCEPTION
      WHEN OTHERS THEN
        app_plsql_log.info(l_module_name ||
                           ' warning: BI24 calculation! error code:' ||
                           SQLCODE || ' error message: ' || SQLERRM ||
                           l_parameter_list);
        l_bi24_r_factor.bi24_unts_on  := 0;
        l_bi24_r_factor.bi24_sls_on   := 0;
        l_bi24_r_factor.bi24_unts_off := 0;
        l_bi24_r_factor.bi24_sls_off  := 0;
        l_bi24_r_factor.r_factor      := 0;
    END;
    IF l_bi24_r_factor.r_factor IS NULL THEN
      l_bi24_r_factor.r_factor := 0;
    END IF;
    RETURN l_bi24_r_factor;
  END get_bi24_r_factor;

  -- BIAS, BI24
  FUNCTION get_head_details(p_mrkt_id                IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                            p_sls_perd_id            IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                            p_trg_perd_id            IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                            p_sls_typ_id             IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                            p_dbt_bilng_day          IN dly_bilng_trnd.prcsng_dt%TYPE,
                            p_sls_typ_id_from_config IN mrkt_config_item.mrkt_config_item_val_txt%TYPE,
                            p_dbt_on_sls_perd_id     IN dly_bilng_trnd.trnd_sls_perd_id%TYPE,
                            p_dbt_on_offr_perd_id    IN dly_bilng_trnd.offr_perd_id%TYPE,
                            p_dbt_off_sls_perd_id    IN dly_bilng_trnd.trnd_sls_perd_id%TYPE,
                            p_dbt_off_offr_perd_id   IN dly_bilng_trnd.offr_perd_id%TYPE,
                            p_dms_on_sls_perd_id     IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                            p_dms_on_offr_perd_id    IN dstrbtd_mrkt_sls.offr_perd_id%TYPE,
                            p_dms_off_sls_perd_id    IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                            p_dms_off_offr_perd_id   IN dstrbtd_mrkt_sls.offr_perd_id%TYPE,
                            p_run_id                 IN NUMBER DEFAULT NULL,
                            p_user_id                IN VARCHAR2 DEFAULT NULL)
    RETURN t_hist_head_aggr;

  -- BIAS, BI24
  FUNCTION get_reproc_trnd(p_mrkt_id                IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                           p_sls_perd_id            IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                           p_trg_perd_id            IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                           p_sls_typ_id             IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                           p_bilng_day              IN dly_bilng_trnd.prcsng_dt%TYPE,
                           p_sls_typ_id_from_config IN mrkt_config_item.mrkt_config_item_val_txt%TYPE,
                           p_dbt_on_sls_perd_id     IN dly_bilng_trnd.trnd_sls_perd_id%TYPE,
                           p_dbt_on_offr_perd_id    IN dly_bilng_trnd.offr_perd_id%TYPE,
                           p_dbt_off_sls_perd_id    IN dly_bilng_trnd.trnd_sls_perd_id%TYPE,
                           p_dbt_off_offr_perd_id   IN dly_bilng_trnd.offr_perd_id%TYPE,
                           p_dms_on_sls_perd_id     IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                           p_dms_on_offr_perd_id    IN dstrbtd_mrkt_sls.offr_perd_id%TYPE,
                           p_dms_off_sls_perd_id    IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                           p_dms_off_offr_perd_id   IN dstrbtd_mrkt_sls.offr_perd_id%TYPE,
                           p_cash_value             IN NUMBER,
                           p_r_factor               IN NUMBER,
                           p_run_id                 IN NUMBER DEFAULT NULL,
                           p_user_id                IN VARCHAR2 DEFAULT NULL)
    RETURN t_reproc;

  -- ESTIMATE (and OVRRD)
  FUNCTION get_reproc_est(p_mrkt_id              IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                          p_sls_perd_id          IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                          p_trg_perd_id          IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                          p_sls_typ_id           IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                          p_bilng_day            IN dly_bilng_trnd.prcsng_dt%TYPE,
                          p_dbt_on_sls_perd_id   IN dly_bilng_trnd.trnd_sls_perd_id%TYPE,
                          p_dbt_on_offr_perd_id  IN dly_bilng_trnd.offr_perd_id%TYPE,
                          p_dbt_off_sls_perd_id  IN dly_bilng_trnd.trnd_sls_perd_id%TYPE,
                          p_dbt_off_offr_perd_id IN dly_bilng_trnd.offr_perd_id%TYPE,
                          p_dms_on_sls_perd_id   IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                          p_dms_on_offr_perd_id  IN dstrbtd_mrkt_sls.offr_perd_id%TYPE,
                          p_dms_off_sls_perd_id  IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                          p_dms_off_offr_perd_id IN dstrbtd_mrkt_sls.offr_perd_id%TYPE,
                          p_cash_value           IN NUMBER,
                          p_r_factor             IN NUMBER,
                          p_use_offers_on_sched  IN CHAR DEFAULT 'N',
                          p_use_offers_off_sched IN CHAR DEFAULT 'N',
                          p_run_id               IN NUMBER DEFAULT NULL,
                          p_user_id              IN VARCHAR2 DEFAULT NULL)
    RETURN t_reproc;

  FUNCTION get_trend_alloc_reproc_ln(p_mrkt_id              IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                                     p_sls_perd_id          IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                     p_trg_perd_id          IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                     p_sls_typ_id           IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                                     p_bilng_day            IN dly_bilng_trnd.prcsng_dt%TYPE,
                                     p_cash_value           IN NUMBER,
                                     p_r_factor             IN NUMBER,
                                     p_use_offers_on_sched  IN CHAR DEFAULT 'N',
                                     p_use_offers_off_sched IN CHAR DEFAULT 'N',
                                     p_run_id               IN NUMBER DEFAULT NULL,
                                     p_user_id              IN VARCHAR2 DEFAULT NULL)
    RETURN obj_trend_alloc_hist_head_line;

  FUNCTION get_trend_alloc_head_view(p_mrkt_id     IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                                     p_sls_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                     p_trg_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                     p_sls_typ_id  IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                                     p_bilng_day   IN dly_bilng_trnd.prcsng_dt%TYPE,
                                     p_run_id      IN NUMBER DEFAULT NULL,
                                     p_user_id     IN VARCHAR2 DEFAULT NULL)
    RETURN obj_trend_alloc_view_table
    PIPELINED AS
    -- local variables
    l_periods     r_periods;
    l_not_planned NUMBER := NULL;
    l_last_dms    DATE;
    l_has_save    CHAR(1);
    l_last_run    DATE;
    l_is_started  CHAR(1);
    l_is_complete CHAR(1);
    -- for LOG
    l_run_id         NUMBER := nvl(p_run_id,
                                   app_plsql_output.generate_new_run_id);
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'GET_TREND_ALLOC_HEAD_VIEW';
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
                                       'p_run_id: ' || to_char(l_run_id) || ', ' ||
                                       'p_user_id: ' || l_user_id || ')';
  
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start' || l_parameter_list);
  
    BEGIN
      BEGIN
        SELECT last_updt_ts
          INTO l_last_dms
          FROM dstrbtd_mrkt_sls
         WHERE mrkt_id = p_mrkt_id
           AND sls_perd_id = p_trg_perd_id
           AND sls_typ_id = p_sls_typ_id
           AND rownum <= 1;
      EXCEPTION
        WHEN no_data_found THEN
          l_last_dms := NULL;
        WHEN OTHERS THEN
          app_plsql_log.info(l_module_name ||
                             ' warning: Other exception when selecting from: dstrbtd_mrkt_sls where mrkt_id=' ||
                             to_char(p_mrkt_id) || ' and sls_perd_id=' ||
                             to_char(p_trg_perd_id) || ' and sls_typ_id=' ||
                             to_char(p_sls_typ_id) || 'error code: ' ||
                             SQLCODE || ' error message: ' || SQLERRM ||
                             l_parameter_list);
          l_last_dms := NULL;
      END;
      SELECT CASE
               WHEN l_last_dms IS NOT NULL THEN
                'Y'
               ELSE
                'N'
             END,
             trunc(l_last_dms),
             CASE
               WHEN p_sls_typ_id IN (marketing_est_id, supply_est_id) THEN
                sct_autclc_est_ind
               WHEN p_sls_typ_id IN (marketing_bst_id, supply_bst_id) THEN
                sct_autclc_bst_ind
               ELSE
                NULL
             END,
             CASE
               WHEN sct_sls_typ_id <= p_sls_typ_id THEN
                'N'
               ELSE
                'Y'
             END
        INTO l_has_save, l_last_run, l_is_started, l_is_complete
        FROM mrkt_sls_perd
       WHERE mrkt_id = p_mrkt_id
         AND sls_perd_id = p_trg_perd_id;
    EXCEPTION
      WHEN OTHERS THEN
        app_plsql_log.info(l_module_name ||
                           ' warning: RECORD not found in table: mrkt_sls_perd where mrkt_id=' ||
                           to_char(p_mrkt_id) || ' and sls_perd_id=' ||
                           to_char(p_trg_perd_id) || l_parameter_list);
        l_has_save    := NULL;
        l_last_run    := NULL;
        l_is_started  := NULL;
        l_is_complete := NULL;
    END;
    -- NOT PLANNED
    -- get CURRENT periods
    l_periods := get_periods(p_mrkt_id       => p_mrkt_id,
                             p_orig_perd_id  => p_sls_perd_id,
                             p_bilng_perd_id => p_sls_perd_id,
                             p_sls_typ_id    => p_sls_typ_id,
                             p_bilng_day     => p_bilng_day,
                             p_run_id        => l_run_id,
                             p_user_id       => l_user_id);
    BEGIN
      SELECT SUM(dly_bilng_trnd.unit_qty)
        INTO l_not_planned
        FROM dly_bilng_trnd, dly_bilng_offr_sku_line
       WHERE 1 = 1
            -- filter: dly_bilng_trnd
         AND dly_bilng_trnd.mrkt_id = p_mrkt_id
         AND (( --on-schedule
              dly_bilng_trnd.trnd_sls_perd_id =
              l_periods.dbt_on_sls_perd_id AND
              dly_bilng_trnd.offr_perd_id = l_periods.dbt_on_offr_perd_id) OR
             ( --off-schedule
              dly_bilng_trnd.trnd_sls_perd_id =
              l_periods.dbt_off_sls_perd_id AND
              dly_bilng_trnd.offr_perd_id = l_periods.dbt_off_offr_perd_id))
         AND trunc(dly_bilng_trnd.prcsng_dt) <= l_periods.dbt_bilng_day
            -- join: dly_bilng_trnd -> dly_bilng_trnd_offr_sku_line
         AND dly_bilng_trnd.dly_bilng_id =
             dly_bilng_offr_sku_line.dly_bilng_id(+)
         AND dly_bilng_offr_sku_line.dly_bilng_id IS NULL;
    EXCEPTION
      WHEN OTHERS THEN
        app_plsql_log.info(l_module_name ||
                           ' warning: RECORD not found in table: dly_bilng_trnd where mrkt_id=' ||
                           to_char(p_mrkt_id) || ' and trnd_sls_perd_id=' ||
                           to_char(p_sls_perd_id) || l_parameter_list);
        l_not_planned := NULL;
    END;
  
    PIPE ROW(obj_trend_alloc_view_line(l_not_planned,
                                       l_has_save,
                                       l_last_run,
                                       l_is_started,
                                       l_is_complete));
  
    app_plsql_log.info(l_module_name || ' end' || l_parameter_list);
  END get_trend_alloc_head_view;

  FUNCTION get_trend_alloc_hist_head(p_mrkt_id     IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                                     p_sls_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                     p_trg_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                     p_sls_typ_id  IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                                     p_bilng_day   IN dly_bilng_trnd.prcsng_dt%TYPE,
                                     p_perd_from   IN NUMBER,
                                     p_perd_to     IN NUMBER,
                                     p_run_id      IN NUMBER DEFAULT NULL,
                                     p_user_id     IN VARCHAR2 DEFAULT NULL)
    RETURN obj_trend_alloc_hist_hd_table
    PIPELINED IS
    -- local variables
    l_periods            r_periods;
    l_tbl_hist_head_aggr t_hist_head_aggr;
    -- for HEAD aggr
    l_bi24_unts_on      NUMBER;
    l_bi24_sls_on       NUMBER;
    l_bi24_unts_off     NUMBER;
    l_bi24_sls_off      NUMBER;
    l_estimate_unts_on  NUMBER;
    l_estimate_sls_on   NUMBER;
    l_estimate_unts_off NUMBER;
    l_estimate_sls_off  NUMBER;
    l_trend_unts_on     NUMBER;
    l_trend_sls_on      NUMBER;
    l_trend_unts_off    NUMBER;
    l_trend_sls_off     NUMBER;
    l_actual_unts_on    NUMBER;
    l_actual_sls_on     NUMBER;
    l_actual_unts_off   NUMBER;
    l_actual_sls_off    NUMBER;
    -- once
    l_sls_typ_id_from_config dly_bilng_trnd_offr_sku_line.sls_typ_id%TYPE;
    --
    l_multplyr NUMBER;
    -- for LOG
    l_run_id         NUMBER := nvl(p_run_id,
                                   app_plsql_output.generate_new_run_id);
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'GET_TREND_ALLOC_HIST_HEAD';
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
                                       'p_perd_from: ' ||
                                       to_char(p_perd_from) || ', ' ||
                                       'p_perd_to: ' || to_char(p_perd_to) || ', ' ||
                                       'p_run_id: ' || to_char(l_run_id) || ', ' ||
                                       'p_user_id: ' || l_user_id || ')';
    --
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start' || l_parameter_list);
    -- sls_typ_id_from_config
    l_sls_typ_id_from_config := get_sls_typ_id_from_config(p_mrkt_id,
                                                           l_run_id,
                                                           l_user_id);
    -- 
    l_multplyr := to_number(substr(to_char(p_sls_perd_id), 1, 4)) -
                  p_perd_to;
    FOR p IN p_perd_to .. p_perd_from LOOP
      -- iterations for FROM periods to TO periods
      FOR i_prd IN (SELECT pa_maps_public.perd_plus(p_mrkt_id,
                                                    p_sls_perd_id -
                                                    (10000 * l_multplyr),
                                                    -1) perd_id
                      FROM dual
                    UNION ALL
                    SELECT p_sls_perd_id - (10000 * l_multplyr) perd_id
                      FROM dual
                    UNION ALL
                    SELECT pa_maps_public.perd_plus(p_mrkt_id,
                                                    p_sls_perd_id -
                                                    (10000 * l_multplyr),
                                                    1) perd_id
                      FROM dual
                     ORDER BY 1) LOOP
        -- get CURRENT periods
        l_periods := get_periods(p_mrkt_id       => p_mrkt_id,
                                 p_orig_perd_id  => p_sls_perd_id,
                                 p_bilng_perd_id => i_prd.perd_id,
                                 p_sls_typ_id    => p_sls_typ_id,
                                 p_bilng_day     => p_bilng_day,
                                 p_run_id        => l_run_id,
                                 p_user_id       => l_user_id);
        -- get_head_details
        l_tbl_hist_head_aggr := get_head_details(p_mrkt_id,
                                                 p_sls_perd_id,
                                                 p_trg_perd_id,
                                                 p_sls_typ_id,
                                                 l_periods.dbt_bilng_day,
                                                 l_sls_typ_id_from_config,
                                                 l_periods.dbt_on_sls_perd_id,
                                                 l_periods.dbt_on_offr_perd_id,
                                                 l_periods.dbt_off_sls_perd_id,
                                                 l_periods.dbt_off_offr_perd_id,
                                                 l_periods.dms_on_sls_perd_id,
                                                 l_periods.dms_on_offr_perd_id,
                                                 l_periods.dms_off_sls_perd_id,
                                                 l_periods.dms_off_offr_perd_id,
                                                 l_run_id,
                                                 l_user_id);
        --
        l_bi24_unts_on      := 0;
        l_bi24_sls_on       := 0;
        l_bi24_unts_off     := 0;
        l_bi24_sls_off      := 0;
        l_trend_unts_on     := 0;
        l_trend_sls_on      := 0;
        l_trend_unts_off    := 0;
        l_trend_sls_off     := 0;
        l_estimate_unts_on  := 0;
        l_estimate_sls_on   := 0;
        l_estimate_unts_off := 0;
        l_estimate_sls_off  := 0;
        l_actual_unts_on    := 0;
        l_actual_sls_on     := 0;
        l_actual_unts_off   := 0;
        l_actual_sls_off    := 0;
        IF l_tbl_hist_head_aggr.count > 0 THEN
          FOR i IN l_tbl_hist_head_aggr.first .. l_tbl_hist_head_aggr.last LOOP
            l_bi24_unts_on      := l_bi24_unts_on + l_tbl_hist_head_aggr(i)
                                  .bi24_unts_on;
            l_bi24_sls_on       := l_bi24_sls_on + l_tbl_hist_head_aggr(i)
                                  .bi24_sls_on;
            l_bi24_unts_off     := l_bi24_unts_off + l_tbl_hist_head_aggr(i)
                                  .bi24_unts_off;
            l_bi24_sls_off      := l_bi24_sls_off + l_tbl_hist_head_aggr(i)
                                  .bi24_sls_off;
            l_trend_unts_on     := l_trend_unts_on + l_tbl_hist_head_aggr(i)
                                  .trend_unts_on;
            l_trend_sls_on      := l_trend_sls_on + l_tbl_hist_head_aggr(i)
                                  .trend_sls_on;
            l_trend_unts_off    := l_trend_unts_off + l_tbl_hist_head_aggr(i)
                                  .trend_unts_off;
            l_trend_sls_off     := l_trend_sls_off + l_tbl_hist_head_aggr(i)
                                  .trend_sls_off;
            l_estimate_unts_on  := l_estimate_unts_on + l_tbl_hist_head_aggr(i)
                                  .estimate_unts_on;
            l_estimate_sls_on   := l_estimate_sls_on + l_tbl_hist_head_aggr(i)
                                  .estimate_sls_on;
            l_estimate_unts_off := l_estimate_unts_off + l_tbl_hist_head_aggr(i)
                                  .estimate_unts_off;
            l_estimate_sls_off  := l_estimate_sls_off + l_tbl_hist_head_aggr(i)
                                  .estimate_sls_off;
            l_actual_unts_on    := l_actual_unts_on + l_tbl_hist_head_aggr(i)
                                  .actual_unts_on;
            l_actual_sls_on     := l_actual_sls_on + l_tbl_hist_head_aggr(i)
                                  .actual_sls_on;
            l_actual_unts_off   := l_actual_unts_off + l_tbl_hist_head_aggr(i)
                                  .actual_unts_off;
            l_actual_sls_off    := l_actual_sls_off + l_tbl_hist_head_aggr(i)
                                  .actual_sls_off;
          END LOOP;
        END IF;
        PIPE ROW(obj_trend_alloc_hist_head_line(l_periods.dbt_on_sls_perd_id /*sls_perd_id*/,
                                                l_periods.dms_on_sls_perd_id /*trg_perd_id*/,
                                                l_periods.dbt_bilng_day /*bilng_day*/,
                                                l_bi24_unts_on,
                                                l_bi24_sls_on,
                                                l_bi24_unts_off,
                                                l_bi24_sls_off,
                                                l_periods.sct_cash_val /*NULL*/ /*cash_vlu*/,
                                                l_periods.sct_r_factor /*NULL*/ /*r_factor*/,
                                                l_estimate_unts_on,
                                                l_estimate_sls_on,
                                                l_estimate_unts_off,
                                                l_estimate_sls_off,
                                                l_trend_unts_on,
                                                l_trend_sls_on,
                                                l_trend_unts_off,
                                                l_trend_sls_off,
                                                l_actual_unts_on,
                                                l_actual_sls_on,
                                                l_actual_unts_off,
                                                l_actual_sls_off));
      END LOOP;
      l_multplyr := l_multplyr - 1;
    END LOOP;
    --
    app_plsql_log.info(l_module_name || ' end' || l_parameter_list);
  END get_trend_alloc_hist_head;

  FUNCTION get_trend_alloc(p_mrkt_id     IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                           p_sls_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                           p_trg_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                           p_sls_typ_id  IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                           p_bilng_day   IN dly_bilng_trnd.prcsng_dt%TYPE,
                           p_run_id      IN NUMBER DEFAULT NULL,
                           p_user_id     IN VARCHAR2 DEFAULT NULL)
    RETURN obj_trend_alloc_hist_hd_table
    PIPELINED IS
    -- local variables
    l_obj_trend_alloc_hist_head_ln obj_trend_alloc_hist_head_line;
    -- once
    l_cash_value           mrkt_sls_perd.sct_cash_val%TYPE;
    l_use_offers_on_sched  mrkt_sls_perd.sct_onsch_est_bi24_ind%TYPE;
    l_use_offers_off_sched mrkt_sls_perd.sct_offsch_est_bi24_ind%TYPE;
    -- for LOG
    l_run_id         NUMBER := nvl(p_run_id,
                                   app_plsql_output.generate_new_run_id);
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'GET_TREND_ALLOC';
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
                                       'p_run_id: ' || to_char(l_run_id) || ', ' ||
                                       'p_user_id: ' || l_user_id || ')';
    --
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start' || l_parameter_list);
    --
    -- ACTUAL record
    BEGIN
      SELECT sct_cash_val, sct_onsch_est_bi24_ind, sct_offsch_est_bi24_ind
        INTO l_cash_value, l_use_offers_on_sched, l_use_offers_off_sched
        FROM mrkt_sls_perd
       WHERE mrkt_id = p_mrkt_id
         AND sls_perd_id = p_trg_perd_id;
    EXCEPTION
      WHEN OTHERS THEN
        app_plsql_log.info(l_module_name ||
                           ' warning: ACTUAL record not found in table: mrkt_sls_perd where mrkt_id=' ||
                           to_char(p_mrkt_id) || ' and sls_perd_id=' ||
                           to_char(p_trg_perd_id) || l_parameter_list);
        l_cash_value           := NULL;
        l_use_offers_on_sched  := NULL;
        l_use_offers_off_sched := NULL;
    END;
    IF l_cash_value IS NULL THEN
      l_cash_value := 0;
    END IF;
    IF l_use_offers_on_sched IS NULL THEN
      l_use_offers_on_sched := 'N';
    END IF;
    IF l_use_offers_off_sched IS NULL THEN
      l_use_offers_off_sched := 'N';
    END IF;
    l_obj_trend_alloc_hist_head_ln := get_trend_alloc_reproc_ln(p_mrkt_id              => p_mrkt_id,
                                                                p_sls_perd_id          => p_sls_perd_id,
                                                                p_trg_perd_id          => p_trg_perd_id,
                                                                p_sls_typ_id           => p_sls_typ_id,
                                                                p_bilng_day            => p_bilng_day,
                                                                p_cash_value           => l_cash_value,
                                                                p_r_factor             => NULL,
                                                                p_use_offers_on_sched  => l_use_offers_on_sched,
                                                                p_use_offers_off_sched => l_use_offers_off_sched,
                                                                p_run_id               => l_run_id,
                                                                p_user_id              => l_user_id);
    PIPE ROW(l_obj_trend_alloc_hist_head_ln);
    --
    app_plsql_log.info(l_module_name || ' end' || l_parameter_list);
  END get_trend_alloc;

  FUNCTION get_trend_alloc_re_proc(p_mrkt_id              IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                                   p_sls_perd_id          IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                   p_trg_perd_id          IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                   p_sls_typ_id           IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                                   p_bilng_day            IN dly_bilng_trnd.prcsng_dt%TYPE,
                                   p_cash_value           IN NUMBER,
                                   p_r_factor             IN NUMBER,
                                   p_use_offers_on_sched  IN CHAR DEFAULT 'N',
                                   p_use_offers_off_sched IN CHAR DEFAULT 'N',
                                   p_run_id               IN NUMBER DEFAULT NULL,
                                   p_user_id              IN VARCHAR2 DEFAULT NULL)
    RETURN obj_trend_alloc_hist_hd_table
    PIPELINED IS
    -- local variables
    l_obj_trend_alloc_hist_head_ln obj_trend_alloc_hist_head_line;
    -- for LOG
    l_run_id         NUMBER := nvl(p_run_id,
                                   app_plsql_output.generate_new_run_id);
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'GET_TREND_ALLOC_RE_PROC';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       to_char(p_mrkt_id) || ', ' ||
                                       'p_sls_perd_id: ' ||
                                       to_char(p_sls_perd_id) || ', ' ||
                                       'p_trg_perd_id: ' ||
                                       to_char(p_trg_perd_id) || ', ' ||
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
                                       'p_run_id: ' || to_char(l_run_id) || ', ' ||
                                       'p_user_id: ' || l_user_id || ')';
    --
  BEGIN
    --
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start' || l_parameter_list);
    --
    l_obj_trend_alloc_hist_head_ln := get_trend_alloc_reproc_ln(p_mrkt_id              => p_mrkt_id,
                                                                p_sls_perd_id          => p_sls_perd_id,
                                                                p_trg_perd_id          => p_trg_perd_id,
                                                                p_sls_typ_id           => p_sls_typ_id,
                                                                p_bilng_day            => p_bilng_day,
                                                                p_cash_value           => p_cash_value,
                                                                p_r_factor             => p_r_factor,
                                                                p_use_offers_on_sched  => p_use_offers_on_sched,
                                                                p_use_offers_off_sched => p_use_offers_off_sched,
                                                                p_run_id               => l_run_id,
                                                                p_user_id              => l_user_id);
    PIPE ROW(l_obj_trend_alloc_hist_head_ln);
    --
    app_plsql_log.info(l_module_name || ' end' || l_parameter_list);
  END get_trend_alloc_re_proc;

  FUNCTION get_head_details(p_mrkt_id                IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                            p_sls_perd_id            IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                            p_trg_perd_id            IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                            p_sls_typ_id             IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                            p_dbt_bilng_day          IN dly_bilng_trnd.prcsng_dt%TYPE,
                            p_sls_typ_id_from_config IN mrkt_config_item.mrkt_config_item_val_txt%TYPE,
                            p_dbt_on_sls_perd_id     IN dly_bilng_trnd.trnd_sls_perd_id%TYPE,
                            p_dbt_on_offr_perd_id    IN dly_bilng_trnd.offr_perd_id%TYPE,
                            p_dbt_off_sls_perd_id    IN dly_bilng_trnd.trnd_sls_perd_id%TYPE,
                            p_dbt_off_offr_perd_id   IN dly_bilng_trnd.offr_perd_id%TYPE,
                            p_dms_on_sls_perd_id     IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                            p_dms_on_offr_perd_id    IN dstrbtd_mrkt_sls.offr_perd_id%TYPE,
                            p_dms_off_sls_perd_id    IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                            p_dms_off_offr_perd_id   IN dstrbtd_mrkt_sls.offr_perd_id%TYPE,
                            p_run_id                 IN NUMBER DEFAULT NULL,
                            p_user_id                IN VARCHAR2 DEFAULT NULL)
    RETURN t_hist_head_aggr IS
    -- local variables
    l_tbl_hist_head_aggr t_hist_head_aggr;
    -- for LOG
    l_run_id         NUMBER := nvl(p_run_id,
                                   app_plsql_output.generate_new_run_id);
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'GET_HEAD_DETAILS';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       to_char(p_mrkt_id) || ', ' ||
                                       'p_sls_perd_id: ' ||
                                       to_char(p_sls_perd_id) || ', ' ||
                                       'p_trg_perd_id: ' ||
                                       to_char(p_trg_perd_id) || ', ' ||
                                       'p_sls_typ_id: ' ||
                                       to_char(p_sls_typ_id) || ', ' ||
                                       'p_dbt_bilng_day: ' ||
                                       to_char(p_dbt_bilng_day, 'yyyymmdd') || ', ' ||
                                       'p_run_id: ' || to_char(l_run_id) || ', ' ||
                                       'p_user_id: ' || l_user_id || ')';
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
      SELECT p_dbt_on_sls_perd_id sls_perd_id,
             offr_sku_line.sku_id,
             MAX(offr.mrkt_id) mrkt_id,
             MAX(offr.veh_id) veh_id,
             MAX(offr.offr_desc_txt) offr_desc_txt,
             MAX(offr_prfl_prc_point.promtn_id) promtn_id,
             MAX(offr_prfl_prc_point.promtn_clm_id) promtn_clm_id,
             MAX(offr_prfl_prc_point.sls_cls_cd) sls_cls_cd,
             round(SUM(CASE
                         WHEN dstrbtd_mrkt_sls.sls_typ_id = 1 AND
                              dly_bilng.trnd_sls_perd_id = p_dbt_on_sls_perd_id AND
                              dly_bilng.offr_perd_id = p_dbt_on_offr_perd_id THEN
                          dly_bilng.unit_qty
                         ELSE
                          0
                       END)) bi24_unts_on,
             round(SUM(CASE
                         WHEN dstrbtd_mrkt_sls.sls_typ_id = 1 AND
                              dly_bilng.trnd_sls_perd_id = p_dbt_on_sls_perd_id AND
                              dly_bilng.offr_perd_id = p_dbt_on_offr_perd_id THEN
                          dly_bilng.unit_qty * dly_bilng.sls_prc_amt /
                          decode(dly_bilng.nr_for_qty, 0, 1, dly_bilng.nr_for_qty) -
                          dly_bilng.comsn_amt - dly_bilng.tax_amt
                         ELSE
                          0
                       END)) bi24_sls_on,
             round(SUM(CASE
                         WHEN dstrbtd_mrkt_sls.sls_typ_id = 1 AND
                              dly_bilng.trnd_sls_perd_id = p_dbt_off_sls_perd_id AND
                              dly_bilng.offr_perd_id = p_dbt_off_offr_perd_id THEN
                          dly_bilng.unit_qty
                         ELSE
                          0
                       END)) bi24_unts_off,
             round(SUM(CASE
                         WHEN dstrbtd_mrkt_sls.sls_typ_id = 1 AND
                              dly_bilng.trnd_sls_perd_id = p_dbt_off_sls_perd_id AND
                              dly_bilng.offr_perd_id = p_dbt_off_offr_perd_id THEN
                          dly_bilng.unit_qty * dly_bilng.sls_prc_amt /
                          decode(dly_bilng.nr_for_qty, 0, 1, dly_bilng.nr_for_qty) -
                          dly_bilng.comsn_amt - dly_bilng.tax_amt
                         ELSE
                          0
                       END)) bi24_sls_off,
             round(SUM(CASE
                         WHEN dstrbtd_mrkt_sls.sls_typ_id = 1 AND
                              dstrbtd_mrkt_sls.sls_perd_id = p_dms_on_sls_perd_id AND
                              dstrbtd_mrkt_sls.offr_perd_id = p_dms_on_offr_perd_id AND
                              nvl(dly_bilng.dly_bilng_id, dly_bilng_id_for_osl) =
                              dly_bilng_id_for_osl THEN
                          dstrbtd_mrkt_sls.unit_qty
                         ELSE
                          0
                       END)) estimate_unts_on,
             round(SUM(CASE
                         WHEN dstrbtd_mrkt_sls.sls_typ_id = 1 AND
                              dstrbtd_mrkt_sls.sls_perd_id = p_dms_on_sls_perd_id AND
                              dstrbtd_mrkt_sls.offr_perd_id = p_dms_on_offr_perd_id AND
                              nvl(dly_bilng.dly_bilng_id, dly_bilng_id_for_osl) =
                              dly_bilng_id_for_osl THEN
                          dstrbtd_mrkt_sls.unit_qty * offr_prfl_prc_point.sls_prc_amt /
                          decode(offr_prfl_prc_point.nr_for_qty,
                                 0,
                                 1,
                                 offr_prfl_prc_point.nr_for_qty) *
                          offr_prfl_prc_point.net_to_avon_fct
                         ELSE
                          0
                       END)) estimate_sls_on,
             round(SUM(CASE
                         WHEN dstrbtd_mrkt_sls.sls_typ_id = 1 AND
                              dstrbtd_mrkt_sls.sls_perd_id = p_dms_off_sls_perd_id AND
                              dstrbtd_mrkt_sls.offr_perd_id = p_dms_off_offr_perd_id AND
                              nvl(dly_bilng.dly_bilng_id, dly_bilng_id_for_osl) =
                              dly_bilng_id_for_osl THEN
                          dstrbtd_mrkt_sls.unit_qty
                         ELSE
                          0
                       END)) estimate_unts_off,
             round(SUM(CASE
                         WHEN dstrbtd_mrkt_sls.sls_typ_id = 1 AND
                              dstrbtd_mrkt_sls.sls_perd_id = p_dms_off_sls_perd_id AND
                              dstrbtd_mrkt_sls.offr_perd_id = p_dms_off_offr_perd_id AND
                              nvl(dly_bilng.dly_bilng_id, dly_bilng_id_for_osl) =
                              dly_bilng_id_for_osl THEN
                          dstrbtd_mrkt_sls.unit_qty * offr_prfl_prc_point.sls_prc_amt /
                          decode(offr_prfl_prc_point.nr_for_qty,
                                 0,
                                 1,
                                 offr_prfl_prc_point.nr_for_qty) *
                          offr_prfl_prc_point.net_to_avon_fct
                         ELSE
                          0
                       END)) estimate_sls_off,
             round(SUM(CASE
                         WHEN dstrbtd_mrkt_sls.sls_typ_id = p_sls_typ_id AND
                              dstrbtd_mrkt_sls.sls_perd_id = p_dms_on_sls_perd_id AND
                              dstrbtd_mrkt_sls.offr_perd_id = p_dms_on_offr_perd_id AND
                              nvl(dly_bilng.dly_bilng_id, dly_bilng_id_for_osl) =
                              dly_bilng_id_for_osl THEN
                          dstrbtd_mrkt_sls.unit_qty
                         ELSE
                          0
                       END)) trend_unts_on,
             round(SUM(CASE
                         WHEN dstrbtd_mrkt_sls.sls_typ_id = p_sls_typ_id AND
                              dstrbtd_mrkt_sls.sls_perd_id = p_dms_on_sls_perd_id AND
                              dstrbtd_mrkt_sls.offr_perd_id = p_dms_on_offr_perd_id AND
                              nvl(dly_bilng.dly_bilng_id, dly_bilng_id_for_osl) =
                              dly_bilng_id_for_osl THEN
                          dstrbtd_mrkt_sls.unit_qty * offr_prfl_prc_point.sls_prc_amt /
                          decode(offr_prfl_prc_point.nr_for_qty,
                                 0,
                                 1,
                                 offr_prfl_prc_point.nr_for_qty) *
                          offr_prfl_prc_point.net_to_avon_fct
                         ELSE
                          0
                       END)) trend_sls_on,
             round(SUM(CASE
                         WHEN dstrbtd_mrkt_sls.sls_typ_id = p_sls_typ_id AND
                              dstrbtd_mrkt_sls.sls_perd_id = p_dms_off_sls_perd_id AND
                              dstrbtd_mrkt_sls.offr_perd_id = p_dms_off_offr_perd_id AND
                              nvl(dly_bilng.dly_bilng_id, dly_bilng_id_for_osl) =
                              dly_bilng_id_for_osl THEN
                          dstrbtd_mrkt_sls.unit_qty
                         ELSE
                          0
                       END)) trend_unts_off,
             round(SUM(CASE
                         WHEN dstrbtd_mrkt_sls.sls_typ_id = p_sls_typ_id AND
                              dstrbtd_mrkt_sls.sls_perd_id = p_dms_off_sls_perd_id AND
                              dstrbtd_mrkt_sls.offr_perd_id = p_dms_off_offr_perd_id AND
                              nvl(dly_bilng.dly_bilng_id, dly_bilng_id_for_osl) =
                              dly_bilng_id_for_osl THEN
                          dstrbtd_mrkt_sls.unit_qty * offr_prfl_prc_point.sls_prc_amt /
                          decode(offr_prfl_prc_point.nr_for_qty,
                                 0,
                                 1,
                                 offr_prfl_prc_point.nr_for_qty) *
                          offr_prfl_prc_point.net_to_avon_fct
                         ELSE
                          0
                       END)) trend_sls_off,
             round(SUM(CASE
                         WHEN dstrbtd_mrkt_sls.sls_typ_id = 6 AND
                              dstrbtd_mrkt_sls.sls_perd_id = p_dms_on_sls_perd_id AND
                              dstrbtd_mrkt_sls.offr_perd_id = p_dms_on_offr_perd_id AND
                              nvl(dly_bilng.dly_bilng_id, dly_bilng_id_for_osl) =
                              dly_bilng_id_for_osl THEN
                          dstrbtd_mrkt_sls.unit_qty
                         ELSE
                          0
                       END)) actual_unts_on,
             round(SUM(CASE
                         WHEN dstrbtd_mrkt_sls.sls_typ_id = 6 AND
                              dstrbtd_mrkt_sls.sls_perd_id = p_dms_on_sls_perd_id AND
                              dstrbtd_mrkt_sls.offr_perd_id = p_dms_on_offr_perd_id AND
                              nvl(dly_bilng.dly_bilng_id, dly_bilng_id_for_osl) =
                              dly_bilng_id_for_osl THEN
                          dstrbtd_mrkt_sls.unit_qty * offr_prfl_prc_point.sls_prc_amt /
                          decode(offr_prfl_prc_point.nr_for_qty,
                                 0,
                                 1,
                                 offr_prfl_prc_point.nr_for_qty) *
                          offr_prfl_prc_point.net_to_avon_fct
                         ELSE
                          0
                       END)) actual_sls_on,
             round(SUM(CASE
                         WHEN dstrbtd_mrkt_sls.sls_typ_id = 6 AND
                              dstrbtd_mrkt_sls.sls_perd_id = p_dms_off_sls_perd_id AND
                              dstrbtd_mrkt_sls.offr_perd_id = p_dms_off_offr_perd_id AND
                              nvl(dly_bilng.dly_bilng_id, dly_bilng_id_for_osl) =
                              dly_bilng_id_for_osl THEN
                          dstrbtd_mrkt_sls.unit_qty
                         ELSE
                          0
                       END)) actual_unts_off,
             round(SUM(CASE
                         WHEN dstrbtd_mrkt_sls.sls_typ_id = 6 AND
                              dstrbtd_mrkt_sls.sls_perd_id = p_dms_off_sls_perd_id AND
                              dstrbtd_mrkt_sls.offr_perd_id = p_dms_off_offr_perd_id AND
                              nvl(dly_bilng.dly_bilng_id, dly_bilng_id_for_osl) =
                              dly_bilng_id_for_osl THEN
                          dstrbtd_mrkt_sls.unit_qty * offr_prfl_prc_point.sls_prc_amt /
                          decode(offr_prfl_prc_point.nr_for_qty,
                                 0,
                                 1,
                                 offr_prfl_prc_point.nr_for_qty) *
                          offr_prfl_prc_point.net_to_avon_fct
                         ELSE
                          0
                       END)) actual_sls_off
        BULK COLLECT
        INTO l_tbl_hist_head_aggr
        FROM (SELECT dly_bilng_trnd.dly_bilng_id,
                     dly_bilng_trnd.mrkt_id,
                     dly_bilng_trnd.offr_perd_id,
                     dly_bilng_trnd.trnd_sls_perd_id,
                     dly_bilng_trnd.sls_prc_amt,
                     dly_bilng_trnd.nr_for_qty,
                     dly_bilng_trnd_offr_sku_line.offr_sku_line_id,
                     dly_bilng_trnd_offr_sku_line.unit_qty,
                     dly_bilng_trnd_offr_sku_line.comsn_amt,
                     dly_bilng_trnd_offr_sku_line.tax_amt,
                     MAX(dly_bilng_trnd.dly_bilng_id) over(PARTITION BY dly_bilng_trnd_offr_sku_line.offr_sku_line_id) dly_bilng_id_for_osl
                FROM dly_bilng_trnd, dly_bilng_trnd_offr_sku_line
               WHERE dly_bilng_trnd.mrkt_id = p_mrkt_id
                 AND (( --on-schedule
                      dly_bilng_trnd.trnd_sls_perd_id = p_dbt_on_sls_perd_id AND
                      dly_bilng_trnd.offr_perd_id = p_dbt_on_offr_perd_id) OR
                     ( --off-schedule
                      dly_bilng_trnd.trnd_sls_perd_id =
                      p_dbt_off_sls_perd_id AND
                      dly_bilng_trnd.offr_perd_id = p_dbt_off_offr_perd_id))
                 AND trunc(dly_bilng_trnd.prcsng_dt) <= p_dbt_bilng_day
                    -- join: dly_bilng_trnd -> dly_bilng_trnd_offr_sku_line
                 AND dly_bilng_trnd.dly_bilng_id =
                     dly_bilng_trnd_offr_sku_line.dly_bilng_id
                    -- filter: p_sls_typ_id
                 AND dly_bilng_trnd_offr_sku_line.sls_typ_id =
                     p_sls_typ_id_from_config) dly_bilng,
             dstrbtd_mrkt_sls,
             offr_sku_line,
             offr_prfl_prc_point,
             offr
       WHERE 1 = 1
            -- join: dly_bilng_trnd_offr_sku_line -> dstrbtd_mrkt_sls
         AND dly_bilng.offr_sku_line_id(+) =
             dstrbtd_mrkt_sls.offr_sku_line_id
            -- filter: mrkt_id, 
         AND dstrbtd_mrkt_sls.mrkt_id = p_mrkt_id
         AND (( --on-schedule
              dstrbtd_mrkt_sls.sls_perd_id = p_dms_on_sls_perd_id AND
              dstrbtd_mrkt_sls.offr_perd_id = p_dms_on_offr_perd_id) OR
             ( --off-schedule
              dstrbtd_mrkt_sls.sls_perd_id = p_dms_off_sls_perd_id AND
              dstrbtd_mrkt_sls.offr_perd_id = p_dms_off_offr_perd_id))
         AND dstrbtd_mrkt_sls.sls_typ_id IN (1, p_sls_typ_id, 6)
            -- additional filtering...
         AND dstrbtd_mrkt_sls.offr_sku_line_id =
             offr_sku_line.offr_sku_line_id
         AND offr_sku_line.dltd_ind <> 'Y'
         AND offr_sku_line.offr_prfl_prcpt_id =
             offr_prfl_prc_point.offr_prfl_prcpt_id
         AND offr_prfl_prc_point.offr_id = offr.offr_id
         AND offr.offr_typ = 'CMP'
         AND offr.ver_id = 0
       GROUP BY p_dbt_on_sls_perd_id, offr_sku_line.sku_id
       ORDER BY p_dbt_on_sls_perd_id, offr_sku_line.sku_id;
    EXCEPTION
      WHEN OTHERS THEN
        app_plsql_log.error(l_module_name || ' error code:' || SQLCODE ||
                            ' error message: ' || SQLERRM ||
                            l_parameter_list);
        RAISE e_get_head_details;
    END;
    --
    app_plsql_log.info(l_module_name || ' end' || l_parameter_list);
    --
    RETURN l_tbl_hist_head_aggr;
  END get_head_details;

  FUNCTION get_reproc_trnd(p_mrkt_id                IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                           p_sls_perd_id            IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                           p_trg_perd_id            IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                           p_sls_typ_id             IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                           p_bilng_day              IN dly_bilng_trnd.prcsng_dt%TYPE,
                           p_sls_typ_id_from_config IN mrkt_config_item.mrkt_config_item_val_txt%TYPE,
                           p_dbt_on_sls_perd_id     IN dly_bilng_trnd.trnd_sls_perd_id%TYPE,
                           p_dbt_on_offr_perd_id    IN dly_bilng_trnd.offr_perd_id%TYPE,
                           p_dbt_off_sls_perd_id    IN dly_bilng_trnd.trnd_sls_perd_id%TYPE,
                           p_dbt_off_offr_perd_id   IN dly_bilng_trnd.offr_perd_id%TYPE,
                           p_dms_on_sls_perd_id     IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                           p_dms_on_offr_perd_id    IN dstrbtd_mrkt_sls.offr_perd_id%TYPE,
                           p_dms_off_sls_perd_id    IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                           p_dms_off_offr_perd_id   IN dstrbtd_mrkt_sls.offr_perd_id%TYPE,
                           p_cash_value             IN NUMBER,
                           p_r_factor               IN NUMBER,
                           p_run_id                 IN NUMBER DEFAULT NULL,
                           p_user_id                IN VARCHAR2 DEFAULT NULL)
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
                                       'p_trg_perd_id: ' ||
                                       to_char(p_trg_perd_id) || ', ' ||
                                       'p_sls_typ_id: ' ||
                                       to_char(p_sls_typ_id) || ', ' ||
                                       'p_bilng_day: ' ||
                                       to_char(p_bilng_day, 'yyyymmdd') || ', ' ||
                                       'p_cash_value: ' ||
                                       to_char(p_cash_value) || ', ' ||
                                       'p_r_factor: ' ||
                                       to_char(p_r_factor) || ', ' ||
                                       'p_run_id: ' || to_char(l_run_id) || ', ' ||
                                       'p_user_id: ' || l_user_id || ')';
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
      SELECT p_mrkt_id,
             p_sls_typ_id,
             offr_sku_line_id,
             p_dbt_on_sls_perd_id,
             p_dbt_on_offr_perd_id,
             p_dbt_off_sls_perd_id,
             p_dbt_off_offr_perd_id,
             p_dms_on_sls_perd_id,
             p_dms_on_offr_perd_id,
             p_dms_off_sls_perd_id,
             p_dms_off_offr_perd_id,
             -- On-Schedule
             -- frcstd_unts_on
             round(SUM(CASE
                         WHEN trnd_sls_perd_id = p_dbt_on_sls_perd_id AND
                              offr_perd_id = p_dbt_on_offr_perd_id THEN
                          CASE
                            WHEN sct_unit_qty IS NOT NULL THEN
                             CASE
                               WHEN total_unit_qty_by_fsc_cd = 0 THEN
                                1 / total_cnt_by_fsc_cd
                               ELSE
                                (osl_unit_qty / total_unit_qty_by_fsc_cd) * sct_unit_qty
                             END
                            ELSE
                             p_r_factor * bias * bi24_adj * osl_unit_qty
                          END
                         ELSE
                          0
                       END)) frcstd_unts_on,
             -- frcstd_sls_on
             round(SUM(CASE
                         WHEN trnd_sls_perd_id = p_dbt_on_sls_perd_id AND
                              offr_perd_id = p_dbt_on_offr_perd_id THEN
                          CASE
                            WHEN sct_unit_qty IS NOT NULL THEN
                             CASE
                               WHEN osl_unit_qty = 0 THEN
                                0 - osl_comsn_amt - osl_tax_amt
                               ELSE
                                (osl_unit_qty / total_unit_qty_by_fsc_cd) * sct_unit_qty *
                                ((osl_unit_qty * dbt_sls_prc_amt / dbt_nr_for_qty -
                                osl_comsn_amt - osl_tax_amt) / osl_unit_qty)
                             END
                            ELSE
                             p_r_factor * bias * bi24_adj *
                             (osl_unit_qty * (dbt_sls_prc_amt / dbt_nr_for_qty) -
                             osl_comsn_amt - osl_tax_amt)
                          END
                         ELSE
                          0
                       END)) frcstd_sls_on,
             -- Off-Schedule
             -- frcstd_unts_off
             round(SUM(CASE
                         WHEN trnd_sls_perd_id = p_dbt_off_sls_perd_id AND
                              offr_perd_id = p_dbt_off_offr_perd_id THEN
                          CASE
                            WHEN sct_unit_qty IS NOT NULL THEN
                             CASE
                               WHEN total_unit_qty_by_fsc_cd = 0 THEN
                                1 / total_cnt_by_fsc_cd
                               ELSE
                                (osl_unit_qty / total_unit_qty_by_fsc_cd) * sct_unit_qty
                             END
                            ELSE
                             p_r_factor * bias * bi24_adj * osl_unit_qty
                          END
                         ELSE
                          0
                       END)) frcstd_unts_off,
             -- frcstd_sls_off
             round(SUM(CASE
                         WHEN trnd_sls_perd_id = p_dbt_off_sls_perd_id AND
                              offr_perd_id = p_dbt_off_offr_perd_id THEN
                          CASE
                            WHEN sct_unit_qty IS NOT NULL THEN
                             CASE
                               WHEN osl_unit_qty = 0 THEN
                                0 - osl_comsn_amt - osl_tax_amt
                               ELSE
                                (osl_unit_qty / total_unit_qty_by_fsc_cd) * sct_unit_qty *
                                ((osl_unit_qty * dbt_sls_prc_amt / dbt_nr_for_qty -
                                osl_comsn_amt - osl_tax_amt) / osl_unit_qty)
                             END
                            ELSE
                             p_r_factor * bias * bi24_adj *
                             (osl_unit_qty * (dbt_sls_prc_amt / dbt_nr_for_qty) -
                             osl_comsn_amt - osl_tax_amt)
                          END
                         ELSE
                          0
                       END)) frcstd_sls_off,
             -- additional...
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
                    p_r_factor * bias * bi24_adj * osl_comsn_amt
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
                    p_r_factor * bias * bi24_adj * osl_comsn_amt
                 END) tax_amt,
             CASE
               WHEN trnd_sls_perd_id = p_dbt_on_sls_perd_id AND
                    offr_perd_id = p_dbt_on_offr_perd_id THEN
                'ON'
               ELSE
                'OFF'
             END on_off_flag
        BULK COLLECT
        INTO l_table_reproc_trnd
        FROM ( -- DBT - inner select
              SELECT dly_bilng_trnd_offr_sku_line.offr_sku_line_id,
                      -- DBT
                      dly_bilng_trnd.trnd_sls_perd_id,
                      dly_bilng_trnd.offr_perd_id,
                      dly_bilng_trnd.unit_qty dbt_unit_qty,
                      dly_bilng_trnd.sls_prc_amt dbt_sls_prc_amt,
                      decode(dly_bilng_trnd.nr_for_qty,
                             0,
                             1,
                             dly_bilng_trnd.nr_for_qty) dbt_nr_for_qty,
                      SUM(dly_bilng_trnd_offr_sku_line.unit_qty) over(PARTITION BY p_mrkt_id, p_sls_perd_id, p_sls_typ_id, dly_bilng_trnd.fsc_cd) total_unit_qty_by_fsc_cd,
                      COUNT(1) over(PARTITION BY p_mrkt_id, p_sls_perd_id, p_sls_typ_id, dly_bilng_trnd.fsc_cd) total_cnt_by_fsc_cd,
                      -- BIAS
                      nvl(bias_pct / 100, 1) bias,
                      -- BI24_ADJ
                      nvl(dly_bilng_adjstmnt.unit_qty /
                          nvl(dly_bilng_trnd.unit_qty, 1),
                          1) bi24_adj,
                      -- OSL
                      dly_bilng_trnd_offr_sku_line.unit_qty  osl_unit_qty,
                      dly_bilng_trnd_offr_sku_line.comsn_amt osl_comsn_amt,
                      dly_bilng_trnd_offr_sku_line.tax_amt   osl_tax_amt,
                      -- FSC
                      sct_fsc_ovrrd.sct_unit_qty,
                      -- additional...
                      offr.veh_id
                FROM dly_bilng_trnd,
                      sct_fsc_ovrrd,
                      dly_bilng_trnd_offr_sku_line,
                      offr_sku_line,
                      offr_prfl_prc_point,
                      offr,
                      mrkt_perd_sku_bias,
                      dly_bilng_adjstmnt
               WHERE 1 = 1
                    -- filter: dly_bilng_trnd
                 AND dly_bilng_trnd.mrkt_id = p_mrkt_id
                 AND dly_bilng_trnd.trnd_sls_perd_id = p_sls_perd_id
                 AND (( --on-schedule
                      dly_bilng_trnd.trnd_sls_perd_id = p_dbt_on_sls_perd_id AND
                      dly_bilng_trnd.offr_perd_id = p_dbt_on_offr_perd_id) OR
                     ( --off-schedule
                      dly_bilng_trnd.trnd_sls_perd_id = p_dbt_off_sls_perd_id AND
                      dly_bilng_trnd.offr_perd_id = p_dbt_off_offr_perd_id))
                 AND trunc(dly_bilng_trnd.prcsng_dt) <= p_bilng_day
                    -- join: dly_bilng_trnd -> sct_fsc_ovrrd
                 AND p_mrkt_id = sct_fsc_ovrrd.mrkt_id(+)
                 AND p_trg_perd_id = sct_fsc_ovrrd.sls_perd_id(+)
                 AND p_sls_typ_id = sct_fsc_ovrrd.sls_typ_id(+)
                 AND dly_bilng_trnd.fsc_cd = sct_fsc_ovrrd.fsc_cd(+)
                    -- join: dly_bilng_trnd -> dly_bilng_trnd_offr_sku_line
                 AND dly_bilng_trnd.dly_bilng_id =
                     dly_bilng_trnd_offr_sku_line.dly_bilng_id
                    -- filter: p_sls_typ_id
                 AND dly_bilng_trnd_offr_sku_line.sls_typ_id =
                     p_sls_typ_id_from_config
                    -- additional filtering...
                 AND dly_bilng_trnd_offr_sku_line.offr_sku_line_id =
                     offr_sku_line.offr_sku_line_id
                 AND offr_sku_line.dltd_ind <> 'Y'
                 AND offr_sku_line.offr_prfl_prcpt_id =
                     offr_prfl_prc_point.offr_prfl_prcpt_id
                 AND offr_prfl_prc_point.offr_id = offr.offr_id
                 AND offr.offr_typ = 'CMP'
                 AND offr.ver_id = 0
                    -- join: dly_bilng_trnd -> mrkt_perd_sku_bias
                 AND p_mrkt_id = mrkt_perd_sku_bias.mrkt_id(+)
                 AND p_trg_perd_id = mrkt_perd_sku_bias.sls_perd_id(+)
                 AND dly_bilng_trnd.sku_id = mrkt_perd_sku_bias.sku_id(+)
                    -- join: dly_bilng_trnd -> dly_bilng_adjstmnt
                 AND dly_bilng_trnd.dly_bilng_id =
                     dly_bilng_adjstmnt.dly_bilng_id(+))
       GROUP BY p_mrkt_id,
                p_sls_typ_id,
                offr_sku_line_id,
                p_dbt_on_sls_perd_id,
                p_dbt_on_offr_perd_id,
                p_dbt_off_sls_perd_id,
                p_dbt_off_offr_perd_id,
                p_dms_on_sls_perd_id,
                p_dms_on_offr_perd_id,
                p_dms_off_sls_perd_id,
                p_dms_off_offr_perd_id,
                veh_id,
                CASE
                  WHEN trnd_sls_perd_id = p_dbt_on_sls_perd_id AND
                       offr_perd_id = p_dbt_on_offr_perd_id THEN
                   'ON'
                  ELSE
                   'OFF'
                END;
    EXCEPTION
      WHEN OTHERS THEN
        app_plsql_log.error(l_module_name || ' error code:' || SQLCODE ||
                            ' error message: ' || SQLERRM ||
                            l_parameter_list);
        RAISE e_get_reproc_trnd;
    END;
    --
    app_plsql_log.info(l_module_name || ' end' || l_parameter_list);
    --
    RETURN l_table_reproc_trnd;
  END get_reproc_trnd;

  FUNCTION get_reproc_est(p_mrkt_id              IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                          p_sls_perd_id          IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                          p_trg_perd_id          IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                          p_sls_typ_id           IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                          p_bilng_day            IN dly_bilng_trnd.prcsng_dt%TYPE,
                          p_dbt_on_sls_perd_id   IN dly_bilng_trnd.trnd_sls_perd_id%TYPE,
                          p_dbt_on_offr_perd_id  IN dly_bilng_trnd.offr_perd_id%TYPE,
                          p_dbt_off_sls_perd_id  IN dly_bilng_trnd.trnd_sls_perd_id%TYPE,
                          p_dbt_off_offr_perd_id IN dly_bilng_trnd.offr_perd_id%TYPE,
                          p_dms_on_sls_perd_id   IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                          p_dms_on_offr_perd_id  IN dstrbtd_mrkt_sls.offr_perd_id%TYPE,
                          p_dms_off_sls_perd_id  IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                          p_dms_off_offr_perd_id IN dstrbtd_mrkt_sls.offr_perd_id%TYPE,
                          p_cash_value           IN NUMBER,
                          p_r_factor             IN NUMBER,
                          p_use_offers_on_sched  IN CHAR DEFAULT 'N',
                          p_use_offers_off_sched IN CHAR DEFAULT 'N',
                          p_run_id               IN NUMBER DEFAULT NULL,
                          p_user_id              IN VARCHAR2 DEFAULT NULL)
    RETURN t_reproc IS
    -- local variables
    l_table_reproc_est     t_reproc;
    l_use_offers_on_sched  CHAR(1) := upper(nvl(p_use_offers_on_sched, 'N'));
    l_use_offers_off_sched CHAR(1) := upper(nvl(p_use_offers_off_sched, 'N'));
    -- for LOG
    l_run_id         NUMBER := nvl(p_run_id,
                                   app_plsql_output.generate_new_run_id);
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'GET_REPROC_EST';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       to_char(p_mrkt_id) || ', ' ||
                                       'p_sls_perd_id: ' ||
                                       to_char(p_sls_perd_id) || ', ' ||
                                       'p_trg_perd_id: ' ||
                                       to_char(p_trg_perd_id) || ', ' ||
                                       'p_sls_typ_id: ' ||
                                       to_char(p_sls_typ_id) || ', ' ||
                                       'p_bilng_day: ' ||
                                       to_char(p_bilng_day, 'yyyymmdd') || ', ' ||
                                       'p_cash_value: ' ||
                                       to_char(p_cash_value) || ', ' ||
                                       'p_r_factor: ' ||
                                       to_char(p_r_factor) || ', ' ||
                                       'p_run_id: ' || to_char(l_run_id) || ', ' ||
                                       'p_user_id: ' || l_user_id || ')';
    -- exception
    e_get_reproc_est EXCEPTION;
    --
  BEGIN
    --
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start' || l_parameter_list);
    -- forecasted - EST
    BEGIN
      SELECT p_mrkt_id,
             p_sls_typ_id,
             dstrbtd_mrkt_sls.offr_sku_line_id,
             p_dbt_on_sls_perd_id,
             p_dbt_on_offr_perd_id,
             p_dbt_off_sls_perd_id,
             p_dbt_off_offr_perd_id,
             p_dms_on_sls_perd_id,
             p_dms_on_offr_perd_id,
             p_dms_off_sls_perd_id,
             p_dms_off_offr_perd_id,
             -- On-Schedule
             -- frcstd_unts_on
             round(SUM(CASE
                         WHEN dstrbtd_mrkt_sls.sls_perd_id = p_dms_on_sls_perd_id AND
                              dstrbtd_mrkt_sls.offr_perd_id = p_dms_on_offr_perd_id THEN
                          nvl(sct_fsc_ovrrd.sct_unit_qty,
                              decode(l_use_offers_on_sched,
                                     'Y',
                                     dstrbtd_mrkt_sls.unit_qty,
                                     0))
                         ELSE
                          0
                       END)) est_unts_on,
             -- frcstd_sls_on
             round(SUM(CASE
                         WHEN dstrbtd_mrkt_sls.sls_perd_id = p_dms_on_sls_perd_id AND
                              dstrbtd_mrkt_sls.offr_perd_id = p_dms_on_offr_perd_id THEN
                          nvl(sct_fsc_ovrrd.sct_unit_qty,
                              decode(l_use_offers_on_sched,
                                     'Y',
                                     dstrbtd_mrkt_sls.unit_qty,
                                     0)) *
                          (offr_prfl_prc_point.sls_prc_amt /
                           decode(offr_prfl_prc_point.nr_for_qty,
                                  0,
                                  1,
                                  offr_prfl_prc_point.nr_for_qty) *
                           offr_prfl_prc_point.net_to_avon_fct /
                           decode(dstrbtd_mrkt_sls.unit_qty,
                                  0,
                                  1,
                                  dstrbtd_mrkt_sls.unit_qty))
                         ELSE
                          0
                       END)) est_sls_on,
             -- Off-Schedule
             -- frcstd_unts_off
             round(SUM(CASE
                         WHEN dstrbtd_mrkt_sls.sls_perd_id = p_dms_off_sls_perd_id AND
                              dstrbtd_mrkt_sls.offr_perd_id = p_dms_off_offr_perd_id THEN
                          nvl(sct_fsc_ovrrd.sct_unit_qty,
                              decode(l_use_offers_off_sched,
                                     'Y',
                                     dstrbtd_mrkt_sls.unit_qty,
                                     0))
                         ELSE
                          0
                       END)) est_unts_off,
             -- frcstd_sls_off
             round(SUM(CASE
                         WHEN dstrbtd_mrkt_sls.sls_perd_id = p_dms_off_sls_perd_id AND
                              dstrbtd_mrkt_sls.offr_perd_id = p_dms_off_offr_perd_id THEN
                          nvl(sct_fsc_ovrrd.sct_unit_qty,
                              decode(l_use_offers_on_sched,
                                     'Y',
                                     dstrbtd_mrkt_sls.unit_qty,
                                     0)) *
                          (offr_prfl_prc_point.sls_prc_amt /
                           decode(offr_prfl_prc_point.nr_for_qty,
                                  0,
                                  1,
                                  offr_prfl_prc_point.nr_for_qty) *
                           offr_prfl_prc_point.net_to_avon_fct /
                           decode(dstrbtd_mrkt_sls.unit_qty,
                                  0,
                                  1,
                                  dstrbtd_mrkt_sls.unit_qty))
                         ELSE
                          0
                       END)) est_sls_off,
             -- additional...
             offr.veh_id,
             NULL comsn_amt,
             NULL tax_amt,
             CASE
               WHEN dstrbtd_mrkt_sls.sls_perd_id = p_dms_on_sls_perd_id AND
                    dstrbtd_mrkt_sls.offr_perd_id = p_dms_on_offr_perd_id THEN
                'ON'
               ELSE
                'OFF'
             END on_off_flag
        BULK COLLECT
        INTO l_table_reproc_est
        FROM dstrbtd_mrkt_sls,
             dly_bilng_trnd_offr_sku_line,
             offr_sku_line,
             offr_prfl_prc_point,
             offr,
             --dly_bilng_trnd,
             sct_fsc_ovrrd,
             (SELECT mrkt_id,
                     MAX(fsc_cd) max_fsc_cd,
                     from_strt_perd_id,
                     to_strt_perd_id,
                     sku_id
                FROM (SELECT mrkt_id,
                             fsc_cd,
                             strt_perd_id from_strt_perd_id,
                             nvl(lead(strt_perd_id, 1)
                                 over(PARTITION BY mrkt_id,
                                      fsc_cd ORDER BY strt_perd_id),
                                 99999999) to_strt_perd_id,
                             sku_id
                        FROM mrkt_fsc
                       WHERE p_mrkt_id = mrkt_fsc.mrkt_id
                         AND p_trg_perd_id >= mrkt_fsc.strt_perd_id
                         AND 'N' = mrkt_fsc.dltd_ind)
               GROUP BY mrkt_id, from_strt_perd_id, to_strt_perd_id, sku_id) mrkt_tmp_fsc
       WHERE 1 = 1
            -- filter: mrkt_id, 
         AND dstrbtd_mrkt_sls.mrkt_id = p_mrkt_id
         AND dstrbtd_mrkt_sls.sls_perd_id = p_trg_perd_id
         AND ( --on-schedule
              dstrbtd_mrkt_sls.offr_perd_id = p_dms_on_offr_perd_id OR
             --off-schedule
              dstrbtd_mrkt_sls.offr_perd_id = p_dms_off_offr_perd_id)
         AND dstrbtd_mrkt_sls.sls_typ_id = 1 /* only ESTIMATE */
            -- dstrbtd_mrkt_sls -> offr_sku_line
            -- and additional filtering...
         AND dstrbtd_mrkt_sls.offr_sku_line_id =
             offr_sku_line.offr_sku_line_id
         AND offr_sku_line.dltd_ind <> 'Y'
         AND offr_sku_line.offr_prfl_prcpt_id =
             offr_prfl_prc_point.offr_prfl_prcpt_id
         AND offr_prfl_prc_point.offr_id = offr.offr_id
         AND offr.offr_typ = 'CMP'
         AND offr.ver_id = 0
            -- join: dly_bilng_trnd_offr_sku_line -> dly_bilng_trnd
         AND offr_sku_line.offr_sku_line_id =
             dly_bilng_trnd_offr_sku_line.offr_sku_line_id(+)
         AND dly_bilng_trnd_offr_sku_line.offr_sku_line_id IS NULL
            --
         AND p_mrkt_id = mrkt_tmp_fsc.mrkt_id
         AND p_trg_perd_id >= mrkt_tmp_fsc.from_strt_perd_id
         AND p_trg_perd_id < mrkt_tmp_fsc.to_strt_perd_id
         AND offr_sku_line.sku_id = mrkt_tmp_fsc.sku_id
            --
         AND p_mrkt_id = sct_fsc_ovrrd.mrkt_id(+)
         AND p_trg_perd_id = sct_fsc_ovrrd.sls_perd_id(+)
         AND p_sls_typ_id = sct_fsc_ovrrd.sls_typ_id(+)
         AND mrkt_tmp_fsc.max_fsc_cd = sct_fsc_ovrrd.fsc_cd(+)
       GROUP BY p_mrkt_id,
                p_sls_typ_id,
                dstrbtd_mrkt_sls.offr_sku_line_id,
                p_dbt_on_sls_perd_id,
                p_dbt_on_offr_perd_id,
                p_dbt_off_sls_perd_id,
                p_dbt_off_offr_perd_id,
                p_dms_on_sls_perd_id,
                p_dms_on_offr_perd_id,
                p_dms_off_sls_perd_id,
                p_dms_off_offr_perd_id,
                offr.veh_id,
                CASE
                  WHEN dstrbtd_mrkt_sls.sls_perd_id = p_dms_on_sls_perd_id AND
                       dstrbtd_mrkt_sls.offr_perd_id = p_dms_on_offr_perd_id THEN
                   'ON'
                  ELSE
                   'OFF'
                END;
    EXCEPTION
      WHEN OTHERS THEN
        app_plsql_log.error(l_module_name || ' error code:' || SQLCODE ||
                            ' error message: ' || SQLERRM ||
                            l_parameter_list);
        RAISE e_get_reproc_est;
    END;
    --
    app_plsql_log.info(l_module_name || ' end' || l_parameter_list);
    --
    RETURN l_table_reproc_est;
  END get_reproc_est;

  FUNCTION get_trend_alloc_reproc_ln(p_mrkt_id              IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                                     p_sls_perd_id          IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                     p_trg_perd_id          IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                     p_sls_typ_id           IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                                     p_bilng_day            IN dly_bilng_trnd.prcsng_dt%TYPE,
                                     p_cash_value           IN NUMBER,
                                     p_r_factor             IN NUMBER,
                                     p_use_offers_on_sched  IN CHAR DEFAULT 'N',
                                     p_use_offers_off_sched IN CHAR DEFAULT 'N',
                                     p_run_id               IN NUMBER DEFAULT NULL,
                                     p_user_id              IN VARCHAR2 DEFAULT NULL)
    RETURN obj_trend_alloc_hist_head_line IS
    -- local variables
    l_table_reproc t_reproc;
    l_periods      r_periods;
    -- lookup: mrkt_config_item_val_txt
    l_sls_typ_id_from_config mrkt_config_item.mrkt_config_item_val_txt%TYPE;
    -- BI24 and r_factor
    l_bi24_r_factor r_bi24_r_factor;
    -- FORCASTED
    l_frcstd_unts_on  NUMBER;
    l_frcstd_sls_on   NUMBER;
    l_frcstd_unts_off NUMBER;
    l_frcstd_sls_off  NUMBER;
    -- DAILY BILLING TREND
    l_trnd_unts_on  NUMBER;
    l_trnd_sls_on   NUMBER;
    l_trnd_unts_off NUMBER;
    l_trnd_sls_off  NUMBER;
    -- ESTIMATE and (OVRRD)
    l_est_unts_on  NUMBER;
    l_est_sls_on   NUMBER;
    l_est_unts_off NUMBER;
    l_est_sls_off  NUMBER;
    -- for LOG
    l_run_id         NUMBER := nvl(p_run_id,
                                   app_plsql_output.generate_new_run_id);
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'GET_TREND_ALLOC_REPROC_LN';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       to_char(p_mrkt_id) || ', ' ||
                                       'p_sls_perd_id: ' ||
                                       to_char(p_sls_perd_id) || ', ' ||
                                       'p_trg_perd_id: ' ||
                                       to_char(p_trg_perd_id) || ', ' ||
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
                                       'p_run_id: ' || to_char(l_run_id) || ', ' ||
                                       'p_user_id: ' || l_user_id || ')';
    --
  BEGIN
    --
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start' || l_parameter_list);
    -- get sls_typ_id_from_config
    l_sls_typ_id_from_config := get_sls_typ_id_from_config(p_mrkt_id,
                                                           l_run_id,
                                                           l_user_id);
    -- get CURRENT periods
    l_periods := get_periods(p_mrkt_id       => p_mrkt_id,
                             p_orig_perd_id  => p_sls_perd_id,
                             p_bilng_perd_id => p_sls_perd_id,
                             p_sls_typ_id    => p_sls_typ_id,
                             p_bilng_day     => p_bilng_day,
                             p_run_id        => l_run_id,
                             p_user_id       => l_user_id);
    -- cash value
    IF p_cash_value IS NOT NULL THEN
      l_periods.sct_cash_val := p_cash_value;
    END IF;
    -- get BI24, r_factor
    l_bi24_r_factor := get_bi24_r_factor(p_mrkt_id                => p_mrkt_id,
                                         p_dbt_bilng_day          => p_bilng_day,
                                         p_sls_typ_id_from_config => l_sls_typ_id_from_config,
                                         p_dbt_on_sls_perd_id     => l_periods.dbt_on_sls_perd_id,
                                         p_dbt_on_offr_perd_id    => l_periods.dbt_on_offr_perd_id,
                                         p_dbt_off_sls_perd_id    => l_periods.dbt_off_sls_perd_id,
                                         p_dbt_off_offr_perd_id   => l_periods.dbt_off_offr_perd_id,
                                         p_dms_on_sls_perd_id     => l_periods.dms_on_sls_perd_id,
                                         p_dms_on_offr_perd_id    => l_periods.dms_on_offr_perd_id,
                                         p_dms_off_sls_perd_id    => l_periods.dms_off_sls_perd_id,
                                         p_dms_off_offr_perd_id   => l_periods.dms_off_offr_perd_id,
                                         p_sct_cash_val           => l_periods.sct_cash_val,
                                         p_r_factor               => NULL /*p_r_factor*/,
                                         p_sls_typ_id             => p_sls_typ_id,
                                         p_run_id                 => l_run_id,
                                         p_user_id                => l_user_id);
    -- FORECASTED
    -- TRND
    l_table_reproc := get_reproc_trnd(p_mrkt_id,
                                      p_sls_perd_id,
                                      p_trg_perd_id,
                                      p_sls_typ_id,
                                      p_bilng_day,
                                      l_sls_typ_id_from_config,
                                      l_periods.dbt_on_sls_perd_id,
                                      l_periods.dbt_on_offr_perd_id,
                                      l_periods.dbt_off_sls_perd_id,
                                      l_periods.dbt_off_offr_perd_id,
                                      l_periods.dms_on_sls_perd_id,
                                      l_periods.dms_on_offr_perd_id,
                                      l_periods.dms_off_sls_perd_id,
                                      l_periods.dms_off_offr_perd_id,
                                      l_periods.sct_cash_val,
                                      l_bi24_r_factor.r_factor,
                                      l_run_id,
                                      l_user_id);
    --
    l_trnd_unts_on  := 0;
    l_trnd_sls_on   := 0;
    l_trnd_unts_off := 0;
    l_trnd_sls_off  := 0;
    IF l_table_reproc.count > 0 THEN
      FOR i IN l_table_reproc.first .. l_table_reproc.last LOOP
        l_trnd_unts_on  := l_trnd_unts_on + l_table_reproc(i)
                          .frcstd_unts_on;
        l_trnd_sls_on   := l_trnd_sls_on + l_table_reproc(i).frcstd_sls_on;
        l_trnd_unts_off := l_trnd_unts_off + l_table_reproc(i)
                          .frcstd_unts_off;
        l_trnd_sls_off  := l_trnd_sls_off + l_table_reproc(i)
                          .frcstd_sls_off;
      END LOOP;
    END IF;
    -- EST and OVRRD
    -- cleanup
    IF l_table_reproc.count > 0 THEN
      l_table_reproc.delete;
    END IF;
    l_table_reproc := get_reproc_est(p_mrkt_id,
                                     p_sls_perd_id,
                                     p_trg_perd_id,
                                     p_sls_typ_id,
                                     p_bilng_day,
                                     l_periods.dbt_on_sls_perd_id,
                                     l_periods.dbt_on_offr_perd_id,
                                     l_periods.dbt_off_sls_perd_id,
                                     l_periods.dbt_off_offr_perd_id,
                                     l_periods.dms_on_sls_perd_id,
                                     l_periods.dms_on_offr_perd_id,
                                     l_periods.dms_off_sls_perd_id,
                                     l_periods.dms_off_offr_perd_id,
                                     l_periods.sct_cash_val,
                                     l_bi24_r_factor.r_factor,
                                     p_use_offers_on_sched,
                                     p_use_offers_off_sched,
                                     l_run_id,
                                     l_user_id);
    --
    l_est_unts_on  := 0;
    l_est_sls_on   := 0;
    l_est_unts_off := 0;
    l_est_sls_off  := 0;
    IF l_table_reproc.count > 0 THEN
      FOR i IN l_table_reproc.first .. l_table_reproc.last LOOP
        l_est_unts_on  := l_est_unts_on + l_table_reproc(i).frcstd_unts_on;
        l_est_sls_on   := l_est_sls_on + l_table_reproc(i).frcstd_sls_on;
        l_est_unts_off := l_est_unts_off + l_table_reproc(i)
                         .frcstd_unts_off;
        l_est_sls_off  := l_est_sls_off + l_table_reproc(i).frcstd_sls_off;
      END LOOP;
    END IF;
    -- cleanup
    IF l_table_reproc.count > 0 THEN
      l_table_reproc.delete;
    END IF;
    -- On-Schedule
    IF p_use_offers_on_sched = 'Y' THEN
      l_frcstd_unts_on := nvl(l_trnd_unts_on, 0) + nvl(l_est_unts_on, 0);
      l_frcstd_sls_on  := nvl(l_trnd_sls_on, 0) + nvl(l_est_sls_on, 0);
    ELSE
      l_frcstd_unts_on := nvl(l_trnd_unts_on, 0);
      l_frcstd_sls_on  := nvl(l_trnd_sls_on, 0);
    END IF;
    -- Off-Schedule
    IF p_use_offers_off_sched = 'Y' THEN
      l_frcstd_unts_off := nvl(l_trnd_unts_off, 0) + nvl(l_est_unts_off, 0);
      l_frcstd_sls_off  := nvl(l_trnd_sls_off, 0) + nvl(l_est_sls_off, 0);
    ELSE
      l_frcstd_unts_off := nvl(l_trnd_unts_off, 0);
      l_frcstd_sls_off  := nvl(l_trnd_sls_off, 0);
    END IF;
    --
    app_plsql_log.info(l_module_name || ' end' || l_parameter_list);
    --
    RETURN obj_trend_alloc_hist_head_line(p_sls_perd_id,
                                          p_trg_perd_id,
                                          p_bilng_day,
                                          l_bi24_r_factor.bi24_unts_on,
                                          l_bi24_r_factor.bi24_sls_on,
                                          l_bi24_r_factor.bi24_unts_off,
                                          l_bi24_r_factor.bi24_sls_off,
                                          l_periods.sct_cash_val,
                                          l_bi24_r_factor.r_factor,
                                          NULL,
                                          NULL,
                                          NULL,
                                          NULL,
                                          l_frcstd_unts_on,
                                          l_frcstd_sls_on,
                                          l_frcstd_unts_off,
                                          l_frcstd_sls_off,
                                          NULL,
                                          NULL,
                                          NULL,
                                          NULL);
  END get_trend_alloc_reproc_ln;

  FUNCTION get_trend_alloc_hist_dtls(p_mrkt_id     IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                                     p_sls_perd_id IN dly_bilng_trnd.trnd_sls_perd_id%TYPE,
                                     p_trg_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                     p_sls_typ_id  IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                                     p_bilng_day   IN dly_bilng.prcsng_dt%TYPE,
                                     p_run_id      IN NUMBER DEFAULT NULL,
                                     p_user_id     IN VARCHAR2 DEFAULT NULL)
    RETURN obj_trend_alloc_hist_dt_table
    PIPELINED IS
    -- local variables
    l_tbl_hist_head_aggr     t_hist_head_aggr;
    l_periods                r_periods;
    l_sls_typ_id_from_config dly_bilng_trnd_offr_sku_line.sls_typ_id%TYPE;
    -- for LOG
    l_run_id         NUMBER := nvl(p_run_id,
                                   app_plsql_output.generate_new_run_id);
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
                                       'p_prcsng_dt: ' ||
                                       to_char(p_bilng_day) || ', ' ||
                                       'p_run_id: ' || to_char(l_run_id) || ', ' ||
                                       'p_user_id: ' || l_user_id || ')';
    --
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start' || l_parameter_list);
    -- sls_typ_id_from_config
    l_sls_typ_id_from_config := get_sls_typ_id_from_config(p_mrkt_id,
                                                           l_run_id,
                                                           l_user_id);
    -- get CURRENT periods
    l_periods := get_periods(p_mrkt_id       => p_mrkt_id,
                             p_orig_perd_id  => p_sls_perd_id,
                             p_bilng_perd_id => p_sls_perd_id,
                             p_sls_typ_id    => p_sls_typ_id,
                             p_bilng_day     => p_bilng_day,
                             p_run_id        => l_run_id,
                             p_user_id       => l_user_id);
    -- get_head_details
    l_tbl_hist_head_aggr := get_head_details(p_mrkt_id,
                                             p_sls_perd_id,
                                             p_trg_perd_id,
                                             p_sls_typ_id,
                                             p_bilng_day,
                                             l_sls_typ_id_from_config,
                                             l_periods.dbt_on_sls_perd_id,
                                             l_periods.dbt_on_offr_perd_id,
                                             l_periods.dbt_off_sls_perd_id,
                                             l_periods.dbt_off_offr_perd_id,
                                             l_periods.dms_on_sls_perd_id,
                                             l_periods.dms_on_offr_perd_id,
                                             l_periods.dms_off_sls_perd_id,
                                             l_periods.dms_off_offr_perd_id,
                                             l_run_id,
                                             l_user_id);
    --
    IF l_tbl_hist_head_aggr.count > 0 THEN
      FOR i IN l_tbl_hist_head_aggr.first .. l_tbl_hist_head_aggr.last LOOP
        FOR i_prd_sch IN (SELECT mrkt_veh.lcl_veh_desc_txt,
                                 catgry.catgry_nm,
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
                                 sku.sku_nm
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
                           WHERE l_tbl_hist_head_aggr(i)
                           .p_sku_id = sku.sku_id
                              AND sku.prfl_cd = prfl.prfl_cd
                              AND prfl.catgry_id = catgry.catgry_id
                              AND mrkt_veh.mrkt_id = l_tbl_hist_head_aggr(i)
                                 .mrkt_id
                              AND mrkt_veh.veh_id = l_tbl_hist_head_aggr(i)
                                 .veh_id
                              AND prfl.brnd_id = v_brnd.brnd_id
                              AND prfl.sgmt_id = sgmt.sgmt_id
                              AND prfl.form_id = form.form_id
                              AND form.form_grp_id = form_grp.form_grp_id
                              AND l_tbl_hist_head_aggr(i)
                                 .promtn_id = promtn.promtn_id(+)
                              AND l_tbl_hist_head_aggr(i)
                                 .promtn_clm_id = promtn_clm.promtn_clm_id(+)
                              AND l_tbl_hist_head_aggr(i)
                                 .sls_cls_cd = sls_cls.sls_cls_cd
                           ORDER BY sku.sku_id) LOOP
          PIPE ROW(obj_trend_alloc_hist_dt_line(i_prd_sch.lcl_veh_desc_txt,
                                                i_prd_sch.catgry_nm,
                                                l_tbl_hist_head_aggr         (i)
                                                .offr_desc_txt,
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
                                                l_tbl_hist_head_aggr         (i)
                                                .bi24_unts_on,
                                                l_tbl_hist_head_aggr         (i)
                                                .bi24_sls_on,
                                                l_tbl_hist_head_aggr         (i)
                                                .bi24_unts_off,
                                                l_tbl_hist_head_aggr         (i)
                                                .bi24_sls_off,
                                                l_tbl_hist_head_aggr         (i)
                                                .estimate_unts_on,
                                                l_tbl_hist_head_aggr         (i)
                                                .estimate_sls_on,
                                                l_tbl_hist_head_aggr         (i)
                                                .estimate_unts_off,
                                                l_tbl_hist_head_aggr         (i)
                                                .estimate_sls_off,
                                                l_tbl_hist_head_aggr         (i)
                                                .trend_unts_on,
                                                l_tbl_hist_head_aggr         (i)
                                                .trend_sls_on,
                                                l_tbl_hist_head_aggr         (i)
                                                .trend_unts_off,
                                                l_tbl_hist_head_aggr         (i)
                                                .trend_sls_off,
                                                l_tbl_hist_head_aggr         (i)
                                                .actual_unts_on,
                                                l_tbl_hist_head_aggr         (i)
                                                .actual_sls_on,
                                                l_tbl_hist_head_aggr         (i)
                                                .actual_unts_off,
                                                l_tbl_hist_head_aggr         (i)
                                                .actual_sls_off));
        END LOOP;
      END LOOP;
    END IF;
    app_plsql_log.info(l_module_name || ' end' || l_parameter_list);
  END get_trend_alloc_hist_dtls;

  PROCEDURE insert_dms(p_table_reproc IN t_reproc,
                       p_type         CHAR,
                       p_user_id      VARCHAR2) IS
  BEGIN
    IF p_table_reproc.count > 0 THEN
      FOR i IN p_table_reproc.first .. p_table_reproc.last LOOP
        IF p_table_reproc(i).offr_sku_line_id IS NOT NULL THEN
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
              (p_table_reproc(i).mrkt_id,
               CASE
                 WHEN TRIM(p_table_reproc(i).on_off_flag) = 'ON' THEN
                  p_table_reproc(i).dms_on_sls_perd_id
                 ELSE
                  p_table_reproc(i).dms_off_sls_perd_id
               END,
               p_table_reproc(i).offr_sku_line_id,
               p_table_reproc(i).sls_typ_id,
               billing_sls_srce_id,
               CASE
                 WHEN TRIM(p_table_reproc(i).on_off_flag) = 'ON' THEN
                  p_table_reproc(i).dms_on_offr_perd_id
                 ELSE
                  p_table_reproc(i).dms_off_offr_perd_id
               END,
               final_sls_stus_cd,
               p_table_reproc(i).veh_id,
               CASE
                 WHEN TRIM(p_table_reproc(i).on_off_flag) = 'ON' THEN
                  p_table_reproc(i).frcstd_unts_on
                 ELSE
                  p_table_reproc(i).frcstd_unts_off
               END,
               p_table_reproc(i).comsn_amt,
               p_table_reproc(i).tax_amt,
               default_cost_amt,
               (SELECT CASE opp.nr_for_qty
                         WHEN 0 THEN
                          0
                         ELSE
                          decode(opp.sls_prc_amt / opp.nr_for_qty * CASE
                                   WHEN TRIM(p_table_reproc(i).on_off_flag) = 'ON' THEN
                                    p_table_reproc(i).frcstd_unts_on
                                   ELSE
                                    p_table_reproc(i).frcstd_unts_off
                                 END,
                                 0,
                                 0,
                                 1 - ((p_table_reproc(i).comsn_amt + p_table_reproc(i).tax_amt) /
                                 (opp.sls_prc_amt / opp.nr_for_qty * CASE
                                   WHEN TRIM(p_table_reproc(i).on_off_flag) = 'ON' THEN
                                    p_table_reproc(i).frcstd_unts_on
                                   ELSE
                                    p_table_reproc(i).frcstd_unts_off
                                 END)))
                       END
                  FROM offr_prfl_prc_point opp, offr_sku_line osl
                 WHERE osl.offr_sku_line_id = p_table_reproc(i)
                      .offr_sku_line_id
                   AND opp.offr_prfl_prcpt_id = osl.offr_prfl_prcpt_id),
               p_user_id,
               p_user_id);
          EXCEPTION
            WHEN OTHERS
            -- log
             THEN
              app_plsql_log.error('ERROR at writing trend allocations in internal function insert_dms : ' ||
                                  p_type || p_table_reproc(i).mrkt_id || ', ' || CASE WHEN
                                  TRIM(p_table_reproc(i).on_off_flag) = 'ON' THEN p_table_reproc(i)
                                  .dms_on_sls_perd_id ELSE p_table_reproc(i)
                                  .dms_off_sls_perd_id
                                  END || ', ' || p_table_reproc(i)
                                  .offr_sku_line_id || ', ' || p_table_reproc(i)
                                  .sls_typ_id || ', ' ||
                                  billing_sls_srce_id || ', ' || CASE WHEN
                                  TRIM(p_table_reproc(i).on_off_flag) = 'ON' THEN p_table_reproc(i)
                                  .dms_on_offr_perd_id ELSE p_table_reproc(i)
                                  .dms_off_offr_perd_id
                                  END || ', ' || final_sls_stus_cd || ', ' || p_table_reproc(i)
                                  .veh_id || ', ' || CASE WHEN
                                  TRIM(p_table_reproc(i).on_off_flag) = 'ON' THEN p_table_reproc(i)
                                  .frcstd_unts_on ELSE p_table_reproc(i)
                                  .frcstd_unts_off
                                  END || ', ' || p_table_reproc(i)
                                  .comsn_amt || ', ' || p_table_reproc(i)
                                  .tax_amt || ', ' || default_cost_amt);
          END;
        END IF;
      END LOOP;
    END IF;
  
  END insert_dms;

  PROCEDURE save_trend_alloctn(p_mrkt_id              IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                               p_sls_perd_id          IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                               p_trg_perd_id          IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                               p_sls_typ_id           IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                               p_bilng_day            IN dly_bilng_trnd.prcsng_dt%TYPE,
                               p_cash_value           IN mrkt_sls_perd.sct_cash_val%TYPE,
                               p_r_factor             IN mrkt_sls_perd.sct_r_factor%TYPE,
                               p_use_offers_on_sched  IN CHAR,
                               p_use_offers_off_sched IN CHAR,
                               p_run_id               IN NUMBER DEFAULT NULL,
                               p_user_id              IN VARCHAR2 DEFAULT NULL,
                               p_stus                 OUT NUMBER) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    -- local variables
    l_ins_cnt      NUMBER;
    l_cycl_cnt     NUMBER;
    l_table_reproc t_reproc;
    --
    l_mrkt_perd_exists        NUMBER;
    l_sct_cash_val            mrkt_sls_perd.sct_cash_val%TYPE;
    l_sct_r_factor            mrkt_sls_perd.sct_r_factor%TYPE;
    l_sct_onsch_est_bi24_ind  mrkt_sls_perd.sct_onsch_est_bi24_ind%TYPE;
    l_sct_offsch_est_bi24_ind mrkt_sls_perd.sct_offsch_est_bi24_ind%TYPE;
    l_sct_sls_typ_id          mrkt_sls_perd.sct_sls_typ_id%TYPE;
    l_sct_prcsng_dt           mrkt_sls_perd.sct_prcsng_dt%TYPE;
    l_est_bilng_dt            mrkt_sls_perd.est_bilng_dt%TYPE;
    l_bst_bilng_dt            mrkt_sls_perd.bst_bilng_dt%TYPE;
    l_max_prcsng_dt           mrkt_sls_perd.sct_prcsng_dt%TYPE;
    -- PERIODS
    l_periods r_periods;
    -- lookup: mrkt_config_item_val_txt
    l_sls_typ_id_from_config mrkt_config_item.mrkt_config_item_val_txt%TYPE;
    -- for LOG
    l_run_id         NUMBER := nvl(p_run_id,
                                   app_plsql_output.generate_new_run_id);
    l_user_id        VARCHAR(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'SAVE_TREND_ALLOCTN';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       to_char(p_mrkt_id) || ', ' ||
                                       'p_sls_perd_id: ' ||
                                       to_char(p_sls_perd_id) || ', ' ||
                                       'p_trg_perd_id: ' ||
                                       to_char(p_trg_perd_id) || ', ' ||
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
                                       'p_run_id: ' || to_char(l_run_id) || ', ' ||
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
    --
    SELECT COUNT(*)
      INTO l_mrkt_perd_exists
      FROM mrkt_sls_perd
     WHERE mrkt_id = p_mrkt_id
       AND sls_perd_id = p_trg_perd_id;
    IF l_mrkt_perd_exists = 1 THEN
      BEGIN
        -- protecting delete and insert process
        p_stus := 2;
        -- set values last used for calculation into mrkt_sls_perd table, if changed
        SELECT sct_cash_val,
               sct_r_factor,
               sct_prcsng_dt,
               est_bilng_dt,
               bst_bilng_dt,
               sct_onsch_est_bi24_ind,
               sct_offsch_est_bi24_ind,
               sct_sls_typ_id
          INTO l_sct_cash_val,
               l_sct_r_factor,
               l_sct_prcsng_dt,
               l_est_bilng_dt,
               l_bst_bilng_dt,
               l_sct_onsch_est_bi24_ind,
               l_sct_offsch_est_bi24_ind,
               l_sct_sls_typ_id
          FROM mrkt_sls_perd
         WHERE mrkt_id = p_mrkt_id
           AND sls_perd_id = p_trg_perd_id;
        IF NOT
            (l_sct_cash_val = p_cash_value AND l_sct_r_factor = p_r_factor AND
            l_sct_onsch_est_bi24_ind = p_use_offers_on_sched AND
            l_sct_offsch_est_bi24_ind = p_use_offers_off_sched AND
            l_sct_sls_typ_id = p_sls_typ_id) THEN
          UPDATE mrkt_sls_perd
             SET sct_cash_val            = p_cash_value,
                 sct_r_factor            = p_r_factor,
                 sct_onsch_est_bi24_ind  = p_use_offers_on_sched,
                 sct_offsch_est_bi24_ind = p_use_offers_off_sched,
                 sct_sls_typ_id          = p_sls_typ_id,
                 last_updt_user_id       = l_user_id
           WHERE mrkt_id = p_mrkt_id
             AND sls_perd_id = p_trg_perd_id;
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
             p_trg_perd_id,
             p_cash_value,
             p_r_factor,
             p_use_offers_on_sched,
             p_use_offers_off_sched,
             p_sls_typ_id,
             l_user_id);
        END IF;
        -- set actual max prcsng date from dly_bilng into appropriate columns
        SELECT MAX(prcsng_dt)
          INTO l_max_prcsng_dt
          FROM dly_bilng_trnd
         WHERE mrkt_id = p_mrkt_id
           AND trnd_sls_perd_id = p_sls_perd_id
           AND prcsng_dt <= p_bilng_day;
        IF l_sct_cash_val <> p_cash_value THEN
          l_sct_prcsng_dt := l_max_prcsng_dt;
        END IF;
        IF p_sls_typ_id IN (marketing_est_id, supply_est_id) THEN
          l_est_bilng_dt := trunc(l_max_prcsng_dt);
        END IF;
        IF p_sls_typ_id IN (marketing_bst_id, supply_bst_id) THEN
          l_bst_bilng_dt := trunc(l_max_prcsng_dt);
        END IF;
        UPDATE mrkt_sls_perd
           SET sct_prcsng_dt     = l_sct_prcsng_dt,
               est_bilng_dt      = l_est_bilng_dt,
               bst_bilng_dt      = l_bst_bilng_dt,
               last_updt_user_id = l_user_id
         WHERE mrkt_id = p_mrkt_id
           AND sls_perd_id = p_trg_perd_id;
        -- get sls_typ_id_from_config
        l_sls_typ_id_from_config := get_sls_typ_id_from_config(p_mrkt_id,
                                                               l_run_id,
                                                               l_user_id);
        -- get CURRENT periods
        l_periods := get_periods(p_mrkt_id       => p_mrkt_id,
                                 p_orig_perd_id  => p_sls_perd_id,
                                 p_bilng_perd_id => p_sls_perd_id,
                                 p_sls_typ_id    => p_sls_typ_id,
                                 p_bilng_day     => p_bilng_day,
                                 p_run_id        => l_run_id,
                                 p_user_id       => l_user_id);
        -- drop all previously existing records from DMS
        DELETE FROM dstrbtd_mrkt_sls
         WHERE mrkt_id = p_mrkt_id
           AND ((sls_perd_id = l_periods.dms_on_sls_perd_id AND
               offr_perd_id = l_periods.dms_on_offr_perd_id) OR
               (sls_perd_id = l_periods.dms_off_sls_perd_id AND
               offr_perd_id = l_periods.dms_off_offr_perd_id))
           AND sls_typ_id = p_sls_typ_id;
        app_plsql_log.info(l_module_name || ' ' || SQL%ROWCOUNT ||
                           ' records DELETED from dstrbtd_mrkt_sls' ||
                           l_parameter_list);
        -- FORECASTED
        -- TRND
        l_table_reproc := get_reproc_trnd(p_mrkt_id                => p_mrkt_id,
                                          p_sls_perd_id            => p_sls_perd_id,
                                          p_trg_perd_id            => p_trg_perd_id,
                                          p_sls_typ_id             => p_sls_typ_id,
                                          p_bilng_day              => p_bilng_day,
                                          p_sls_typ_id_from_config => l_sls_typ_id_from_config,
                                          p_dbt_on_sls_perd_id     => l_periods.dbt_on_sls_perd_id,
                                          p_dbt_on_offr_perd_id    => l_periods.dbt_on_offr_perd_id,
                                          p_dbt_off_sls_perd_id    => l_periods.dbt_off_sls_perd_id,
                                          p_dbt_off_offr_perd_id   => l_periods.dbt_off_offr_perd_id,
                                          p_dms_on_sls_perd_id     => l_periods.dms_on_sls_perd_id,
                                          p_dms_on_offr_perd_id    => l_periods.dms_on_offr_perd_id,
                                          p_dms_off_sls_perd_id    => l_periods.dms_off_sls_perd_id,
                                          p_dms_off_offr_perd_id   => l_periods.dms_off_offr_perd_id,
                                          p_cash_value             => p_cash_value,
                                          p_r_factor               => p_r_factor,
                                          p_run_id                 => l_run_id,
                                          p_user_id                => l_user_id);
        --insert_dms(l_table_reproc, 'T', l_user_id);
        IF l_table_reproc.count > 0 THEN
          l_cycl_cnt := 0;
          l_ins_cnt  := 0;
          FOR i IN l_table_reproc.first .. l_table_reproc.last LOOP
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
                (l_table_reproc(i).mrkt_id,
                 CASE WHEN TRIM(l_table_reproc(i).on_off_flag) = 'ON' THEN l_table_reproc(i)
                 .dms_on_sls_perd_id ELSE l_table_reproc(i)
                 .dms_off_sls_perd_id END,
                 l_table_reproc(i).offr_sku_line_id,
                 l_table_reproc(i).sls_typ_id,
                 billing_sls_srce_id,
                 CASE WHEN TRIM(l_table_reproc(i).on_off_flag) = 'ON' THEN l_table_reproc(i)
                 .dms_on_offr_perd_id ELSE l_table_reproc(i)
                 .dms_off_offr_perd_id END,
                 final_sls_stus_cd,
                 l_table_reproc(i).veh_id,
                 CASE WHEN TRIM(l_table_reproc(i).on_off_flag) = 'ON' THEN l_table_reproc(i)
                 .frcstd_unts_on ELSE l_table_reproc(i).frcstd_unts_off END,
                 l_table_reproc(i).comsn_amt,
                 l_table_reproc(i).tax_amt,
                 default_cost_amt,
                 (SELECT CASE
                           WHEN nvl(opp.sls_prc_amt, 0) = 0 THEN
                            0
                           WHEN nvl(opp.nr_for_qty, 0) = 0 THEN
                            0
                           WHEN nvl(CASE
                                      WHEN TRIM(l_table_reproc(i).on_off_flag) = 'ON' THEN
                                       l_table_reproc(i).frcstd_unts_on
                                      ELSE
                                       l_table_reproc(i).frcstd_unts_off
                                    END,
                                    0) = 0 THEN
                            0
                           ELSE
                            ((opp.sls_prc_amt / opp.nr_for_qty * CASE
                              WHEN TRIM(l_table_reproc(i).on_off_flag) = 'ON' THEN
                               l_table_reproc(i).frcstd_unts_on
                              ELSE
                               l_table_reproc(i).frcstd_unts_off
                            END) - l_table_reproc(i).comsn_amt - l_table_reproc(i)
                            .tax_amt) / (opp.sls_prc_amt / opp.nr_for_qty * CASE
                              WHEN TRIM(l_table_reproc(i).on_off_flag) = 'ON' THEN
                               l_table_reproc(i).frcstd_unts_on
                              ELSE
                               l_table_reproc(i).frcstd_unts_off
                            END)
                         END
                    FROM offr_prfl_prc_point opp, offr_sku_line osl
                   WHERE osl.offr_sku_line_id = l_table_reproc(i)
                        .offr_sku_line_id
                     AND opp.offr_prfl_prcpt_id = osl.offr_prfl_prcpt_id),
                 p_user_id,
                 p_user_id);
              -- count REAL insert
              l_ins_cnt := l_ins_cnt + 1;
              p_stus    := 0;
            EXCEPTION
              WHEN OTHERS
              -- log
               THEN
                app_plsql_log.error('ERROR at INSERT INTO dstrbtd_mrkt_sls (T) ' || l_table_reproc(i)
                                    .mrkt_id || ', ' || CASE WHEN
                                    TRIM(l_table_reproc(i).on_off_flag) = 'ON' THEN l_table_reproc(i)
                                    .dms_on_sls_perd_id ELSE l_table_reproc(i)
                                    .dms_off_sls_perd_id
                                    END || ', ' || l_table_reproc(i)
                                    .offr_sku_line_id || ', ' || l_table_reproc(i)
                                    .sls_typ_id || ', ' ||
                                    billing_sls_srce_id || ', ' || CASE WHEN
                                    TRIM(l_table_reproc(i).on_off_flag) = 'ON' THEN l_table_reproc(i)
                                    .dms_on_offr_perd_id ELSE l_table_reproc(i)
                                    .dms_off_offr_perd_id
                                    END || ', ' || final_sls_stus_cd || ', ' || l_table_reproc(i)
                                    .veh_id || ', ' || CASE WHEN
                                    TRIM(l_table_reproc(i).on_off_flag) = 'ON' THEN l_table_reproc(i)
                                    .frcstd_unts_on ELSE l_table_reproc(i)
                                    .frcstd_unts_off
                                    END || ', ' || l_table_reproc(i)
                                    .comsn_amt || ', ' || l_table_reproc(i)
                                    .tax_amt || ', ' || default_cost_amt);
                RAISE;
            END;
          END LOOP;
          app_plsql_log.info(l_module_name || ' trnd (cycle): ' ||
                             l_cycl_cnt || ' trnd (insert): ' || l_ins_cnt ||
                             l_parameter_list);
        END IF;
        -- EST and OVRRD
        l_table_reproc.delete;
        l_table_reproc := get_reproc_est(p_mrkt_id              => p_mrkt_id,
                                         p_sls_perd_id          => p_sls_perd_id,
                                         p_trg_perd_id          => p_trg_perd_id,
                                         p_sls_typ_id           => p_sls_typ_id,
                                         p_bilng_day            => p_bilng_day,
                                         p_dbt_on_sls_perd_id   => l_periods.dbt_on_sls_perd_id,
                                         p_dbt_on_offr_perd_id  => l_periods.dbt_on_offr_perd_id,
                                         p_dbt_off_sls_perd_id  => l_periods.dbt_off_sls_perd_id,
                                         p_dbt_off_offr_perd_id => l_periods.dbt_off_offr_perd_id,
                                         p_dms_on_sls_perd_id   => l_periods.dms_on_sls_perd_id,
                                         p_dms_on_offr_perd_id  => l_periods.dms_on_offr_perd_id,
                                         p_dms_off_sls_perd_id  => l_periods.dms_off_sls_perd_id,
                                         p_dms_off_offr_perd_id => l_periods.dms_off_offr_perd_id,
                                         p_cash_value           => p_cash_value,
                                         p_r_factor             => p_r_factor,
                                         p_use_offers_on_sched  => p_use_offers_on_sched,
                                         p_use_offers_off_sched => p_use_offers_off_sched,
                                         p_run_id               => l_run_id,
                                         p_user_id              => l_user_id);
        --insert_dms(l_table_reproc, 'E', l_user_id);
        IF l_table_reproc.count > 0 THEN
          l_cycl_cnt := 0;
          l_ins_cnt  := 0;
          FOR i IN l_table_reproc.first .. l_table_reproc.last LOOP
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
                (l_table_reproc(i).mrkt_id,
                 CASE WHEN TRIM(l_table_reproc(i).on_off_flag) = 'ON' THEN l_table_reproc(i)
                 .dms_on_sls_perd_id ELSE l_table_reproc(i)
                 .dms_off_sls_perd_id END,
                 l_table_reproc(i).offr_sku_line_id,
                 l_table_reproc(i).sls_typ_id,
                 billing_sls_srce_id,
                 CASE WHEN TRIM(l_table_reproc(i).on_off_flag) = 'ON' THEN l_table_reproc(i)
                 .dms_on_offr_perd_id ELSE l_table_reproc(i)
                 .dms_off_offr_perd_id END,
                 final_sls_stus_cd,
                 l_table_reproc(i).veh_id,
                 CASE WHEN TRIM(l_table_reproc(i).on_off_flag) = 'ON' THEN l_table_reproc(i)
                 .frcstd_unts_on ELSE l_table_reproc(i).frcstd_unts_off END,
                 l_table_reproc(i).comsn_amt,
                 l_table_reproc(i).tax_amt,
                 default_cost_amt,
                 (SELECT CASE
                           WHEN nvl(opp.sls_prc_amt, 0) +
                                nvl(opp.nr_for_qty, 0) +
                                nvl(CASE
                                      WHEN TRIM(l_table_reproc(i).on_off_flag) = 'ON' THEN
                                       l_table_reproc(i).frcstd_unts_on
                                      ELSE
                                       l_table_reproc(i).frcstd_unts_off
                                    END,
                                    0) = 0 THEN
                            0
                           ELSE
                            ((opp.sls_prc_amt / opp.nr_for_qty * CASE
                              WHEN TRIM(l_table_reproc(i).on_off_flag) = 'ON' THEN
                               l_table_reproc(i).frcstd_unts_on
                              ELSE
                               l_table_reproc(i).frcstd_unts_off
                            END) - l_table_reproc(i).comsn_amt - l_table_reproc(i)
                            .tax_amt) / (opp.sls_prc_amt / opp.nr_for_qty * CASE
                              WHEN TRIM(l_table_reproc(i).on_off_flag) = 'ON' THEN
                               l_table_reproc(i).frcstd_unts_on
                              ELSE
                               l_table_reproc(i).frcstd_unts_off
                            END)
                         END
                    FROM offr_prfl_prc_point opp, offr_sku_line osl
                   WHERE osl.offr_sku_line_id = l_table_reproc(i)
                        .offr_sku_line_id
                     AND opp.offr_prfl_prcpt_id = osl.offr_prfl_prcpt_id),
                 p_user_id,
                 p_user_id);
              -- count REAL insert
              l_ins_cnt := l_ins_cnt + 1;
              p_stus    := 0;
            EXCEPTION
              WHEN OTHERS
              -- log
               THEN
                app_plsql_log.error('ERROR at INSERT INTO dstrbtd_mrkt_sls (E) ' || l_table_reproc(i)
                                    .mrkt_id || ', ' || CASE WHEN
                                    TRIM(l_table_reproc(i).on_off_flag) = 'ON' THEN l_table_reproc(i)
                                    .dms_on_sls_perd_id ELSE l_table_reproc(i)
                                    .dms_off_sls_perd_id
                                    END || ', ' || l_table_reproc(i)
                                    .offr_sku_line_id || ', ' || l_table_reproc(i)
                                    .sls_typ_id || ', ' ||
                                    billing_sls_srce_id || ', ' || CASE WHEN
                                    TRIM(l_table_reproc(i).on_off_flag) = 'ON' THEN l_table_reproc(i)
                                    .dms_on_offr_perd_id ELSE l_table_reproc(i)
                                    .dms_off_offr_perd_id
                                    END || ', ' || final_sls_stus_cd || ', ' || l_table_reproc(i)
                                    .veh_id || ', ' || CASE WHEN
                                    TRIM(l_table_reproc(i).on_off_flag) = 'ON' THEN l_table_reproc(i)
                                    .frcstd_unts_on ELSE l_table_reproc(i)
                                    .frcstd_unts_off
                                    END || ', ' || l_table_reproc(i)
                                    .comsn_amt || ', ' || l_table_reproc(i)
                                    .tax_amt || ', ' || default_cost_amt);
                RAISE;
            END;
          END LOOP;
          app_plsql_log.info(l_module_name || ' est (cycle): ' ||
                             l_cycl_cnt || ' est (insert): ' || l_ins_cnt ||
                             l_parameter_list);
        END IF;
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK;
          p_stus := 2;
          app_plsql_log.info(l_module_name || ' FAILED, error code: ' ||
                             SQLCODE || ' error message: ' || SQLERRM ||
                             l_parameter_list);
      END;
      IF p_stus = 0 THEN
        COMMIT;
        app_plsql_log.info(l_module_name || ' COMMIT, status_code: ' ||
                           to_char(p_stus) || l_parameter_list);
      ELSE
        ROLLBACK;
        app_plsql_log.info(l_module_name || ' ROLLBACK, status_code: ' ||
                           to_char(p_stus) || l_parameter_list);
      END IF;
    ELSE
      ROLLBACK;
      app_plsql_log.info(l_module_name ||
                         ' there is NOTHING to PROCESS in MRKT_SLS_PERD for mrkt_id=' ||
                         to_char(p_mrkt_id) || ' and sls_perd_id=' ||
                         to_char(p_trg_perd_id) || l_parameter_list);
    END IF;
    --
    app_plsql_log.info(l_module_name || ' end, status_code: ' ||
                       to_char(p_stus) || l_parameter_list);
    --
  END save_trend_alloctn;

  FUNCTION get_saved_trend_head(p_mrkt_id     IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                                p_sls_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                p_trg_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                p_sls_typ_id  IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                                p_bilng_day   IN dly_bilng_trnd.prcsng_dt%TYPE,
                                p_run_id      IN NUMBER DEFAULT NULL,
                                p_user_id     IN VARCHAR2 DEFAULT NULL)
    RETURN obj_trend_alloc_hist_hd_table
    PIPELINED AS
    -- local variables
    l_periods            r_periods;
    l_tbl_hist_head_aggr t_hist_head_aggr;
    -- added
    l_last_dms dstrbtd_mrkt_sls.last_updt_ts%TYPE;
    -- added end
    -- for HEAD aggr
    l_bi24_unts_on      NUMBER;
    l_bi24_sls_on       NUMBER;
    l_bi24_unts_off     NUMBER;
    l_bi24_sls_off      NUMBER;
    l_estimate_unts_on  NUMBER;
    l_estimate_sls_on   NUMBER;
    l_estimate_unts_off NUMBER;
    l_estimate_sls_off  NUMBER;
    l_trend_unts_on     NUMBER;
    l_trend_sls_on      NUMBER;
    l_trend_unts_off    NUMBER;
    l_trend_sls_off     NUMBER;
    l_actual_unts_on    NUMBER;
    l_actual_sls_on     NUMBER;
    l_actual_unts_off   NUMBER;
    l_actual_sls_off    NUMBER;
    -- once
    l_sls_typ_id_from_config dly_bilng_trnd_offr_sku_line.sls_typ_id%TYPE;
    -- for LOG
    l_run_id         NUMBER := nvl(p_run_id,
                                   app_plsql_output.generate_new_run_id);
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'GET_SAVED_TREND_HEAD';
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
                                       'p_run_id: ' || to_char(l_run_id) || ', ' ||
                                       'p_user_id: ' || l_user_id || ')';
    --
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start' || l_parameter_list);
    -- added
    BEGIN
      SELECT last_updt_ts
        INTO l_last_dms
        FROM dstrbtd_mrkt_sls
       WHERE mrkt_id = p_mrkt_id
         AND sls_perd_id = p_trg_perd_id
         AND sls_typ_id = p_sls_typ_id
         AND rownum <= 1;
    EXCEPTION
      WHEN no_data_found THEN
        l_last_dms := NULL;
      WHEN OTHERS THEN
        app_plsql_log.info(l_module_name ||
                           ' warning: Other exception when selecting from: dstrbtd_mrkt_sls where mrkt_id=' ||
                           to_char(p_mrkt_id) || ' and sls_perd_id=' ||
                           to_char(p_trg_perd_id) || ' and sls_typ_id=' ||
                           to_char(p_sls_typ_id) || 'error code: ' ||
                           SQLCODE || ' error message: ' || SQLERRM ||
                           l_parameter_list);
        l_last_dms := NULL;
    END;
    CASE
      WHEN l_last_dms IS NOT NULL THEN
        --Added end
        -- sls_typ_id_from_config
        l_sls_typ_id_from_config := get_sls_typ_id_from_config(p_mrkt_id,
                                                               l_run_id,
                                                               l_user_id);
        -- iterations
        -- get CURRENT periods
        l_periods := get_periods(p_mrkt_id       => p_mrkt_id,
                                 p_orig_perd_id  => p_sls_perd_id,
                                 p_bilng_perd_id => p_sls_perd_id,
                                 p_sls_typ_id    => p_sls_typ_id,
                                 p_bilng_day     => p_bilng_day,
                                 p_run_id        => l_run_id,
                                 p_user_id       => l_user_id);
        -- get_head_details
        l_tbl_hist_head_aggr := get_head_details(p_mrkt_id,
                                                 p_sls_perd_id,
                                                 p_trg_perd_id,
                                                 p_sls_typ_id,
                                                 l_periods.dbt_bilng_day,
                                                 l_sls_typ_id_from_config,
                                                 l_periods.dbt_on_sls_perd_id,
                                                 l_periods.dbt_on_offr_perd_id,
                                                 l_periods.dbt_off_sls_perd_id,
                                                 l_periods.dbt_off_offr_perd_id,
                                                 l_periods.dms_on_sls_perd_id,
                                                 l_periods.dms_on_offr_perd_id,
                                                 l_periods.dms_off_sls_perd_id,
                                                 l_periods.dms_off_offr_perd_id,
                                                 l_run_id,
                                                 l_user_id);
        --
        l_bi24_unts_on      := 0;
        l_bi24_sls_on       := 0;
        l_bi24_unts_off     := 0;
        l_bi24_sls_off      := 0;
        l_trend_unts_on     := 0;
        l_trend_sls_on      := 0;
        l_trend_unts_off    := 0;
        l_trend_sls_off     := 0;
        l_estimate_unts_on  := 0;
        l_estimate_sls_on   := 0;
        l_estimate_unts_off := 0;
        l_estimate_sls_off  := 0;
        l_actual_unts_on    := 0;
        l_actual_sls_on     := 0;
        l_actual_unts_off   := 0;
        l_actual_sls_off    := 0;
        IF l_tbl_hist_head_aggr.count > 0 THEN
          FOR i IN l_tbl_hist_head_aggr.first .. l_tbl_hist_head_aggr.last LOOP
            l_bi24_unts_on      := l_bi24_unts_on + l_tbl_hist_head_aggr(i)
                                  .bi24_unts_on;
            l_bi24_sls_on       := l_bi24_sls_on + l_tbl_hist_head_aggr(i)
                                  .bi24_sls_on;
            l_bi24_unts_off     := l_bi24_unts_off + l_tbl_hist_head_aggr(i)
                                  .bi24_unts_off;
            l_bi24_sls_off      := l_bi24_sls_off + l_tbl_hist_head_aggr(i)
                                  .bi24_sls_off;
            l_trend_unts_on     := l_trend_unts_on + l_tbl_hist_head_aggr(i)
                                  .trend_unts_on;
            l_trend_sls_on      := l_trend_sls_on + l_tbl_hist_head_aggr(i)
                                  .trend_sls_on;
            l_trend_unts_off    := l_trend_unts_off + l_tbl_hist_head_aggr(i)
                                  .trend_unts_off;
            l_trend_sls_off     := l_trend_sls_off + l_tbl_hist_head_aggr(i)
                                  .trend_sls_off;
            l_estimate_unts_on  := l_estimate_unts_on + l_tbl_hist_head_aggr(i)
                                  .estimate_unts_on;
            l_estimate_sls_on   := l_estimate_sls_on + l_tbl_hist_head_aggr(i)
                                  .estimate_sls_on;
            l_estimate_unts_off := l_estimate_unts_off + l_tbl_hist_head_aggr(i)
                                  .estimate_unts_off;
            l_estimate_sls_off  := l_estimate_sls_off + l_tbl_hist_head_aggr(i)
                                  .estimate_sls_off;
            l_actual_unts_on    := l_actual_unts_on + l_tbl_hist_head_aggr(i)
                                  .actual_unts_on;
            l_actual_sls_on     := l_actual_sls_on + l_tbl_hist_head_aggr(i)
                                  .actual_sls_on;
            l_actual_unts_off   := l_actual_unts_off + l_tbl_hist_head_aggr(i)
                                  .actual_unts_off;
            l_actual_sls_off    := l_actual_sls_off + l_tbl_hist_head_aggr(i)
                                  .actual_sls_off;
          END LOOP;
        END IF;
        PIPE ROW(obj_trend_alloc_hist_head_line(l_periods.dbt_on_sls_perd_id /*sls_perd_id*/,
                                                l_periods.dms_on_sls_perd_id /*trg_perd_id*/,
                                                l_periods.dbt_bilng_day /*bilng_day*/,
                                                l_bi24_unts_on,
                                                l_bi24_sls_on,
                                                l_bi24_unts_off,
                                                l_bi24_sls_off,
                                                l_periods.sct_cash_val /*cash_vlu*/,
                                                l_periods.sct_r_factor /*r_factor*/,
                                                l_estimate_unts_on,
                                                l_estimate_sls_on,
                                                l_estimate_unts_off,
                                                l_estimate_sls_off,
                                                l_trend_unts_on,
                                                l_trend_sls_on,
                                                l_trend_unts_off,
                                                l_trend_sls_off,
                                                l_actual_unts_on,
                                                l_actual_sls_on,
                                                l_actual_unts_off,
                                                l_actual_sls_off));
        -- added
      ELSE
        PIPE ROW(obj_trend_alloc_hist_head_line(NULL /*sls_perd_id*/,
                                                NULL /*trg_perd_id*/,
                                                NULL /*bilng_day*/,
                                                NULL,
                                                NULL,
                                                NULL,
                                                NULL,
                                                NULL /*cash_vlu*/,
                                                NULL /*r_factor*/,
                                                NULL,
                                                NULL,
                                                NULL,
                                                NULL,
                                                NULL,
                                                NULL,
                                                NULL,
                                                NULL,
                                                NULL,
                                                NULL,
                                                NULL,
                                                NULL));
      
    END CASE;
    -- added end
    app_plsql_log.info(l_module_name || ' end' || l_parameter_list);
  END get_saved_trend_head;

  ---------------------------------------------
  -- inherited from WEDEV.TRND_ALOCTN package -
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
    USING (SELECT p_mrkt_id          AS p_mrkt_id,
                  p_trnd_sls_perd_id AS p_trnd_sls_perd_id
             FROM dual) m
    ON (mtsp.mrkt_id = m.p_mrkt_id AND mtsp.trnd_sls_perd_id = m.p_trnd_sls_perd_id)
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

  PROCEDURE crct_gta(p_mrkt_id               IN mrkt.mrkt_id%TYPE,
                     p_sls_perd_id           IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                     p_sls_aloc_auto_stus_id IN CHAR,
                     p_sls_aloc_manl_stus_id IN CHAR,
                     p_forc_mtch_ind         IN CHAR,
                     p_run_id                IN NUMBER DEFAULT NULL,
                     p_user_id               IN VARCHAR2 DEFAULT NULL) IS
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
                        'p_run_id: ' || to_char(l_run_id) || ', ' ||
                        'p_user_id: ' || l_user_id || ')';
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

  PROCEDURE process_jobs(p_run_id  IN NUMBER DEFAULT NULL,
                         p_user_id IN VARCHAR2 DEFAULT NULL) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    -- local variables
    l_stus                   NUMBER;
    l_sls_typ_id_from_config dly_bilng_trnd_offr_sku_line.sls_typ_id%TYPE;
    l_periods                r_periods;
    l_bi24_r_factor          r_bi24_r_factor;
    -- for LOG
    l_run_id         NUMBER := nvl(p_run_id,
                                   app_plsql_output.generate_new_run_id);
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'PROCESS_JOBS';
    l_parameter_list VARCHAR2(2048);
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    ---------------------------------------
    -- sales camapaign / target campaign --
    ---------------------------------------
    FOR rec IN (SELECT mrkt_sls_perd.mrkt_id,
                       mrkt_sls_perd.sct_sls_typ_id,
                       pa_manl_trend_adjstmnt.get_sales_campaign(p_mrkt_id      => mrkt_sls_perd.mrkt_id,
                                                                 p_trgt_perd_id => mrkt_sls_perd.sls_perd_id,
                                                                 p_sls_typ_id   => mrkt_sls_perd.sct_sls_typ_id) sls_perd_id,
                       mrkt_sls_perd.sls_perd_id trg_perd_id,
                       mrkt_sls_perd.sct_prcsng_dt bilng_day,
                       nvl(mrkt_sls_perd.sct_cash_val, 0) sct_cash_val,
                       nvl(mrkt_sls_perd.sct_r_factor, 0) sct_r_factor,
                       nvl(mrkt_sls_perd.sct_onsch_est_bi24_ind, 0) sct_onsch_est_bi24_ind,
                       nvl(mrkt_sls_perd.sct_offsch_est_bi24_ind, 0) sct_offsch_est_bi24_ind
                  FROM mrkt_config_item, mrkt_sls_perd
                 WHERE 1 = 1
                   AND (mrkt_sls_perd.sct_autclc_bst_ind = 'Y' OR
                       mrkt_sls_perd.sct_autclc_est_ind = 'Y')
                   AND mrkt_sls_perd.sct_sls_typ_id IS NOT NULL
                   AND mrkt_sls_perd.mrkt_id = mrkt_config_item.mrkt_id
                   AND mrkt_config_item.config_item_id = 12005
                   AND mrkt_config_item.mrkt_config_item_val_txt = 'Y'
                 ORDER BY mrkt_sls_perd.mrkt_id, mrkt_sls_perd.sls_perd_id) LOOP
      l_parameter_list := ' (rec: mrkt_id:' || to_char(rec.mrkt_id) || ', ' ||
                          'sls_perd_id: ' || to_char(rec.sls_perd_id) || ', ' ||
                          'trg_perd_id: ' || to_char(rec.trg_perd_id) || ', ' ||
                          'sct_sls_typ_id: ' || to_char(rec.sct_sls_typ_id) || ', ' ||
                          'bilng_day: ' || to_char(rec.bilng_day) || ', ' ||
                          'p_run_id: ' || to_char(l_run_id) || ', ' ||
                          'p_user_id: ' || l_user_id || ')';
      app_plsql_log.info(l_module_name || ' - TREND SALES CALCULATE start' ||
                         l_parameter_list);
      UPDATE mrkt_sls_perd
         SET sct_aloctn_user_id = l_user_id,
             sct_aloctn_strt_ts = SYSDATE,
             sct_aloctn_end_ts  = NULL
       WHERE mrkt_id = rec.mrkt_id
         AND sls_perd_id = rec.sls_perd_id
         AND sct_sls_typ_id = rec.sct_sls_typ_id;
      -- get sls_typ_id_from_config
      l_sls_typ_id_from_config := get_sls_typ_id_from_config(rec.mrkt_id,
                                                             l_run_id,
                                                             l_user_id);
      -- get CURRENT periods
      l_periods := get_periods(p_mrkt_id       => rec.mrkt_id,
                               p_orig_perd_id  => rec.sls_perd_id,
                               p_bilng_perd_id => rec.sls_perd_id,
                               p_sls_typ_id    => rec.sct_sls_typ_id,
                               p_bilng_day     => rec.bilng_day,
                               p_run_id        => l_run_id,
                               p_user_id       => l_user_id);
      -- cash value
      IF rec.sct_cash_val IS NOT NULL THEN
        l_periods.sct_cash_val := rec.sct_cash_val;
      END IF;
      -- get BI24, r_factor
      l_bi24_r_factor := get_bi24_r_factor(p_mrkt_id                => rec.mrkt_id,
                                           p_dbt_bilng_day          => rec.bilng_day,
                                           p_sls_typ_id_from_config => l_sls_typ_id_from_config,
                                           p_dbt_on_sls_perd_id     => l_periods.dbt_on_sls_perd_id,
                                           p_dbt_on_offr_perd_id    => l_periods.dbt_on_offr_perd_id,
                                           p_dbt_off_sls_perd_id    => l_periods.dbt_off_sls_perd_id,
                                           p_dbt_off_offr_perd_id   => l_periods.dbt_off_offr_perd_id,
                                           p_dms_on_sls_perd_id     => l_periods.dms_on_sls_perd_id,
                                           p_dms_on_offr_perd_id    => l_periods.dms_on_offr_perd_id,
                                           p_dms_off_sls_perd_id    => l_periods.dms_off_sls_perd_id,
                                           p_dms_off_offr_perd_id   => l_periods.dms_off_offr_perd_id,
                                           p_sct_cash_val           => l_periods.sct_cash_val,
                                           p_r_factor               => rec.sct_r_factor,
                                           p_sls_typ_id             => rec.sct_sls_typ_id,
                                           p_run_id                 => l_run_id,
                                           p_user_id                => l_user_id);
    
      -- SAVE
      l_stus := 0;
      save_trend_alloctn(p_mrkt_id              => rec.mrkt_id,
                         p_sls_perd_id          => rec.sls_perd_id,
                         p_trg_perd_id          => rec.trg_perd_id,
                         p_sls_typ_id           => rec.sct_sls_typ_id,
                         p_bilng_day            => rec.bilng_day,
                         p_cash_value           => l_periods.sct_cash_val,
                         p_r_factor             => l_bi24_r_factor.r_factor,
                         p_use_offers_on_sched  => rec.sct_onsch_est_bi24_ind,
                         p_use_offers_off_sched => rec.sct_offsch_est_bi24_ind,
                         p_run_id               => l_run_id,
                         p_user_id              => l_user_id,
                         p_stus                 => l_stus);
      IF l_stus = 0 THEN
        UPDATE mrkt_sls_perd
           SET sct_aloctn_end_ts = SYSDATE
         WHERE mrkt_id = rec.mrkt_id
           AND sls_perd_id = rec.sls_perd_id
           AND sct_sls_typ_id = rec.sct_sls_typ_id;
        --
        COMMIT;
      ELSE
        ROLLBACK;
      END IF;
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
                           p_run_id      IN NUMBER DEFAULT NULL,
                           p_user_id     IN VARCHAR2 DEFAULT NULL) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    -- for LOG
    l_run_id         NUMBER := nvl(p_run_id,
                                   app_plsql_output.generate_new_run_id);
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'CLC_CATGRY_SLS';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id:' || to_char(p_mrkt_id) || ', ' ||
                                       'p_sls_perd_id: ' ||
                                       to_char(p_sls_perd_id) || ', ' ||
                                       'p_run_id: ' || to_char(l_run_id) || ', ' ||
                                       'p_user_id: ' || l_user_id || ')';
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
        app_plsql_log.info(l_module_name || ' FAILED, error code: ' ||
                           SQLCODE || ' error message: ' || SQLERRM ||
                           l_parameter_list);
    END;
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
                       p_run_id      IN NUMBER DEFAULT NULL,
                       p_user_id     IN VARCHAR2 DEFAULT NULL) IS
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
                                       'p_run_id: ' || to_char(l_run_id) || ', ' ||
                                       'p_user_id: ' || l_user_id || ')';
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
    app_plsql_log.info(l_module_name || ' find price-change OSLs: ' ||
                       reset_records_temp.count || l_parameter_list);
    reset_records := reset_records MULTISET UNION DISTINCT
                     reset_records_temp;
    IF control_parameters_ok THEN
      app_plsql_log.info(l_module_name || ' running auto-match for ' ||
                         p_mrkt_id || '/' || p_sls_perd_id ||
                         l_parameter_list);
      -- record start of Sales Allocation in mrkt_trnd_perd table
      -- identify MANUAL_NOT_SUGGESTED and MANUAL_EXCLUDED records if re-
      -- processing requested
      SELECT DISTINCT dly_bilng_id
        BULK COLLECT
        INTO reset_records_temp
        FROM dly_bilng_trnd
       WHERE mrkt_id = p_mrkt_id
         AND trnd_sls_perd_id = p_sls_perd_id
         AND ((trnd_aloctn_manul_stus_id = manual_not_suggested) OR
             (trnd_aloctn_manul_stus_id = manual_excluded));
      app_plsql_log.info(l_module_name ||
                         ' reset manual exluded/not_suggested: ' ||
                         reset_records_temp.count || l_parameter_list);
      reset_records := reset_records MULTISET UNION DISTINCT
                       reset_records_temp;
      -- reset all identified daily billing records for re-processing
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
        app_plsql_log.info(l_module_name || ' duplicate line numbers: ' ||
                           line_nr_duplicates.count || l_parameter_list);
      END IF;
      DELETE dly_bilng_trnd_temp;
      DELETE dly_bilng_trnd_osl_temp;
      -- AUTOMATCH PROCESS STARTS
      -- auto-match process starts here
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
            IF sales_channel_match = 'Y' OR sales_channel_used = 'N' THEN
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
          IF db_rec.finshd_gds_cd IS NOT NULL AND match_method IS NULL THEN
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
              IF sales_channel_match = 'Y' OR sales_channel_used = 'N' THEN
                IF osl_rec.plnd_veh_ind = 'Y' THEN
                  planned_matches := planned_matches + 1;
                END IF;
                --match_type := NO_MATCH;
                match_type := direct;
                IF (osl_rec.qty_diff = 0 AND
                   osl_rec.prc_diff <= l_unit_prc_auto_mtch_tolr_amt) OR
                   (l_unit_prc_mtch_ind = 'Y' AND
                   osl_rec.unit_prc_diff <= l_unit_prc_auto_mtch_tolr_amt) THEN
                  match_type := direct;
                ELSIF (osl_rec.qty_diff = 0 AND
                      osl_rec.prc_diff <= l_unit_prc_manul_mtch_tolr_amt) OR
                      (l_unit_prc_mtch_ind = 'Y' AND
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
                 osl_rec.dms_unit_qty, --db_rec.unit_qty,
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
                ELSIF num_direct_lnm_matches = 0 AND
                      num_suggested_lnm_matches > 0 THEN
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
        app_plsql_log.info(l_module_name || ' Processing daily billing: ' ||
                           db_rec.dly_bilng_id || ' end' ||
                           l_parameter_list);
      END LOOP;
      -- end of auto_match process
      app_plsql_log.info(l_module_name || ' auto-match end' ||
                         l_parameter_list);
      -- AUTOMATCH PROCESS ENDS
      -- record derived status/sku values back on to dly_bilng_trnd table and
      -- validate sales class
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
      -- create dly_bilng_trnd_OFFR_SKU_LINE records for all identified matches
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
      app_plsql_log.info(l_module_name || ' apply DMS changes' ||
                         l_parameter_list);
      -- free up remaining collections
      line_nr_duplicates.delete;
      -- dms_changes.DELETE;
      procs_multmtch(p_mrkt_id, p_sls_perd_id, demand_actuals);
      procs_multmtch(p_mrkt_id, p_sls_perd_id, billed_actuals);
      app_plsql_log.info(l_module_name || ' CONTRL_TOTL_SCRN: correct GTA' ||
                         l_parameter_list);
      crct_gta(p_mrkt_id,
               p_sls_perd_id,
               auto_matched,
               manual_not_processed,
               'N',
               l_run_id,
               l_user_id);
      crct_gta(p_mrkt_id,
               p_sls_perd_id,
               auto_suggested_single,
               manual_not_processed,
               'N',
               l_run_id,
               l_user_id);
      crct_gta(p_mrkt_id,
               p_sls_perd_id,
               auto_suggested_multi,
               manual_not_processed,
               'N',
               l_run_id,
               l_user_id);
      UPDATE dly_bilng_trnd
         SET dly_bilng_trnd.trnd_aloctn_manul_stus_id = manual_matched
       WHERE mrkt_id = p_mrkt_id
         AND trnd_sls_perd_id = p_sls_perd_id
         AND dly_bilng_trnd.trnd_aloctn_auto_stus_id IN
             (auto_matched, auto_suggested_single, auto_suggested_multi);
      clc_catgry_sls(p_mrkt_id, p_sls_perd_id, l_run_id, l_user_id);
      UPDATE mrkt_trnd_sls_perd
         SET trnd_aloctn_auto_user_id = p_user_nm,
             trnd_aloctn_auto_end_ts  = SYSDATE
       WHERE mrkt_id = p_mrkt_id
         AND trnd_sls_perd_id = p_sls_perd_id;
      COMMIT;
      --
    END IF;
    -- end auto process
    app_plsql_log.info(l_module_name || ' end' || l_parameter_list);
  EXCEPTION
    -- for any error log the error message and re-raise the exception
    WHEN OTHERS THEN
      ROLLBACK;
      app_plsql_log.info(l_module_name || ' FAILED, error code: ' ||
                         SQLCODE || ' error message: ' || SQLERRM ||
                         l_parameter_list);
      RAISE;
  END auto_procs;

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
                    WHERE 1 = 1
                      AND mrkt_id IN
                          (SELECT mrkt_id
                             FROM mrkt_config_item
                            WHERE config_item_id = 12005
                              AND mrkt_config_item_val_txt = 'Y')
                    GROUP BY mrkt_id, sls_perd_id)
                  SELECT db.mrkt_id, db.sls_perd_id, db.max_dly_bilng_id
                    FROM db, dly_bilng_trnd
                   WHERE db.max_dly_bilng_id =
                         dly_bilng_trnd.dly_bilng_id(+)
                     AND dly_bilng_trnd.dly_bilng_id IS NULL
                   ORDER BY db.mrkt_id, db.sls_perd_id) LOOP
      -- auto_procs
      l_parameter_list := ' (sysdate: ' ||
                          to_char(SYSDATE, 'yyyy-mm-dd hh24:mi:ss') || ', ' ||
                          'rec: mrkt_id: ' || to_char(rec.mrkt_id) || ', ' ||
                          'sls_perd_id: ' || to_char(rec.sls_perd_id) || ', ' ||
                          'max_dly_bilng_id: ' ||
                          to_char(rec.max_dly_bilng_id) || ', ' ||
                          'p_run_id: ' || to_char(l_run_id) || ', ' ||
                          'p_user_id: ' || l_user_id || ')';
      app_plsql_log.info(l_module_name || ' TREND AUTO JOB START' ||
                         l_parameter_list);
      auto_procs(p_mrkt_id     => rec.mrkt_id,
                 p_sls_perd_id => rec.sls_perd_id,
                 p_user_nm     => 'TREND JOB',
                 p_run_id      => l_run_id,
                 p_user_id     => l_user_id);
      app_plsql_log.info(l_module_name || ' TREND AUTO JOB END' ||
                         l_parameter_list);
    END LOOP;
    -- process_jobs
    process_jobs(p_run_id => l_run_id, p_user_id => l_user_id);
    app_plsql_log.info(l_module_name || ' end' || l_parameter_list);
  END process_jobs_new_periods;
  --
  PROCEDURE set_sct_autclc(p_mrkt_id     IN mrkt_sls_perd.mrkt_id%TYPE,
                           p_trg_perd_id IN mrkt_sls_perd.sls_perd_id%TYPE,
                           p_sls_typ_id  IN mrkt_sls_perd.sct_sls_typ_id%TYPE,
                           p_autclc_ind  IN mrkt_sls_perd.sct_autclc_est_ind%TYPE,
                           p_run_id      IN NUMBER DEFAULT NULL,
                           p_user_id     IN VARCHAR2 DEFAULT NULL,
                           p_stus        OUT NUMBER) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    -- for LOG
    l_run_id         NUMBER := nvl(p_run_id,
                                   app_plsql_output.generate_new_run_id);
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'SET_SCT_AUTCLC';
    l_parameter_list VARCHAR2(2048);
    -- local variables
    l_autclc_est_ind mrkt_sls_perd.sct_autclc_est_ind%TYPE;
    l_autclc_bst_ind mrkt_sls_perd.sct_autclc_bst_ind%TYPE;
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    l_parameter_list := ' (mrkt_id: ' || to_char(p_mrkt_id) || ', ' ||
                        'trg_perd_id: ' || to_char(p_trg_perd_id) || ', ' ||
                        'sls_typ_id: ' || to_char(p_sls_typ_id) || ', ' ||
                        'autclc_ind: ' || to_char(p_autclc_ind) || ', ' ||
                        'p_run_id: ' || to_char(l_run_id) || ', ' ||
                        'p_user_id: ' || l_user_id || ')';
    app_plsql_log.info(l_module_name || ' start ' || l_parameter_list);
    p_stus := 0;
    BEGIN
      SELECT sct_autclc_est_ind, sct_autclc_bst_ind
        INTO l_autclc_est_ind, l_autclc_bst_ind
        FROM mrkt_sls_perd
       WHERE mrkt_id = p_mrkt_id
         AND sls_perd_id = p_trg_perd_id;
      --
      CASE
        WHEN p_sls_typ_id IN (marketing_est_id, supply_est_id) THEN
          l_autclc_est_ind := p_autclc_ind;
          IF l_autclc_bst_ind IS NOT NULL THEN
            l_autclc_bst_ind := 'N';
          END IF;
        WHEN p_sls_typ_id IN (marketing_bst_id, supply_bst_id) THEN
          l_autclc_bst_ind := p_autclc_ind;
          IF l_autclc_est_ind IS NOT NULL THEN
            l_autclc_est_ind := 'N';
          END IF;
        WHEN p_sls_typ_id IN (marketing_fst_id, supply_fst_id) THEN
          IF l_autclc_est_ind IS NOT NULL THEN
            l_autclc_est_ind := 'N';
          END IF;
          IF l_autclc_bst_ind IS NOT NULL THEN
            l_autclc_bst_ind := 'N';
          END IF;
        ELSE
          p_stus := 3;
      END CASE;
      IF p_stus = 0 THEN
        UPDATE mrkt_sls_perd
           SET sct_sls_typ_id     = p_sls_typ_id,
               sct_autclc_est_ind = l_autclc_est_ind,
               sct_autclc_bst_ind = l_autclc_bst_ind
         WHERE mrkt_id = p_mrkt_id
           AND sls_perd_id = p_trg_perd_id;
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
    app_plsql_log.info(l_module_name || ' end, status_code: ' ||
                       to_char(p_stus) || l_parameter_list);
  END set_sct_autclc;

END pa_trend_alloc;
/
