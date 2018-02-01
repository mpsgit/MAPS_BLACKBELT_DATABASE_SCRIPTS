create or replace PACKAGE pa_trend_alloc AS

  g_package_name CONSTANT VARCHAR2(30) := 'PA_TREND_ALLOC';
  --
  c_sls_typ_grp_nm_bi24   CONSTANT trend_alloc_hist_dtls.sls_typ_grp_nm%TYPE := 'BI24';
  c_sls_typ_grp_nm_dms    CONSTANT trend_alloc_hist_dtls.sls_typ_grp_nm%TYPE := 'ESTIMATE, TREND, ACTUAL';
  c_sls_typ_grp_nm_fc_dbt CONSTANT trend_alloc_hist_dtls.sls_typ_grp_nm%TYPE := 'FORECASTED_DBT';
  c_sls_typ_grp_nm_fc_dms CONSTANT trend_alloc_hist_dtls.sls_typ_grp_nm%TYPE := 'FORECASTED_DMS';

  TYPE r_periods IS RECORD(
    sls_perd_id        dly_bilng_trnd.trnd_sls_perd_id%TYPE,
    trg_perd_id        dly_bilng_trnd.offr_perd_id%TYPE,
    bilng_day          dly_bilng_trnd.prcsng_dt%TYPE,
    sct_cash_value_on  NUMBER,
    sct_cash_value_off NUMBER);

  TYPE r_bi24_ind IS RECORD(
    sku_id         offr_sku_line.sku_id%TYPE,
    veh_id         offr.veh_id%TYPE,
    offr_id        offr.offr_id%TYPE,
    promtn_id      offr_prfl_prc_point.promtn_id%TYPE,
    promtn_clm_id  offr_prfl_prc_point.promtn_clm_id%TYPE,
    sls_cls_cd     offr_prfl_prc_point.sls_cls_cd%TYPE,
    offst_lbl_id   ta_dict.lbl_id%TYPE,
    cash_value     NUMBER,
    r_factor       NUMBER,
    sls_typ_lbl_id ta_dict.lbl_id%TYPE,
    units          NUMBER,
    sales          NUMBER);
  TYPE t_bi24_ind IS TABLE OF r_bi24_ind INDEX BY VARCHAR2(512);

  TYPE r_hist_detail IS RECORD(
    sku_id         offr_sku_line.sku_id%TYPE,
    veh_id         offr.veh_id%TYPE,
    offr_id        offr.offr_id%TYPE,
    promtn_id      offr_prfl_prc_point.promtn_id%TYPE,
    promtn_clm_id  offr_prfl_prc_point.promtn_clm_id%TYPE,
    sls_cls_cd     offr_prfl_prc_point.sls_cls_cd%TYPE,
    perd_part      custm_seg_mstr.perd_part%TYPE,
    offst_lbl_id   ta_dict.lbl_id%TYPE,
    cash_value     NUMBER,
    r_factor       NUMBER,
    sls_typ_lbl_id ta_dict.lbl_id%TYPE,
    units          NUMBER,
    sales          NUMBER);
  TYPE t_hist_detail IS TABLE OF r_hist_detail;

  TYPE r_hist_head IS RECORD(
    offst_lbl_id   ta_dict.lbl_id%TYPE,
    cash_value     NUMBER,
    r_factor       NUMBER,
    sls_typ_lbl_id ta_dict.lbl_id%TYPE,
    units          NUMBER,
    sales          NUMBER);
  TYPE t_hist_head IS TABLE OF r_hist_head INDEX BY VARCHAR2(128);

  TYPE r_hist_dtl2 IS RECORD(
    dtl_id         NUMBER,
    catgry_id      prfl.catgry_id%TYPE,
    brnd_id        prfl.brnd_id%TYPE,
    sgmt_id        prfl.sgmt_id%TYPE,
    form_id        prfl.form_id%TYPE,
    prfl_cd        sku.prfl_cd%TYPE,
    fsc_cd         mrkt_fsc.fsc_cd%TYPE,
    promtn_id      offr_prfl_prc_point.promtn_id%TYPE,
    promtn_clm_id  offr_prfl_prc_point.promtn_clm_id%TYPE,
    sls_cls_cd     offr_prfl_prc_point.sls_cls_cd%TYPE,
    veh_id         offr.veh_id%TYPE,
    sku_id         offr_sku_line.sku_id%TYPE,
    offr_id        offr.offr_id%TYPE,
    offst_lbl_id   ta_dict.lbl_id%TYPE,
    cash_value     NUMBER,
    r_factor       NUMBER,
    units          NUMBER,
    sales          NUMBER,
    sls_typ_lbl_id ta_dict.lbl_id%TYPE);
  TYPE t_hist_dtl2 IS TABLE OF r_hist_dtl2 INDEX BY VARCHAR2(256);

  TYPE r_hist_prd_dtl IS RECORD(
    trgt_perd_id   dstrbtd_mrkt_sls.sls_perd_id%TYPE,
    intr_perd_id   dstrbtd_mrkt_sls.sls_perd_id%TYPE,
    disc_perd_id   dstrbtd_mrkt_sls.sls_perd_id%TYPE,
    sls_cls_cd     offr_prfl_prc_point.sls_cls_cd%TYPE,
    fsc_cd         mrkt_fsc.fsc_cd%TYPE,
    bias           mrkt_perd_sku_bias.bias_pct%TYPE,
    bi24_adj       dly_bilng_adjstmnt.unit_qty%TYPE,
    sls_prc_amt    dly_bilng_trnd.sls_prc_amt%TYPE,
    nr_for_qty     dly_bilng_trnd.nr_for_qty%TYPE,
    offst_lbl_id   ta_dict.lbl_id%TYPE,
    sls_typ_lbl_id ta_dict.lbl_id%TYPE,
    units          NUMBER,
    sales          NUMBER);
  TYPE t_hist_prd_dtl IS TABLE OF r_hist_prd_dtl INDEX BY VARCHAR2(128);

  TYPE r_save IS RECORD(
    offst_lbl_id      ta_dict.lbl_id%TYPE,
    trgt_sls_perd_id  dstrbtd_mrkt_sls.sls_perd_id%TYPE,
    trgt_offr_perd_id dstrbtd_mrkt_sls.offr_perd_id%TYPE);
  TYPE t_save IS TABLE OF r_save INDEX BY VARCHAR2(128);

  TYPE r_reproc IS RECORD(
    offr_sku_line_id dstrbtd_mrkt_sls.offr_sku_line_id%TYPE,
    rul_nm           custm_seg_mstr.rul_nm%TYPE,
    offst_lbl_id     ta_dict.lbl_id%TYPE,
    sls_typ_lbl_id   ta_dict.lbl_id%TYPE,
    units            NUMBER,
    sales            NUMBER,
    veh_id           offr.veh_id%TYPE,
    comsn_amt        NUMBER,
    tax_amt          NUMBER);
  TYPE t_reproc IS TABLE OF r_reproc;

  TYPE r_reproc_save IS RECORD(
    offr_sku_line_id dstrbtd_mrkt_sls.offr_sku_line_id%TYPE,
    offst_lbl_id     ta_dict.lbl_id%TYPE,
    sls_typ_lbl_id   ta_dict.lbl_id%TYPE,
    units            NUMBER,
    sales            NUMBER,
    veh_id           offr.veh_id%TYPE,
    comsn_amt        NUMBER,
    tax_amt          NUMBER);
  TYPE t_reproc_save IS TABLE OF r_reproc_save INDEX BY VARCHAR2(128);

  TYPE r_reproc_est2 IS RECORD(
    offr_sku_line_id dstrbtd_mrkt_sls.offr_sku_line_id%TYPE,
    rul_nm           custm_seg_mstr.rul_nm%TYPE,
    offst_lbl_id     ta_dict.lbl_id%TYPE,
    x_sls_typ_lbl_id ta_dict.lbl_id%TYPE,
    units_bi24       NUMBER,
    sales_bi24       NUMBER,
    sls_typ_lbl_id   ta_dict.lbl_id%TYPE,
    units_forecasted NUMBER,
    sales_forecasted NUMBER,
    veh_id           offr.veh_id%TYPE,
    comsn_amt        NUMBER,
    tax_amt          NUMBER);
  TYPE t_reproc_est2 IS TABLE OF r_reproc_est2;

  TYPE r_rules IS RECORD(
    rul_nm       custm_seg_mstr.rul_nm%TYPE,
    offst_lbl_id ta_dict.lbl_id%TYPE,
    catgry_id    prfl.catgry_id%TYPE,
    sls_cls_cd   offr_prfl_prc_point.sls_cls_cd%TYPE,
    veh_id       offr.veh_id%TYPE,
    perd_part    custm_seg_mstr.perd_part%TYPE,
    sku_id       offr_sku_line.sku_id%TYPE);
  TYPE t_rules IS TABLE OF r_rules INDEX BY VARCHAR2(256);

  TYPE r_reproc_rule IS RECORD(
    rul_nm       custm_seg_mstr.rul_nm%TYPE,
    sls_perd_id  dstrbtd_mrkt_sls.sls_perd_id%TYPE,
    bilng_day    dly_bilng_trnd.prcsng_dt%TYPE,
    isselected   CHAR(1),
    units_bi24   NUMBER,
    sales_bi24   NUMBER,
    units_actual NUMBER,
    sales_actual NUMBER);
  TYPE t_reproc_rule IS TABLE OF r_reproc_rule INDEX BY VARCHAR2(128);

  TYPE r_current IS RECORD(
    offst_lbl_id ta_dict.lbl_id%TYPE,
    catgry_id    prfl.catgry_id%TYPE,
    sls_cls_cd   offr_prfl_prc_point.sls_cls_cd%TYPE,
    veh_id       offr.veh_id%TYPE,
    perd_part    custm_seg_mstr.perd_part%TYPE,
    sku_id       offr_sku_line.sku_id%TYPE,
    units        NUMBER,
    sales        NUMBER);
  TYPE t_current_est2 IS TABLE OF r_current;
  TYPE t_current IS TABLE OF r_current INDEX BY PLS_INTEGER;

  FUNCTION get_ta_config(p_mrkt_id        IN ta_config.mrkt_id%TYPE,
                         p_sls_perd_id    IN ta_config.eff_sls_perd_id%TYPE,
                         p_sls_typ_id     IN ta_config.trgt_sls_typ_id%TYPE,
                         p_sls_typ_grp_nm IN ta_config.sls_typ_grp_nm%TYPE DEFAULT NULL,
                         p_user_id        IN VARCHAR2 DEFAULT NULL,
                         p_run_id         IN NUMBER DEFAULT NULL)
    RETURN obj_ta_config_table
    PIPELINED;

  FUNCTION get_rule_nm(p_mrkt_id        IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                       p_campgn_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                       p_sls_typ_id     IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                       p_offst_lbl_id   IN custm_seg_mstr.offst_lbl_id%TYPE,
                       p_catgry_id      IN custm_seg_mstr.catgry_id%TYPE,
                       p_sls_cls_cd     IN custm_seg_mstr.sls_cls_cd%TYPE,
                       p_veh_id         IN custm_seg_mstr.veh_id%TYPE,
                       p_perd_part      IN custm_seg_mstr.perd_part%TYPE,
                       p_sku_id         IN dly_bilng_trnd.sku_id%TYPE)
    RETURN custm_seg_mstr.rul_nm%TYPE;

  FUNCTION get_r_factor(p_mrkt_id        IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                        p_campgn_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                        p_sls_typ_id     IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                        p_offst_lbl_id   IN custm_seg_mstr.offst_lbl_id%TYPE,
                        p_catgry_id      IN custm_seg_mstr.catgry_id%TYPE,
                        p_sls_cls_cd     IN custm_seg_mstr.sls_cls_cd%TYPE,
                        p_veh_id         IN custm_seg_mstr.veh_id%TYPE,
                        p_perd_part      IN custm_seg_mstr.perd_part%TYPE,
                        p_sku_id         IN dly_bilng_trnd.sku_id%TYPE)
    RETURN custm_rul_perd.r_factor%TYPE;

  FUNCTION get_use_estimate(p_mrkt_id        IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                            p_campgn_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                            p_sls_typ_id     IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                            p_offst_lbl_id   IN custm_seg_mstr.offst_lbl_id%TYPE,
                            p_catgry_id      IN custm_seg_mstr.catgry_id%TYPE,
                            p_sls_cls_cd     IN custm_seg_mstr.sls_cls_cd%TYPE,
                            p_veh_id         IN custm_seg_mstr.veh_id%TYPE,
                            p_perd_part      IN custm_seg_mstr.perd_part%TYPE,
                            p_sku_id         IN dly_bilng_trnd.sku_id%TYPE)
    RETURN custm_rul_perd.use_estimate%TYPE;

  FUNCTION get_trend_alloc_head_view(p_mrkt_id        IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                                     p_campgn_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                     p_sls_typ_id     IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                                     p_bilng_day      IN dly_bilng_trnd.prcsng_dt%TYPE,
                                     p_user_id        IN VARCHAR2 DEFAULT NULL)
    RETURN obj_pa_trend_alloc_view_table
    PIPELINED;

  FUNCTION get_trend_alloc_hist_head(p_mrkt_id        IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                                     p_campgn_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                     p_sls_typ_id     IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                                     p_bilng_day      IN dly_bilng_trnd.prcsng_dt%TYPE,
                                     p_perd_from      IN NUMBER,
                                     p_perd_to        IN NUMBER,
                                     p_user_id        IN VARCHAR2 DEFAULT NULL)
    RETURN obj_pa_trend_alloc_hist_hd_tbl
    PIPELINED;

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
    PIPELINED;

  FUNCTION get_trend_alloc_hist_dtls(p_mrkt_id      IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                                     p_sls_perd_id  IN dly_bilng_trnd.trnd_sls_perd_id%TYPE,
                                     p_trg_perd_id  IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                     p_sls_typ_id   IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                                     p_bilng_day    IN dly_bilng.prcsng_dt%TYPE,
                                     p_offst_lbl_id IN ta_dict.lbl_id%TYPE DEFAULT NULL,
                                     p_user_id      IN VARCHAR2 DEFAULT NULL)
    RETURN obj_pa_trend_alloc_hist_dt_tbl
    PIPELINED;

  FUNCTION get_rules(p_mrkt_id        IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                     p_campgn_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                     p_sls_typ_id     IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                     p_bilng_day      IN dly_bilng_trnd.prcsng_dt%TYPE,
                     p_user_id        IN VARCHAR2 DEFAULT NULL,
                     p_run_id         IN NUMBER DEFAULT NULL)
    RETURN obj_pa_trend_alloc_rules_table
    PIPELINED;

  FUNCTION get_trend_alloc_resegment(p_mrkt_id        IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                                     p_campgn_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                     p_sls_typ_id     IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                                     p_bilng_day      IN dly_bilng_trnd.prcsng_dt%TYPE,
                                     p_perd_from      IN NUMBER,
                                     p_perd_to        IN NUMBER,
                                     p_rules          IN obj_pa_trend_alloc_rules_table,
                                     p_user_id        IN VARCHAR2 DEFAULT NULL)
    RETURN obj_pa_trend_alloc_sgmnt_table
    PIPELINED;

  FUNCTION get_trend_alloc_current(p_mrkt_id        IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                                   p_campgn_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                   p_sls_typ_id     IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                                   p_bilng_day      IN dly_bilng_trnd.prcsng_dt%TYPE,
                                   p_user_id        IN VARCHAR2 DEFAULT NULL)
    RETURN obj_pa_trend_alloc_crrnt_table
    PIPELINED;

  FUNCTION get_trend_alloc_prod_dtls(p_mrkt_id        IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                                     p_campgn_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                                     p_sls_typ_id     IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                                     p_bilng_day      IN dly_bilng_trnd.prcsng_dt%TYPE,
                                     p_fsc_cd_list    IN fsc_cd_list_array,
                                     p_user_id        IN VARCHAR2 DEFAULT NULL)
    RETURN obj_pa_trend_alloc_prd_dtl_tbl
    PIPELINED;

  PROCEDURE save_rules(p_mrkt_id        IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                       p_campgn_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                       p_sls_typ_id     IN dstrbtd_mrkt_sls.sls_typ_id%TYPE,
                       p_bilng_day      IN dly_bilng_trnd.prcsng_dt%TYPE,
                       p_rules          IN obj_pa_trend_alloc_rules_table,
                       p_user_id        IN VARCHAR2 DEFAULT NULL,
                       p_stus           OUT NUMBER);

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
                               p_stus                 OUT NUMBER);

  PROCEDURE set_sct_autclc(p_mrkt_id        IN mrkt_trnd_sls_perd_sls_typ.mrkt_id%TYPE,
                           p_campgn_perd_id IN mrkt_trnd_sls_perd_sls_typ.trnd_sls_perd_id%TYPE,
                           p_sls_typ_id     IN mrkt_trnd_sls_perd_sls_typ.sls_typ_id%TYPE,
                           p_autclc_ind     IN mrkt_trnd_sls_perd_sls_typ.sct_autclc_ind%TYPE,
                           p_user_id        IN VARCHAR2 DEFAULT NULL,
                           p_stus           OUT NUMBER);

  ---------------------------------------------
  -- inherited from TRND_ALOCTN package -
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
  supply_est_id    CONSTANT NUMBER := 103;
  supply_bst_id    CONSTANT NUMBER := 104;
  supply_fst_id    CONSTANT NUMBER := 105;
  --
  sc_est_day_0     CONSTANT NUMBER := 300;
  sc_bst_day_0     CONSTANT NUMBER := 400;
  bst_day_0        CONSTANT NUMBER := 500;
  est_day_0        CONSTANT NUMBER := 600;
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
  estimate             CONSTANT NUMBER := 1;
  operational_estimate CONSTANT NUMBER := 2;
  demand_actuals       CONSTANT NUMBER := 6;
  billed_actuals       CONSTANT NUMBER := 7;
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
  PROCEDURE process_jobs(p_user_id IN VARCHAR2 DEFAULT NULL,
                         p_run_id  IN NUMBER DEFAULT NULL);

  PROCEDURE procs_multmtch(p_mrkt_id     IN mrkt.mrkt_id%TYPE,
                           p_sls_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                           p_sls_typ_id  IN dly_bilng_offr_sku_line.sls_typ_id%TYPE);

  PROCEDURE auto_procs(p_mrkt_id     IN mrkt.mrkt_id%TYPE,
                       p_sls_perd_id IN dstrbtd_mrkt_sls.sls_perd_id%TYPE,
                       p_user_nm     IN VARCHAR,
                       p_user_id     IN VARCHAR2 DEFAULT NULL,
                       p_run_id      IN NUMBER DEFAULT NULL);

  -- process new periods (load data from dly_bilng to dly_bilng_trnd)
  PROCEDURE process_jobs_new_periods;

  ---------------------------------------------
  -- inherited from SLS_ALOCTN package -
  ---------------------------------------------
  TYPE tbl_key_value IS TABLE OF VARCHAR(255) INDEX BY VARCHAR(255);

  TYPE tbl_key_values IS TABLE OF tbl_key_value INDEX BY VARCHAR(255);

  TYPE tbl_key_keys_values IS TABLE OF tbl_key_values INDEX BY VARCHAR(255);

  PROCEDURE unplan_offr_creation(p_mrkt_id     IN NUMBER,
                                 p_sls_perd_id IN NUMBER,
                                 p_user_id     IN VARCHAR2 DEFAULT NULL,
                                 p_run_id      IN NUMBER DEFAULT NULL,
                                 p_stus        OUT NUMBER);
  --
END pa_trend_alloc;