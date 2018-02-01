create or replace PACKAGE pa_sims_mantnc AS

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

  --------------------------- Supply Chain Throw Forward % ----------------------------------

  PROCEDURE set_throw_forwrd_prct(p_mrkt_id               IN NUMBER,
                                  p_throw_forwrd_prct_lst IN obj_mrktprdthrwfrwrdprct_table,
                                  p_user_id               IN VARCHAR2,
                                  p_stus                  OUT NUMBER);

  FUNCTION get_throw_forwrd_prct(p_mrkt_id   IN NUMBER,
                                 p_perd_list IN number_array)
    RETURN obj_mrktprdthrwfrwrdprct_table
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
show error
