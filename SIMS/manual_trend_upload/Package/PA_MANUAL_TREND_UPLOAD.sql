CREATE OR REPLACE PACKAGE pa_manual_trend_upload IS

  FUNCTION get_product_table(p_mrkt_id IN NUMBER,
                             p_sls_perd_id IN NUMBER,
                             p_trgt_perd_id IN NUMBER,
                             p_sls_typ_id IN NUMBER)
    RETURN obj_manl_trend_upload_table PIPELINED;

END pa_manual_trend_upload;
/
