CREATE OR REPLACE PACKAGE pa_manual_trend_upload IS

  g_package_name           CONSTANT VARCHAR2(30) := 'PA_MANUAL_TREND_UPLOAD';

  co_exec_status_success   CONSTANT NUMBER := 1;
  co_exec_status_failed    CONSTANT NUMBER := 0;

  FUNCTION get_product_table(p_mrkt_id      IN NUMBER,
                             p_sls_perd_id  IN NUMBER,
                             p_trgt_perd_id IN NUMBER,
                             p_sls_typ_id   IN NUMBER)
    RETURN obj_manl_trend_upload_table PIPELINED;

  PROCEDURE save_upload_data(p_table   IN obj_manl_trend_upl_save_table,
                             p_status OUT VARCHAR2);

  PROCEDURE delete_upload_data(p_fsc_cd_arr  IN number_array,
                               p_status     OUT VARCHAR2);

END pa_manual_trend_upload;
/
