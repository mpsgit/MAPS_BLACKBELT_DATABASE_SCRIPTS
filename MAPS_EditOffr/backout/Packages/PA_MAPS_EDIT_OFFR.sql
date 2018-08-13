CREATE OR REPLACE PACKAGE pa_maps_edit_offr AS
  g_package_name         CONSTANT VARCHAR2(30) := 'PA_MAPS_EDIT_OFFR';
  cfg_pricing_market     CONSTANT NUMBER := 6000;
  cfg_pricing_start_perd CONSTANT NUMBER := 6006;
  cfg_target_strategy    CONSTANT NUMBER := 6009;
  row_limit              CONSTANT NUMBER := 10000;

  SUBTYPE single_char IS CHAR(1);
  FUNCTION get_edit_offr_table(p_filters IN obj_edit_offr_filter_table)
    RETURN obj_edit_offr_table
    PIPELINED;

  FUNCTION get_offr(p_get_offr IN obj_get_offr_table)
    RETURN obj_edit_offr_table
    PIPELINED;

  FUNCTION get_history(p_get_offr IN obj_get_offr_table)
    RETURN obj_edit_offr_hist_table
    PIPELINED;

  PROCEDURE save_edit_offr_table(p_data_line IN obj_edit_offr_table,
                                 p_result    OUT obj_edit_offr_save_table);

  PROCEDURE set_history(p_get_offr IN obj_get_offr_table,
                        p_result   OUT NUMBER);

  PROCEDURE merge_history(p_get_offr IN obj_edit_offr_table);

  PROCEDURE lock_offr(p_lock_offr IN obj_edit_offr_lock_table,
                      r_lock_offr OUT obj_edit_offr_lock_res_table);

  PROCEDURE unlock_offr(p_unlock_offr IN obj_edit_offr_lock_table,
                        r_unlock_offr OUT obj_edit_offr_lock_res_table);

  FUNCTION get_sls_typ_and_grp(p_mrkt_perd IN obj_edit_offr_mrkt_perd_table)
    RETURN obj_edit_offr_sls_typ_table
    PIPELINED;

END pa_maps_edit_offr;
/
