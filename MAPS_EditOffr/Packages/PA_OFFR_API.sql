CREATE OR REPLACE PACKAGE pa_offer_api AS

  g_package_name         CONSTANT VARCHAR2(30) := 'PA_OFFER_API';
  cfg_pricing_market     CONSTANT NUMBER := 6000;
  cfg_pricing_start_perd CONSTANT NUMBER := 6006;
  cfg_target_strategy    CONSTANT NUMBER := 6009;
  --row_limit              CONSTANT NUMBER := 10000;

  SUBTYPE single_char IS CHAR(1);

  FUNCTION get_offr(p_get_offr IN obj_get_offr_api_table)
    RETURN obj_offer_api_table
    PIPELINED;

  FUNCTION get_edit_offr_table(p_filters IN obj_offer_api_filter)
    RETURN obj_offer_api_table
    PIPELINED;

  PROCEDURE save_offer_api_table(p_ctrl_id IN NUMBER, p_status OUT VARCHAR2);

END pa_offer_api;
/
