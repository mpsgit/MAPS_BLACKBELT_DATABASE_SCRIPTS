CREATE OR REPLACE PACKAGE pa_maps_edit_offr AS

  /************************************************************************************************************
  * Modification History
  *
  * Date         : 2018
  * Developer    : Blackbelt
  * Description  : Initial creation
  *
  * Date         : 25-Sep-2018
  * Developer    : Fiona Lindsay
  * Description  : Brazil Perfect Billing Phase 2
  *                - added logic to handle new Web Position
  *                - default new copy offer energy chart parameter to FALSE
  *                - added logic for Scented Page type
  *                - removed logic from Add Price Point where micro indicator was mistakely copied from Offer
  * 
  ************************************************************************************************************/

  g_package_name           CONSTANT VARCHAR2(30) := 'PA_MAPS_EDIT_OFFR';

  co_exec_status_success   CONSTANT NUMBER := 1;
  co_exec_status_failed    CONSTANT NUMBER := 0;
  co_exec_status_prcpnt_ex CONSTANT NUMBER := 2;
  
  co_eo_stat_success       CONSTANT NUMBER := 1;
  co_eo_stat_error         CONSTANT NUMBER := 0;
  co_eo_stat_part_lines    CONSTANT NUMBER := 2;
  co_eo_stat_lock_failure  CONSTANT NUMBER := 3;

  cfg_pricing_market       CONSTANT NUMBER := 6000;
  cfg_pricing_start_perd   CONSTANT NUMBER := 6006;
  cfg_target_strategy      CONSTANT NUMBER := 6009;
  row_limit                CONSTANT NUMBER := 10000;

  SUBTYPE single_char IS CHAR(1);

  FUNCTION get_scenario_list(p_mrkt_id      IN number_array,
                             p_offr_perd_id IN number_array,
                             p_veh_id       IN number_array) RETURN obj_scenario_table PIPELINED;

  FUNCTION get_edit_offr_table(p_filters IN obj_edit_offr_filter_table,
                               p_pagination IN CHAR DEFAULT 'N')
    RETURN obj_edit_offr_table
    PIPELINED;

  FUNCTION get_offr(p_get_offr   IN obj_get_offr_table,
                    p_pagination IN CHAR DEFAULT 'N',
                    p_scnrio_id  IN NUMBER DEFAULT 0)
    RETURN obj_edit_offr_table
    PIPELINED;

  FUNCTION get_offr_pivot(p_get_offr   IN obj_get_offr_table)
   RETURN obj_edit_offr_table
   PIPELINED;

  FUNCTION get_history(p_get_offr IN obj_get_offr_table)
    RETURN obj_edit_offr_hist_table
    PIPELINED;

  PROCEDURE save_edit_offr_table(p_data_line IN obj_edit_offr_table,
                                 p_result    OUT obj_edit_offr_save_table,
                                 p_pagination IN CHAR DEFAULT 'N');

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

  PROCEDURE add_offer(p_mrkt_id                IN NUMBER,
                      p_offr_perd_id           IN NUMBER,
                      p_veh_id                 IN NUMBER,
                      p_offr_desc_txt          IN VARCHAR2,
                      p_mrkt_veh_perd_sctn_id  IN NUMBER,
                      p_sctn_page_ofs_nr       IN NUMBER,
                      p_featrd_side_cd         IN VARCHAR2,
                      p_pg_wght                IN NUMBER,
                      p_offr_typ               IN VARCHAR2,
                      p_scnrio_id              IN NUMBER,
                      p_scnrio_nm              IN VARCHAR2,
                      p_prfl_cd_list           IN number_array,
                      p_user_nm                IN VARCHAR2,
                      p_clstr_id               IN NUMBER,
                      p_status                OUT NUMBER,
                      p_edit_offr_table       OUT obj_edit_offr_table,
                      p_pagination             IN CHAR DEFAULT 'N');
                      
  PROCEDURE add_concepts_to_offr(p_offr_id          IN NUMBER,
                                 p_prfl_cd_list     IN number_array,
                                 p_user_nm          IN VARCHAR2,
                                 p_clstr_id         IN NUMBER,
                                 p_status          OUT NUMBER,
                                 p_edit_offr_table OUT obj_edit_offr_table,
                                 p_pagination       IN CHAR DEFAULT 'N');

  PROCEDURE add_prcpoints_to_offr(p_offr_id                  IN NUMBER,
                                  p_offr_prfl_prcpt_id_list  IN number_array,
                                  p_user_nm                  IN VARCHAR2,
                                  p_clstr_id                 IN NUMBER,
                                  p_status                  OUT NUMBER,
                                  p_edit_offr_table         OUT obj_edit_offr_table,
                                  p_pagination               IN CHAR DEFAULT 'N');

  PROCEDURE copy_offer(p_copy_offr_table   IN obj_copy_offr_table,
                       p_user_nm           IN VARCHAR2,
                       p_status           OUT NUMBER,
                       p_edit_offr_table  OUT obj_edit_offr_table,
                       p_pagination        IN CHAR DEFAULT 'N');

  PROCEDURE delete_offers(p_osl_records      IN obj_edit_offr_table,
                          p_edit_offr_table OUT obj_edit_offr_table);

  PROCEDURE delete_prcpoints(p_osl_records      IN obj_edit_offr_table,
                             p_edit_offr_table OUT obj_edit_offr_table,
                             p_pagination       IN CHAR DEFAULT 'N');

  PROCEDURE copy_prcpts_to_offr(p_offr_prfl_prcpt_ids IN number_array,
                                p_trgt_offr_id        IN NUMBER,
                                p_trgt_pg_ofs_nr      IN NUMBER,
                                p_trgt_ftrd_side_cd   IN VARCHAR2,
                                p_user_nm             IN VARCHAR2,
                                p_clstr_id            IN NUMBER,
                                p_move_ind            IN VARCHAR2,
                                p_status             OUT NUMBER,
                                p_edit_offr_table    OUT obj_edit_offr_table);

PROCEDURE get_sprd_data(p_mrkt_id      IN NUMBER,
                         p_offr_perd_id IN NUMBER,
                         p_veh_id       IN NUMBER,
                         p_ver_id       IN NUMBER,
                         p_sprd_nr      IN NUMBER,
                         p_page_data    OUT CLOB,
                         p_img_url      OUT VARCHAR2);

  PROCEDURE set_sprd_data(p_mrkt_id      IN NUMBER,
                          p_offr_perd_id IN NUMBER,
                          p_veh_id       IN NUMBER,
                          p_ver_id       IN NUMBER,
                          p_sprd_nr      IN NUMBER,
                          p_user_id      IN VARCHAR2,
                          p_page_data    IN CLOB,
						              p_img_url	     in varchar2,
                          p_status      OUT NUMBER,
                          p_error_txt   OUT VARCHAR2);

END pa_maps_edit_offr;
/
