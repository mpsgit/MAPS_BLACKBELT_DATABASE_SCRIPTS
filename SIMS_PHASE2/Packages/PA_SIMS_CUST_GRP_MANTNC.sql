CREATE OR REPLACE PACKAGE PA_SIMS_CUST_GRP_MANTNC AS

  g_package_name CONSTANT VARCHAR2(30) := 'PA_SIMS_CUST_GRP_MANTNC';

  FUNCTION get_cust_grps(p_mrkt_id      IN mrkt_perd.mrkt_id%TYPE,
                         p_trgt_perd_id IN mrkt_perd.perd_id%TYPE)
    RETURN obj_cust_grp_mantnc_table
    PIPELINED;

  FUNCTION get_cust_grp_actv(p_mrkt_id IN mrkt_perd.mrkt_id%TYPE)
    RETURN obj_cust_grp_mantnc_ga_table
    PIPELINED;

  PROCEDURE set_cust_grp_actv(p_mrkt_id           IN NUMBER,
                              p_rul_id            IN NUMBER,
                              p_trgt_perd_id_list IN number_array,
                              p_user_id           IN VARCHAR2,
                              p_stus              OUT NUMBER);

  PROCEDURE set_cust_grps(p_mrkt_id     IN NUMBER,
                          p_rul_id      IN NUMBER DEFAULT NULL,
                          p_rul_nm      IN VARCHAR2,
                          p_rul_desc    IN VARCHAR2,
                          p_sku_id_list IN number_array,
                          p_user_id     IN VARCHAR2,
                          p_stus        OUT NUMBER);

END PA_SIMS_CUST_GRP_MANTNC;
/
show error
