CREATE OR REPLACE TYPE obj_edit_offr_filter_line FORCE AS OBJECT
(
                             p_mrkt_id        NUMBER,
                             p_offr_perd_id   NUMBER,
                             p_ver_id         NUMBER,
                             p_veh_id         number_array,
                             p_brchr_plcmt_id number_array,
                             p_catgry_id      number_array,
                             p_brnd_id        number_array,
                             p_prfl_cd        number_array,
                             p_sku_id         number_array,
                             p_fsc_cd         edit_offr_varchar_array,
                             p_sls_typ        NUMBER,
                             p_dltd_ind       NUMBER,
                             p_offr_id        number_array,
                             p_enrgy_chrt_postn_id number_array,
                             p_offr_typ       VARCHAR2(5),
                             p_scnrio_id      number_array
);
/
