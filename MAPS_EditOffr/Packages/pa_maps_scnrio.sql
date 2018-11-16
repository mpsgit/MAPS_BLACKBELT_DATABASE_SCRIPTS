CREATE OR REPLACE PACKAGE pa_maps_scnrio AS

  g_package_name           CONSTANT VARCHAR2(30) := 'pa_maps_scnrio';

  FUNCTION get_scnrio(p_filters IN obj_get_scnrio_table) RETURN OBJ_get_scnrio_offr_TABLE PIPELINED;

  FUNCTION get_offr(p_get_offr IN OBJ_get_scnrio_offr_TABLE, p_scnrio_id IN NUMBER, p_scnrio_desc_txt IN VARCHAR2)
    RETURN OBJ_SCNRIO_TABLE
    PIPELINED;
    

  FUNCTION get_pivot_offr(p_get_offr IN OBJ_get_scnrio_offr_TABLE, p_scnrio_id IN NUMBER, p_scnrio_desc_txt IN VARCHAR2)
    RETURN OBJ_pivot_TABLE
    PIPELINED;    

  function get_scnrio_table( p_filters    IN OBJ_scnrio_filter_TABLE)
          return OBJ_SCNRIO_TABLE pipelined;

  function get_pivot_table( p_filters    IN OBJ_pivot_filter_TABLE)
          return OBJ_pivot_TABLE pipelined;

END pa_maps_scnrio;
/
CREATE OR REPLACE PACKAGE BODY pa_maps_scnrio
AS

  FUNCTION get_offr(p_get_offr IN OBJ_get_scnrio_offr_TABLE, p_scnrio_id IN NUMBER, p_scnrio_desc_txt IN VARCHAR2)
    RETURN OBJ_SCNRIO_TABLE
    PIPELINED AS
    -- local variables

    l_offr_perd_id offr.offr_perd_id%TYPE;
    l_mrkt_id      offr.mrkt_id%TYPE;
    l_ver_id       offr.ver_id%TYPE;
    l_sls_typ      dstrbtd_mrkt_sls.sls_typ_id%TYPE;
    l_on_schedule  NUMBER;

    -- for LOG
    l_run_id  NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id VARCHAR(35) := USER();
    --
    l_module_name VARCHAR2(30) := 'SNCRIO_GET_OFFR';

    l_min_mrkt_id      offr.mrkt_id%TYPE;
    l_min_offr_perd_id offr.offr_perd_id%TYPE;
    l_min_ver_id       offr.ver_id%TYPE;
    l_min_sls_typ      dstrbtd_mrkt_sls.sls_typ_id%TYPE;

  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start');

    l_on_schedule := 1;
--    dbms_output.put_line('GET OFFER IN SCNRIO START');

    SELECT MAX(NVL(sls_typ_id, -1)) INTO l_sls_typ FROM TABLE(p_get_offr);
    SELECT MAX(o.mrkt_id)
      INTO l_mrkt_id
      FROM TABLE(p_get_offr) poi
      JOIN offr o
        ON o.offr_id = poi.offr_id;
    SELECT MAX(o.ver_id)
      INTO l_ver_id
      FROM TABLE(p_get_offr) poi
      JOIN offr o
        ON o.offr_id = poi.offr_id;
    SELECT MAX(o.offr_perd_id)
      INTO l_offr_perd_id
      FROM TABLE(p_get_offr) poi
      JOIN offr o
        ON o.offr_id = poi.offr_id;

 /*   SELECT MIN(NVL(sls_typ_id, -1)) INTO l_min_sls_typ FROM TABLE(p_get_offr);
    SELECT MIN(o.mrkt_id)
      INTO l_min_mrkt_id
      FROM TABLE(p_get_offr) poi
      JOIN offr o
        ON o.offr_id = poi.offr_id;
    SELECT MIN(o.ver_id)
      INTO l_min_ver_id
      FROM TABLE(p_get_offr) poi
      JOIN offr o
        ON o.offr_id = poi.offr_id;
    SELECT MIN(o.offr_perd_id)
      INTO l_min_offr_perd_id
      FROM TABLE(p_get_offr) poi
      JOIN offr o
        ON o.offr_id = poi.offr_id;
*/

/*
    IF l_min_sls_typ <> l_sls_typ OR l_min_mrkt_id <> l_mrkt_id OR   l_min_ver_id <> l_ver_id OR  l_min_offr_perd_id <> l_offr_perd_id
    THEN
      RAISE_APPLICATION_ERROR(-20100, 'market, campaign, version and sales type must be the same to use this function');
    END IF;
*/
    IF l_sls_typ = -1 THEN
      l_sls_typ := NULL;
    END IF;

    dbms_output.put_line(l_sls_typ || l_mrkt_id || l_ver_id ||
                         l_offr_perd_id );


    FOR rec IN (

                  WITH mrkt_tmp_fsc_master AS
                   (SELECT mrkt_id, sku_id, fsc_cd
                      FROM (SELECT mrkt_id
                                  ,sku_id
                                  ,mstr_fsc_cd fsc_cd
                                  ,strt_perd_id from_strt_perd_id
                                  ,nvl(lead(strt_perd_id, 1)
                                       over(PARTITION BY mrkt_id,
                                            sku_id ORDER BY strt_perd_id),
                                       99999999) to_strt_perd_id
                              FROM mstr_fsc_asgnmt
                             WHERE l_mrkt_id = mrkt_id
                               AND l_offr_perd_id >= strt_perd_id)
                     WHERE l_offr_perd_id >= from_strt_perd_id
                       AND l_offr_perd_id < to_strt_perd_id),
                  mrkt_tmp_fsc AS
                   (SELECT mrkt_id, sku_id, MAX(fsc_cd) fsc_cd
                      FROM (SELECT mrkt_id
                                  ,sku_id
                                  ,fsc_cd fsc_cd
                                  ,strt_perd_id from_strt_perd_id
                                  ,nvl(lead(strt_perd_id, 1)
                                       over(PARTITION BY mrkt_id,
                                            fsc_cd ORDER BY strt_perd_id),
                                       99999999) to_strt_perd_id
                              FROM mrkt_fsc
                             WHERE l_mrkt_id = mrkt_id
                               AND l_offr_perd_id >= strt_perd_id
                               AND 'N' = dltd_ind)
                     WHERE l_offr_perd_id >= from_strt_perd_id
                       AND l_offr_perd_id < to_strt_perd_id
                     GROUP BY mrkt_id, sku_id)

                  SELECT o.offr_id AS intrnl_offr_id
                        ,l_sls_typ AS sls_typ
                        ,o.mrkt_id AS mrkt_id
                        ,o.offr_perd_id AS offr_perd_id
                        ,osl_current.sls_perd_id AS sls_perd_id
                        ,osl_current.offr_sku_line_id AS offr_sku_line_id
                        ,o.veh_id AS veh_id
                        ,o.brchr_plcmt_id AS brchr_plcmnt_id
                        ,brchr_plcmt.brchr_plcmt_nm AS brchr_plcmnt_nm
                        ,mrkt_veh_perd_sctn.sctn_nm AS brchr_sctn_nm
                        ,o.enrgy_chrt_postn_id AS enrgy_chrt_postn_id
                        ,decode(o.mrkt_veh_perd_sctn_id,
                                NULL,
                                o.mrkt_veh_perd_sctn_id,
                                mvps.strtg_page_nr + o.sctn_page_ofs_nr +
                                decode(o.featrd_side_cd, 1, 1, 0, 0, 2, 0, 0)) AS pg_nr
                        ,decode(o.mrkt_veh_perd_sctn_id,
                                NULL,
                                o.mrkt_veh_perd_sctn_id,
                                mvps.strtg_page_nr + o.sctn_page_ofs_nr +
                                offr_prfl_prc_point.pg_ofs_nr +
                                decode(offr_prfl_prc_point.featrd_side_cd,
                                       1,
                                       1,
                                       0,
                                       0,
                                       2,
                                       0,
                                       0)) AS pp_pg_nr
                        ,prfl.catgry_id AS ctgry_id
                        ,brnd_grp.brnd_fmly_id AS brnd_id
                        ,prfl.sgmt_id AS sgmt_id
                        ,prfl.form_id AS form_id
                        ,form.form_grp_id AS form_grp_id
                        ,offr_prfl_prc_point.prfl_cd AS prfl_cd
                        ,osl_current.sku_id AS sku_id
                        ,nvl(mrkt_tmp_fsc_master.fsc_cd, mrkt_tmp_fsc.fsc_cd) AS fsc_cd
                        ,prfl.prod_typ_id AS prod_typ_id
                        ,prfl.gendr_id AS gender_id
                        ,osl_current.sls_cls_cd AS sls_cls_cd
                        ,o.offr_desc_txt AS offr_desc_txt
                        ,o.offr_ntes_txt AS offr_notes_txt
                        ,substr(TRIM(o.offr_lyot_cmnts_txt), 0, 3000) AS offr_lyot_cmnts_txt
                        ,o.featrd_side_cd AS featrd_side_cd
                        ,offr_prfl_prc_point.featrd_side_cd AS concept_featrd_side_cd
                        ,offr_sls_cls_sku.micr_ncpsltn_ind AS micr_ncpsltn_ind
                        ,offr_prfl_prc_point.cnsmr_invstmt_bdgt_id AS cnsmr_invstmt_bdgt_id
                        ,offr_prfl_prc_point.pymt_typ AS pymt_typ
                        ,offr_prfl_prc_point.promtn_id AS promtn_id
                        ,offr_prfl_prc_point.promtn_clm_id AS promtn_clm_id
                        ,offr_prfl_prc_point.tax_type_id AS tax_type_id
                        ,offr_sls_cls_sku.wsl_ind AS wsl_ind
                        ,offr_prfl_prc_point.comsn_typ AS comsn_typ
                        ,offr_sku_set.offr_sku_set_id AS offr_sku_set_id
                        ,osl_current.set_cmpnt_qty AS cmpnt_qty
                        ,offr_prfl_prc_point.nr_for_qty AS nr_for_qty
                        ,offr_prfl_prc_point.net_to_avon_fct AS nta_factor
                        ,osl_current.actual_nta AS actual_nta
                        ,osl_current.sku_cost_amt AS sku_cost
                        ,o.ver_id AS ver_id
                        ,offr_prfl_prc_point.sls_prc_amt AS sls_prc_amt
                        ,CASE
                           WHEN osl_current.offr_sku_line_id IS NOT NULL THEN
                            nvl(osl_current.reg_prc_amt, 0)
                           ELSE
                            NULL
                         END AS reg_prc_amt --sku reg prcb?l
                        ,osl_current.line_nr AS line_nr
                        ,osl_current.sum_unit_qty AS unit_qty
                        ,osl_current.dltd_ind AS dltd_ind
                        ,o.creat_ts AS created_ts
                        ,o.creat_user_id AS created_user_id
                        ,o.last_updt_user_id AS last_updt_user_id
                        ,o.last_updt_ts AS last_updt_ts
                        ,mrkt_veh_perd_sctn.mrkt_veh_perd_sctn_id AS mrkt_veh_perd_sctn_id
                        ,mrkt_veh_perd_sctn.sctn_seq_nr AS sctn_seq_nr
                        ,prfl.prfl_nm AS prfl_nm
                        ,osl_current.sku_nm AS sku_nm
                        ,comsn_typ.comsn_typ_desc_txt AS comsn_typ_desc_txt
                        ,mrkt_tax_typ.tax_typ_desc_txt AS tax_typ_desc_txt
                        ,offr_sku_set.offr_sku_set_nm AS offr_sku_set_nm
                        ,o.pg_wght_pct AS pg_wght
                        ,CASE
                           WHEN offr_prfl_prc_point.featrd_side_cd = 1 THEN
                            (SELECT MAX(decode(o.mrkt_veh_perd_sctn_id,
                                               NULL,
                                               o.mrkt_veh_perd_sctn_id,
                                               mvps.strtg_page_nr +
                                               o.sctn_page_ofs_nr +
                                               offr_prfl_prc_point.pg_ofs_nr +
                                               decode(offr_prfl_prc_point.featrd_side_cd,
                                                      1,
                                                      1,
                                                      0,
                                                      0,
                                                      2,
                                                      1,
                                                      0))) over(PARTITION BY o.offr_id)
                               FROM dual) - 1
                           ELSE
                            (SELECT MIN(decode(o.mrkt_veh_perd_sctn_id,
                                               NULL,
                                               o.mrkt_veh_perd_sctn_id,
                                               mvps.strtg_page_nr +
                                               o.sctn_page_ofs_nr +
                                               offr_prfl_prc_point.pg_ofs_nr +
                                               decode(offr_prfl_prc_point.featrd_side_cd,
                                                      1,
                                                      1,
                                                      0,
                                                      0,
                                                      2,
                                                      0,
                                                      0))) over(PARTITION BY o.offr_id)
                               FROM dual)
                         END AS sprd_nr
                        ,offr_prfl_prc_point.offr_prfl_prcpt_id AS offr_prfl_prcpt_id
                        ,o.offr_typ AS offr_typ
                        ,(SELECT sls_typ_nm
                            FROM sls_typ
                           WHERE sls_typ_id = l_sls_typ) AS sls_typ_nm
                        ,veh.veh_desc_txt AS veh_desc_txt
                        ,enrgy_chrt_postn.enrgy_chrt_postn_nm AS enrgy_chrt_postn_nm
                        ,catgry.catgry_nm AS ctgry_nm
                        ,brnd_fmly.brnd_fmly_nm AS brnd_nm
                        ,sgmt.sgmt_nm AS sgmt_nm
                        ,form.form_desc_txt AS form_desc_txt
                        ,form_grp.form_grp_desc_txt AS form_grp_desc_txt
                        ,prod_typ.prod_typ_desc_txt AS prod_typ_desc_txt
                        ,CASE prfl.gendr_id
                           WHEN 1 THEN
                            'MALE'
                           WHEN 2 THEN
                            'FEMALE'
                           ELSE
                            'UNISEX'
                         END gender_desc
                        ,sls_cls.sls_cls_desc_txt AS sls_cls_desc_txt
                        ,CASE o.featrd_side_cd
                           WHEN '0' THEN
                            'LEFT'
                           WHEN '1' THEN
                            'RIGHT'
                           ELSE
                            'BOTH'
                         END featrd_side_desc
                        ,CASE offr_prfl_prc_point.featrd_side_cd
                           WHEN '0' THEN
                            'LEFT'
                           WHEN '1' THEN
                            'RIGHT'
                           ELSE
                            'BOTH'
                         END concept_featrd_side_desc
                        ,cnsmr_invstmt_bdgt.cnsmr_invstmt_bdgt_desc_txt AS cnsmr_invstmt_bdgt_desc_txt
                        ,pymt_typ.pymt_typ_desc_txt AS pymt_typ_desc_txt
                        ,promtn.promtn_desc_txt AS promtn_desc_txt
                        ,promtn_clm.promtn_clm_desc_txt AS promtn_clm_desc_txt
                        ,ver.ver_desc_txt AS ver_desc_txt
                        ,mrkt.mrkt_nm AS mrkt_nm
                        ,prfl.trgt_cnsmr_grp_id AS trgt_cnsmr_grp_id
                        ,trgt_cnsmr_grp.trgt_cnsmr_grp_desc_txt AS trgt_cnsmr_grp_desc_txt
                        ,o.micr_ncpsltn_desc_txt AS micr_ncpsltn_desc_txt
                        ,o.ssnl_evnt_id AS ssnl_evnt_id
                        ,ssnl_evnt.ssnl_evnt_desc_txt AS ssnl_evnt_desc_txt
                        ,offr_prfl_sls_cls_plcmt.pg_wght_pct AS pp_pg_wght_pct
                        ,(mrkt_veh_perd_sctn.strtg_page_nr +
                         mrkt_veh_perd_sctn.strtg_page_side_nr) AS brchr_sub_sctn_strtg_pg
                        ,(mrkt_veh_perd_sctn.strtg_page_nr +
                         mrkt_veh_perd_sctn.strtg_page_side_nr +
                         mrkt_veh_perd_sctn.pg_cnt) AS brchr_sub_sctn_end_pg
                        ,catgry.bus_id AS bus_id
                        ,bus.bus_nm AS bus_nm
                        ,o.offr_link_id AS offr_link_id
                        ,osl_current.offr_sku_line_link_id AS offr_sku_line_link_id
                    FROM (SELECT *
                            FROM offr
                           WHERE offr_id IN
                                 (SELECT offr_id FROM TABLE(p_get_offr))
                             AND offr.mrkt_id = l_mrkt_id
                             AND offr.offr_perd_id = l_offr_perd_id
                             AND offr.ver_id = l_ver_id) o
                        ,(SELECT offr_sku_line.offr_sku_line_id
                                ,offr_sku_line.offr_sku_line_link_id
                                ,offr_sku_line.sku_id
                                ,offr_sku_line.featrd_side_cd
                                ,offr_sku_line.pg_ofs_nr
                                ,offr_sku_line.prfl_cd
                                ,offr_sku_line.sls_cls_cd
                                ,offr_sku_line.offr_id
                                ,offr_sku_line.offr_sku_set_id
                                ,offr_sku_line.offr_prfl_prcpt_id
                                ,offr_sku_line.dltd_ind
                                ,offr_sku_line.line_nr
                                ,offr_sku_line.set_cmpnt_qty
                                ,offr_sku_line.mrkt_id
                                ,dms.actual_nta
                                 --,dms_current.sum_unit_qty AS sum_unit_qty
                                ,CASE
                                   WHEN actual_sku.wghtd_avg_cost_amt IS NOT NULL THEN
                                    actual_sku.wghtd_avg_cost_amt
                                   ELSE
                                    planned_sku.wghtd_avg_cost_amt
                                 END sku_cost_amt
                                ,l_ver_id AS ver_id
                                ,sku_reg_prc.reg_prc_amt
                                ,sku.sku_nm
                                ,l_sls_typ AS sales_type
                                ,(SELECT MAX(sls_typ_id)
                                    FROM dstrbtd_mrkt_sls
                                   WHERE mrkt_id = l_mrkt_id
                                     AND offr_perd_id = l_offr_perd_id
                                     AND sls_perd_id = l_offr_perd_id
                                     AND veh_id = offr_sku_line.veh_id
                                     AND ver_id = l_ver_id) max_sales_type
                                ,dms.sum_unit_qty
                                ,sls_perd_id
                            FROM offr_sku_line
                                 --,(SELECT SUM(unit_qty) AS sum_unit_qty, offr_sku_line_id,sls_typ_id FROM dstrbtd_mrkt_sls WHERE sls_typ_id = l_sls_typ AND mrkt_id = l_mrkt_id AND offr_perd_id = l_offr_perd_id GROUP BY offr_sku_line_id,sls_Typ_id) dms_current
                                ,sku_cost actual_sku
                                ,sku_cost planned_sku
                                ,sku_reg_prc
                                ,sku
                                ,offr o
                                ,(SELECT SUM(unit_qty) AS sum_unit_qty
                                        ,offr_sku_line_id
                                        ,sls_perd_id
                                        ,MAX(net_to_avon_fct) AS actual_nta
                                    FROM dstrbtd_mrkt_sls
                                   WHERE sls_typ_id = l_sls_typ
                                     AND mrkt_id = l_mrkt_id
                                     AND offr_perd_id = l_offr_perd_id
                                     AND CASE
                                           WHEN l_on_schedule = 1
                                                AND offr_perd_id = sls_perd_id THEN
                                            1
                                           WHEN l_on_schedule = 0 THEN
                                            1
                                           ELSE
                                            0
                                         END = 1

                                  --  AND sls_perd_id = l_offr_perd_id --fcs case when on schedule and off schedule
                                   GROUP BY offr_sku_line_id
                                           ,sls_typ_id
                                           ,sls_perd_id) dms
                           WHERE o.offr_id = offr_sku_line.offr_id
                             AND o.mrkt_id = l_mrkt_id
                             AND o.offr_perd_id = l_offr_perd_id
                             AND o.ver_id = l_ver_id
                             AND dms.offr_sku_line_id =
                                 offr_sku_line.offr_sku_line_id

                             AND offr_sku_line.sku_id = actual_sku.sku_id(+)
                             AND actual_sku.cost_typ(+) = 'A'
                             AND actual_sku.mrkt_id(+) = l_mrkt_id
                             AND actual_sku.offr_perd_id(+) = l_offr_perd_id
                             AND offr_sku_line.sku_id = planned_sku.sku_id(+)
                             AND planned_sku.mrkt_id(+) = l_mrkt_id
                             AND planned_sku.offr_perd_id(+) = l_offr_perd_id
                             AND planned_sku.cost_typ(+) = 'P'
                             AND sku_reg_prc.mrkt_id(+) = l_mrkt_id
                             AND sku_reg_prc.offr_perd_id(+) = l_offr_perd_id
                             AND sku_reg_prc.sku_id(+) = offr_sku_line.sku_id
                             AND sku.sku_id(+) = offr_sku_line.sku_id) osl_current
                        ,offr_prfl_prc_point
                        ,prfl
                        ,form
                        ,offr_sku_set
                        ,offr_sls_cls_sku
                        ,offr_prfl_sls_cls_plcmt
                        ,mrkt_veh_perd_sctn
                        ,(SELECT *
                            FROM mrkt_veh_perd_sctn t2
                           WHERE t2.mrkt_id = l_mrkt_id
                             AND t2.ver_id = l_ver_id
                             AND t2.offr_perd_id = l_offr_perd_id
                          UNION
                          SELECT DISTINCT t2.mrkt_id
                                         ,t2.offr_perd_id
                                         ,t2.ver_id
                                         ,1
                                         ,0
                                         ,t2.veh_id
                                         ,0
                                         ,0
                                         ,NULL
                                         ,0
                                         ,0
                                         ,USER
                                         ,SYSDATE
                                         ,USER
                                         ,SYSDATE
                            FROM mrkt_veh_perd_ver t2
                           WHERE t2.mrkt_id = l_mrkt_id
                             AND t2.ver_id = l_ver_id
                             AND t2.offr_perd_id = l_offr_perd_id) mvps
                        ,comsn_typ
                        ,mrkt_tax_typ
                        ,brnd_grp
                        ,brnd
                        ,brnd_fmly
                        ,mrkt_tmp_fsc
                        ,mrkt_tmp_fsc_master
                        ,veh
                        ,enrgy_chrt_postn
                        ,catgry
                        ,bus
                        ,sgmt
                        ,prod_typ
                        ,sls_cls
                        ,cnsmr_invstmt_bdgt
                        ,pymt_typ
                        ,promtn
                        ,promtn_clm
                        ,ver
                        ,form_grp
                        ,mrkt
                        ,brchr_plcmt
                        ,trgt_cnsmr_grp
                        ,ssnl_evnt
               WHERE
                  --mrkt_tmp_fsc and master
                   osl_current.sku_id = mrkt_tmp_fsc_master.sku_id(+)
               AND osl_current.sku_id = mrkt_tmp_fsc.sku_id(+)
               AND mrkt_tmp_fsc_master.mrkt_id(+) = osl_current.mrkt_id
               AND mrkt_tmp_fsc.mrkt_id(+) = osl_current.mrkt_id
                  --ssnl_evnt
               AND ssnl_evnt.ssnl_evnt_id(+) = o.ssnl_evnt_id
                  --trgt_cnsmr_grp
               AND trgt_cnsmr_grp.trgt_cnsmr_grp_id(+) =
                   prfl.trgt_cnsmr_grp_id
                  --brchr_plcmt
               AND brchr_plcmt.brchr_plcmt_id(+) = o.brchr_plcmt_id
                  --mrkt
               AND mrkt.mrkt_id = o.mrkt_id
                  --veh
               AND o.veh_id = veh.veh_id
                  --enrgy_chrt_postn
               AND enrgy_chrt_postn.enrgy_chrt_postn_id(+) =
                   o.enrgy_chrt_postn_id
                  --catgry
               AND catgry.catgry_id(+) = prfl.catgry_id
               AND bus.bus_id(+) = catgry.bus_id
                  --sgmt
               AND sgmt.sgmt_id(+) = prfl.sgmt_id
                  --form_grp
               AND form_grp.form_grp_id(+) = form.form_grp_id
                  --prod_typ
               AND prod_typ.prod_typ_id(+) = prfl.prod_typ_id
                  --sls_cls
               AND sls_cls.sls_cls_cd(+) = osl_current.sls_cls_cd
                  --cnsmr_invstmt_bdgt
               AND cnsmr_invstmt_bdgt.cnsmr_invstmt_bdgt_id(+) =
                   offr_prfl_prc_point.cnsmr_invstmt_bdgt_id
                  --pymt_typ
               AND pymt_typ.pymt_typ(+) = offr_prfl_prc_point.pymt_typ
                  --promtn
               AND promtn.promtn_id(+) = offr_prfl_prc_point.promtn_id
                  --promtn_clm
               AND promtn_clm.promtn_clm_id(+) =
                   offr_prfl_prc_point.promtn_clm_id
                  --ver
               AND ver.ver_id = o.ver_id
                  --offr outer join on selected version
               AND o.offr_id = osl_current.offr_id(+)
               --AND o.offr_typ = 'CMP'
               AND o.mrkt_id = l_mrkt_id
               AND o.offr_perd_id = l_offr_perd_id
               AND o.ver_id = l_ver_id
               --AND osl_current.dltd_ind <> 'Y'
                  --mrkt_veh_perd_sctn
               AND mrkt_veh_perd_sctn.mrkt_veh_perd_sctn_id(+) =
                   o.mrkt_veh_perd_sctn_id
               AND mrkt_veh_perd_sctn.mrkt_id(+) = o.mrkt_id
               AND mrkt_veh_perd_sctn.offr_perd_id(+) = o.offr_perd_id
               AND mrkt_veh_perd_sctn.brchr_plcmt_id(+) = o.brchr_plcmt_id
               AND mrkt_veh_perd_sctn.ver_id(+) = o.ver_id
               AND mrkt_veh_perd_sctn.veh_id(+) = o.veh_id
                  --offr prfl prc point
               AND offr_prfl_prc_point.offr_prfl_prcpt_id(+) =
                   osl_current.offr_prfl_prcpt_id
                  --brnd
               AND brnd.brnd_id(+) = prfl.brnd_id
               AND brnd_grp.brnd_grp_id(+) = brnd.brnd_grp_id
                  --brnd_fmly
               AND brnd_grp.brnd_fmly_id = brnd_fmly.brnd_fmly_id(+)
                  --new prfl and form table
               AND prfl.prfl_cd(+) = offr_prfl_prc_point.prfl_cd
               AND prfl.form_id = form.form_id(+)

                  --offr sku set
               AND offr_sku_set.offr_sku_set_id(+) =
                   osl_current.offr_sku_set_id

                  --offr sls cls sku
               AND offr_sls_cls_sku.offr_id(+) = osl_current.offr_id
               AND offr_sls_cls_sku.sls_cls_cd(+) = osl_current.sls_cls_cd
               AND offr_sls_cls_sku.prfl_cd(+) = osl_current.prfl_cd
               AND offr_sls_cls_sku.pg_ofs_nr(+) = osl_current.pg_ofs_nr
               AND offr_sls_cls_sku.featrd_side_cd(+) =
                   osl_current.featrd_side_cd
               AND offr_sls_cls_sku.sku_id(+) = osl_current.sku_id

                  --offr_prfl_sls_cls_plcmt
               AND offr_prfl_sls_cls_plcmt.offr_id(+) =
                   offr_prfl_prc_point.offr_id
               AND offr_prfl_sls_cls_plcmt.sls_cls_cd(+) =
                   offr_prfl_prc_point.sls_cls_cd
               AND offr_prfl_sls_cls_plcmt.pg_ofs_nr(+) =
                   offr_prfl_prc_point.pg_ofs_nr
               AND offr_prfl_sls_cls_plcmt.featrd_side_cd(+) =
                   offr_prfl_prc_point.featrd_side_cd
               AND offr_prfl_sls_cls_plcmt.prfl_cd(+) =
                   offr_prfl_prc_point.prfl_cd
                  --comsn_typ
               AND comsn_typ.comsn_typ(+) = offr_prfl_prc_point.comsn_typ
                  --tax_typ
               AND mrkt_tax_typ.mrkt_id(+) = offr_prfl_prc_point.mrkt_id
               AND mrkt_tax_typ.tax_type_id(+) =
                   offr_prfl_prc_point.tax_type_id
                  --mvps
               AND mvps.mrkt_veh_perd_sctn_id(+) =
                   decode(o.mrkt_veh_perd_sctn_id,
                          NULL,
                          1,
                          o.mrkt_veh_perd_sctn_id)
               AND mvps.veh_id = o.veh_id)
     LOOP

      PIPE ROW(OBJ_SCNRIO_LINE(rec.mrkt_id,
                              rec.mrkt_nm,
                              rec.offr_perd_id,
                              rec.sls_perd_id,
                              rec.offr_link_id,
                              rec.offr_sku_line_link_id,
                              rec.offr_sku_line_id,
                              rec.veh_id,
                              rec.veh_desc_txt,
                              rec.brchr_plcmnt_id,
                              rec.brchr_plcmnt_nm,
                              rec.brchr_sctn_nm,
                              rec.sctn_seq_nr,
                              rec.enrgy_chrt_postn_id,
                              rec.enrgy_chrt_postn_nm,
                              rec.pg_nr,
                              rec.pp_pg_nr,
                              rec.ctgry_id,
                              rec.ctgry_nm,
                              rec.brnd_id,
                              rec.brnd_nm,
                              rec.sgmt_id,
                              rec.sgmt_nm,
                              rec.form_id,
                              rec.form_desc_txt,
                              rec.form_grp_id,
                              rec.form_grp_desc_txt,
                              rec.prfl_cd,
                              rec.sku_id,
                              rec.fsc_cd,
                              rec.prod_typ_id,
                              rec.prod_typ_desc_txt,
                              rec.gender_id,
                              rec.gender_desc,
                              rec.sls_cls_cd,
                              rec.sls_cls_desc_txt,
                              rec.offr_desc_txt,
                              rec.offr_notes_txt,
                              rec.offr_lyot_cmnts_txt,
                              rec.featrd_side_cd,
                              rec.featrd_side_desc,
                              rec.concept_featrd_side_cd,
                              rec.concept_featrd_side_desc,
                              rec.micr_ncpsltn_ind,
                              rec.cnsmr_invstmt_bdgt_id,
                              rec.cnsmr_invstmt_bdgt_desc_Txt,
                              rec.pymt_typ,
                              rec.pymt_typ_desc_txt,
                              rec.promtn_id,
                              rec.promtn_desc_txt,
                              rec.promtn_clm_id,
                              rec.promtn_clm_desc_txt,
                              rec.comsn_typ,
                              rec.tax_type_id,
                              rec.wsl_ind,
                              rec.offr_sku_set_id,
                              rec.cmpnt_qty,
                              rec.nr_for_qty,
                              rec.nta_factor,
                              rec.actual_nta,
                              rec.sku_cost,
                              rec.ver_id,
                              rec.ver_desc_txt,
                              rec.sls_prc_amt,
                              rec.reg_prc_amt,
                              rec.line_nr,
                              rec.unit_qty,
                              rec.dltd_ind,
                              rec.created_ts,
                              rec.created_user_id,
                              rec.last_updt_ts,
                              rec.last_updt_user_id,
                              rec.intrnl_offr_id,
                              rec.mrkt_veh_perd_sctn_id,
                              rec.prfl_nm,
                              rec.sku_nm,
                              rec.comsn_typ_desc_txt,
                              rec.tax_typ_desc_txt,
                              rec.offr_sku_set_nm,
                              rec.sls_typ,
                              rec.sls_typ_nm,
                              rec.pg_wght,
                              rec.sprd_nr,
                              rec.offr_prfl_prcpt_id,
                              rec.trgt_cnsmr_grp_id,
                              rec.trgt_cnsmr_grp_desc_txt,
                              rec.MICR_NCPSLTN_DESC_TXT,
                              rec.SSNL_EVNT_ID,
                              rec.SSNL_EVNT_DESC_TXT,
                              rec.pp_pg_wght_pct,
                              rec.brchr_sub_sctn_strtg_pg,
                              rec.brchr_sub_Sctn_end_pg,
                              rec.bus_id,
                              rec.bus_nm,
                              p_scnrio_id,
                              p_scnrio_desc_txt,
                              rec.offr_typ));
    END LOOP;
    app_plsql_log.info(l_module_name || ' stop');
  EXCEPTION
    WHEN OTHERS THEN
      app_plsql_log.info('Error in get offr scnrio' || SQLERRM(SQLCODE));

  END get_offr;
  
  
FUNCTION get_pivot_offr(p_get_offr IN OBJ_get_scnrio_offr_TABLE, p_scnrio_id IN NUMBER, p_scnrio_desc_txt IN VARCHAR2)
    RETURN OBJ_pivot_TABLE
    PIPELINED AS
    -- local variables

    l_offr_perd_id offr.offr_perd_id%TYPE;
    l_mrkt_id      offr.mrkt_id%TYPE;
    l_ver_id       offr.ver_id%TYPE;
    l_sls_typ      dstrbtd_mrkt_sls.sls_typ_id%TYPE;
    l_on_schedule  NUMBER;

    -- for LOG
    l_run_id  NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id VARCHAR(35) := USER();
    --
    l_module_name VARCHAR2(30) := 'SNCRIO_GET_OFFR';

    l_min_mrkt_id      offr.mrkt_id%TYPE;
    l_min_offr_perd_id offr.offr_perd_id%TYPE;
    l_min_ver_id       offr.ver_id%TYPE;
    l_min_sls_typ      dstrbtd_mrkt_sls.sls_typ_id%TYPE;

  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start');

    l_on_schedule := 1;
--    dbms_output.put_line('GET OFFER IN SCNRIO START');

    SELECT MAX(NVL(sls_typ_id, -1)) INTO l_sls_typ FROM TABLE(p_get_offr);
    SELECT MAX(o.mrkt_id)
      INTO l_mrkt_id
      FROM TABLE(p_get_offr) poi
      JOIN offr o
        ON o.offr_id = poi.offr_id;
    SELECT MAX(o.ver_id)
      INTO l_ver_id
      FROM TABLE(p_get_offr) poi
      JOIN offr o
        ON o.offr_id = poi.offr_id;
    SELECT MAX(o.offr_perd_id)
      INTO l_offr_perd_id
      FROM TABLE(p_get_offr) poi
      JOIN offr o
        ON o.offr_id = poi.offr_id;

 /*   SELECT MIN(NVL(sls_typ_id, -1)) INTO l_min_sls_typ FROM TABLE(p_get_offr);
    SELECT MIN(o.mrkt_id)
      INTO l_min_mrkt_id
      FROM TABLE(p_get_offr) poi
      JOIN offr o
        ON o.offr_id = poi.offr_id;
    SELECT MIN(o.ver_id)
      INTO l_min_ver_id
      FROM TABLE(p_get_offr) poi
      JOIN offr o
        ON o.offr_id = poi.offr_id;
    SELECT MIN(o.offr_perd_id)
      INTO l_min_offr_perd_id
      FROM TABLE(p_get_offr) poi
      JOIN offr o
        ON o.offr_id = poi.offr_id;
*/

/*
    IF l_min_sls_typ <> l_sls_typ OR l_min_mrkt_id <> l_mrkt_id OR   l_min_ver_id <> l_ver_id OR  l_min_offr_perd_id <> l_offr_perd_id
    THEN
      RAISE_APPLICATION_ERROR(-20100, 'market, campaign, version and sales type must be the same to use this function');
    END IF;
*/
    IF l_sls_typ = -1 THEN
      l_sls_typ := NULL;
    END IF;

    dbms_output.put_line(l_sls_typ || l_mrkt_id || l_ver_id ||
                         l_offr_perd_id );


    FOR rec IN (

                  WITH mrkt_tmp_fsc_master AS
                   (SELECT mrkt_id, sku_id, fsc_cd
                      FROM (SELECT mrkt_id
                                  ,sku_id
                                  ,mstr_fsc_cd fsc_cd
                                  ,strt_perd_id from_strt_perd_id
                                  ,nvl(lead(strt_perd_id, 1)
                                       over(PARTITION BY mrkt_id,
                                            sku_id ORDER BY strt_perd_id),
                                       99999999) to_strt_perd_id
                              FROM mstr_fsc_asgnmt
                             WHERE l_mrkt_id = mrkt_id
                               AND l_offr_perd_id >= strt_perd_id)
                     WHERE l_offr_perd_id >= from_strt_perd_id
                       AND l_offr_perd_id < to_strt_perd_id),
                  mrkt_tmp_fsc AS
                   (SELECT mrkt_id, sku_id, MAX(fsc_cd) fsc_cd
                      FROM (SELECT mrkt_id
                                  ,sku_id
                                  ,fsc_cd fsc_cd
                                  ,strt_perd_id from_strt_perd_id
                                  ,nvl(lead(strt_perd_id, 1)
                                       over(PARTITION BY mrkt_id,
                                            fsc_cd ORDER BY strt_perd_id),
                                       99999999) to_strt_perd_id
                              FROM mrkt_fsc
                             WHERE l_mrkt_id = mrkt_id
                               AND l_offr_perd_id >= strt_perd_id
                               AND 'N' = dltd_ind)
                     WHERE l_offr_perd_id >= from_strt_perd_id
                       AND l_offr_perd_id < to_strt_perd_id
                     GROUP BY mrkt_id, sku_id)

                  SELECT o.offr_id AS intrnl_offr_id
                        ,l_sls_typ AS sls_typ
                        ,o.mrkt_id AS mrkt_id
                        ,o.offr_perd_id AS offr_perd_id
                        ,osl_current.sls_perd_id AS sls_perd_id
                        ,osl_current.offr_sku_line_id AS offr_sku_line_id
                        ,o.veh_id AS veh_id
                        ,o.brchr_plcmt_id AS brchr_plcmnt_id
                        ,brchr_plcmt.brchr_plcmt_nm AS brchr_plcmnt_nm
                        ,mrkt_veh_perd_sctn.sctn_nm AS brchr_sctn_nm
                        ,o.enrgy_chrt_postn_id AS enrgy_chrt_postn_id
                        ,decode(o.mrkt_veh_perd_sctn_id,
                                NULL,
                                o.mrkt_veh_perd_sctn_id,
                                mvps.strtg_page_nr + o.sctn_page_ofs_nr +
                                decode(o.featrd_side_cd, 1, 1, 0, 0, 2, 0, 0)) AS pg_nr
                        ,decode(o.mrkt_veh_perd_sctn_id,
                                NULL,
                                o.mrkt_veh_perd_sctn_id,
                                mvps.strtg_page_nr + o.sctn_page_ofs_nr +
                                offr_prfl_prc_point.pg_ofs_nr +
                                decode(offr_prfl_prc_point.featrd_side_cd,
                                       1,
                                       1,
                                       0,
                                       0,
                                       2,
                                       0,
                                       0)) AS pp_pg_nr
                        ,prfl.catgry_id AS ctgry_id
                        ,brnd_grp.brnd_fmly_id AS brnd_id
                        ,prfl.sgmt_id AS sgmt_id
                        ,prfl.form_id AS form_id
                        ,form.form_grp_id AS form_grp_id
                        ,offr_prfl_prc_point.prfl_cd AS prfl_cd
                        ,osl_current.sku_id AS sku_id
                        ,nvl(mrkt_tmp_fsc_master.fsc_cd, mrkt_tmp_fsc.fsc_cd) AS fsc_cd
                        ,prfl.prod_typ_id AS prod_typ_id
                        ,prfl.gendr_id AS gender_id
                        ,osl_current.sls_cls_cd AS sls_cls_cd
                        ,o.offr_desc_txt AS offr_desc_txt
                        ,o.offr_ntes_txt AS offr_notes_txt
                        ,substr(TRIM(o.offr_lyot_cmnts_txt), 0, 3000) AS offr_lyot_cmnts_txt
                        ,o.featrd_side_cd AS featrd_side_cd
                        ,offr_prfl_prc_point.featrd_side_cd AS concept_featrd_side_cd
                        ,offr_sls_cls_sku.micr_ncpsltn_ind AS micr_ncpsltn_ind
                        ,offr_prfl_prc_point.cnsmr_invstmt_bdgt_id AS cnsmr_invstmt_bdgt_id
                        ,offr_prfl_prc_point.pymt_typ AS pymt_typ
                        ,offr_prfl_prc_point.promtn_id AS promtn_id
                        ,offr_prfl_prc_point.promtn_clm_id AS promtn_clm_id
                        ,offr_prfl_prc_point.tax_type_id AS tax_type_id
                        ,offr_sls_cls_sku.wsl_ind AS wsl_ind
                        ,offr_prfl_prc_point.comsn_typ AS comsn_typ
                        ,offr_sku_set.offr_sku_set_id AS offr_sku_set_id
                        ,osl_current.set_cmpnt_qty AS cmpnt_qty
                        ,offr_prfl_prc_point.nr_for_qty AS nr_for_qty
                        ,offr_prfl_prc_point.net_to_avon_fct AS nta_factor
                        ,osl_current.actual_nta AS actual_nta
                        ,osl_current.sku_cost_amt AS sku_cost
                        ,o.ver_id AS ver_id
                        ,offr_prfl_prc_point.sls_prc_amt AS sls_prc_amt
                        ,CASE
                           WHEN osl_current.offr_sku_line_id IS NOT NULL THEN
                            nvl(osl_current.reg_prc_amt, 0)
                           ELSE
                            NULL
                         END AS reg_prc_amt --sku reg prcb?l
                        ,osl_current.line_nr AS line_nr
                        ,osl_current.sum_unit_qty AS unit_qty
                        ,osl_current.dltd_ind AS dltd_ind
                        ,o.creat_ts AS created_ts
                        ,o.creat_user_id AS created_user_id
                        ,o.last_updt_user_id AS last_updt_user_id
                        ,o.last_updt_ts AS last_updt_ts
                        ,mrkt_veh_perd_sctn.mrkt_veh_perd_sctn_id AS mrkt_veh_perd_sctn_id
                        ,mrkt_veh_perd_sctn.sctn_seq_nr AS sctn_seq_nr
                        ,prfl.prfl_nm AS prfl_nm
                        ,osl_current.sku_nm AS sku_nm
                        ,comsn_typ.comsn_typ_desc_txt AS comsn_typ_desc_txt
                        ,mrkt_tax_typ.tax_typ_desc_txt AS tax_typ_desc_txt
                        ,offr_sku_set.offr_sku_set_nm AS offr_sku_set_nm
                        ,o.pg_wght_pct AS pg_wght
                        ,CASE
                           WHEN offr_prfl_prc_point.featrd_side_cd = 1 THEN
                            (SELECT MAX(decode(o.mrkt_veh_perd_sctn_id,
                                               NULL,
                                               o.mrkt_veh_perd_sctn_id,
                                               mvps.strtg_page_nr +
                                               o.sctn_page_ofs_nr +
                                               offr_prfl_prc_point.pg_ofs_nr +
                                               decode(offr_prfl_prc_point.featrd_side_cd,
                                                      1,
                                                      1,
                                                      0,
                                                      0,
                                                      2,
                                                      1,
                                                      0))) over(PARTITION BY o.offr_id)
                               FROM dual) - 1
                           ELSE
                            (SELECT MIN(decode(o.mrkt_veh_perd_sctn_id,
                                               NULL,
                                               o.mrkt_veh_perd_sctn_id,
                                               mvps.strtg_page_nr +
                                               o.sctn_page_ofs_nr +
                                               offr_prfl_prc_point.pg_ofs_nr +
                                               decode(offr_prfl_prc_point.featrd_side_cd,
                                                      1,
                                                      1,
                                                      0,
                                                      0,
                                                      2,
                                                      0,
                                                      0))) over(PARTITION BY o.offr_id)
                               FROM dual)
                         END AS sprd_nr
                        ,offr_prfl_prc_point.offr_prfl_prcpt_id AS offr_prfl_prcpt_id
                        ,o.offr_typ AS offr_typ
                        ,(SELECT sls_typ_nm
                            FROM sls_typ
                           WHERE sls_typ_id = l_sls_typ) AS sls_typ_nm
                        ,veh.veh_desc_txt AS veh_desc_txt
                        ,enrgy_chrt_postn.enrgy_chrt_postn_nm AS enrgy_chrt_postn_nm
                        ,catgry.catgry_nm AS ctgry_nm
                        ,brnd_fmly.brnd_fmly_nm AS brnd_nm
                        ,sgmt.sgmt_nm AS sgmt_nm
                        ,form.form_desc_txt AS form_desc_txt
                        ,form_grp.form_grp_desc_txt AS form_grp_desc_txt
                        ,prod_typ.prod_typ_desc_txt AS prod_typ_desc_txt
                        ,CASE prfl.gendr_id
                           WHEN 1 THEN
                            'MALE'
                           WHEN 2 THEN
                            'FEMALE'
                           ELSE
                            'UNISEX'
                         END gender_desc
                        ,sls_cls.sls_cls_desc_txt AS sls_cls_desc_txt
                        ,CASE o.featrd_side_cd
                           WHEN '0' THEN
                            'LEFT'
                           WHEN '1' THEN
                            'RIGHT'
                           ELSE
                            'BOTH'
                         END featrd_side_desc
                        ,CASE offr_prfl_prc_point.featrd_side_cd
                           WHEN '0' THEN
                            'LEFT'
                           WHEN '1' THEN
                            'RIGHT'
                           ELSE
                            'BOTH'
                         END concept_featrd_side_desc
                        ,cnsmr_invstmt_bdgt.cnsmr_invstmt_bdgt_desc_txt AS cnsmr_invstmt_bdgt_desc_txt
                        ,pymt_typ.pymt_typ_desc_txt AS pymt_typ_desc_txt
                        ,promtn.promtn_desc_txt AS promtn_desc_txt
                        ,promtn_clm.promtn_clm_desc_txt AS promtn_clm_desc_txt
                        ,ver.ver_desc_txt AS ver_desc_txt
                        ,mrkt.mrkt_nm AS mrkt_nm
                        ,prfl.trgt_cnsmr_grp_id AS trgt_cnsmr_grp_id
                        ,trgt_cnsmr_grp.trgt_cnsmr_grp_desc_txt AS trgt_cnsmr_grp_desc_txt
                        ,o.micr_ncpsltn_desc_txt AS micr_ncpsltn_desc_txt
                        ,o.ssnl_evnt_id AS ssnl_evnt_id
                        ,ssnl_evnt.ssnl_evnt_desc_txt AS ssnl_evnt_desc_txt
                        ,offr_prfl_sls_cls_plcmt.pg_wght_pct AS pp_pg_wght_pct
                        ,(mrkt_veh_perd_sctn.strtg_page_nr +
                         mrkt_veh_perd_sctn.strtg_page_side_nr) AS brchr_sub_sctn_strtg_pg
                        ,(mrkt_veh_perd_sctn.strtg_page_nr +
                         mrkt_veh_perd_sctn.strtg_page_side_nr +
                         mrkt_veh_perd_sctn.pg_cnt) AS brchr_sub_sctn_end_pg
                        ,catgry.bus_id AS bus_id
                        ,bus.bus_nm AS bus_nm
                        ,o.offr_link_id AS offr_link_id
                        ,osl_current.offr_sku_line_link_id AS offr_sku_line_link_id
                    FROM (SELECT *
                            FROM offr
                           WHERE offr_id IN
                                 (SELECT offr_id FROM TABLE(p_get_offr))
                             AND offr.mrkt_id = l_mrkt_id
                             AND offr.offr_perd_id = l_offr_perd_id
                             AND offr.ver_id = l_ver_id) o
                        ,(SELECT offr_sku_line.offr_sku_line_id
                                ,offr_sku_line.offr_sku_line_link_id
                                ,offr_sku_line.sku_id
                                ,offr_sku_line.featrd_side_cd
                                ,offr_sku_line.pg_ofs_nr
                                ,offr_sku_line.prfl_cd
                                ,offr_sku_line.sls_cls_cd
                                ,offr_sku_line.offr_id
                                ,offr_sku_line.offr_sku_set_id
                                ,offr_sku_line.offr_prfl_prcpt_id
                                ,offr_sku_line.dltd_ind
                                ,offr_sku_line.line_nr
                                ,offr_sku_line.set_cmpnt_qty
                                ,offr_sku_line.mrkt_id
                                ,dms.actual_nta
                                 --,dms_current.sum_unit_qty AS sum_unit_qty
                                ,CASE
                                   WHEN actual_sku.wghtd_avg_cost_amt IS NOT NULL THEN
                                    actual_sku.wghtd_avg_cost_amt
                                   ELSE
                                    planned_sku.wghtd_avg_cost_amt
                                 END sku_cost_amt
                                ,l_ver_id AS ver_id
                                ,sku_reg_prc.reg_prc_amt
                                ,sku.sku_nm
                                ,l_sls_typ AS sales_type
                                ,(SELECT MAX(sls_typ_id)
                                    FROM dstrbtd_mrkt_sls
                                   WHERE mrkt_id = l_mrkt_id
                                     AND offr_perd_id = l_offr_perd_id
                                     AND sls_perd_id = l_offr_perd_id
                                     AND veh_id = offr_sku_line.veh_id
                                     AND ver_id = l_ver_id) max_sales_type
                                ,dms.sum_unit_qty
                                ,sls_perd_id
                            FROM offr_sku_line
                                 --,(SELECT SUM(unit_qty) AS sum_unit_qty, offr_sku_line_id,sls_typ_id FROM dstrbtd_mrkt_sls WHERE sls_typ_id = l_sls_typ AND mrkt_id = l_mrkt_id AND offr_perd_id = l_offr_perd_id GROUP BY offr_sku_line_id,sls_Typ_id) dms_current
                                ,sku_cost actual_sku
                                ,sku_cost planned_sku
                                ,sku_reg_prc
                                ,sku
                                ,offr o
                                ,(SELECT SUM(unit_qty) AS sum_unit_qty
                                        ,offr_sku_line_id
                                        ,sls_perd_id
                                        ,MAX(net_to_avon_fct) AS actual_nta
                                    FROM dstrbtd_mrkt_sls
                                   WHERE sls_typ_id = l_sls_typ
                                     AND mrkt_id = l_mrkt_id
                                     AND offr_perd_id = l_offr_perd_id
                                     AND CASE
                                           WHEN l_on_schedule = 1
                                                AND offr_perd_id = sls_perd_id THEN
                                            1
                                           WHEN l_on_schedule = 0 THEN
                                            1
                                           ELSE
                                            0
                                         END = 1

                                  --  AND sls_perd_id = l_offr_perd_id --fcs case when on schedule and off schedule
                                   GROUP BY offr_sku_line_id
                                           ,sls_typ_id
                                           ,sls_perd_id) dms
                           WHERE o.offr_id = offr_sku_line.offr_id
                             AND o.mrkt_id = l_mrkt_id
                             AND o.offr_perd_id = l_offr_perd_id
                             AND o.ver_id = l_ver_id
                             AND dms.offr_sku_line_id =
                                 offr_sku_line.offr_sku_line_id

                             AND offr_sku_line.sku_id = actual_sku.sku_id(+)
                             AND actual_sku.cost_typ(+) = 'A'
                             AND actual_sku.mrkt_id(+) = l_mrkt_id
                             AND actual_sku.offr_perd_id(+) = l_offr_perd_id
                             AND offr_sku_line.sku_id = planned_sku.sku_id(+)
                             AND planned_sku.mrkt_id(+) = l_mrkt_id
                             AND planned_sku.offr_perd_id(+) = l_offr_perd_id
                             AND planned_sku.cost_typ(+) = 'P'
                             AND sku_reg_prc.mrkt_id(+) = l_mrkt_id
                             AND sku_reg_prc.offr_perd_id(+) = l_offr_perd_id
                             AND sku_reg_prc.sku_id(+) = offr_sku_line.sku_id
                             AND sku.sku_id(+) = offr_sku_line.sku_id) osl_current
                        ,offr_prfl_prc_point
                        ,prfl
                        ,form
                        ,offr_sku_set
                        ,offr_sls_cls_sku
                        ,offr_prfl_sls_cls_plcmt
                        ,mrkt_veh_perd_sctn
                        ,(SELECT *
                            FROM mrkt_veh_perd_sctn t2
                           WHERE t2.mrkt_id = l_mrkt_id
                             AND t2.ver_id = l_ver_id
                             AND t2.offr_perd_id = l_offr_perd_id
                          UNION
                          SELECT DISTINCT t2.mrkt_id
                                         ,t2.offr_perd_id
                                         ,t2.ver_id
                                         ,1
                                         ,0
                                         ,t2.veh_id
                                         ,0
                                         ,0
                                         ,NULL
                                         ,0
                                         ,0
                                         ,USER
                                         ,SYSDATE
                                         ,USER
                                         ,SYSDATE
                            FROM mrkt_veh_perd_ver t2
                           WHERE t2.mrkt_id = l_mrkt_id
                             AND t2.ver_id = l_ver_id
                             AND t2.offr_perd_id = l_offr_perd_id) mvps
                        ,comsn_typ
                        ,mrkt_tax_typ
                        ,brnd_grp
                        ,brnd
                        ,brnd_fmly
                        ,mrkt_tmp_fsc
                        ,mrkt_tmp_fsc_master
                        ,veh
                        ,enrgy_chrt_postn
                        ,catgry
                        ,bus
                        ,sgmt
                        ,prod_typ
                        ,sls_cls
                        ,cnsmr_invstmt_bdgt
                        ,pymt_typ
                        ,promtn
                        ,promtn_clm
                        ,ver
                        ,form_grp
                        ,mrkt
                        ,brchr_plcmt
                        ,trgt_cnsmr_grp
                        ,ssnl_evnt
               WHERE
                  --mrkt_tmp_fsc and master
                   osl_current.sku_id = mrkt_tmp_fsc_master.sku_id(+)
               AND osl_current.sku_id = mrkt_tmp_fsc.sku_id(+)
               AND mrkt_tmp_fsc_master.mrkt_id(+) = osl_current.mrkt_id
               AND mrkt_tmp_fsc.mrkt_id(+) = osl_current.mrkt_id
                  --ssnl_evnt
               AND ssnl_evnt.ssnl_evnt_id(+) = o.ssnl_evnt_id
                  --trgt_cnsmr_grp
               AND trgt_cnsmr_grp.trgt_cnsmr_grp_id(+) =
                   prfl.trgt_cnsmr_grp_id
                  --brchr_plcmt
               AND brchr_plcmt.brchr_plcmt_id(+) = o.brchr_plcmt_id
                  --mrkt
               AND mrkt.mrkt_id = o.mrkt_id
                  --veh
               AND o.veh_id = veh.veh_id
                  --enrgy_chrt_postn
               AND enrgy_chrt_postn.enrgy_chrt_postn_id(+) =
                   o.enrgy_chrt_postn_id
                  --catgry
               AND catgry.catgry_id(+) = prfl.catgry_id
               AND bus.bus_id(+) = catgry.bus_id
                  --sgmt
               AND sgmt.sgmt_id(+) = prfl.sgmt_id
                  --form_grp
               AND form_grp.form_grp_id(+) = form.form_grp_id
                  --prod_typ
               AND prod_typ.prod_typ_id(+) = prfl.prod_typ_id
                  --sls_cls
               AND sls_cls.sls_cls_cd(+) = osl_current.sls_cls_cd
                  --cnsmr_invstmt_bdgt
               AND cnsmr_invstmt_bdgt.cnsmr_invstmt_bdgt_id(+) =
                   offr_prfl_prc_point.cnsmr_invstmt_bdgt_id
                  --pymt_typ
               AND pymt_typ.pymt_typ(+) = offr_prfl_prc_point.pymt_typ
                  --promtn
               AND promtn.promtn_id(+) = offr_prfl_prc_point.promtn_id
                  --promtn_clm
               AND promtn_clm.promtn_clm_id(+) =
                   offr_prfl_prc_point.promtn_clm_id
                  --ver
               AND ver.ver_id = o.ver_id
                  --offr outer join on selected version
               AND o.offr_id = osl_current.offr_id(+)
               --AND o.offr_typ = 'CMP'
               AND o.mrkt_id = l_mrkt_id
               AND o.offr_perd_id = l_offr_perd_id
               AND o.ver_id = l_ver_id
               --AND osl_current.dltd_ind <> 'Y'
                  --mrkt_veh_perd_sctn
               AND mrkt_veh_perd_sctn.mrkt_veh_perd_sctn_id(+) =
                   o.mrkt_veh_perd_sctn_id
               AND mrkt_veh_perd_sctn.mrkt_id(+) = o.mrkt_id
               AND mrkt_veh_perd_sctn.offr_perd_id(+) = o.offr_perd_id
               AND mrkt_veh_perd_sctn.brchr_plcmt_id(+) = o.brchr_plcmt_id
               AND mrkt_veh_perd_sctn.ver_id(+) = o.ver_id
               AND mrkt_veh_perd_sctn.veh_id(+) = o.veh_id
                  --offr prfl prc point
               AND offr_prfl_prc_point.offr_prfl_prcpt_id(+) =
                   osl_current.offr_prfl_prcpt_id
                  --brnd
               AND brnd.brnd_id(+) = prfl.brnd_id
               AND brnd_grp.brnd_grp_id(+) = brnd.brnd_grp_id
                  --brnd_fmly
               AND brnd_grp.brnd_fmly_id = brnd_fmly.brnd_fmly_id(+)
                  --new prfl and form table
               AND prfl.prfl_cd(+) = offr_prfl_prc_point.prfl_cd
               AND prfl.form_id = form.form_id(+)

                  --offr sku set
               AND offr_sku_set.offr_sku_set_id(+) =
                   osl_current.offr_sku_set_id

                  --offr sls cls sku
               AND offr_sls_cls_sku.offr_id(+) = osl_current.offr_id
               AND offr_sls_cls_sku.sls_cls_cd(+) = osl_current.sls_cls_cd
               AND offr_sls_cls_sku.prfl_cd(+) = osl_current.prfl_cd
               AND offr_sls_cls_sku.pg_ofs_nr(+) = osl_current.pg_ofs_nr
               AND offr_sls_cls_sku.featrd_side_cd(+) =
                   osl_current.featrd_side_cd
               AND offr_sls_cls_sku.sku_id(+) = osl_current.sku_id

                  --offr_prfl_sls_cls_plcmt
               AND offr_prfl_sls_cls_plcmt.offr_id(+) =
                   offr_prfl_prc_point.offr_id
               AND offr_prfl_sls_cls_plcmt.sls_cls_cd(+) =
                   offr_prfl_prc_point.sls_cls_cd
               AND offr_prfl_sls_cls_plcmt.pg_ofs_nr(+) =
                   offr_prfl_prc_point.pg_ofs_nr
               AND offr_prfl_sls_cls_plcmt.featrd_side_cd(+) =
                   offr_prfl_prc_point.featrd_side_cd
               AND offr_prfl_sls_cls_plcmt.prfl_cd(+) =
                   offr_prfl_prc_point.prfl_cd
                  --comsn_typ
               AND comsn_typ.comsn_typ(+) = offr_prfl_prc_point.comsn_typ
                  --tax_typ
               AND mrkt_tax_typ.mrkt_id(+) = offr_prfl_prc_point.mrkt_id
               AND mrkt_tax_typ.tax_type_id(+) =
                   offr_prfl_prc_point.tax_type_id
                  --mvps
               AND mvps.mrkt_veh_perd_sctn_id(+) =
                   decode(o.mrkt_veh_perd_sctn_id,
                          NULL,
                          1,
                          o.mrkt_veh_perd_sctn_id)
               AND mvps.veh_id = o.veh_id)
     LOOP

      PIPE ROW(OBJ_pivot_LINE(rec.mrkt_id,
                              rec.mrkt_nm,
                              rec.offr_perd_id,
                              rec.sls_perd_id,
                              rec.offr_link_id,
                              rec.offr_sku_line_link_id,
                              rec.offr_sku_line_id,
                              rec.veh_id,
                              rec.veh_desc_txt,
                              rec.brchr_plcmnt_id,
                              rec.brchr_plcmnt_nm,
                              rec.brchr_sctn_nm,
                              rec.sctn_seq_nr,
                              rec.enrgy_chrt_postn_id,
                              rec.enrgy_chrt_postn_nm,
                              rec.pg_nr,
                              rec.pp_pg_nr,
                              rec.ctgry_id,
                              rec.ctgry_nm,
                              rec.brnd_id,
                              rec.brnd_nm,
                              rec.sgmt_id,
                              rec.sgmt_nm,
                              rec.form_id,
                              rec.form_desc_txt,
                              rec.form_grp_id,
                              rec.form_grp_desc_txt,
                              rec.prfl_cd,
                              rec.sku_id,
                              rec.fsc_cd,
                              rec.prod_typ_id,
                              rec.prod_typ_desc_txt,
                              rec.gender_id,
                              rec.gender_desc,
                              rec.sls_cls_cd,
                              rec.sls_cls_desc_txt,
                              rec.offr_desc_txt,
                              rec.offr_notes_txt,
                              rec.offr_lyot_cmnts_txt,
                              rec.featrd_side_cd,
                              rec.featrd_side_desc,
                              rec.concept_featrd_side_cd,
                              rec.concept_featrd_side_desc,
                              rec.micr_ncpsltn_ind,
                              rec.cnsmr_invstmt_bdgt_id,
                              rec.cnsmr_invstmt_bdgt_desc_Txt,
                              rec.pymt_typ,
                              rec.pymt_typ_desc_txt,
                              rec.promtn_id,
                              rec.promtn_desc_txt,
                              rec.promtn_clm_id,
                              rec.promtn_clm_desc_txt,
                              rec.comsn_typ,
                              rec.tax_type_id,
                              rec.wsl_ind,
                              rec.offr_sku_set_id,
                              rec.cmpnt_qty,
                              rec.nr_for_qty,
                              rec.nta_factor,
                              rec.actual_nta,
                              rec.sku_cost,
                              rec.ver_id,
                              rec.ver_desc_txt,
                              rec.sls_prc_amt,
                              rec.reg_prc_amt,
                              rec.line_nr,
                              rec.unit_qty,
                              rec.dltd_ind,
                              rec.created_ts,
                              rec.created_user_id,
                              rec.last_updt_ts,
                              rec.last_updt_user_id,
                              rec.intrnl_offr_id,
                              rec.mrkt_veh_perd_sctn_id,
                              rec.prfl_nm,
                              rec.sku_nm,
                              rec.comsn_typ_desc_txt,
                              rec.tax_typ_desc_txt,
                              rec.offr_sku_set_nm,
                              rec.sls_typ,
                              rec.sls_typ_nm,
                              rec.pg_wght,
                              rec.sprd_nr,
                              rec.offr_prfl_prcpt_id,
                              rec.trgt_cnsmr_grp_id,
                              rec.trgt_cnsmr_grp_desc_txt,
                              rec.MICR_NCPSLTN_DESC_TXT,
                              rec.SSNL_EVNT_ID,
                              rec.SSNL_EVNT_DESC_TXT,
                              rec.pp_pg_wght_pct,
                              rec.brchr_sub_sctn_strtg_pg,
                              rec.brchr_sub_Sctn_end_pg,
                              rec.bus_id,
                              rec.bus_nm,
                              p_scnrio_id,
                              p_scnrio_desc_txt,
                              rec.offr_typ));
    END LOOP;
    app_plsql_log.info(l_module_name || ' stop');
  EXCEPTION
    WHEN OTHERS THEN
      app_plsql_log.info('Error in get offr scnrio' || SQLERRM(SQLCODE));

  END get_pivot_offr;

  FUNCTION get_scnrio(p_filters IN obj_get_scnrio_table) RETURN OBJ_get_scnrio_offr_TABLE PIPELINED AS

    -- local variables
    CURSOR c_p_filter IS
      SELECT * FROM TABLE(p_filters);
    -- for LOG
    l_run_id  NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id VARCHAR(35) := USER();
    --
    l_module_name         VARCHAR2(30) := 'GET_SCENRIO_OFFERS';
    l_get_scnrio_offr_table      OBJ_get_scnrio_offr_TABLE;

    l_mrkt_id NUMBER;
    l_offr_perd_id NUMBER;

begin
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start');
    l_get_scnrio_offr_table := OBJ_get_scnrio_offr_TABLE();

  FOR p_filter IN c_p_filter LOOP --Filters from the screen loop

   SELECT MAX(o.mrkt_id), MAX(o.offr_perd_id)
      INTO l_mrkt_id, l_offr_perd_id
      FROM offr o
      JOIN what_if_tran wit
        ON wit.offr_id = o.offr_id
     WHERE wit.scnrio_id = p_filter.scnrio_id
     AND o.ver_id = p_filter.ver_id;

     FOR offrs IN (
            SELECT offr_id AS offr_id
              FROM offr
             WHERE mrkt_id = l_mrkt_id
               AND offr_perd_id = l_offr_perd_id
               AND offr_typ = 'CMP'
               AND ver_id = p_filter.ver_id
            UNION
            SELECT o.offr_id AS offr_id
              FROM offr o
              JOIN what_if_tran wit
                ON wit.offr_id = o.offr_id
              JOIN what_if_scnrio wis
                ON wit.scnrio_id = wis.scnrio_id
             WHERE wis.enbl_scnrio_ind = 'Y'
               AND o.mrkt_id = l_mrkt_id
               AND o.offr_perd_id = l_offr_perd_id
               AND o.offr_typ = 'WIF'
               AND wit.scnrio_id = p_filter.scnrio_id
               AND wit.tran_typ = 'WIF'
               AND ver_id = p_filter.ver_id
            MINUS
            SELECT o.offr_link_id AS offr_id
              FROM offr o
              JOIN what_if_tran wit
                ON wit.offr_id = o.offr_id
              JOIN what_if_scnrio wis
                ON wit.scnrio_id = wis.scnrio_id
             WHERE wis.enbl_scnrio_ind = 'Y'
               AND o.mrkt_id = l_mrkt_id
               AND o.offr_perd_id = l_offr_perd_id
               AND o.offr_typ = 'WIF'
               AND ver_id = p_filter.ver_id
               AND wit.scnrio_id = p_filter.scnrio_id
               AND wit.tran_typ = 'WIF' )

          LOOP --get valid offer id-s loop
              l_get_scnrio_offr_table.extend();
              l_get_scnrio_offr_table(l_get_scnrio_offr_table.last) := OBJ_get_scnrio_offr_line(offrs.offr_id, p_filter.sls_typ_id);
   END LOOP; --get valid offer id-s loop

      FOR rec IN (SELECT * FROM TABLE(l_get_scnrio_offr_table))
     LOOP --Result loop
     PIPE row(OBJ_get_scnrio_offr_line(rec.offr_id,rec.sls_typ_id));

    END LOOP; --pipe rows loop
        l_get_scnrio_offr_table.delete();

    END LOOP;--filter loop

        app_plsql_log.info(l_module_name || ' stop');
     EXCEPTION when others then
      app_plsql_log.info('Error in PA_MAPS_SCNRIO' || sqlerrm(sqlcode));

END get_scnrio;

function get_scnrio_table( p_filters    IN OBJ_scnrio_filter_TABLE)
          return OBJ_SCNRIO_TABLE pipelined
          as

    -- local variables
    CURSOR c_p_filter IS
      SELECT * FROM TABLE(p_filters) ORDER BY p_mrkt_id, p_offr_perd_id;
    -- for LOG
    l_run_id  NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id VARCHAR(35) := USER();
    --
    l_module_name    VARCHAR2(30) := 'GET_SCNRIO_TABLE';
    l_get_offr_table OBJ_get_scnrio_offr_TABLE;
   --l_OBJ_SCNRIO_TABLE OBJ_SCNRIO_TABLE;
   --l_obj_get_scnrio_table obj_get_scnrio_table;



begin
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start');

    l_get_offr_table := OBJ_get_scnrio_offr_TABLE();

  FOR p_filter IN c_p_filter LOOP --Filters from the screen loop
      IF p_filter.p_offr_typ = 'CMP' THEN
    FOR offrs IN (
        --Return every offer id where not every item is disabled.
        SELECT o.offr_id AS p_offr_id,
               p_filter.p_sls_typ AS p_sls_typ
          FROM  offr o
         WHERE
                o.offr_typ = 'CMP'
                AND o.mrkt_id = p_filter.p_mrkt_id
                AND o.offr_perd_id = p_filter.p_offr_perd_id
                AND o.ver_id = p_filter.p_ver_id
      ) LOOP --get valid offer id-s loop
      --DELME
     -- DBMS_OUTPUT.PUT_LINE(OFFRS.P_OFFR_ID || ' OFFERS LOOP IN CMP');
              l_get_offr_table.extend();
              l_get_offr_table(l_get_offr_table.last) := OBJ_get_scnrio_offr_line(offrs.p_offr_id, offrs.p_sls_typ);
   END LOOP; --get valid offer id-s loop
   FOR rec IN (SELECT * FROM TABLE(pa_maps_scnrio.get_offr(l_get_offr_table,NULL,NULL)))
     LOOP
             -- dbms_output.put_line('inside result loop');
    PIPE row(OBJ_SCNRIO_LINE(rec.mrkt_id,
              rec.mrkt_nm,
              rec.offr_perd_id,
              rec.sls_perd_id,
              rec.offr_link_id,
              rec.offr_sku_line_link_id,
              rec.offr_sku_line_id,
              rec.veh_id,
              rec.veh_desc_txt,
              rec.brchr_plcmnt_id,
              rec.brchr_plcmnt_nm,
              rec.brchr_sctn_nm,
              rec.sctn_seq_nr,
              rec.enrgy_chrt_postn_id,
              rec.enrgy_chrt_postn_nm,
              rec.pg_nr,
              rec.pp_pg_nr,
              rec.ctgry_id,
              rec.ctgry_nm,
              rec.brnd_id,
              rec.brnd_nm,
              rec.sgmt_id,
              rec.sgmt_nm,
              rec.form_id,
              rec.form_desc_txt,
              rec.form_grp_id,
              rec.form_grp_desc_txt,
              rec.prfl_cd,
              rec.sku_id,
              rec.fsc_cd,
              rec.prod_typ_id,
              rec.prod_typ_desc_txt,
              rec.gender_id,
              rec.gender_desc,
              rec.sls_cls_cd,
              rec.sls_cls_desc_txt,
              rec.offr_desc_txt,
              rec.offr_notes_txt,
              rec.offr_lyot_cmnts_txt,
              rec.featrd_side_cd,
              rec.featrd_side_desc,
              rec.concept_featrd_side_cd,
              rec.concept_featrd_side_desc,
              rec.micr_ncpsltn_ind,
              rec.cnsmr_invstmt_bdgt_id,
              rec.cnsmr_invstmt_bdgt_desc_Txt,
              rec.pymt_typ,
              rec.pymt_typ_desc_txt,
              rec.promtn_id,
              rec.promtn_desc_txt,
              rec.promtn_clm_id,
              rec.promtn_clm_desc_txt,
              rec.comsn_typ,
              rec.tax_type_id,
              rec.wsl_ind,
              rec.offr_sku_set_id,
              rec.cmpnt_qty,
              rec.nr_for_qty,
              rec.nta_factor,
              rec.actual_nta,
              rec.sku_cost,
              rec.ver_id,
              rec.ver_desc_txt,
              rec.sls_prc_amt,
              rec.reg_prc_amt,
              rec.line_nr,
              rec.unit_qty,
              rec.dltd_ind,
              rec.created_ts,
              rec.created_user_id,
              rec.last_updt_ts,
              rec.last_updt_user_id,
              rec.intrnl_offr_id,
              rec.mrkt_veh_perd_sctn_id,
              rec.prfl_nm,
              rec.sku_nm,
              rec.comsn_typ_desc_txt,
              rec.tax_typ_desc_txt,
              rec.offr_sku_set_nm,
              rec.sls_typ,
              rec.sls_typ_nm,
              rec.pg_wght,
              rec.sprd_nr,
              rec.offr_prfl_prcpt_id,
              rec.trgt_cnsmr_grp_id,
              rec.trgt_cnsmr_grp_desc_txt,
              rec.MICR_NCPSLTN_DESC_TXT,
              rec.SSNL_EVNT_ID,
              rec.SSNL_EVNT_DESC_TXT,
              rec.pp_pg_wght_pct,
              rec.brchr_sub_sctn_strtg_pg,
              rec.brchr_sub_Sctn_end_pg,
              rec.bus_id,
              rec.bus_nm,
              rec.scnrio_id,
              rec.scnrio_desc_txt,
              rec.offr_typ
              ));
              END LOOP;

   l_get_offr_table.delete(); --empty the offer id-s for every filter line


     ELSIF p_filter.p_offr_typ = 'WIF' THEN
    FOR scnrios IN (
          SELECT scnrio_id, scnrio_desc_txt,p_filter.p_sls_typ
        FROM what_if_scnrio
       WHERE mrkt_id = p_filter.p_mrkt_id
         AND (strt_perd_id = p_filter.p_offr_perd_id
          OR end_perd_id = p_filter.p_offr_perd_id)
         AND enbl_scnrio_ind = 'Y'
         AND shr_ind = 'Y'
         AND what_if_Scnrio.scnrio_id IN (SELECT scnrio_id FROM scnrio_slct WHERE mrkt_id = p_filter.p_mrkt_id AND offr_perd_id = p_filter.p_offr_perd_id) 
      ) LOOP --every offer id in a scnrio loop
      --DELME
     -- DBMS_OUTPUT.PUT_LINE(SCNRIOS.SCNRIO_ID || ' SCENARIO LOOP');

      FOR offrs IN (
      SELECT * FROM TABLE(pa_maps_scnrio.get_scnrio(obj_get_scnrio_table(obj_get_scnrio_line(scnrio_id => scnrios.scnrio_id,sls_typ_id => p_filter.p_sls_typ,ver_id => p_filter.p_ver_id))))
      ) LOOP
              l_get_offr_table.extend();
              l_get_offr_table(l_get_offr_table.last) := OBJ_get_scnrio_offr_line(offrs.offr_id, offrs.sls_typ_id);
        END LOOP;
    FOR rec IN (SELECT * FROM TABLE(pa_maps_scnrio.get_offr(l_get_offr_table,scnrios.scnrio_id,scnrios.scnrio_desc_txt)))
     LOOP

    PIPE row(OBJ_SCNRIO_LINE(rec.mrkt_id,
              rec.mrkt_nm,
              rec.offr_perd_id,
              rec.sls_perd_id,
              rec.offr_link_id,
              rec.offr_sku_line_link_id,
              rec.offr_sku_line_id,
              rec.veh_id,
              rec.veh_desc_txt,
              rec.brchr_plcmnt_id,
              rec.brchr_plcmnt_nm,
              rec.brchr_sctn_nm,
              rec.sctn_seq_nr,
              rec.enrgy_chrt_postn_id,
              rec.enrgy_chrt_postn_nm,
              rec.pg_nr,
              rec.pp_pg_nr,
              rec.ctgry_id,
              rec.ctgry_nm,
              rec.brnd_id,
              rec.brnd_nm,
              rec.sgmt_id,
              rec.sgmt_nm,
              rec.form_id,
              rec.form_desc_txt,
              rec.form_grp_id,
              rec.form_grp_desc_txt,
              rec.prfl_cd,
              rec.sku_id,
              rec.fsc_cd,
              rec.prod_typ_id,
              rec.prod_typ_desc_txt,
              rec.gender_id,
              rec.gender_desc,
              rec.sls_cls_cd,
              rec.sls_cls_desc_txt,
              rec.offr_desc_txt,
              rec.offr_notes_txt,
              rec.offr_lyot_cmnts_txt,
              rec.featrd_side_cd,
              rec.featrd_side_desc,
              rec.concept_featrd_side_cd,
              rec.concept_featrd_side_desc,
              rec.micr_ncpsltn_ind,
              rec.cnsmr_invstmt_bdgt_id,
              rec.cnsmr_invstmt_bdgt_desc_Txt,
              rec.pymt_typ,
              rec.pymt_typ_desc_txt,
              rec.promtn_id,
              rec.promtn_desc_txt,
              rec.promtn_clm_id,
              rec.promtn_clm_desc_txt,
              rec.comsn_typ,
              rec.tax_type_id,
              rec.wsl_ind,
              rec.offr_sku_set_id,
              rec.cmpnt_qty,
              rec.nr_for_qty,
              rec.nta_factor,
              rec.actual_nta,
              rec.sku_cost,
              rec.ver_id,
              rec.ver_desc_txt,
              rec.sls_prc_amt,
              rec.reg_prc_amt,
              rec.line_nr,
              rec.unit_qty,
              rec.dltd_ind,
              rec.created_ts,
              rec.created_user_id,
              rec.last_updt_ts,
              rec.last_updt_user_id,
              rec.intrnl_offr_id,
              rec.mrkt_veh_perd_sctn_id,
              rec.prfl_nm,
              rec.sku_nm,
              rec.comsn_typ_desc_txt,
              rec.tax_typ_desc_txt,
              rec.offr_sku_set_nm,
              rec.sls_typ,
              rec.sls_typ_nm,
              rec.pg_wght,
              rec.sprd_nr,
              rec.offr_prfl_prcpt_id,
              rec.trgt_cnsmr_grp_id,
              rec.trgt_cnsmr_grp_desc_txt,
              rec.MICR_NCPSLTN_DESC_TXT,
              rec.SSNL_EVNT_ID,
              rec.SSNL_EVNT_DESC_TXT,
              rec.pp_pg_wght_pct,
              rec.brchr_sub_sctn_strtg_pg,
              rec.brchr_sub_Sctn_end_pg,
              rec.bus_id,
              rec.bus_nm,
              rec.scnrio_id,
              rec.scnrio_desc_txt,
              rec.offr_typ
              ));

     END LOOP; --end of records loop
     l_get_offr_table.delete(); --empty the offer id-s for every filter line
     END LOOP; --end of scnrio loop
     ELSE
       BEGIN
       dbms_output.put_line('error offer typ');

       END;
     END IF;
  END LOOP;--Filters from the screen loop
        app_plsql_log.info(l_module_name || ' stop');
     EXCEPTION
       WHEN NO_DATA_NEEDED THEN
         app_plsql_log.info('no more data needed from get scnrio table');
                     when others then
                       app_plsql_log.info('Error in get scnrio table' || sqlerrm(sqlcode));

  end get_scnrio_table;
  
function get_pivot_table( p_filters    IN OBJ_pivot_filter_TABLE)
          return OBJ_pivot_TABLE pipelined
          as

    -- local variables
    CURSOR c_p_filter IS
      SELECT * FROM TABLE(p_filters) ORDER BY p_mrkt_id, p_offr_perd_id;
    -- for LOG
    l_run_id  NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id VARCHAR(35) := USER();
    --
    l_module_name    VARCHAR2(30) := 'GET_SCNRIO_TABLE';
    l_get_offr_table OBJ_get_scnrio_offr_TABLE;
   --l_OBJ_SCNRIO_TABLE OBJ_SCNRIO_TABLE;
   --l_obj_get_scnrio_table obj_get_scnrio_table;



begin
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start');

    l_get_offr_table := OBJ_get_scnrio_offr_TABLE();

  FOR p_filter IN c_p_filter LOOP --Filters from the screen loop
      IF p_filter.p_offr_typ = 'CMP' THEN
    FOR offrs IN (
        --Return every offer id where not every item is disabled.
        SELECT o.offr_id AS p_offr_id,
               p_filter.p_sls_typ AS p_sls_typ
          FROM  offr o
         WHERE
                o.offr_typ = 'CMP'
                AND o.mrkt_id = p_filter.p_mrkt_id
                AND o.offr_perd_id = p_filter.p_offr_perd_id
                AND o.ver_id = p_filter.p_ver_id
                AND o.veh_id IN (SELECT * FROM TABLE(p_filter.p_veh_id))
      ) LOOP --get valid offer id-s loop
      --DELME
     -- DBMS_OUTPUT.PUT_LINE(OFFRS.P_OFFR_ID || ' OFFERS LOOP IN CMP');
              l_get_offr_table.extend();
              l_get_offr_table(l_get_offr_table.last) := OBJ_get_scnrio_offr_line(offrs.p_offr_id, offrs.p_sls_typ);
   END LOOP; --get valid offer id-s loop
   FOR rec IN (SELECT * FROM TABLE(pa_maps_scnrio.get_pivot_offr(l_get_offr_table,NULL,NULL)))
     LOOP
             -- dbms_output.put_line('inside result loop');
    PIPE row(OBJ_pivot_LINE(rec.mrkt_id,
              rec.mrkt_nm,
              rec.offr_perd_id,
              rec.sls_perd_id,
              rec.offr_link_id,
              rec.offr_sku_line_link_id,
              rec.offr_sku_line_id,
              rec.veh_id,
              rec.veh_desc_txt,
              rec.brchr_plcmnt_id,
              rec.brchr_plcmnt_nm,
              rec.brchr_sctn_nm,
              rec.sctn_seq_nr,
              rec.enrgy_chrt_postn_id,
              rec.enrgy_chrt_postn_nm,
              rec.pg_nr,
              rec.pp_pg_nr,
              rec.ctgry_id,
              rec.ctgry_nm,
              rec.brnd_id,
              rec.brnd_nm,
              rec.sgmt_id,
              rec.sgmt_nm,
              rec.form_id,
              rec.form_desc_txt,
              rec.form_grp_id,
              rec.form_grp_desc_txt,
              rec.prfl_cd,
              rec.sku_id,
              rec.fsc_cd,
              rec.prod_typ_id,
              rec.prod_typ_desc_txt,
              rec.gender_id,
              rec.gender_desc,
              rec.sls_cls_cd,
              rec.sls_cls_desc_txt,
              rec.offr_desc_txt,
              rec.offr_notes_txt,
              rec.offr_lyot_cmnts_txt,
              rec.featrd_side_cd,
              rec.featrd_side_desc,
              rec.concept_featrd_side_cd,
              rec.concept_featrd_side_desc,
              rec.micr_ncpsltn_ind,
              rec.cnsmr_invstmt_bdgt_id,
              rec.cnsmr_invstmt_bdgt_desc_Txt,
              rec.pymt_typ,
              rec.pymt_typ_desc_txt,
              rec.promtn_id,
              rec.promtn_desc_txt,
              rec.promtn_clm_id,
              rec.promtn_clm_desc_txt,
              rec.comsn_typ,
              rec.tax_type_id,
              rec.wsl_ind,
              rec.offr_sku_set_id,
              rec.cmpnt_qty,
              rec.nr_for_qty,
              rec.nta_factor,
              rec.actual_nta,
              rec.sku_cost,
              rec.ver_id,
              rec.ver_desc_txt,
              rec.sls_prc_amt,
              rec.reg_prc_amt,
              rec.line_nr,
              rec.unit_qty,
              rec.dltd_ind,
              rec.created_ts,
              rec.created_user_id,
              rec.last_updt_ts,
              rec.last_updt_user_id,
              rec.intrnl_offr_id,
              rec.mrkt_veh_perd_sctn_id,
              rec.prfl_nm,
              rec.sku_nm,
              rec.comsn_typ_desc_txt,
              rec.tax_typ_desc_txt,
              rec.offr_sku_set_nm,
              rec.sls_typ,
              rec.sls_typ_nm,
              rec.pg_wght,
              rec.sprd_nr,
              rec.offr_prfl_prcpt_id,
              rec.trgt_cnsmr_grp_id,
              rec.trgt_cnsmr_grp_desc_txt,
              rec.MICR_NCPSLTN_DESC_TXT,
              rec.SSNL_EVNT_ID,
              rec.SSNL_EVNT_DESC_TXT,
              rec.pp_pg_wght_pct,
              rec.brchr_sub_sctn_strtg_pg,
              rec.brchr_sub_Sctn_end_pg,
              rec.bus_id,
              rec.bus_nm,
              rec.scnrio_id,
              rec.scnrio_desc_txt,
              rec.offr_typ
              ));
              END LOOP;

   l_get_offr_table.delete(); --empty the offer id-s for every filter line


     ELSIF p_filter.p_offr_typ = 'WIF' THEN
    FOR scnrios IN (
          SELECT scnrio_id, scnrio_desc_txt,p_filter.p_sls_typ
        FROM what_if_scnrio
       WHERE mrkt_id = p_filter.p_mrkt_id
         AND (strt_perd_id = p_filter.p_offr_perd_id
          OR end_perd_id = p_filter.p_offr_perd_id)
         AND enbl_scnrio_ind = 'Y'
         AND shr_ind = 'Y'
         AND what_if_Scnrio.scnrio_id IN (SELECT * FROM TABLE(p_filter.p_scnrio_id))
      ) LOOP --every offer id in a scnrio loop
      --DELME
     -- DBMS_OUTPUT.PUT_LINE(SCNRIOS.SCNRIO_ID || ' SCENARIO LOOP');

      FOR offrs IN (
      SELECT * FROM TABLE(pa_maps_scnrio.get_scnrio(obj_get_scnrio_table(obj_get_scnrio_line(scnrio_id => scnrios.scnrio_id,sls_typ_id => p_filter.p_sls_typ,ver_id => p_filter.p_ver_id))))
      ) LOOP
              l_get_offr_table.extend();
              l_get_offr_table(l_get_offr_table.last) := OBJ_get_scnrio_offr_line(offrs.offr_id, offrs.sls_typ_id);
        END LOOP;
    FOR rec IN (SELECT * FROM TABLE(pa_maps_scnrio.get_pivot_offr(l_get_offr_table,scnrios.scnrio_id,scnrios.scnrio_desc_txt)))
     LOOP

    PIPE row(OBJ_pivot_LINE(rec.mrkt_id,
              rec.mrkt_nm,
              rec.offr_perd_id,
              rec.sls_perd_id,
              rec.offr_link_id,
              rec.offr_sku_line_link_id,
              rec.offr_sku_line_id,
              rec.veh_id,
              rec.veh_desc_txt,
              rec.brchr_plcmnt_id,
              rec.brchr_plcmnt_nm,
              rec.brchr_sctn_nm,
              rec.sctn_seq_nr,
              rec.enrgy_chrt_postn_id,
              rec.enrgy_chrt_postn_nm,
              rec.pg_nr,
              rec.pp_pg_nr,
              rec.ctgry_id,
              rec.ctgry_nm,
              rec.brnd_id,
              rec.brnd_nm,
              rec.sgmt_id,
              rec.sgmt_nm,
              rec.form_id,
              rec.form_desc_txt,
              rec.form_grp_id,
              rec.form_grp_desc_txt,
              rec.prfl_cd,
              rec.sku_id,
              rec.fsc_cd,
              rec.prod_typ_id,
              rec.prod_typ_desc_txt,
              rec.gender_id,
              rec.gender_desc,
              rec.sls_cls_cd,
              rec.sls_cls_desc_txt,
              rec.offr_desc_txt,
              rec.offr_notes_txt,
              rec.offr_lyot_cmnts_txt,
              rec.featrd_side_cd,
              rec.featrd_side_desc,
              rec.concept_featrd_side_cd,
              rec.concept_featrd_side_desc,
              rec.micr_ncpsltn_ind,
              rec.cnsmr_invstmt_bdgt_id,
              rec.cnsmr_invstmt_bdgt_desc_Txt,
              rec.pymt_typ,
              rec.pymt_typ_desc_txt,
              rec.promtn_id,
              rec.promtn_desc_txt,
              rec.promtn_clm_id,
              rec.promtn_clm_desc_txt,
              rec.comsn_typ,
              rec.tax_type_id,
              rec.wsl_ind,
              rec.offr_sku_set_id,
              rec.cmpnt_qty,
              rec.nr_for_qty,
              rec.nta_factor,
              rec.actual_nta,
              rec.sku_cost,
              rec.ver_id,
              rec.ver_desc_txt,
              rec.sls_prc_amt,
              rec.reg_prc_amt,
              rec.line_nr,
              rec.unit_qty,
              rec.dltd_ind,
              rec.created_ts,
              rec.created_user_id,
              rec.last_updt_ts,
              rec.last_updt_user_id,
              rec.intrnl_offr_id,
              rec.mrkt_veh_perd_sctn_id,
              rec.prfl_nm,
              rec.sku_nm,
              rec.comsn_typ_desc_txt,
              rec.tax_typ_desc_txt,
              rec.offr_sku_set_nm,
              rec.sls_typ,
              rec.sls_typ_nm,
              rec.pg_wght,
              rec.sprd_nr,
              rec.offr_prfl_prcpt_id,
              rec.trgt_cnsmr_grp_id,
              rec.trgt_cnsmr_grp_desc_txt,
              rec.MICR_NCPSLTN_DESC_TXT,
              rec.SSNL_EVNT_ID,
              rec.SSNL_EVNT_DESC_TXT,
              rec.pp_pg_wght_pct,
              rec.brchr_sub_sctn_strtg_pg,
              rec.brchr_sub_Sctn_end_pg,
              rec.bus_id,
              rec.bus_nm,
              rec.scnrio_id,
              rec.scnrio_desc_txt,
              rec.offr_typ
              ));

     END LOOP; --end of records loop
     l_get_offr_table.delete(); --empty the offer id-s for every filter line
     END LOOP; --end of scnrio loop
     ELSE
       BEGIN
       dbms_output.put_line('error offer typ');

       END;
     END IF;
  END LOOP;--Filters from the screen loop
        app_plsql_log.info(l_module_name || ' stop');
     EXCEPTION
       WHEN NO_DATA_NEEDED THEN
         app_plsql_log.info('no more data needed from get scnrio table');
                     when others then
                       app_plsql_log.info('Error in get scnrio table' || sqlerrm(sqlcode));

  end get_pivot_table;  
  

END pa_maps_scnrio;
/
