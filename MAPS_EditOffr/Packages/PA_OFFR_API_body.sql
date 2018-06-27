CREATE OR REPLACE PACKAGE BODY pa_offer_api AS
  -- co_config_item_id CONSTANT config_item.config_item_id%TYPE := 3250;
  --  co_ci_defval_offr_id   CONSTANT config_item.config_item_id%TYPE := 9200;
  --  co_ci_defval_prcpt_id  CONSTANT config_item.config_item_id%TYPE := 9210;
  --  co_sls_typ_estimate    CONSTANT sls_typ.sls_typ_id%TYPE := 1;
  --  co_sls_typ_op_estimate CONSTANT sls_typ.sls_typ_id%TYPE := 2;
  e_oscs_dup_val EXCEPTION;
  FUNCTION get_offr(p_get_offr IN obj_get_offr_api_table)
    RETURN obj_offer_api_table
    PIPELINED AS
    -- local variables
  
    l_offr_perd_id offr.offr_perd_id%TYPE;
    l_mrkt_id      offr.mrkt_id%TYPE;
    l_ver_id       offr.ver_id%TYPE;
    l_sls_typ      dstrbtd_mrkt_sls.sls_typ_id%TYPE;
    l_on_schedule  NUMBER;
    l_veh_ids      number_array;
    -- for LOG
    l_run_id  NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id VARCHAR(35) := USER();
    --
    l_module_name VARCHAR2(30) := 'OFFER API';
  
    l_min_mrkt_id      offr.mrkt_id%TYPE;
    l_min_offr_perd_id offr.offr_perd_id%TYPE;
    l_min_ver_id       offr.ver_id%TYPE;
    l_min_sls_typ      dstrbtd_mrkt_sls.sls_typ_id%TYPE;
  
    --pricing corridor variables
    pricing_used     single_char := 'N';
    prc_enabled      single_char;
    prc_strt_perd_id mrkt_perd.perd_id%TYPE;
  
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start');
  
    SELECT MAX(p_sls_typ) INTO l_sls_typ FROM TABLE(p_get_offr);
    SELECT MAX(o.mrkt_id)
      INTO l_mrkt_id
      FROM TABLE(p_get_offr) poi
      JOIN offr o
        ON o.offr_id = poi.p_offr_id;
    SELECT MAX(p_on_schedule) INTO l_on_schedule FROM TABLE(p_get_offr);
    SELECT MAX(o.ver_id)
      INTO l_ver_id
      FROM TABLE(p_get_offr) poi
      JOIN offr o
        ON o.offr_id = poi.p_offr_id;
    SELECT MAX(o.offr_perd_id)
      INTO l_offr_perd_id
      FROM TABLE(p_get_offr) poi
      JOIN offr o
        ON o.offr_id = poi.p_offr_id;
    SELECT DISTINCT (o.veh_id)
      BULK COLLECT
      INTO l_veh_ids
      FROM TABLE(p_get_offr) poi
      JOIN offr o
        ON o.offr_id = poi.p_offr_id;
    SELECT MIN(p_sls_typ) INTO l_min_sls_typ FROM TABLE(p_get_offr);
    SELECT MIN(o.mrkt_id)
      INTO l_min_mrkt_id
      FROM TABLE(p_get_offr) poi
      JOIN offr o
        ON o.offr_id = poi.p_offr_id;
    SELECT MIN(o.ver_id)
      INTO l_min_ver_id
      FROM TABLE(p_get_offr) poi
      JOIN offr o
        ON o.offr_id = poi.p_offr_id;
    SELECT MIN(o.offr_perd_id)
      INTO l_min_offr_perd_id
      FROM TABLE(p_get_offr) poi
      JOIN offr o
        ON o.offr_id = poi.p_offr_id;
    IF l_min_sls_typ <> l_sls_typ
       OR l_min_mrkt_id <> l_mrkt_id
       OR l_min_ver_id <> l_ver_id
       OR l_min_offr_perd_id <> l_offr_perd_id THEN
      raise_application_error(-20100,
                              'market, campaign, version and sales type must be the same to use this function');
    END IF;
  
    dbms_output.put_line(l_sls_typ || l_mrkt_id || l_ver_id ||
                         l_offr_perd_id);
    BEGIN
      --    dbms_output.put_line('prc enabled check'||' mrkt_id:'||l_mrkt_id);
      SELECT mrkt_config_item_val_txt
        INTO prc_enabled
        FROM mrkt_config_item
       WHERE mrkt_id = l_mrkt_id
         AND config_item_id = cfg_pricing_market;
      --    dbms_output.put_line('prc perd check');
      SELECT to_number(mrkt_config_item_val_txt)
        INTO prc_strt_perd_id
        FROM mrkt_config_item
       WHERE mrkt_id = l_mrkt_id
         AND config_item_id = cfg_pricing_start_perd;
    
      IF prc_enabled = 'Y'
         AND prc_strt_perd_id IS NOT NULL
         AND l_offr_perd_id >= prc_strt_perd_id THEN
        pricing_used := 'Y';
      END IF;
    
    EXCEPTION
      WHEN OTHERS THEN
        pricing_used := 'N';
        --        app_plsql_log.info('Error in get offr' || SQLERRM(SQLCODE));
    END;
  
    FOR rec IN (
                
                  WITH latest_version_and_sls_typ AS
                   (SELECT asl.ver_id
                          ,asl.sls_typ_id
                          ,asl.offr_perd_id
                          ,asl.veh_id
                      FROM (SELECT ver_id
                                  ,MAX(sls_typ_id) sls_typ_id
                                  ,offr_perd_id
                                  ,veh_id
                              FROM (SELECT ver_id
                                          ,sls_typ_id
                                          ,offr_perd_id
                                          ,veh_id
                                          ,seq_nr
                                          ,MAX(seq_nr) over(PARTITION BY offr_perd_id, veh_id) max_seq_nr
                                      FROM (SELECT dstrbtd_mrkt_sls.ver_id
                                                  ,dstrbtd_mrkt_sls.sls_typ_id
                                                  ,dstrbtd_mrkt_sls.offr_perd_id
                                                  ,dstrbtd_mrkt_sls.veh_id
                                                  ,MAX(ver.seq_nr) AS seq_nr
                                              FROM dstrbtd_mrkt_sls
                                              JOIN ver
                                                ON ver.ver_id =
                                                   dstrbtd_mrkt_sls.ver_id
                                             WHERE dstrbtd_mrkt_sls.mrkt_id =
                                                   l_mrkt_id
                                               AND dstrbtd_mrkt_sls.offr_perd_id =
                                                   l_offr_perd_id
                                               AND dstrbtd_mrkt_sls.sls_perd_id =
                                                   l_offr_perd_id
                                               AND dstrbtd_mrkt_sls.ver_id <>
                                                   l_ver_id
                                               AND CASE
                                                     WHEN l_ver_id = 0
                                                          AND ver.seq_nr <= 1500 THEN
                                                      1
                                                     WHEN l_ver_id <> 0
                                                          AND
                                                          ver.seq_nr <
                                                          (SELECT seq_nr
                                                             FROM ver
                                                            WHERE ver_id = l_ver_id) THEN
                                                      1
                                                     ELSE
                                                      0
                                                   END = 1
                                             GROUP BY dstrbtd_mrkt_sls.ver_id
                                                     ,dstrbtd_mrkt_sls.sls_typ_id
                                                     ,dstrbtd_mrkt_sls.offr_perd_id
                                                     ,dstrbtd_mrkt_sls.veh_id))
                             WHERE max_seq_nr = seq_nr
                               AND seq_nr <> 100
                             GROUP BY ver_id, offr_perd_id, veh_id) asl),
                  latest_osl_stuff AS
                   (SELECT osl.sku_id
                          ,osl.offr_prfl_prcpt_id
                          ,o.mrkt_id
                          ,o.offr_perd_id
                          ,osl.offr_sku_line_link_id
                          ,SUM(dms.unit_qty) AS unit_qty
                          ,dms.sls_typ_id
                          ,SUM(dms.net_to_avon_fct * dms.unit_qty) /
                           SUM(dms.unit_qty) AS net_to_avon_fct
                      FROM offr o
                      JOIN offr_sku_line osl
                        ON osl.offr_id = o.offr_id
                      JOIN dstrbtd_mrkt_sls dms
                        ON dms.offr_sku_line_id = osl.offr_sku_line_id
                      JOIN latest_version_and_sls_typ
                        ON o.offr_perd_id =
                           latest_version_and_sls_typ.offr_perd_id
                       AND o.veh_id = latest_version_and_sls_typ.veh_id
                       AND o.ver_id = latest_version_and_sls_typ.ver_id
                       AND o.offr_perd_id =
                           latest_version_and_sls_typ.offr_perd_id
                       AND dms.sls_typ_id =
                           latest_version_and_sls_typ.sls_typ_id
                       AND dms.unit_qty <> 0
                     GROUP BY osl.offr_sku_line_id
                             ,osl.offr_sku_line_link_id
                             ,osl.sku_id
                             ,osl.offr_prfl_prcpt_id
                             ,o.ver_id
                             ,dms.sls_typ_id
                             ,o.mrkt_id
                             ,o.offr_perd_id),
                  mrkt_tmp_fsc_master AS
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
                         --latest version calculations
                        ,osl_latest.net_to_avon_fct AS lv_nta
                        ,osl_latest.offr_prfl_sls_prc_amt AS lv_sp
                        ,osl_latest.reg_prc_amt AS lv_rp
                        ,CASE
                           WHEN osl_latest.reg_prc_amt = 0 THEN
                            0
                           WHEN osl_latest.offr_prfl_sls_prc_amt = 0 THEN
                            100
                           ELSE
                            round(100 - (osl_latest.offr_prfl_sls_prc_amt /
                                  osl_latest.nr_for_qty /
                                  osl_latest.reg_prc_amt) * 100)
                         END lv_discount
                         
                        ,osl_latest.unit_qty AS lv_units
                        ,osl_latest.unit_qty * osl_latest.sku_cost_amt AS lv_total_cost
                        ,CASE
                           WHEN osl_latest.nr_for_qty = 0 THEN
                            0
                           ELSE
                            round(osl_latest.offr_prfl_sls_prc_amt /
                                  osl_latest.nr_for_qty * osl_latest.unit_qty *
                                  osl_latest.net_to_avon_fct,
                                  4)
                         END lv_gross_sales
                        ,CASE
                           WHEN osl_latest.nr_for_qty = 0 THEN
                            0
                           ELSE
                            round(osl_latest.offr_prfl_sls_prc_amt /
                                  osl_latest.nr_for_qty * osl_latest.unit_qty *
                                  osl_latest.net_to_avon_fct -
                                  osl_latest.sku_cost_amt *
                                  osl_latest.unit_qty,
                                  4)
                         END lv_dp_cash
                        ,CASE
                           WHEN osl_latest.nr_for_qty = 0
                                OR osl_latest.net_to_avon_fct = 0
                                OR osl_latest.unit_qty = 0
                                OR osl_latest.offr_prfl_sls_prc_amt = 0 THEN
                            0
                           ELSE
                            round((osl_latest.offr_prfl_sls_prc_amt /
                                  osl_latest.nr_for_qty * osl_latest.unit_qty *
                                  osl_latest.net_to_avon_fct -
                                  osl_latest.sku_cost_amt *
                                  osl_latest.unit_qty) /
                                  (osl_latest.offr_prfl_sls_prc_amt /
                                  osl_latest.nr_for_qty * osl_latest.unit_qty *
                                  osl_latest.net_to_avon_fct),
                                  4) * 100
                         END lv_dp_percent
                         --/latest version calculations
                        ,o.mrkt_id AS mrkt_id
                        ,o.offr_perd_id AS offr_perd_id
                        ,osl_current.sls_perd_id AS sls_perd_id
                        ,osl_current.offr_sku_line_id AS offr_sku_line_id
                        ,o.veh_id AS veh_id
                        ,o.brchr_plcmt_id AS brchr_plcmnt_id
                        ,brchr_plcmt.brchr_plcmt_nm AS brchr_plcmnt_nm
                        ,mrkt_veh_perd_sctn.sctn_nm AS brchr_sctn_nm
                        ,o.enrgy_chrt_postn_id AS enrgy_chrt_postn_id
                        ,MIN(decode(o.mrkt_veh_perd_sctn_id,
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
                                           0))) over(PARTITION BY o.offr_id) AS pg_nr
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
                         --pricing corridors
                        ,coalesce((SELECT MAX(spa) keep(dense_rank LAST ORDER BY skuid, sumu, vehid)
                                    FROM (SELECT osl_sp.sku_id AS skuid
                                                ,SUM(dms_sp.unit_qty) sumu
                                                ,osl_sp.veh_id vehid
                                                ,MAX(oppp.sls_prc_amt) keep(dense_rank LAST ORDER BY osl_sp.sku_id, osl_sp.veh_id) spa
                                            FROM offr_prfl_prc_point oppp
                                                ,offr_sku_line       osl_sp
                                                ,dstrbtd_mrkt_sls    dms_sp
                                                ,offr                o_sp
                                           WHERE oppp.offr_prfl_prcpt_id =
                                                 osl_sp.offr_prfl_prcpt_id
                                             AND osl_sp.mrkt_id = l_mrkt_id
                                             AND osl_sp.offr_perd_id =
                                                 (SELECT MAX(tpymp.prev_yr_perd_id) keep(dense_rank LAST ORDER BY tpymp.last_updt_ts) AS prev_yr_perd_id
                                                    FROM trnsfrm_prev_yr_mrkt_perd tpymp
                                                   WHERE tpymp.mrkt_id =
                                                         l_mrkt_id
                                                     AND tpymp.perd_id =
                                                         l_offr_perd_id)
                                             AND dms_sp.offr_sku_line_id =
                                                 osl_sp.offr_sku_line_id
                                             AND dms_sp.sls_typ_id IN (6, 7)
                                             AND o_sp.ver_id = 0
                                             AND o_sp.offr_id =
                                                 osl_sp.offr_id
                                           GROUP BY osl_sp.sku_id
                                                   ,osl_sp.veh_id
                                                   ,oppp.offr_prfl_prcpt_id)
                                   WHERE vehid = o.veh_id
                                     AND skuid = osl_current.sku_id),
                                  (SELECT MAX(spa) keep(dense_rank LAST ORDER BY skuid, sumu, vehid)
                                     FROM (SELECT osl_sp.sku_id AS skuid
                                                 ,SUM(dms_sp.unit_qty) sumu
                                                 ,osl_sp.veh_id vehid
                                                 ,MAX(oppp.sls_prc_amt) keep(dense_rank LAST ORDER BY osl_sp.sku_id, osl_sp.veh_id) spa
                                             FROM offr_prfl_prc_point oppp
                                                 ,offr_sku_line       osl_sp
                                                 ,dstrbtd_mrkt_sls    dms_sp
                                                 ,offr                o_sp
                                            WHERE oppp.offr_prfl_prcpt_id =
                                                  osl_sp.offr_prfl_prcpt_id
                                              AND osl_sp.mrkt_id = l_mrkt_id
                                              AND osl_sp.offr_perd_id =
                                                  (SELECT MAX(tpymp.prev_yr_perd_id) keep(dense_rank LAST ORDER BY tpymp.last_updt_ts) AS prev_yr_perd_id
                                                     FROM trnsfrm_prev_yr_mrkt_perd tpymp
                                                    WHERE tpymp.mrkt_id =
                                                          l_mrkt_id
                                                      AND tpymp.perd_id =
                                                          l_offr_perd_id)
                                              AND dms_sp.offr_sku_line_id =
                                                  osl_sp.offr_sku_line_id
                                              AND dms_sp.sls_typ_id IN (6, 7)
                                              AND o_sp.ver_id = 0
                                              AND o_sp.offr_id = osl_sp.offr_id
                                            GROUP BY osl_sp.sku_id
                                                    ,osl_sp.veh_id
                                                    ,oppp.offr_prfl_prcpt_id)
                                    WHERE vehid <> o.veh_id
                                      AND skuid = osl_current.sku_id)) AS pc_sp_py
                        ,(SELECT srp.reg_prc_amt
                            FROM sku_reg_prc srp, sku s, mrkt_sku ms
                           WHERE s.prfl_cd = offr_prfl_prc_point.prfl_cd
                             AND ms.mrkt_id = l_mrkt_id
                             AND ms.sku_id = osl_current.sku_id
                             AND nvl(ms.dltd_ind, 'N') != 'Y'
                             AND srp.sku_id = s.sku_id
                             AND srp.mrkt_id = l_mrkt_id
                             AND srp.offr_perd_id = l_offr_perd_id
                             AND srp.reg_prc_amt = osl_current.reg_prc_amt
                             AND rownum < 2) AS pc_rp
                        ,CASE
                           WHEN pricing_used = 'Y' THEN
                            (SELECT MAX(mpsp.sku_prc_amt) opt_prc_amt
                               FROM mrkt_perd_sku_prc mpsp
                                   ,prc_lvl_typ       plt
                                   ,mrkt_prc_lvl      mpl
                              WHERE mpsp.mrkt_id = l_mrkt_id
                                AND mpsp.offr_perd_id = l_offr_perd_id
                                AND mpsp.sku_id IN
                                    (SELECT s.sku_id
                                       FROM sku s, sku_reg_prc srp, mrkt_sku ms
                                      WHERE s.prfl_cd =
                                            offr_prfl_prc_point.prfl_cd
                                        AND ms.mrkt_id = l_mrkt_id
                                        AND ms.sku_id = s.sku_id
                                        AND nvl(ms.dltd_ind, 'N') != 'Y'
                                        AND srp.mrkt_id = mpsp.mrkt_id
                                        AND srp.offr_perd_id = mpsp.offr_perd_id
                                        AND srp.sku_id = s.sku_id
                                        AND srp.reg_prc_amt =
                                            osl_current.reg_prc_amt)
                                AND plt.prc_lvl_typ_cd = mpsp.prc_lvl_typ_cd
                                AND plt.prc_lvl_typ_cd = 'SP'
                                AND nvl(plt.dltd_ind, 'N') != 'Y'
                                AND mpl.mrkt_id = mpsp.mrkt_id
                                AND mpl.prc_lvl_typ_cd = mpsp.prc_lvl_typ_cd
                                AND (mpl.strt_perd_id IS NULL OR
                                    mpl.strt_perd_id <= l_offr_perd_id)
                                AND (mpl.end_perd_id IS NULL OR
                                    mpl.end_perd_id >= l_offr_perd_id)
                              GROUP BY plt.seq_nr
                                      ,plt.prc_lvl_typ_id
                                      ,plt.prc_lvl_typ_cd
                                      ,plt.prc_lvl_typ_sdesc_txt
                                      ,mpl.prc_lvl_rng_pct
                                      ,nvl(mpsp.cmpnt_discnt_pct,
                                           mpl.dfalt_cmpnt_discnt_pct))
                           ELSE
                            NULL
                         END pc_sp
                        ,CASE
                           WHEN pricing_used = 'Y' THEN
                            (SELECT MAX(mpsp.sku_prc_amt) opt_prc_amt
                               FROM mrkt_perd_sku_prc mpsp
                                   ,prc_lvl_typ       plt
                                   ,mrkt_prc_lvl      mpl
                              WHERE mpsp.mrkt_id = l_mrkt_id
                                AND mpsp.offr_perd_id = l_offr_perd_id
                                AND mpsp.sku_id IN
                                    (SELECT s.sku_id
                                       FROM sku s, sku_reg_prc srp, mrkt_sku ms
                                      WHERE s.prfl_cd =
                                            offr_prfl_prc_point.prfl_cd
                                        AND ms.mrkt_id = l_mrkt_id
                                        AND ms.sku_id = s.sku_id
                                        AND nvl(ms.dltd_ind, 'N') != 'Y'
                                        AND srp.mrkt_id = mpsp.mrkt_id
                                        AND srp.offr_perd_id = mpsp.offr_perd_id
                                        AND srp.sku_id = s.sku_id
                                        AND srp.reg_prc_amt =
                                            osl_current.reg_prc_amt)
                                AND plt.prc_lvl_typ_cd = mpsp.prc_lvl_typ_cd
                                AND plt.prc_lvl_typ_cd = 'VSP'
                                AND nvl(plt.dltd_ind, 'N') != 'Y'
                                AND mpl.mrkt_id = mpsp.mrkt_id
                                AND mpl.prc_lvl_typ_cd = mpsp.prc_lvl_typ_cd
                                AND (mpl.strt_perd_id IS NULL OR
                                    mpl.strt_perd_id <= l_offr_perd_id)
                                AND (mpl.end_perd_id IS NULL OR
                                    mpl.end_perd_id >= l_offr_perd_id)
                              GROUP BY plt.seq_nr
                                      ,plt.prc_lvl_typ_id
                                      ,plt.prc_lvl_typ_cd
                                      ,plt.prc_lvl_typ_sdesc_txt
                                      ,mpl.prc_lvl_rng_pct
                                      ,nvl(mpsp.cmpnt_discnt_pct,
                                           mpl.dfalt_cmpnt_discnt_pct))
                           ELSE
                            NULL
                         END pc_vsp
                        ,CASE
                           WHEN pricing_used = 'Y' THEN
                            (SELECT MAX(mpsp.sku_prc_amt) opt_prc_amt
                               FROM mrkt_perd_sku_prc mpsp
                                   ,prc_lvl_typ       plt
                                   ,mrkt_prc_lvl      mpl
                              WHERE mpsp.mrkt_id = l_mrkt_id
                                AND mpsp.offr_perd_id = l_offr_perd_id
                                AND mpsp.sku_id IN
                                    (SELECT s.sku_id
                                       FROM sku s, sku_reg_prc srp, mrkt_sku ms
                                      WHERE s.prfl_cd =
                                            offr_prfl_prc_point.prfl_cd
                                        AND ms.mrkt_id = l_mrkt_id
                                        AND ms.sku_id = s.sku_id
                                        AND nvl(ms.dltd_ind, 'N') != 'Y'
                                        AND srp.mrkt_id = mpsp.mrkt_id
                                        AND srp.offr_perd_id = mpsp.offr_perd_id
                                        AND srp.sku_id = s.sku_id
                                        AND srp.reg_prc_amt =
                                            osl_current.reg_prc_amt)
                                AND plt.prc_lvl_typ_cd = mpsp.prc_lvl_typ_cd
                                AND plt.prc_lvl_typ_cd = 'HIT'
                                AND nvl(plt.dltd_ind, 'N') != 'Y'
                                AND mpl.mrkt_id = mpsp.mrkt_id
                                AND mpl.prc_lvl_typ_cd = mpsp.prc_lvl_typ_cd
                                AND (mpl.strt_perd_id IS NULL OR
                                    mpl.strt_perd_id <= l_offr_perd_id)
                                AND (mpl.end_perd_id IS NULL OR
                                    mpl.end_perd_id >= l_offr_perd_id)
                              GROUP BY plt.seq_nr
                                      ,plt.prc_lvl_typ_id
                                      ,plt.prc_lvl_typ_cd
                                      ,plt.prc_lvl_typ_sdesc_txt
                                      ,mpl.prc_lvl_rng_pct
                                      ,nvl(mpsp.cmpnt_discnt_pct,
                                           mpl.dfalt_cmpnt_discnt_pct))
                           ELSE
                            NULL
                         END pc_hit
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
--                        ,o.offr_typ
                        ,(SELECT sls_typ_nm
                            FROM sls_typ
                           WHERE sls_typ_id = l_sls_typ) AS sls_typ_nm
                        ,veh.veh_desc_txt AS veh_desc_txt
                        ,enrgy_chrt_postn.enrgy_chrt_postn_nm AS enrgy_chrt_postn_nm
                        ,catgry.catgry_nm AS ctgry_nm
                        ,brnd.brnd_nm AS brnd_nm
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
                    FROM (SELECT *
                            FROM offr
                           WHERE offr_id IN
                                 (SELECT p_offr_id FROM TABLE(p_get_offr))
                             AND offr.mrkt_id = l_mrkt_id
                             AND offr.offr_perd_id = l_offr_perd_id
                             AND offr.ver_id = l_ver_id) o
                        ,(SELECT offr_sku_line.offr_sku_line_id
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
                                  
                                  --                                     AND sls_perd_id = l_offr_perd_id --fcs case when on schedule and off schedule
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
                        ,(SELECT osl_ltst.*
                                ,CASE
                                   WHEN actual_sku.wghtd_avg_cost_amt IS NOT NULL THEN
                                    actual_sku.wghtd_avg_cost_amt
                                   ELSE
                                    planned_sku.wghtd_avg_cost_amt
                                 END sku_cost_amt
                                ,sku_reg_prc.reg_prc_amt
                                ,offr_prfl_prc_point.nr_for_qty
                                ,offr_prfl_prc_point.sls_prc_amt AS offr_prfl_sls_prc_amt
                          
                            FROM latest_osl_stuff    osl_ltst
                                ,offr_prfl_prc_point
                                ,sku_reg_prc
                                ,sku_cost            actual_sku
                                ,sku_cost            planned_sku
                           WHERE offr_prfl_prc_point.offr_prfl_prcpt_id =
                                 osl_ltst.offr_prfl_prcpt_id
                             AND osl_ltst.sku_id = actual_sku.sku_id(+)
                             AND actual_sku.cost_typ(+) = 'A'
                             AND actual_sku.mrkt_id(+) = l_mrkt_id
                             AND actual_sku.offr_perd_id(+) = l_offr_perd_id
                             AND osl_ltst.sku_id = planned_sku.sku_id(+)
                             AND planned_sku.mrkt_id(+) = l_mrkt_id
                             AND planned_sku.offr_perd_id(+) = l_offr_perd_id
                             AND planned_sku.cost_typ(+) = 'P'
                             AND sku_reg_prc.sku_id(+) = osl_ltst.sku_id
                             AND sku_reg_prc.offr_perd_id(+) = l_offr_perd_id
                             AND sku_reg_prc.mrkt_id(+) = l_mrkt_id
                             AND osl_ltst.mrkt_id = l_mrkt_id
                             AND osl_ltst.offr_perd_id = l_offr_perd_id) osl_latest
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
                        ,mrkt_tmp_fsc
                        ,mrkt_tmp_fsc_master
                        ,veh
                        ,enrgy_chrt_postn
                        ,catgry
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
                   WHERE
                  --mrkt_tmp_fsc and master
                   osl_current.sku_id = mrkt_tmp_fsc_master.sku_id(+)
               AND osl_current.sku_id = mrkt_tmp_fsc.sku_id(+)
               AND mrkt_tmp_fsc_master.mrkt_id(+) = osl_current.mrkt_id
               AND mrkt_tmp_fsc.mrkt_id(+) = osl_current.mrkt_id
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
               AND o.offr_typ = 'CMP'
               AND o.mrkt_id = l_mrkt_id
               AND o.offr_perd_id = l_offr_perd_id
               AND o.ver_id = l_ver_id
               AND osl_current.offr_sku_line_id =
                   osl_latest.offr_sku_line_link_id(+)
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
               AND mvps.veh_id = o.veh_id) LOOP
      PIPE ROW(obj_offer_api_line(rec.mrkt_id,
                                  rec.mrkt_nm,
                                  rec.offr_perd_id,
                                  rec.sls_perd_id,
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
                                  rec.cnsmr_invstmt_bdgt_desc_txt,
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
                                  rec.sku_cost,
                                  rec.lv_nta,
                                  rec.lv_sp,
                                  rec.lv_rp,
                                  rec.lv_discount,
                                  rec.lv_units,
                                  rec.lv_total_cost,
                                  rec.lv_gross_sales,
                                  rec.lv_dp_cash,
                                  rec.lv_dp_percent,
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
                                  rec.pc_sp_py,
                                  rec.pc_rp,
                                  rec.pc_sp,
                                  rec.pc_vsp,
                                  rec.pc_hit,
                                  rec.pg_wght,
                                  rec.sprd_nr,
                                  rec.offr_prfl_prcpt_id));
    END LOOP;
    app_plsql_log.info(l_module_name || ' stop');
  EXCEPTION
    WHEN OTHERS THEN
      app_plsql_log.info('Error in get offr' || SQLERRM(SQLCODE));
  END get_offr;

  FUNCTION get_edit_offr_table(p_filters IN obj_offer_api_filter)
    RETURN obj_offer_api_table
    PIPELINED AS
  
    -- local variables
    -- for LOG
    l_run_id  NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id VARCHAR(35) := USER();
    --
    l_module_name    VARCHAR2(30) := 'GET_OFFER_API_TABLE';
    l_get_offr_table obj_get_offr_api_table;
    --l_obj_edit_offr_table obj_offer_api_table;
    l_offr_api_filter obj_offer_api_fltr_tbl;
  
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start');
    l_get_offr_table := obj_get_offr_api_table();
    -- l_obj_edit_offr_table := obj_offer_api_table();
    l_offr_api_filter := obj_offer_api_fltr_tbl();
  
    FOR m IN 1 .. p_filters.p_mrkt_id.count LOOP
      FOR o IN 1 .. p_filters.p_offr_perd_id.count LOOP
        l_offr_api_filter.extend();
        l_offr_api_filter(l_offr_api_filter.last) := obj_offer_api_fltr_line(p_filters.p_mrkt_id(m),
                                                                             p_filters.p_offr_perd_id(o),
                                                                             p_filters.p_ver_id,
                                                                             p_filters.p_veh_id,
                                                                             p_filters.p_brchr_plcmt_id,
                                                                             p_filters.p_catgry_id,
                                                                             p_filters.p_brnd_id,
                                                                             p_filters.p_prfl_cd,
                                                                             p_filters.p_sku_id,
                                                                             p_filters.p_fsc_cd,
                                                                             p_filters.p_sls_typ,
                                                                             p_filters.p_dltd_ind,
                                                                             p_filters.p_offr_id,
                                                                             p_filters.p_enrgy_chrt_postn_id,
                                                                             p_filters.p_on_schedule);
        --dbms_output.put_line(filter_test.p_mrkt_id(m) || ', ' || filter_test.p_offr_perd_id(o));
      END LOOP;
    END LOOP;
  
    FOR p_filter IN (SELECT * FROM TABLE(l_offr_api_filter)) LOOP
      FOR offrs IN (
                    --Return every offer id where not every item is disabled.
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
                                 WHERE p_filter.p_mrkt_id = mrkt_id
                                   AND p_filter.p_offr_perd_id >= strt_perd_id)
                         WHERE p_filter.p_offr_perd_id >= from_strt_perd_id
                           AND p_filter.p_offr_perd_id < to_strt_perd_id),
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
                                 WHERE p_filter.p_mrkt_id = mrkt_id
                                   AND p_filter.p_offr_perd_id >= strt_perd_id
                                   AND 'N' = dltd_ind)
                         WHERE p_filter.p_offr_perd_id >= from_strt_perd_id
                           AND p_filter.p_offr_perd_id < to_strt_perd_id
                         GROUP BY mrkt_id, sku_id)
                      SELECT o.offr_id          AS p_offr_id
                            ,p_filter.p_sls_typ AS p_sls_typ
                        FROM offr                o
                            ,offr_sku_line       osl
                            ,offr_prfl_prc_point
                            ,prfl
                            ,brnd
                            ,brnd_grp
                            ,mrkt_tmp_fsc
                            ,mrkt_tmp_fsc_master
                            ,dstrbtd_mrkt_sls
                       WHERE --offr_sku_line join
                       o.offr_id = osl.offr_id(+)
                   AND o.offr_typ = 'CMP'
                   AND o.mrkt_id = p_filter.p_mrkt_id
                   AND o.offr_perd_id = p_filter.p_offr_perd_id
                   AND o.ver_id = p_filter.p_ver_id
                      --prfl, brnd, brnd_grp join
                   AND brnd.brnd_id(+) = prfl.brnd_id
                   AND brnd_grp.brnd_grp_id(+) = brnd.brnd_grp_id
                   AND prfl.prfl_cd(+) = offr_prfl_prc_point.prfl_cd
                   AND o.offr_perd_id = p_filter.p_offr_perd_id
                      --mrkt_tmp_fsc and master
                   AND osl.sku_id = mrkt_tmp_fsc_master.sku_id(+)
                   AND osl.sku_id = mrkt_tmp_fsc.sku_id(+)
                   AND mrkt_tmp_fsc_master.mrkt_id(+) = osl.mrkt_id
                   AND mrkt_tmp_fsc.mrkt_id(+) = osl.mrkt_id
                      --offr_prfl_prc_point
                   AND offr_prfl_prc_point.offr_prfl_prcpt_id(+) =
                       osl.offr_prfl_prcpt_id
                   AND dstrbtd_mrkt_sls.offr_sku_line_id(+) =
                       osl.offr_sku_line_id
                      --FILTERS
                   AND dstrbtd_mrkt_sls.sls_typ_id(+) = p_filter.p_sls_typ
                   AND CASE
                         WHEN p_filter.p_offr_id IS NULL THEN
                          1
                         WHEN o.offr_id IN
                              (SELECT * FROM TABLE(p_filter.p_offr_id)) THEN
                          1
                         ELSE
                          0
                       END = 1
                   AND CASE
                         WHEN p_filter.p_veh_id IS NULL THEN
                          1
                         WHEN o.veh_id IN
                              (SELECT * FROM TABLE(p_filter.p_veh_id)) THEN
                          1
                         ELSE
                          0
                       END = 1
                   AND CASE
                         WHEN p_filter.p_brchr_plcmt_id IS NULL THEN
                          1
                         WHEN o.brchr_plcmt_id IN
                              (SELECT * FROM TABLE(p_filter.p_brchr_plcmt_id)) THEN
                          1
                         ELSE
                          0
                       END = 1
                   AND CASE
                         WHEN p_filter.p_catgry_id IS NULL THEN
                          1
                         WHEN prfl.catgry_id IN
                              (SELECT * FROM TABLE(p_filter.p_catgry_id)) THEN
                          1
                         ELSE
                          0
                       END = 1
                   AND CASE
                         WHEN p_filter.p_brnd_id IS NULL THEN
                          1
                         WHEN brnd_grp.brnd_fmly_id IN
                              (SELECT * FROM TABLE(p_filter.p_brnd_id)) THEN
                          1
                         ELSE
                          0
                       END = 1
                   AND CASE
                         WHEN p_filter.p_prfl_cd IS NULL THEN
                          1
                         WHEN offr_prfl_prc_point.prfl_cd IN
                              (SELECT * FROM TABLE(p_filter.p_prfl_cd)) THEN
                          1
                         ELSE
                          0
                       END = 1
                   AND CASE
                         WHEN p_filter.p_sku_id IS NULL THEN
                          1
                         WHEN osl.sku_id IN
                              (SELECT * FROM TABLE(p_filter.p_sku_id)) THEN
                          1
                         ELSE
                          0
                       END = 1
                   AND CASE
                         WHEN p_filter.p_fsc_cd IS NULL THEN
                          1
                         WHEN nvl(mrkt_tmp_fsc_master.fsc_cd,
                                  mrkt_tmp_fsc.fsc_cd) IN
                              (SELECT * FROM TABLE(p_filter.p_fsc_cd)) THEN
                          1
                         ELSE
                          0
                       END = 1
                   AND CASE
                         WHEN p_filter.p_enrgy_chrt_postn_id IS NULL THEN
                          1
                         WHEN o.enrgy_chrt_postn_id IN
                              (SELECT *
                                 FROM TABLE(p_filter.p_enrgy_chrt_postn_id)) THEN
                          1
                         ELSE
                          0
                       END = 1
                       GROUP BY o.offr_id) LOOP
        --get valid offer id-s loop
      
        l_get_offr_table.extend();
        l_get_offr_table(l_get_offr_table.last) := obj_get_offr_api_line(offrs.p_offr_id,
                                                                         offrs.p_sls_typ,
                                                                         p_filter.p_on_schedule);
      END LOOP; --get valid offer id-s loop
    
      FOR rec IN (SELECT o.*
                    FROM TABLE(get_offr(l_get_offr_table)) o, mrkt_veh mv
                   WHERE mv.mrkt_id(+) = o.mrkt_id
                     AND mv.veh_id(+) = o.veh_id
                   ORDER BY mv.seq_nr
                           ,o.pg_nr
                           ,o.intrnl_offr_id
                           ,o.prfl_nm
                           ,o.sku_nm) LOOP
        --Result loop
      
        PIPE ROW(obj_offer_api_line(rec.mrkt_id,
                                    rec.mrkt_nm,
                                    rec.offr_perd_id,
                                    rec.sls_perd_id,
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
                                    rec.cnsmr_invstmt_bdgt_desc_txt,
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
                                    rec.sku_cost,
                                    rec.lv_nta,
                                    rec.lv_sp,
                                    rec.lv_rp,
                                    rec.lv_discount,
                                    rec.lv_units,
                                    rec.lv_total_cost,
                                    rec.lv_gross_sales,
                                    rec.lv_dp_cash,
                                    rec.lv_dp_percent,
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
                                    rec.pc_sp_py,
                                    rec.pc_rp,
                                    rec.pc_sp,
                                    rec.pc_vsp,
                                    rec.pc_hit,
                                    rec.pg_wght,
                                    rec.sprd_nr,
                                    rec.offr_prfl_prcpt_id));
      
      END LOOP; --result loop
    
      l_get_offr_table.delete(); --empty the offer id-s for every filter line
    END LOOP; --Filters from the screen loop
    app_plsql_log.info(l_module_name || ' stop');
  EXCEPTION
    WHEN no_data_needed THEN
      app_plsql_log.info('no mor data needed from offer api');
    WHEN OTHERS THEN
      app_plsql_log.info('Error in offer api' || SQLERRM(SQLCODE));
    
  END get_edit_offr_table;
    
  PROCEDURE save_offer_api_lines(p_offr_id   IN NUMBER,
                                 p_sls_typ   IN NUMBER,
                                 p_ctrl_id   IN NUMBER) IS
  
    l_module_name          VARCHAR2(30) := 'save_offer_api_lines_OFFER_API';
    l_log                  VARCHAR2(1000);
    l_cnt                  NUMBER;
    l_sku_cnt              NUMBER;
    l_old_micr_ncpsltn_ind CHAR(1);
    l_old_sls_prc_amt      NUMBER;
    l_old_cmpnt_qty        NUMBER;
    l_old_unit_qty         NUMBER;
    l_old_nr_for_qty       NUMBER;
    l_no_sku_line          BOOLEAN;
    l_oss_id_to_zero       NUMBER := NULL;
    l_rowcount             NUMBER;
  
    CURSOR c_p_data_line IS
      SELECT * FROM offr_api_stg WHERE ctrl_id = p_ctrl_id AND
             intrnl_offr_id = p_offr_id
         AND sls_typ        = p_sls_typ;
  
  BEGIN
    app_plsql_log.info(l_module_name || ' start');
  
    -- insert into offr_prfl_sls_cls_plcmt, offr_sls_cls_sku, pg_wght if necessary
    l_log := 'insert (offr_prfl_sls_cls_plcmt, offr_sls_cls_sku, pg_wght)';
    FOR rec IN (
      SELECT osl.pg_ofs_nr
            ,l.*
        FROM (SELECT * FROM offr_api_stg WHERE ctrl_id = p_ctrl_id) l
            ,offr_sku_line osl
       WHERE osl.offr_sku_line_id = l.offr_sku_line_id
         AND l.intrnl_offr_id     = p_offr_id
         AND l.sls_typ            = p_sls_typ
    )
    LOOP
      SELECT COUNT(*)
        INTO l_sku_cnt
        FROM offr_sku_line osl
       WHERE osl.offr_id = p_offr_id
         AND osl.prfl_cd = rec.prfl_cd;
  
      SELECT COUNT(*)
        INTO l_cnt
        FROM offr_prfl_sls_cls_plcmt p
       WHERE p.offr_id        = rec.intrnl_offr_id
         AND p.sls_cls_cd     = rec.sls_cls_cd
         AND p.prfl_cd        = rec.prfl_cd
         AND p.pg_ofs_nr      = rec.pg_ofs_nr
         AND p.featrd_side_cd = rec.concept_featrd_side_cd;
  
      IF l_cnt = 0 THEN
        INSERT INTO offr_prfl_sls_cls_plcmt
        (
          offr_id, sls_cls_cd, prfl_cd, pg_ofs_nr, featrd_side_cd, mrkt_id, veh_id, offr_perd_id, sku_cnt,
          pg_wght_pct, sku_offr_strgth_pct, pg_typ_id, creat_user_id, last_updt_user_id
        )
        VALUES
        (
          rec.intrnl_offr_id, rec.sls_cls_cd, rec.prfl_cd, rec.pg_ofs_nr, rec.concept_featrd_side_cd, rec.mrkt_id,
          rec.veh_id, rec.offr_perd_id, l_sku_cnt, rec.pg_wght, 0, 1, rec.user_id, rec.user_id
        );
      END IF;
  
      SELECT COUNT(*)
        INTO l_cnt
        FROM offr_sls_cls_sku s
       WHERE s.offr_id        = rec.intrnl_offr_id
         AND s.sls_cls_cd     = rec.sls_cls_cd
         AND s.prfl_cd        = rec.prfl_cd
         AND s.pg_ofs_nr      = rec.pg_ofs_nr
         AND s.featrd_side_cd = rec.concept_featrd_side_cd
         AND s.sku_id         = rec.sku_id;
  
      IF l_cnt = 0 THEN
        INSERT INTO offr_sls_cls_sku
        (
          offr_id, sls_cls_cd, prfl_cd, pg_ofs_nr, featrd_side_cd, sku_id, mrkt_id, smplg_ind, hero_ind,
          micr_ncpsltn_ind, reg_prc_amt, incntv_id, cost_amt, mltpl_ind, cmltv_ind, wsl_ind,
          creat_user_id, last_updt_user_id
        )
        VALUES
        (
          rec.intrnl_offr_id, rec.sls_cls_cd, rec.prfl_cd, rec.pg_ofs_nr, rec.concept_featrd_side_cd, rec.sku_id,
          rec.mrkt_id, 'N', 'N', rec.micr_ncpsltn_ind, rec.reg_prc_amt, NULL, NULL, 'N', 'N', rec.wsl_ind,
          rec.user_id, rec.user_id
        );
      END IF;
  
      SELECT COUNT(*)
        INTO l_cnt
        FROM pg_wght w
       WHERE w.pg_wght_pct = rec.pg_wght;
  
      IF l_cnt = 0 THEN
        INSERT INTO pg_wght
        (
          pg_wght_pct, mrkt_id, pg_wght_desc_txt, creat_user_id, last_updt_user_id
        )
        VALUES
        (
          rec.pg_wght, rec.mrkt_id, to_char(rec.pg_wght) || '%', rec.user_id, rec.user_id
        );
      END IF;
    END LOOP;
    app_plsql_log.info(l_log || ' finished');
  
    l_log := 'mrkt_veh_perd_sctn';
    MERGE INTO mrkt_veh_perd_sctn s
    USING (SELECT DISTINCT l.brchr_sctn_nm
                          ,l.brchr_plcmnt_id
                          ,l.mrkt_veh_perd_sctn_id
                          ,l.mrkt_id
                          ,l.offr_perd_id
                          ,l.ver_id
                          ,l.veh_id
                          ,l.user_id
           FROM (SELECT * FROM offr_api_stg WHERE ctrl_id = p_ctrl_id) l
           WHERE l.intrnl_offr_id = p_offr_id
             AND l.sls_typ        = p_sls_typ) dl
    ON (s.mrkt_veh_perd_sctn_id = dl.mrkt_veh_perd_sctn_id
    AND s.mrkt_id               = dl.mrkt_id
    AND s.offr_perd_id          = dl.offr_perd_id
    AND s.brchr_plcmt_id        = dl.brchr_plcmnt_id
    AND s.ver_id                = dl.ver_id
    AND s.veh_id                = dl.veh_id
    )
    WHEN MATCHED THEN UPDATE
    SET s.sctn_nm           = dl.brchr_sctn_nm
       ,s.last_updt_user_id = dl.user_id;
  
    l_rowcount := SQL%ROWCOUNT;
    app_plsql_log.info(l_log || ' finished, merge rowcount: ' || l_rowcount);
  
    l_log := 'offr_sku_set';
    MERGE INTO offr_sku_set s
    USING (SELECT CASE
                    WHEN MIN(oss.set_prc_amt) <> SUM(l.sls_prc_amt * osl.set_cmpnt_qty) THEN
                      3
                    ELSE
                      MIN(oss.set_prc_typ_id)
                  END AS set_prc_typ_id
                 ,CASE
                    WHEN MIN(oss.set_prc_amt) <> SUM(l.sls_prc_amt * osl.set_cmpnt_qty) THEN
                      SUM(l.sls_prc_amt * osl.set_cmpnt_qty)
                    ELSE
                      MIN(oss.set_prc_amt)
                  END AS sum_prc_amt
                 ,l.offr_sku_set_nm
                 ,l.offr_sku_set_id
                 ,l.concept_featrd_side_cd
                 ,l.user_id
             FROM offr_sku_line osl
                 ,offr_sku_set oss
                 ,(SELECT * FROM offr_api_stg WHERE ctrl_id = p_ctrl_id) l
            WHERE osl.offr_sku_line_id = l.offr_sku_line_id
              AND osl.offr_sku_set_id  = oss.offr_sku_set_id
              AND l.intrnl_offr_id     = p_offr_id
              AND l.sls_typ            = p_sls_typ
           GROUP BY l.offr_sku_set_id
                   ,l.offr_sku_set_id
                   ,l.offr_sku_set_nm
                   ,l.concept_featrd_side_cd
                   ,l.user_id) dl
    ON (s.offr_sku_set_id = dl.offr_sku_set_id)
    WHEN MATCHED THEN UPDATE
    SET s.set_prc_amt       = dl.sum_prc_amt
       ,s.set_prc_typ_id    = dl.set_prc_typ_id
       ,s.offr_sku_set_nm   = dl.offr_sku_set_nm
       ,s.featrd_side_cd    = dl.concept_featrd_side_cd
       ,s.last_updt_user_id = dl.user_id;
  
    l_rowcount := SQL%ROWCOUNT;
    app_plsql_log.info(l_log || ' finished, merge rowcount: ' || l_rowcount);
  
    l_log := 'offr_sls_cls_sku';
    FOR rec IN (
      SELECT osl.pg_ofs_nr
            ,osl.featrd_side_cd AS old_conc_featrd_side_cd
            ,l.sku_id
            ,l.wsl_ind
            ,l.concept_featrd_side_cd
            ,l.intrnl_offr_id
            ,l.sls_cls_cd
            ,l.prfl_cd
            ,l.micr_ncpsltn_ind
            ,l.user_id
        FROM (SELECT * FROM offr_api_stg WHERE ctrl_id = p_ctrl_id) l
            ,offr_sku_line osl
       WHERE osl.offr_sku_line_id = l.offr_sku_line_id
         AND l.intrnl_offr_id     = p_offr_id
         AND l.sls_typ            = p_sls_typ
    )
    LOOP
      BEGIN
        UPDATE offr_sls_cls_sku s
           SET s.wsl_ind          = rec.wsl_ind
              ,s.featrd_side_cd   = rec.concept_featrd_side_cd
              ,s.micr_ncpsltn_ind = rec.micr_ncpsltn_ind
              ,s.last_updt_user_id = rec.user_id
         WHERE s.offr_id        = rec.intrnl_offr_id
           AND s.sls_cls_cd     = rec.sls_cls_cd
           AND s.prfl_cd        = rec.prfl_cd
           AND s.pg_ofs_nr      = rec.pg_ofs_nr
           AND s.featrd_side_cd = rec.old_conc_featrd_side_cd
           AND s.sku_id         = rec.sku_id;
      EXCEPTION
        WHEN dup_val_on_index THEN
          -- offr_sls_cls_sku couldn't have been updated (already exists with this concept feature side)
          RAISE e_oscs_dup_val;
      END;
    END LOOP;
  
    app_plsql_log.info(l_log || ' finished');
  
    -- update dstrbtd_mrkt_sls
    l_log := 'dstrbtd_mrkt_sls';
    FOR r_data_line IN c_p_data_line LOOP
  
      l_no_sku_line := FALSE;
      BEGIN
        SELECT micr_ncpsltn_ind
          INTO l_old_micr_ncpsltn_ind
          FROM offr_sls_cls_sku oscs
         WHERE oscs.offr_id        = r_data_line.intrnl_offr_id
           AND oscs.sls_cls_cd     = r_data_line.sls_cls_cd
           AND oscs.prfl_cd        = r_data_line.prfl_cd
           AND oscs.pg_ofs_nr      = (SELECT osl.pg_ofs_nr
                                        FROM offr_sku_line osl
                                       WHERE osl.offr_sku_line_id = r_data_line.offr_sku_line_id)
           AND oscs.featrd_side_cd = r_data_line.concept_featrd_side_cd
           AND oscs.sku_id         = r_data_line.sku_id;
      EXCEPTION
        WHEN no_data_found THEN
          -- there is no offr_sku_line from this offr_id
          l_no_sku_line := TRUE;
      END;
  
      IF l_no_sku_line = FALSE THEN
        SELECT oppp.sls_prc_amt
              ,oppp.nr_for_qty
              ,osl.set_cmpnt_qty
          INTO l_old_sls_prc_amt
              ,l_old_nr_for_qty
              ,l_old_cmpnt_qty
          FROM offr_prfl_prc_point oppp
              ,offr_sku_line osl
         WHERE osl.offr_prfl_prcpt_id = oppp.offr_prfl_prcpt_id
           AND osl.offr_sku_line_id = r_data_line.offr_sku_line_id;
  /*
        SELECT COUNT(*) -- check config_item
          INTO l_cnt
          FROM mrkt_config_item m
         WHERE m.config_item_id = co_config_item_id
           AND m.mrkt_config_item_val_txt = 'Y'
           AND m.mrkt_id = r_data_line.mrkt_id;
  */
        SELECT SUM(dms.unit_qty) old_unit_qty
          INTO l_old_unit_qty
          FROM dstrbtd_mrkt_sls dms
         WHERE dms.mrkt_id          = r_data_line.mrkt_id
           AND dms.offr_perd_id     = r_data_line.offr_perd_id
           AND dms.sls_perd_id      = r_data_line.offr_perd_id
           AND dms.offr_sku_line_id = r_data_line.offr_sku_line_id
           AND dms.sls_typ_id       = r_data_line.sls_typ
           AND dms.ver_id           = r_data_line.ver_id;
  
        IF l_oss_id_to_zero = r_data_line.offr_sku_set_id OR
          (l_cnt > 0
          AND (l_old_sls_prc_amt <> r_data_line.sls_prc_amt
            OR l_old_nr_for_qty <> r_data_line.nr_for_qty
            OR l_old_cmpnt_qty <> r_data_line.cmpnt_qty
            OR NVL(l_old_micr_ncpsltn_ind, -1) <> NVL(r_data_line.micr_ncpsltn_ind, -1))
          AND l_old_unit_qty = r_data_line.unit_qty)
        THEN
          l_oss_id_to_zero := NVL(r_data_line.offr_sku_set_id, -1);
          UPDATE dstrbtd_mrkt_sls dms
             SET dms.unit_qty = 0
                ,dms.tax_amt   = pa_maps_gta.pri_get_tax_amount(
                                         dms.mrkt_id,
                                         r_data_line.tax_type_id,
                                         dms.offr_perd_id)
                ,dms.comsn_amt = pa_maps_gta.get_commission_percentage(
                                         dms.mrkt_id,
                                         dms.offr_perd_id,
                                         r_data_line.comsn_typ)
                ,dms.last_updt_user_id = r_data_line.user_id
           WHERE dms.mrkt_id           = r_data_line.mrkt_id
             AND dms.offr_perd_id      = r_data_line.offr_perd_id
             AND CASE
                   WHEN r_data_line.offr_sku_set_id IS NULL
                    AND dms.offr_sku_line_id = r_data_line.offr_sku_line_id THEN
                     1
                   WHEN r_data_line.offr_sku_set_id IS NOT NULL
                    AND dms.offr_sku_line_id IN (SELECT offr_sku_line_id
                                                 FROM offr_sku_line
                                                WHERE offr_sku_set_id = r_data_line.offr_sku_set_id
                                              ) THEN
                     1
                   ELSE
                     0
                 END = 1
             AND dms.sls_typ_id = r_data_line.sls_typ
             AND dms.ver_id     = r_data_line.ver_id;
        END IF;
      END IF;
    END LOOP;
  
    IF l_oss_id_to_zero IS NULL THEN
      MERGE INTO dstrbtd_mrkt_sls dms
      USING (SELECT *
               FROM offr_api_stg WHERE ctrl_id = p_ctrl_id
                AND intrnl_offr_id = p_offr_id
                AND sls_typ        = p_sls_typ) dl
      ON (dms.offr_sku_line_id = dl.offr_sku_line_id
      AND dms.offr_perd_id     = dl.offr_perd_id
      AND dms.sls_perd_id      = dl.offr_perd_id
      AND dms.veh_id           = dl.veh_id
      AND dms.ver_id           = dl.ver_id
      AND dms.sls_typ_id       = dl.sls_typ)
      WHEN MATCHED THEN UPDATE
      SET dms.unit_qty = dl.unit_qty
         ,dms.tax_amt   = pa_maps_gta.pri_get_tax_amount(
                                         dms.mrkt_id,
                                         dl.tax_type_id,
                                         dms.offr_perd_id)
         ,dms.comsn_amt = pa_maps_gta.get_commission_percentage(
                                         dms.mrkt_id,
                                         dms.offr_perd_id,
                                         dl.comsn_typ)
         ,dms.last_updt_user_id = dl.user_id;
  
      -- Calculate the offset units
      pa_maps_gta.set_offset_units(poffer_id => p_offr_id, psales_type_id => p_sls_typ);
    END IF;
  
    app_plsql_log.info(l_log || ' finished');
  
    l_log := 'offr_sku_line';
    MERGE INTO offr_sku_line osl
    USING (SELECT *
             FROM offr_api_stg WHERE ctrl_id = p_ctrl_id
              AND intrnl_offr_id = p_offr_id
              AND sls_typ        = p_sls_typ) dl
    ON (osl.offr_sku_line_id = dl.offr_sku_line_id)
    WHEN MATCHED THEN UPDATE
    SET osl.featrd_side_cd = dl.concept_featrd_side_cd
       ,osl.set_cmpnt_qty = dl.cmpnt_qty
       ,osl.dltd_ind = dl.dltd_ind
       ,osl.sls_prc_amt = dl.sls_prc_amt
       ,osl.last_updt_user_id = dl.user_id;
  
    l_rowcount := SQL%ROWCOUNT;
    app_plsql_log.info(l_log || ' finished, merge rowcount: ' || l_rowcount);
  
    l_log := 'offr';
    MERGE INTO offr o
    USING (SELECT DISTINCT intrnl_offr_id
                          ,enrgy_chrt_postn_id
                          ,offr_desc_txt
                          ,offr_notes_txt
                          ,offr_lyot_cmnts_txt
                          ,featrd_side_cd
                          ,(SELECT MAX(NVL(oscs.micr_ncpsltn_ind, 'N'))
                              FROM offr_sls_cls_sku oscs,
                                   offr_sku_line osl
                             WHERE oscs.offr_id        = osl.offr_id
                               AND oscs.sls_cls_cd     = osl.sls_cls_cd
                               AND oscs.prfl_cd        = osl.prfl_cd
                               AND oscs.pg_ofs_nr      = osl.pg_ofs_nr
                               AND oscs.featrd_side_cd = osl.featrd_side_cd
                               AND oscs.sku_id         = osl.sku_id
                               AND oscs.offr_id        = intrnl_offr_id) AS micr_ncpsltn_ind
                          ,pg_wght
                          --,offr_typ
                          ,user_id
             FROM offr_api_stg
            WHERE ctrl_id = p_ctrl_id AND intrnl_offr_id = p_offr_id
              AND sls_typ        = p_sls_typ) dl
    ON (o.offr_id = dl.intrnl_offr_id)
    WHEN MATCHED THEN UPDATE
    SET o.enrgy_chrt_postn_id   = dl.enrgy_chrt_postn_id
       ,o.offr_desc_txt         = dl.offr_desc_txt
       ,o.offr_ntes_txt         = dl.offr_notes_txt
       ,o.offr_lyot_cmnts_txt   = dl.offr_lyot_cmnts_txt
       ,o.featrd_side_cd        = dl.featrd_side_cd
       ,o.micr_ncpsltn_desc_txt = CASE
                                    WHEN NVL(dl.micr_ncpsltn_ind, -1) <> NVL(o.micr_ncpsltn_ind, -1) AND NVL(dl.micr_ncpsltn_ind, 'N') = 'Y' THEN
                                      'Microencapsulation'
                                    WHEN NVL(dl.micr_ncpsltn_ind, -1) <> NVL(o.micr_ncpsltn_ind, -1) AND NVL(dl.micr_ncpsltn_ind, 'N') = 'N' THEN
                                      NULL
                                    ELSE
                                      o.micr_ncpsltn_desc_txt
                                  END
       ,o.micr_ncpsltn_ind      = dl.micr_ncpsltn_ind
       ,o.pg_wght_pct           = dl.pg_wght
      -- ,o.offr_typ              = dl.offr_typ
       ,o.last_updt_user_id     = dl.user_id;
  
    l_rowcount := SQL%ROWCOUNT;
    app_plsql_log.info(l_log || ' finished, merge rowcount: ' || l_rowcount);
  
    l_log := 'offr_prfl_prc_point';
    MERGE INTO offr_prfl_prc_point p
    USING (SELECT DISTINCT osl.offr_prfl_prcpt_id
                          ,l.concept_featrd_side_cd
                          ,l.cnsmr_invstmt_bdgt_id
                          ,l.pymt_typ
                          ,l.promtn_id
                          ,l.promtn_clm_id
                          ,l.comsn_typ
                          ,l.nr_for_qty
                          ,l.sls_prc_amt
                          ,l.tax_type_id
                          ,pa_maps_gta.pri_get_tax_amount(l.mrkt_id, l.tax_type_id, l.offr_perd_id) AS tax_amt
                          ,pa_maps_gta.get_commission_percentage(l.mrkt_id, l.offr_perd_id, l.comsn_typ) AS comsn_amt
                          ,pa_maps_gta.get_gta_without_price_point(
                               pa_maps_gta.pri_get_gta_method_id(oppp.mrkt_id, oppp.offr_perd_id),
                               l.sls_prc_amt,
                               oppp.chrty_amt,
                               oppp.awrd_sls_prc_amt + l.sls_prc_amt,
                               pa_maps_gta.get_commission_percentage(l.mrkt_id, l.offr_perd_id, l.comsn_typ),
                               pa_maps_gta.pri_get_tax_amount(l.mrkt_id, l.tax_type_id, l.offr_perd_id),
                               oppp.roylt_pct) AS net_to_avon_fct
                          ,l.user_id
             FROM (SELECT * FROM offr_api_stg WHERE ctrl_id = p_ctrl_id) l
                 ,offr_sku_line osl
                 ,offr_prfl_prc_point oppp
            WHERE osl.offr_sku_line_id    = l.offr_sku_line_id
              AND oppp.offr_prfl_prcpt_id = osl.offr_prfl_prcpt_id
              AND l.intrnl_offr_id        = p_offr_id
              AND l.sls_typ               = p_sls_typ) dl
    ON (p.offr_prfl_prcpt_id = dl.offr_prfl_prcpt_id)
    WHEN MATCHED THEN UPDATE
    SET p.featrd_side_cd        = dl.concept_featrd_side_cd
       ,p.cnsmr_invstmt_bdgt_id = dl.cnsmr_invstmt_bdgt_id
       ,p.pymt_typ              = dl.pymt_typ
       ,p.promtn_id             = dl.promtn_id
       ,p.promtn_clm_id         = dl.promtn_clm_id
       ,p.comsn_typ             = dl.comsn_typ
       ,p.nr_for_qty            = dl.nr_for_qty
       ,p.sls_prc_amt           = dl.sls_prc_amt
       ,p.tax_type_id           = dl.tax_type_id
       ,p.tax_amt               = dl.tax_amt
       ,p.comsn_amt             = dl.comsn_amt
       ,p.net_to_avon_fct       = dl.net_to_avon_fct
       ,p.last_updt_user_id     = dl.user_id;
  
    l_rowcount := SQL%ROWCOUNT;
    app_plsql_log.info(l_log || ' finished, merge rowcount: ' || l_rowcount);
  
    l_log := 'dstrbtd_mrkt_sls:gta';
    FOR r_data_line IN (
      SELECT DISTINCT mrkt_id
                     ,veh_id
                     ,offr_perd_id
                     ,ver_id
                     ,sls_typ
                     ,intrnl_offr_id
                     ,user_id
        FROM offr_api_stg
       WHERE ctrl_id = p_ctrl_id
         AND intrnl_offr_id = p_offr_id
         AND sls_typ        = p_sls_typ
    )
    LOOP
      UPDATE dstrbtd_mrkt_sls dms
         SET dms.net_to_avon_fct =
           pa_maps_gta.get_gta_without_price_point(
                       pa_maps_gta.pri_get_gta_method_id(dms.mrkt_id,
                                                         dms.offr_perd_id
                                                        ),
                                (SELECT oppp.sls_prc_amt
                                   FROM offr_prfl_prc_point oppp,
                                        offr_sku_line osl
                                  WHERE dms.offr_sku_line_id = osl.offr_sku_line_id
                                    AND osl.offr_prfl_prcpt_id = oppp.offr_prfl_prcpt_id),
                                (SELECT oppp.chrty_amt
                                   FROM offr_prfl_prc_point oppp,
                                        offr_sku_line osl
                                  WHERE dms.offr_sku_line_id = osl.offr_sku_line_id
                                    AND osl.offr_prfl_prcpt_id = oppp.offr_prfl_prcpt_id),
                                (SELECT oppp.sls_prc_amt + oppp.awrd_sls_prc_amt
                                   FROM offr_prfl_prc_point oppp,
                                        offr_sku_line osl
                                  WHERE dms.offr_sku_line_id = osl.offr_sku_line_id
                                    AND osl.offr_prfl_prcpt_id = oppp.offr_prfl_prcpt_id),
                                dms.comsn_amt,
                                dms.tax_amt,
                                (SELECT oppp.roylt_pct
                                   FROM offr_prfl_prc_point oppp,
                                        offr_sku_line osl
                                  WHERE dms.offr_sku_line_id = osl.offr_sku_line_id
                                    AND osl.offr_prfl_prcpt_id = oppp.offr_prfl_prcpt_id)
                        )
         ,last_updt_user_id  = r_data_line.user_id
      WHERE dms.mrkt_id      = r_data_line.mrkt_id
        AND dms.veh_id       = r_data_line.veh_id
        AND dms.offr_perd_id = r_data_line.offr_perd_id
        AND dms.ver_id       = r_data_line.ver_id
        AND dms.sls_typ_id   = r_data_line.sls_typ
        AND r_data_line.intrnl_offr_id =
                  (SELECT oppp.offr_id
                     FROM offr_prfl_prc_point oppp, offr_sku_line osl
                    WHERE dms.offr_sku_line_id = osl.offr_sku_line_id
                      AND osl.offr_prfl_prcpt_id = oppp.offr_prfl_prcpt_id);
    END LOOP;
  
    app_plsql_log.info(l_log || ' finished');
  
    -- delete offr_prfl_sls_cls_plcmt when there is no child in offr_prfl_prc_point and offr_sls_cls_sku
    l_log := 'delete offr_sls_cls_sku and offr_prfl_sls_cls_plcmt';
    FOR rec IN (
      SELECT osl.pg_ofs_nr
            ,l.*
        FROM (SELECT * FROM offr_api_stg WHERE ctrl_id = p_ctrl_id) l
            ,offr_sku_line osl
       WHERE osl.offr_sku_line_id = l.offr_sku_line_id
         AND l.intrnl_offr_id     = p_offr_id
         AND l.sls_typ            = p_sls_typ
    )
    LOOP
      SELECT COUNT(*)
        INTO l_cnt
        FROM offr_sls_cls_sku s
       WHERE NOT EXISTS (SELECT *
                           FROM offr_sku_line osl
                          WHERE osl.offr_id        = s.offr_id
                            AND osl.sls_cls_cd     = s.sls_cls_cd
                            AND osl.prfl_cd        = s.prfl_cd
                            AND osl.pg_ofs_nr      = s.pg_ofs_nr
                            AND osl.featrd_side_cd = s.featrd_side_cd
                            AND osl.sku_id         = s.sku_id)
         AND s.offr_id        = rec.intrnl_offr_id
         AND s.sls_cls_cd     = rec.sls_cls_cd
         AND s.prfl_cd        = rec.prfl_cd
         AND s.pg_ofs_nr      = rec.pg_ofs_nr
         AND s.featrd_side_cd = rec.concept_featrd_side_cd
         AND s.sku_id         = rec.sku_id;
  
      IF l_cnt > 0 THEN
        DELETE FROM offr_sls_cls_sku s
         WHERE s.offr_id        = rec.intrnl_offr_id
           AND s.sls_cls_cd     = rec.sls_cls_cd
           AND s.prfl_cd        = rec.prfl_cd
           AND s.pg_ofs_nr      = rec.pg_ofs_nr
           AND s.featrd_side_cd = rec.concept_featrd_side_cd
           AND s.sku_id         = rec.sku_id;
      END IF;
  
      SELECT COUNT(*)
        INTO l_cnt
        FROM offr_prfl_sls_cls_plcmt p
       WHERE NOT EXISTS (SELECT *
                           FROM offr_prfl_prc_point s
                          WHERE s.offr_id        = p.offr_id
                            AND s.sls_cls_cd     = p.sls_cls_cd
                            AND s.prfl_cd        = p.prfl_cd
                            AND s.pg_ofs_nr      = p.pg_ofs_nr
                            AND s.featrd_side_cd = p.featrd_side_cd)
         AND NOT EXISTS (SELECT *
                           FROM offr_sls_cls_sku s
                          WHERE s.offr_id        = p.offr_id
                            AND s.sls_cls_cd     = p.sls_cls_cd
                            AND s.prfl_cd        = p.prfl_cd
                            AND s.pg_ofs_nr      = p.pg_ofs_nr
                            AND s.featrd_side_cd = p.featrd_side_cd)
         AND p.offr_id        = rec.intrnl_offr_id
         AND p.sls_cls_cd     = rec.sls_cls_cd
         AND p.prfl_cd        = rec.prfl_cd
         AND p.pg_ofs_nr      = rec.pg_ofs_nr
         AND p.featrd_side_cd = rec.concept_featrd_side_cd;
  
      IF l_cnt > 0 THEN
        DELETE FROM offr_prfl_sls_cls_plcmt p
         WHERE p.offr_id        = rec.intrnl_offr_id
           AND p.sls_cls_cd     = rec.sls_cls_cd
           AND p.prfl_cd        = rec.prfl_cd
           AND p.pg_ofs_nr      = rec.pg_ofs_nr
           AND p.featrd_side_cd = rec.concept_featrd_side_cd;
      END IF;
    END LOOP;
  
    app_plsql_log.info(l_log || ' finished');
  
    l_log := 'delete due to item disabling';
    FOR rec IN (
      SELECT l.*
        FROM offr_api_stg l
       WHERE l.ctrl_id = p_ctrl_id
         AND l.intrnl_offr_id = p_offr_id
         AND l.sls_typ        = p_sls_typ
      )
    LOOP
        BEGIN
          SELECT cnt
            INTO l_cnt
            FROM (
              SELECT osl.offr_prfl_prcpt_id, COUNT(*) cnt
                FROM offr_sku_line osl
               WHERE osl.offr_prfl_prcpt_id = rec.offr_prfl_prcpt_id
              GROUP BY osl.offr_prfl_prcpt_id
              HAVING MIN(NVL(osl.dltd_ind, 'N')) = 'Y'
          );
        EXCEPTION
          WHEN no_data_found THEN
            l_cnt := 0;
        END;
  
        IF l_cnt > 0 THEN
          FOR r_osl IN (
            SELECT offr_sku_line_id
              FROM offr_sku_line
             WHERE offr_prfl_prcpt_id = rec.offr_prfl_prcpt_id
          )
          LOOP
            DELETE FROM dly_bilng_offr_sku_line
             WHERE offr_sku_line_id = r_osl.offr_sku_line_id;
  
            DELETE FROM dly_bilng_trnd_offr_sku_line
             WHERE offr_sku_line_id = r_osl.offr_sku_line_id;
  
            DELETE FROM dstrbtd_mrkt_sls
             WHERE offr_sku_line_id = r_osl.offr_sku_line_id;
  
            DELETE FROM est_lnkg
             WHERE r_osl.offr_sku_line_id IN (offr_sku_line_id, offr_gift_sku_line_id);
          END LOOP;
  
          DELETE FROM offr_sku_line osl
          WHERE osl.offr_prfl_prcpt_id = rec.offr_prfl_prcpt_id;
  
          DELETE FROM offr_prfl_prc_point oppp
           WHERE oppp.offr_prfl_prcpt_id = rec.offr_prfl_prcpt_id;
        END IF;
    END LOOP;
  
    app_plsql_log.info(l_log || ' finished');
    app_plsql_log.info(l_module_name || ' stop');
  
  EXCEPTION
    WHEN OTHERS THEN
      app_plsql_log.info(l_module_name || ', ' || l_log || ', offr_id: ' || p_offr_id ||
                         ', sls_typ: ' || p_sls_typ || ', Error: ' || SQLERRM);
      RAISE;
  END save_offer_api_lines;  
    
    
  PROCEDURE save_offer_api_table(p_ctrl_id IN NUMBER, p_status OUT VARCHAR2) AS
    --p_result 0 ora error,1 successful, 2 osl count diff fount, 3 lock problem
  
    l_run_id         NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id        VARCHAR2(35) := USER();
    l_module_name    VARCHAR2(30) := 'save_offer_api_table_OFFER_API';
    --l_rowcount       NUMBER;
    l_offr_table     obj_get_offr_api_table := obj_get_offr_api_table();
    l_get_offr_table obj_offer_api_table;

  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start');
  
    FOR mrkt_prd_rec IN (SELECT DISTINCT mrkt_id, offr_perd_id
                           FROM offr_api_stg
                          WHERE ctrl_id = p_ctrl_id -- FROM TABLE(p_data_line)
                         ) LOOP
      l_offr_table.delete;
      FOR rec IN (SELECT intrnl_offr_id, sls_typ
                    FROM offr_api_stg --TABLE(p_data_line)
                   WHERE mrkt_id = mrkt_prd_rec.mrkt_id
                     AND offr_perd_id = mrkt_prd_rec.offr_perd_id
                     AND ctrl_id = p_ctrl_id
                   GROUP BY intrnl_offr_id, sls_typ) LOOP
        l_offr_table.extend;
        l_offr_table(l_offr_table.last) := obj_get_offr_api_line(rec.intrnl_offr_id,
                                                                 rec.sls_typ,
                                                                 1);
      END LOOP;
    
      SELECT obj_offer_api_line(mrkt_id,
                                mrkt_nm,
                                offr_perd_id,
                                sls_perd_id,
                                offr_sku_line_id,
                                veh_id,
                                veh_desc_txt,
                                brchr_plcmnt_id,
                                brchr_plcmnt_nm,
                                brchr_sctn_nm,
                                sctn_seq_nr,
                                enrgy_chrt_postn_id,
                                enrgy_chrt_postn_nm,
                                pg_nr,
                                ctgry_id,
                                ctgry_nm,
                                brnd_id,
                                brnd_nm,
                                sgmt_id,
                                sgmt_nm,
                                form_id,
                                form_desc_txt,
                                form_grp_id,
                                form_grp_desc_txt,
                                prfl_cd,
                                sku_id,
                                fsc_cd,
                                prod_typ_id,
                                prod_typ_desc_txt,
                                gender_id,
                                gender_desc,
                                sls_cls_cd,
                                sls_cls_desc_txt,
                                offr_desc_txt,
                                offr_notes_txt,
                                offr_lyot_cmnts_txt,
                                featrd_side_cd,
                                featrd_side_desc,
                                concept_featrd_side_cd,
                                concept_featrd_side_desc,
                                micr_ncpsltn_ind,
                                cnsmr_invstmt_bdgt_id,
                                cnsmr_invstmt_bdgt_desc_txt,
                                pymt_typ,
                                pymt_typ_desc_txt,
                                promtn_id,
                                promtn_desc_txt,
                                promtn_clm_id,
                                promtn_clm_desc_txt,
                                comsn_typ,
                                tax_type_id,
                                wsl_ind,
                                offr_sku_set_id,
                                cmpnt_qty,
                                nr_for_qty,
                                nta_factor,
                                sku_cost,
                                lv_nta,
                                lv_sp,
                                lv_rp,
                                lv_discount,
                                lv_units,
                                lv_total_cost,
                                lv_gross_sales,
                                lv_dp_cash,
                                lv_dp_percent,
                                ver_id,
                                ver_desc_txt,
                                sls_prc_amt,
                                reg_prc_amt,
                                line_nr,
                                unit_qty,
                                dltd_ind,
                                created_ts,
                                created_user_id,
                                last_updt_ts,
                                last_updt_user_id,
                                intrnl_offr_id,
                                mrkt_veh_perd_sctn_id,
                                prfl_nm,
                                sku_nm,
                                comsn_typ_desc_txt,
                                tax_typ_desc_txt,
                                offr_sku_set_nm,
                                sls_typ,
                                sls_typ_nm,
                                pc_sp_py,
                                pc_rp,
                                pc_sp,
                                pc_vsp,
                                pc_hit,
                                pg_wght,
                                sprd_nr,
                                offr_prfl_prcpt_id)
        BULK COLLECT
        INTO l_get_offr_table
        FROM TABLE(pa_offer_api.get_offr(l_offr_table));
    
      -- check if the received osl -ids are the same as
      FOR offr_sls IN (SELECT offr_id, sls_typ, 1 AS diff -- different lines
                         FROM ((SELECT intrnl_offr_id AS offr_id
                                      ,sls_typ
                                      ,offr_sku_line_id
                                  FROM TABLE(l_get_offr_table)
                                MINUS
                                SELECT intrnl_offr_id   AS offr_id
                                      ,sls_typ
                                      ,offr_sku_line_id FROM offr_api_stg WHERE ctrl_id = p_ctrl_id AND mrkt_id = mrkt_prd_rec.mrkt_id AND offr_perd_id = mrkt_prd_rec.offr_perd_id)
                               UNION ALL
                               (SELECT intrnl_offr_id AS offr_id
                                      ,sls_typ
                                      ,offr_sku_line_id
                                 FROM offr_api_stg WHERE ctrl_id = p_ctrl_id AND mrkt_id = mrkt_prd_rec.mrkt_id
                                   AND offr_perd_id =
                                       mrkt_prd_rec.offr_perd_id
                                MINUS
                                SELECT intrnl_offr_id AS offr_id
                                      ,sls_typ
                                      ,offr_sku_line_id
                                  FROM TABLE(l_get_offr_table)))
                       UNION
                       SELECT offr_id, sls_typ, 0 AS diff -- identical lines
                         FROM (SELECT intrnl_offr_id AS offr_id
                                     ,sls_typ
                                     ,offr_sku_line_id
                                 FROM TABLE(l_get_offr_table)
                               INTERSECT
                               SELECT intrnl_offr_id AS offr_id
                                     ,sls_typ
                                     ,offr_sku_line_id
                                FROM offr_api_stg WHERE ctrl_id = p_ctrl_id AND mrkt_id = mrkt_prd_rec.mrkt_id
                                  AND offr_perd_id =
                                      mrkt_prd_rec.offr_perd_id)) LOOP
        app_plsql_log.info('save: offr_id: ' || offr_sls.offr_id || ', sls_typ: ' || offr_sls.sls_typ || ', ctrl_id: ' || p_ctrl_id);
              BEGIN
                save_offer_api_lines(offr_sls.offr_id,
                                     offr_sls.sls_typ,
                                     p_ctrl_id);
                COMMIT; --save changes for the offer
              EXCEPTION
                WHEN OTHERS THEN
                  ROLLBACK; --no changes saved for the offer
                  p_status:= 'FAILED';
                  app_plsql_log.info('save failed: offr_id: ' || offr_sls.offr_id || ', sls_typ: ' || offr_sls.sls_typ);
               END;
     
        /*p_result.extend();
        p_result(p_result.last) := obj_edit_offr_save_line(l_result,
                                                           offr_sls.offr_id,
                                                           offr_sls.sls_typ);*/
      END LOOP; --offr and sls typ loop
    END LOOP; --mrkt_prd_rec
  p_status:='SUCCESS';
    app_plsql_log.info(l_module_name || ' stop');
  EXCEPTION
    WHEN OTHERS THEN
      app_plsql_log.info(l_module_name || ' Error in save offr api' ||
                         SQLERRM(SQLCODE));
  END save_offer_api_table;
  
  FUNCTION get_sku_cost(p_offr_perd_from IN offr.offr_perd_id%TYPE,
                        p_offr_perd_to   IN offr.offr_perd_id%TYPE)
    RETURN obj_sku_cost_table
    PIPELINED IS
  BEGIN
    FOR rec IN (
      SELECT *
        FROM sku_cost s
       WHERE s.offr_perd_id IN (p_offr_perd_from, p_offr_perd_to)
         AND NVL(s.hold_costs_ind, 'N') = 'N'
      )
      LOOP
        PIPE ROW (obj_sku_cost_line(rec.mrkt_id,
                                    rec.offr_perd_id,
                                    rec.sku_id,
                                    rec.cost_typ,
                                    rec.crncy_cd,
                                    rec.wghtd_avg_cost_amt,
                                    rec.creat_user_id,
                                    rec.creat_ts,
                                    rec.last_updt_user_id,
                                    rec.last_updt_ts,
                                    rec.hold_costs_ind,
                                    rec.lcl_cost_ind
        ));
      END LOOP;
  END get_sku_cost;

END pa_offer_api;
/
