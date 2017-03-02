CREATE OR REPLACE PACKAGE pa_sku_bias AS
  /*********************************************************
  * History
  * Created by   : Schiff Gy
  * Date         : 12/10/2016
  * Description  : First created 
  ******************************************************/

  g_package_name CONSTANT VARCHAR2(30) := 'PA_SKU_BIAS';
  --
  PROCEDURE set_sku_bias(p_mrkt_id      IN NUMBER,
                         p_sls_perd_id  IN NUMBER,
                         p_sku_id       IN NUMBER,
                         p_new_sku_bias IN NUMBER,
                         p_user_id      IN VARCHAR2,
                         p_stus         OUT NUMBER);

  FUNCTION get_sku_bias(p_mrkt_id IN NUMBER, p_sls_perd_id IN NUMBER)
    RETURN obj_sku_bias_mantnc_table
    PIPELINED;

  FUNCTION get_sku_bias2(p_mrkt_id      IN NUMBER,
                         p_sls_perd_id  IN NUMBER,
                         p_sku_id_array IN number_array)
    RETURN obj_sku_bias_mantnc_table
    PIPELINED;

  --------- new procedures ----------------------

  PROCEDURE set_sku_bias_new(p_mrkt_id      IN NUMBER,
                             p_sls_perd_id  IN NUMBER,
                             p_sls_typ_id   IN NUMBER,
                             p_sku_id       IN NUMBER,
                             p_new_sku_bias IN NUMBER,
                             p_user_id      IN VARCHAR2,
                             p_stus         OUT NUMBER);

  function GET_SKU_BIAS_NEW(P_MRKT_ID in number,
                            p_sls_typ_id   IN NUMBER,
                            p_sls_perd_id IN NUMBER)
    RETURN pa_sku_bias_mantnc_table
    PIPELINED;

  FUNCTION GET_SKU_BIAS2_new(p_mrkt_id      IN NUMBER,
                         P_SLS_PERD_ID      in number,
                         P_SLS_TYP_ID       in number,
                         p_sku_id_array     IN number_array)
    RETURN PA_SKU_BIAS_MANTNC_TABLE
    PIPELINED;
    
  FUNCTION GET_SKU_BIAS_newest(p_mrkt_id      IN NUMBER,
                         P_SLS_PERD_ID      in number,
                         P_SLS_TYP_ID       in number,
                         p_sku_id_array     IN number_array)
    RETURN PA_SKU_BIAS_MANTNC_TABLE
    PIPELINED;

END pa_sku_bias;
/


CREATE OR REPLACE PACKAGE BODY pa_sku_bias AS

  FUNCTION get_sku_bias(p_mrkt_id     IN NUMBER, -- 68
                        p_sls_perd_id IN NUMBER -- 20170305
                        ) RETURN obj_sku_bias_mantnc_table
    PIPELINED AS
    -- for LOG
    l_module_name    VARCHAR2(30) := 'GET_SKU_BIAS';
    l_parameter_list VARCHAR2(512) := '(p_mrkt_id: ' || to_char(p_mrkt_id) || ', ' ||
                                      'p_sls_perd_id: ' ||
                                      to_char(p_sls_perd_id) || ')';
    --
    l_run_id  NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id VARCHAR(35) := USER();
    --
    CURSOR cc IS
      WITH mpsb AS
       (SELECT psb.sku_id,
               psb.bias_pct,
               psb.last_updt_ts,
               u.user_frst_nm || ' ' || u.user_last_nm last_updt_nm
          FROM mrkt_perd_sku_bias psb
          LEFT JOIN mps_user u
            ON psb.last_updt_user_id = u.user_nm
         WHERE psb.mrkt_id = p_mrkt_id
           AND psb.sls_perd_id = p_sls_perd_id),
      act_fsc AS
       (SELECT mrkt_fsc.sku_id, MAX(mrkt_fsc.strt_perd_id) max_perd_id
          FROM mrkt_fsc
         WHERE mrkt_fsc.mrkt_id = p_mrkt_id
           AND mrkt_fsc.strt_perd_id <= p_sls_perd_id
         GROUP BY mrkt_fsc.sku_id),
      all_fsc AS
       (SELECT mrkt_fsc.strt_perd_id,
               mrkt_fsc.fsc_cd,
               mrkt_fsc.sku_id,
               mrkt_fsc.prod_desc_txt
          FROM mrkt_fsc
         WHERE mrkt_fsc.strt_perd_id <= p_sls_perd_id
           AND mrkt_fsc.mrkt_id = p_mrkt_id)
      SELECT obj_sku_bias_mantnc_line(all_fsc.fsc_cd,
                                      all_fsc.prod_desc_txt,
                                      sku.sku_id,
                                      sku.lcl_sku_nm,
                                      'A',
                                      nvl2(offr_sku_line.sku_id, 'P', 'N'),
                                      mpsb.bias_pct,
                                      mpsb.last_updt_ts,
                                      mpsb.last_updt_nm) cline
        FROM act_fsc,
             all_fsc,
             mpsb,
             (SELECT sms.mrkt_id, sms.sku_id, sms.lcl_sku_nm
                FROM (SELECT mrkt_sku.mrkt_id AS mrkt_id,
                             mrkt_sku.sku_id,
                             mrkt_sku.lcl_sku_nm
                        FROM mrkt_sku
                       WHERE mrkt_sku.mrkt_id = p_mrkt_id
                         AND mrkt_sku.dltd_ind NOT IN ('Y', 'y')
                         AND pa_maps.get_sls_cls_cd(p_sls_perd_id,
                                                    p_mrkt_id,
                                                    mrkt_sku.avlbl_perd_id,
                                                    mrkt_sku.intrdctn_perd_id,
                                                    mrkt_sku.demo_ofs_nr,
                                                    mrkt_sku.demo_durtn_nr,
                                                    mrkt_sku.new_durtn_nr,
                                                    mrkt_sku.stus_perd_id,
                                                    mrkt_sku.dspostn_perd_id,
                                                    mrkt_sku.on_stus_perd_id) > -1) sms,
                     (SELECT DISTINCT sku_id
                        FROM sku_reg_prc
                       WHERE sku_reg_prc.mrkt_id = p_mrkt_id
                         AND sku_reg_prc.offr_perd_id = p_sls_perd_id) sku_reg_prc,
                     (SELECT DISTINCT sku_id
                        FROM sku_cost
                       WHERE sku_cost.mrkt_id = p_mrkt_id
                         AND sku_cost.offr_perd_id = p_sls_perd_id
                         AND sku_cost.cost_typ = 'P') sku_cost
               WHERE sms.sku_id = sku_reg_prc.sku_id
                 AND sms.sku_id = sku_cost.sku_id) sku,
             (SELECT DISTINCT offr_sku_line.sku_id
                FROM offr, offr_sku_line
               WHERE offr.offr_id = offr_sku_line.offr_id
                 AND offr_sku_line.mrkt_id = p_mrkt_id
                 AND offr.mrkt_id = p_mrkt_id
                 AND offr.ver_id = 0
                 AND offr.offr_perd_id = p_sls_perd_id
                 AND offr.offr_typ = 'CMP'
                 AND offr_sku_line.offr_perd_id = p_sls_perd_id
                 AND offr_sku_line.dltd_ind NOT IN ('Y', 'y')) offr_sku_line
       WHERE sku.sku_id = offr_sku_line.sku_id(+)
         AND sku.sku_id = act_fsc.sku_id(+)
         AND act_fsc.sku_id = all_fsc.sku_id
         AND act_fsc.max_perd_id = all_fsc.strt_perd_id
         AND sku.sku_id = mpsb.sku_id(+)
       ORDER BY all_fsc.fsc_cd * 1;
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || l_parameter_list || ' start');
    FOR rec IN cc LOOP
      PIPE ROW(rec.cline);
    END LOOP;
    app_plsql_log.info(l_module_name || l_parameter_list || ' stop');
  END get_sku_bias;

  FUNCTION get_sku_bias2(p_mrkt_id      IN NUMBER,
                         p_sls_perd_id  IN NUMBER,
                         p_sku_id_array IN number_array)
    RETURN obj_sku_bias_mantnc_table
    PIPELINED AS
    -- for LOG
    l_module_name    VARCHAR2(30) := 'GET_SKU_BIAS2';
    l_parameter_list VARCHAR2(512) := '(p_mrkt_id: ' || to_char(p_mrkt_id) || ', ' ||
                                      'p_sls_perd_id: ' ||
                                      to_char(p_sls_perd_id) || ', ' ||
                                      'p_sku_id_array: ' ||
                                      to_char(p_sku_id_array(p_sku_id_array.first)) || '~' ||
                                      to_char(p_sku_id_array(p_sku_id_array.last)) || ')';
    --
    l_run_id  NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id VARCHAR(35) := USER();
    --
    CURSOR cc IS
      WITH mpsb AS
       (SELECT psb.sku_id,
               psb.bias_pct,
               psb.last_updt_ts,
               u.user_frst_nm || ' ' || u.user_last_nm last_updt_nm
          FROM mrkt_perd_sku_bias psb
          LEFT JOIN mps_user u
            ON psb.last_updt_user_id = u.user_nm
         WHERE psb.mrkt_id = p_mrkt_id
           AND psb.sls_perd_id = p_sls_perd_id
           AND sku_id IN (SELECT column_value FROM TABLE(p_sku_id_array))),
      act_fsc AS
       (SELECT mrkt_fsc.sku_id, MAX(mrkt_fsc.strt_perd_id) max_perd_id
          FROM mrkt_fsc
         WHERE mrkt_fsc.mrkt_id = p_mrkt_id
           AND mrkt_fsc.strt_perd_id <= p_sls_perd_id
           AND sku_id IN (SELECT column_value FROM TABLE(p_sku_id_array))
         GROUP BY mrkt_fsc.sku_id),
      all_fsc AS
       (SELECT mrkt_fsc.strt_perd_id,
               mrkt_fsc.fsc_cd,
               mrkt_fsc.sku_id,
               mrkt_fsc.prod_desc_txt
          FROM mrkt_fsc
         WHERE mrkt_fsc.strt_perd_id <= p_sls_perd_id
           AND mrkt_fsc.mrkt_id = p_mrkt_id
           AND sku_id IN (SELECT column_value FROM TABLE(p_sku_id_array)))
      SELECT obj_sku_bias_mantnc_line(all_fsc.fsc_cd,
                                      all_fsc.prod_desc_txt,
                                      sku.sku_id,
                                      sku.lcl_sku_nm,
                                      'A',
                                      nvl2(offr_sku_line.sku_id, 'P', 'N'),
                                      mpsb.bias_pct,
                                      mpsb.last_updt_ts,
                                      mpsb.last_updt_nm) cline
        FROM act_fsc,
             all_fsc,
             mpsb,
             (SELECT mrkt_sku.*
                FROM (SELECT *
                        FROM mrkt_sku
                       WHERE mrkt_sku.mrkt_id = p_mrkt_id
                         AND mrkt_sku.dltd_ind NOT IN ('Y', 'y')
                         AND pa_maps.get_sls_cls_cd(p_sls_perd_id,
                                                    p_mrkt_id,
                                                    mrkt_sku.avlbl_perd_id,
                                                    mrkt_sku.intrdctn_perd_id,
                                                    mrkt_sku.demo_ofs_nr,
                                                    mrkt_sku.demo_durtn_nr,
                                                    mrkt_sku.new_durtn_nr,
                                                    mrkt_sku.stus_perd_id,
                                                    mrkt_sku.dspostn_perd_id,
                                                    mrkt_sku.on_stus_perd_id) > -1) mrkt_sku,
                     (SELECT *
                        FROM sku_reg_prc
                       WHERE mrkt_id = p_mrkt_id
                         AND offr_perd_id = p_sls_perd_id) sku_reg_prc,
                     (SELECT *
                        FROM sku_cost
                       WHERE mrkt_id = p_mrkt_id
                         AND offr_perd_id = p_sls_perd_id
                         AND cost_typ = 'P') sku_cost
               WHERE mrkt_sku.sku_id = sku_reg_prc.sku_id
                 AND mrkt_sku.sku_id = sku_cost.sku_id
                 AND mrkt_sku.sku_id IN
                     (SELECT column_value FROM TABLE(p_sku_id_array))) sku,
             (SELECT DISTINCT sku_id
                FROM offr, offr_sku_line
               WHERE offr.offr_id = offr_sku_line.offr_id
                 AND offr_sku_line.mrkt_id = p_mrkt_id
                 AND offr.mrkt_id = p_mrkt_id
                 AND offr.ver_id = 0
                 AND offr.offr_perd_id = p_sls_perd_id
                 AND offr.offr_typ = 'CMP'
                 AND offr_sku_line.offr_perd_id = p_sls_perd_id
                 AND offr_sku_line.dltd_ind NOT IN ('Y', 'y')
                 AND offr_sku_line.sku_id IN
                     (SELECT column_value FROM TABLE(p_sku_id_array))) offr_sku_line
       WHERE sku.sku_id = offr_sku_line.sku_id(+)
         AND sku.sku_id = act_fsc.sku_id(+)
         AND act_fsc.sku_id = all_fsc.sku_id
         AND act_fsc.max_perd_id = all_fsc.strt_perd_id
         AND sku.sku_id = mpsb.sku_id(+)
       ORDER BY all_fsc.fsc_cd * 1;
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info('After SET, ' || l_module_name || l_parameter_list ||
                       ' start');
    FOR rec IN cc LOOP
      PIPE ROW(rec.cline);
    END LOOP;
    app_plsql_log.info('After SET, ' || l_module_name || l_parameter_list ||
                       ' stop');
  END get_sku_bias2;

  PROCEDURE set_sku_bias(p_mrkt_id      IN NUMBER,
                         p_sls_perd_id  IN NUMBER,
                         p_sku_id       IN NUMBER,
                         p_new_sku_bias IN NUMBER,
                         p_user_id      IN VARCHAR2,
                         p_stus         OUT NUMBER) AS
    -- TODO - check if the sku is active, if not refuse handling the record                       
    /*********************************************************
    * INPUT p_new_sku_bias IS NULL means do nothing
    *
    * Possible OUT Values
    * 0 - success
    * 1 - New BIAS_PCT value is out of range 0 to 1000
    * 2 - database error in DELETE, UPDATE or INSERT statements
    * 3 - Obligatory foreign keys (MRKT_SKU and MRKT_PERD) not found
    * 4 - Inactive SKU, change refused 
    ******************************************************/
    counter1  NUMBER;
    counter2  NUMBER;
    counter3  NUMBER;
    phcounter NUMBER;
  BEGIN
    p_stus := 0;
    -- p_new_sku_bias IS NULL means do nothing
    IF p_new_sku_bias IS NULL THEN
      NULL;
      -- p_new_sku_bias must be between 0 and 1000
    ELSIF p_new_sku_bias BETWEEN 0 AND 1000 THEN
      SELECT COUNT(*)
        INTO counter1
        FROM mrkt_perd_sku_prc mpsp
       WHERE mpsp.mrkt_id = p_mrkt_id
         AND mpsp.offr_perd_id = p_sls_perd_id
         AND mpsp.sku_id = p_sku_id
         AND mpsp.prc_lvl_typ_cd = 'RP';
      SELECT COUNT(*)
        INTO counter2
        FROM sku_cost sc
       WHERE sc.mrkt_id = p_mrkt_id
         AND sc.offr_perd_id = p_sls_perd_id
         AND sc.sku_id = p_sku_id
         AND cost_typ = 'P';
      SELECT ph,
             CASE
               WHEN pa_maps_public.get_sls_cls_cd(p_sls_perd_id,
                                                  p_mrkt_id,
                                                  s.avlbl_perd_id,
                                                  s.intrdctn_perd_id,
                                                  s.demo_ofs_nr,
                                                  s.demo_durtn_nr,
                                                  s.new_durtn_nr,
                                                  s.stus_perd_id,
                                                  s.dspostn_perd_id,
                                                  s.on_stus_perd_id) > '-1' THEN
                1
               ELSE
                0
             END ind
        INTO phcounter, counter3
        FROM (SELECT 1 ph FROM dual) dd
        LEFT JOIN mrkt_sku s
          ON s.mrkt_id = p_mrkt_id
         AND s.sku_id = p_sku_id;
      IF counter1 + counter2 + counter3 = 3 THEN
        -- active SKU
        -- upsert using p_new_bias
        SELECT COUNT(*)
          INTO counter1
          FROM mrkt_perd
         WHERE mrkt_id = p_mrkt_id
           AND perd_id = p_sls_perd_id;
        SELECT COUNT(*)
          INTO counter2
          FROM mrkt_sku
         WHERE mrkt_id = p_mrkt_id
           AND sku_id = p_sku_id;
        IF counter1 + counter2 = 2 THEN
          BEGIN
            SAVEPOINT before_upsert;
            MERGE INTO mrkt_perd_sku_bias trgt
            USING (SELECT p_mrkt_id t1, p_sls_perd_id t2, p_sku_id t3
                     FROM dual) src
            ON (trgt.mrkt_id = src.t1 AND trgt.sls_perd_id = src.t2 AND trgt.sku_id = src.t3)
            WHEN MATCHED THEN
              UPDATE
                 SET trgt.bias_pct          = p_new_sku_bias,
                     trgt.last_updt_user_id = p_user_id
            WHEN NOT MATCHED THEN
              INSERT
                (mrkt_id,
                 sls_perd_id,
                 sls_typ_id,
                 sku_id,
                 bias_pct,
                 creat_user_id,
                 last_updt_user_id)
              VALUES
                (p_mrkt_id,
                 p_sls_perd_id,
                 3,
                 p_sku_id,
                 p_new_sku_bias,
                 p_user_id,
                 p_user_id);
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO before_changes;
              p_stus := 2; -- Database Error
          END;
        ELSE
          p_stus := 3; -- Missing foreign keys
        END IF; -- cc2
      ELSE
        p_stus := 4; -- Passive SKU
      END IF; --ccc3
    ELSE
      p_stus := 1; -- Negative value
    END IF; -- 1 1000
  END set_sku_bias;

  -------------- new procedures -----------------------  

  function GET_SKU_BIAS_NEW(P_MRKT_ID     in number, -- 68
                            p_sls_typ_id   IN NUMBER, -- 3
                            p_sls_perd_id IN NUMBER -- 20170305
                            ) RETURN pa_sku_bias_mantnc_table
    PIPELINED AS
    -- for LOG
    l_module_name    VARCHAR2(30) := 'GET_SKU_BIAS';
    L_PARAMETER_LIST varchar2(512) := '(p_mrkt_id: ' || TO_CHAR(P_MRKT_ID) || ', ' ||
                                      'p_sls_typ_id: ' || to_char(p_mrkt_id) || ', ' ||
                                      'p_sls_perd_id: ' || to_char(p_sls_perd_id) || ')';
    --
    l_run_id  NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id VARCHAR(35) := USER();
    --
    CURSOR cc IS
      WITH mpsb AS
       (SELECT psb.sku_id,
               psb.sls_typ_id,
               psb.bias_pct,
               psb.last_updt_ts,
               u.user_frst_nm || ' ' || u.user_last_nm last_updt_nm
          FROM mrkt_perd_sku_bias psb
          LEFT JOIN mps_user u
            ON psb.last_updt_user_id = u.user_nm
         WHERE psb.mrkt_id = p_mrkt_id
           and PSB.SLS_PERD_ID = P_SLS_PERD_ID
           and psb.sls_typ_id = p_sls_typ_id ),
      act_fsc AS
       (SELECT mrkt_fsc.sku_id, MAX(mrkt_fsc.strt_perd_id) max_perd_id
          FROM mrkt_fsc
         WHERE mrkt_fsc.mrkt_id = p_mrkt_id
           AND mrkt_fsc.strt_perd_id <= p_sls_perd_id
         GROUP BY mrkt_fsc.sku_id),
      all_fsc AS
       (SELECT mrkt_fsc.strt_perd_id,
               mrkt_fsc.fsc_cd,
               mrkt_fsc.sku_id,
               mrkt_fsc.prod_desc_txt
          FROM mrkt_fsc
         WHERE mrkt_fsc.strt_perd_id <= p_sls_perd_id
           AND mrkt_fsc.mrkt_id = p_mrkt_id)
      SELECT pa_sku_bias_mantnc_line(all_fsc.fsc_cd,
                                     all_fsc.prod_desc_txt,
                                     sku.sku_id,
                                     sku.lcl_sku_nm,
                                     'A',
                                     NVL2(OFFR_SKU_LINE.SKU_ID, 'P', 'N'),
                                     p_sls_typ_id,
                                     mpsb.bias_pct,
                                     mpsb.last_updt_ts,
                                     mpsb.last_updt_nm) cline
        FROM act_fsc,
             all_fsc,
             mpsb,
             (SELECT sms.mrkt_id,
                     sms.sku_id,
                     SMS.LCL_SKU_NM,
                     p_sls_typ_id
                FROM (SELECT mrkt_sku.mrkt_id AS mrkt_id,
                             mrkt_sku.sku_id,
                             mrkt_sku.lcl_sku_nm
                        FROM mrkt_sku
                       WHERE mrkt_sku.mrkt_id = p_mrkt_id
                         AND mrkt_sku.dltd_ind NOT IN ('Y', 'y')
                         AND pa_maps.get_sls_cls_cd(p_sls_perd_id,
                                                    p_mrkt_id,
                                                    mrkt_sku.avlbl_perd_id,
                                                    mrkt_sku.intrdctn_perd_id,
                                                    mrkt_sku.demo_ofs_nr,
                                                    mrkt_sku.demo_durtn_nr,
                                                    mrkt_sku.new_durtn_nr,
                                                    mrkt_sku.stus_perd_id,
                                                    mrkt_sku.dspostn_perd_id,
                                                    mrkt_sku.on_stus_perd_id) > -1) sms,
                     (SELECT DISTINCT sku_id
                        FROM sku_reg_prc
                       WHERE sku_reg_prc.mrkt_id = p_mrkt_id
                         AND sku_reg_prc.offr_perd_id = p_sls_perd_id) sku_reg_prc,
                     (SELECT DISTINCT sku_id
                        FROM sku_cost
                       WHERE sku_cost.mrkt_id = p_mrkt_id
                         AND sku_cost.offr_perd_id = p_sls_perd_id
                         AND sku_cost.cost_typ = 'P') sku_cost
               WHERE sms.sku_id = sku_reg_prc.sku_id
                 AND sms.sku_id = sku_cost.sku_id) sku,
             (SELECT DISTINCT offr_sku_line.sku_id
                FROM offr, offr_sku_line
               WHERE offr.offr_id = offr_sku_line.offr_id
                 AND offr_sku_line.mrkt_id = p_mrkt_id
                 AND offr.mrkt_id = p_mrkt_id
                 AND offr.ver_id = 0
                 AND offr.offr_perd_id = p_sls_perd_id
                 AND offr.offr_typ = 'CMP'
                 AND offr_sku_line.offr_perd_id = p_sls_perd_id
                 AND offr_sku_line.dltd_ind NOT IN ('Y', 'y')) offr_sku_line
       WHERE sku.sku_id = offr_sku_line.sku_id(+)
         AND sku.sku_id = act_fsc.sku_id(+)
         AND act_fsc.sku_id = all_fsc.sku_id
         AND act_fsc.max_perd_id = all_fsc.strt_perd_id
         and SKU.SKU_ID = MPSB.SKU_ID(+)
--         AND sku.sls_typ_id = mpsb.sls_typ_id(+)
       ORDER BY all_fsc.fsc_cd * 1;
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || l_parameter_list || ' start');
    FOR rec IN cc LOOP
      PIPE ROW(rec.cline);
    END LOOP;
    app_plsql_log.info(l_module_name || l_parameter_list || ' stop');
  END get_sku_bias_new;

  FUNCTION get_sku_bias2_new(p_mrkt_id          IN NUMBER,
                             P_SLS_PERD_ID      in number,
                             P_SLS_TYP_ID       in number,
                             p_sku_id_array IN number_array)
    RETURN pa_sku_bias_mantnc_table
    PIPELINED AS
    -- for LOG
    l_module_name    VARCHAR2(30) := 'GET_SKU_BIAS2';
    l_parameter_list VARCHAR2(512) := '(p_mrkt_id: ' || to_char(p_mrkt_id) || ', ' ||
                                      'p_sls_perd_id: ' ||
                                      TO_CHAR(P_SLS_PERD_ID) || ', ' ||
                                      'p_sls_typ_id: ' ||
                                      TO_CHAR(P_SLS_typ_ID) || ', ' ||
                                      'p_sku_id_array: ' ||
                                      to_char(p_sku_id_array.first) ||  '~' ||
                                      to_char(p_sku_id_array.last) || ')';
    --
    l_run_id  NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id VARCHAR(35) := USER();
    --
    CURSOR cc IS
      WITH mpsb AS
       (SELECT psb.sku_id,
               psb.bias_pct,
               psb.last_updt_ts,
               u.user_frst_nm || ' ' || u.user_last_nm last_updt_nm
          FROM mrkt_perd_sku_bias psb
          LEFT JOIN mps_user u
            ON psb.last_updt_user_id = u.user_nm
         WHERE psb.mrkt_id = p_mrkt_id
           and PSB.SLS_PERD_ID = P_SLS_PERD_ID
           and PSB.SLS_TYP_ID = P_SLS_typ_ID
           and PSB.SKU_ID in
               (SELECT column_value FROM TABLE(p_sku_id_array))),
      act_fsc AS
       (SELECT mrkt_fsc.sku_id, MAX(mrkt_fsc.strt_perd_id) max_perd_id
          FROM mrkt_fsc
         WHERE mrkt_fsc.mrkt_id = p_mrkt_id
           AND mrkt_fsc.strt_perd_id <= p_sls_perd_id
           AND sku_id IN
               (SELECT column_value FROM TABLE(p_sku_id_array))
         GROUP BY mrkt_fsc.sku_id),
      all_fsc AS
       (SELECT mrkt_fsc.strt_perd_id,
               mrkt_fsc.fsc_cd,
               mrkt_fsc.sku_id,
               mrkt_fsc.prod_desc_txt
          FROM mrkt_fsc
         WHERE mrkt_fsc.strt_perd_id <= p_sls_perd_id
           AND mrkt_fsc.mrkt_id = p_mrkt_id
           and SKU_ID in
               (SELECT column_value FROM TABLE(p_sku_id_array)))
      SELECT pa_sku_bias_mantnc_line(all_fsc.fsc_cd,
                                     all_fsc.prod_desc_txt,
                                     sku.sku_id,
                                     sku.lcl_sku_nm,
                                     'A',
                                     NVL2(OFFR_SKU_LINE.SKU_ID, 'P', 'N'),
                                     p_sls_typ_id,
                                     mpsb.bias_pct,
                                     mpsb.last_updt_ts,
                                     mpsb.last_updt_nm) cline
        FROM act_fsc,
             all_fsc,
             mpsb,
             (SELECT mrkt_sku.*
                FROM (SELECT *
                        FROM mrkt_sku
                       WHERE mrkt_sku.mrkt_id = p_mrkt_id
                         AND mrkt_sku.dltd_ind NOT IN ('Y', 'y')
                         AND pa_maps.get_sls_cls_cd(p_sls_perd_id,
                                                    p_mrkt_id,
                                                    mrkt_sku.avlbl_perd_id,
                                                    mrkt_sku.intrdctn_perd_id,
                                                    mrkt_sku.demo_ofs_nr,
                                                    mrkt_sku.demo_durtn_nr,
                                                    mrkt_sku.new_durtn_nr,
                                                    mrkt_sku.stus_perd_id,
                                                    mrkt_sku.dspostn_perd_id,
                                                    mrkt_sku.on_stus_perd_id) > -1) mrkt_sku,
                     (SELECT *
                        FROM sku_reg_prc
                       WHERE mrkt_id = p_mrkt_id
                         AND offr_perd_id = p_sls_perd_id) sku_reg_prc,
                     (SELECT *
                        FROM sku_cost
                       WHERE mrkt_id = p_mrkt_id
                         AND offr_perd_id = p_sls_perd_id
                         AND cost_typ = 'P') sku_cost
                         WHERE mrkt_sku.sku_id = sku_reg_prc.sku_id
                 AND mrkt_sku.sku_id = sku_cost.sku_id
                 and MRKT_SKU.SKU_ID in
                     (select column_value from table(P_SKU_ID_ARRAY))
                 ) sku,
             (SELECT DISTINCT sku_id
                FROM offr, offr_sku_line
               WHERE offr.offr_id = offr_sku_line.offr_id
                 AND offr_sku_line.mrkt_id = p_mrkt_id
                 AND offr.mrkt_id = p_mrkt_id
                 AND offr.ver_id = 0
                 AND offr.offr_perd_id = p_sls_perd_id
                 AND offr.offr_typ = 'CMP'
                 AND offr_sku_line.offr_perd_id = p_sls_perd_id
                 AND offr_sku_line.dltd_ind NOT IN ('Y', 'y')
                 AND offr_sku_line.sku_id IN
                     (select column_value from table(P_SKU_ID_ARRAY))) offr_sku_line
       WHERE sku.sku_id = offr_sku_line.sku_id(+)
         AND sku.sku_id = act_fsc.sku_id(+)
         AND act_fsc.sku_id = all_fsc.sku_id
         AND act_fsc.max_perd_id = all_fsc.strt_perd_id
         AND sku.sku_id = mpsb.sku_id(+)
       ORDER BY all_fsc.fsc_cd * 1;
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info('After SET, ' || l_module_name || l_parameter_list ||
                       ' start');
    FOR rec IN cc LOOP
      PIPE ROW(rec.cline);
    END LOOP;
    app_plsql_log.info('After SET, ' || l_module_name || l_parameter_list ||
                       ' stop');
  END get_sku_bias2_new;

  PROCEDURE set_sku_bias_new(p_mrkt_id      IN NUMBER,
                             p_sls_perd_id  IN NUMBER,
                             p_sls_typ_id   IN NUMBER,
                             p_sku_id       IN NUMBER,
                             p_new_sku_bias IN NUMBER,
                             p_user_id      IN VARCHAR2,
                             p_stus         OUT NUMBER) AS
    -- TODO - check if the sku is active, if not refuse handling the record                       
    /*********************************************************
    * INPUT p_new_sku_bias IS NULL means do nothing
    *
    * Possible OUT Values
    * 0 - success
    * 1 - New BIAS_PCT value is out of range 0 to 1000
    * 2 - database error in DELETE, UPDATE or INSERT statements
    * 3 - Obligatory foreign keys (MRKT_SKU and MRKT_PERD) not found
    * 4 - Inactive SKU, change refused 
    ******************************************************/
    counter1  NUMBER;
    counter2  NUMBER;
    counter3  NUMBER;
    phcounter NUMBER;
  BEGIN
    p_stus := 0;
    -- p_new_sku_bias IS NULL means do nothing
    IF p_new_sku_bias IS NULL THEN
      NULL;
      -- p_new_sku_bias must be between 0 and 1000
    ELSIF p_new_sku_bias BETWEEN 0 AND 1000 THEN
      SELECT COUNT(*)
        INTO counter1
        FROM mrkt_perd_sku_prc mpsp
       WHERE mpsp.mrkt_id = p_mrkt_id
         AND mpsp.offr_perd_id = p_sls_perd_id
         AND mpsp.sku_id = p_sku_id
         AND mpsp.prc_lvl_typ_cd = 'RP';
      SELECT COUNT(*)
        INTO counter2
        FROM sku_cost sc
       WHERE sc.mrkt_id = p_mrkt_id
         AND sc.offr_perd_id = p_sls_perd_id
         AND sc.sku_id = p_sku_id
         AND cost_typ = 'P';
      SELECT ph,
             CASE
               WHEN pa_maps_public.get_sls_cls_cd(p_sls_perd_id,
                                                  p_mrkt_id,
                                                  s.avlbl_perd_id,
                                                  s.intrdctn_perd_id,
                                                  s.demo_ofs_nr,
                                                  s.demo_durtn_nr,
                                                  s.new_durtn_nr,
                                                  s.stus_perd_id,
                                                  s.dspostn_perd_id,
                                                  s.on_stus_perd_id) > '-1' THEN
                1
               ELSE
                0
             END ind
        INTO phcounter, counter3
        FROM (SELECT 1 ph FROM dual) dd
        LEFT JOIN mrkt_sku s
          ON s.mrkt_id = p_mrkt_id
         AND s.sku_id = p_sku_id;
      IF counter1 + counter2 + counter3 = 3 THEN
        -- active SKU
        -- upsert using p_new_bias
        SELECT COUNT(*)
          INTO counter1
          FROM mrkt_perd
         WHERE mrkt_id = p_mrkt_id
           AND perd_id = p_sls_perd_id;
        SELECT COUNT(*)
          INTO counter2
          FROM mrkt_sku
         WHERE mrkt_id = p_mrkt_id
           AND sku_id = p_sku_id;
        IF counter1 + counter2 = 2 THEN
          BEGIN
            SAVEPOINT before_upsert;
            merge into MRKT_PERD_SKU_BIAS TRGT
            USING (SELECT p_mrkt_id t1, p_sls_perd_id t2, p_sku_id t3, p_sls_typ_id as t4
                     FROM dual) src
            ON (trgt.mrkt_id = src.t1 AND trgt.sls_perd_id = src.t2 AND trgt.sku_id = src.t3 AND trgt.sls_typ_id = src.t4)
            WHEN MATCHED THEN
              UPDATE
                 set TRGT.BIAS_PCT          = P_NEW_SKU_BIAS,
                     trgt.last_updt_user_id = p_user_id
            WHEN NOT MATCHED THEN
              INSERT
                (mrkt_id,
                 sls_perd_id,
                 sls_typ_id,
                 sku_id,
                 bias_pct,
                 creat_user_id,
                 last_updt_user_id)
              VALUES
                (p_mrkt_id,
                 P_SLS_PERD_ID,
                 p_sls_typ_id,
                 p_sku_id,
                 p_new_sku_bias,
                 p_user_id,
                 p_user_id);
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO before_changes;
              p_stus := 2; -- Database Error
          END;
        ELSE
          p_stus := 3; -- Missing foreign keys
        END IF; -- cc2
      ELSE
        p_stus := 4; -- Passive SKU
      END IF; --ccc3
    ELSE
      p_stus := 1; -- Negative value
    END IF; -- 1 1000
  END set_sku_bias_new;

  function get_sku_bias_newest(p_mrkt_id      in number,
                         p_sls_perd_id      in number,
                         p_sls_typ_id       in number,
                         p_sku_id_array     in number_array)
    return pa_sku_bias_mantnc_table
    pipelined as
    -- for LOG
    l_module_name    VARCHAR2(30) := 'GET_SKU_BIAS2';
    l_parameter_list VARCHAR2(512) := '(p_mrkt_id: ' || to_char(p_mrkt_id) || ', ' ||
                                      'p_sls_perd_id: ' ||
                                      TO_CHAR(P_SLS_PERD_ID) || ', ' ||
                                      'p_sls_typ_id: ' ||
                                      TO_CHAR(P_SLS_typ_ID) || ', ' ||
                                      'p_sku_id_array: ' ||
                                      to_char(nvl(p_sku_id_array.FIRST,-1)) ||  '~' ||
                                      to_char(nvl(p_sku_id_array.last,-1)) || ')';
    --
    l_run_id  NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id VARCHAR(35) := USER();
    --
    L_SKU_LIST_SIZE number := P_SKU_ID_ARRAY.COUNT;
    
    CURSOR cc IS
      WITH mpsb AS
       (SELECT psb.sku_id,
               psb.bias_pct,
               psb.last_updt_ts,
               u.user_frst_nm || ' ' || u.user_last_nm last_updt_nm
          FROM mrkt_perd_sku_bias psb
          LEFT JOIN mps_user u
            ON psb.last_updt_user_id = u.user_nm
         WHERE psb.mrkt_id = p_mrkt_id
           and PSB.SLS_PERD_ID = P_SLS_PERD_ID
           and PSB.SLS_TYP_ID = P_SLS_TYP_ID
           and (L_SKU_LIST_SIZE=0 or PSB.SKU_ID in (select column_value from table(P_SKU_ID_ARRAY)))),
        TRGT_PERD as
          (select PA_MAPS_PUBLIC.PERD_PLUS(P_MRKT_ID   => P_MRKT_ID,
                                           P_PERD1     => P_SLS_PERD_ID,
                                           p_perd_diff => tpo.offst) TRGT_PERD_ID
            from
              (select distinct OFFST_VAL_TRGT_SLS offst 
                 from TA_CONFIG
                 where MRKT_ID=P_MRKT_ID
                   and SLS_TYP_GRP_NM='BI24'
                   and trgt_sls_typ_id=p_sls_typ_id) tpo)
      select PA_SKU_BIAS_MANTNC_LINE(SKU.FSC_CD,
                                     PA_MAPS_PUBLIC.GET_FSC_DESC(P_MRKT_ID => P_MRKT_ID,
                                                                 P_OFFR_PERD_ID => P_SLS_perd_ID,
                                                                 P_FSC_CD => SKU.FSC_CD),
                                     sku.sku_id,
                                     sku.lcl_sku_nm,
                                     'A',
                                     NVL2(OFFR_SKU_LINE.SKU_ID, 'P', 'N'),
                                     p_sls_typ_id,
                                     mpsb.bias_pct,
                                     mpsb.last_updt_ts,
                                     MPSB.LAST_UPDT_NM) CLINE
        from MPSB,
             (select MRKT_SKU.SKU_ID, MRKT_SKU.LCL_SKU_NM,
                     PA_MAPS_PUBLIC.GET_MSTR_FSC_CD (P_MRKT_ID => P_MRKT_ID,
                                                     P_SKU_ID  => MRKT_SKU.SKU_ID,
                                                     P_PERD_ID => P_SLS_PERD_ID) FSC_CD
                FROM (SELECT sku_id,lcl_sku_nm
                        FROM mrkt_sku
                       WHERE mrkt_sku.mrkt_id = p_mrkt_id
                         AND mrkt_sku.dltd_ind NOT IN ('Y', 'y')
                         AND pa_maps.get_sls_cls_cd(p_sls_perd_id,
                                                    p_mrkt_id,
                                                    mrkt_sku.avlbl_perd_id,
                                                    mrkt_sku.intrdctn_perd_id,
                                                    mrkt_sku.demo_ofs_nr,
                                                    mrkt_sku.demo_durtn_nr,
                                                    mrkt_sku.new_durtn_nr,
                                                    mrkt_sku.stus_perd_id,
                                                    mrkt_sku.dspostn_perd_id,
                                                    MRKT_SKU.ON_STUS_PERD_ID) > -1) MRKT_SKU,
                     (SELECT sku_id
                        FROM sku_reg_prc
                       WHERE mrkt_id = p_mrkt_id
                         and OFFR_PERD_ID = P_SLS_PERD_ID) SKU_REG_PRC,
                     (SELECT sku_id
                        FROM sku_cost
                       WHERE mrkt_id = p_mrkt_id
                         AND offr_perd_id = p_sls_perd_id
                         AND cost_typ = 'P') sku_cost
                         WHERE mrkt_sku.sku_id = sku_reg_prc.sku_id
                 AND mrkt_sku.sku_id = sku_cost.sku_id
                 and (l_sku_list_size=0 or MRKT_SKU.SKU_ID in (select column_value from table(P_SKU_ID_ARRAY)))
                 ) sku,
             (SELECT DISTINCT sku_id
                FROM offr, offr_sku_line
               WHERE offr.offr_id = offr_sku_line.offr_id
                 AND offr_sku_line.mrkt_id = p_mrkt_id
                 AND offr.mrkt_id = p_mrkt_id
                 AND offr.ver_id = 0
                 AND offr.offr_perd_id = p_sls_perd_id
                 AND offr.offr_typ = 'CMP'
                 AND offr_sku_line.offr_perd_id = p_sls_perd_id
                 AND offr_sku_line.dltd_ind NOT IN ('Y', 'y')
                 AND (l_sku_list_size=0 or offr_sku_line.sku_id IN (select column_value from table(P_SKU_ID_ARRAY)))) offr_sku_line
       WHERE sku.sku_id = offr_sku_line.sku_id(+)
         and SKU.SKU_ID = MPSB.SKU_ID(+)
       ORDER BY sku.fsc_cd * 1;
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info('After SET, ' || l_module_name || l_parameter_list ||
                       ' start');
    FOR rec IN cc LOOP
      PIPE ROW(rec.cline);
    END LOOP;
    app_plsql_log.info('After SET, ' || l_module_name || l_parameter_list ||
                       ' stop');
  end get_sku_bias_newest;

END pa_sku_bias;
/
