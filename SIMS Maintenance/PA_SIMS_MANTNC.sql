CREATE OR REPLACE package pa_sims_mantnc as 

  g_package_name CONSTANT VARCHAR2(30) := 'PA_SIMS_MANTNC';
  --
  --sales type ids
  marketing_bst_id CONSTANT NUMBER := 4;
  marketing_est_id CONSTANT NUMBER := 3;
  marketing_fst_id CONSTANT NUMBER := 5;

--  supply_bst_id CONSTANT NUMBER := 104;
--  supply_est_id CONSTANT NUMBER := 103;
--  supply_fst_id CONSTANT NUMBER := 105;

----- for testing (not enough data generated for 103 and 104 sls_type)
  supply_bst_id CONSTANT NUMBER := 4;
  supply_est_id CONSTANT NUMBER := 3;
  supply_fst_id CONSTANT NUMBER := 5;
  
  FUNCTION sct_trend_check_rpt (p_mrkt_id             IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                                p_campgn_sls_perd_id  IN dstrbtd_mrkt_sls.sls_perd_id%TYPE) 
   RETURN sct_trend_check_rpt_table 
   pipelined;
   
  FUNCTION sct_dly_updt_rpt (p_mrkt_id             IN dstrbtd_mrkt_sls.mrkt_id%TYPE,
                             p_campgn_sls_perd_id  IN dstrbtd_mrkt_sls.sls_perd_id%TYPE) 
   RETURN sct_dly_updt_rpt_table 
   pipelined;

end pa_sims_mantnc;
/


CREATE OR REPLACE package body pa_sims_mantnc as

  function sct_trend_check_rpt (p_mrkt_id             in dstrbtd_mrkt_sls.mrkt_id%type,
                                p_campgn_sls_perd_id  in dstrbtd_mrkt_sls.sls_perd_id%type) 
   return sct_trend_check_rpt_table 
   pipelined AS
   
   l_actual_perd_id NUMBER := p_campgn_sls_perd_id;
   l_next_perd_id   NUMBER := PA_MAPS_PUBLIC.PERD_PLUS(p_mrkt_id,p_campgn_sls_perd_id,1);
   l_next1_perd_id  NUMBER := PA_MAPS_PUBLIC.PERD_PLUS(p_mrkt_id,p_campgn_sls_perd_id,2);
   l_prev_perd_id   NUMBER := PA_MAPS_PUBLIC.PERD_PLUS(p_mrkt_id,p_campgn_sls_perd_id,-1);
   
   l_last_accs_dt   DATE;
   
   CURSOR cc IS
     WITH details AS
       (SELECT CASE WHEN dstrbtd_mrkt_sls.sls_perd_id = l_actual_perd_id AND dstrbtd_mrkt_sls.sls_typ_id = supply_bst_id
               THEN 1
               WHEN dstrbtd_mrkt_sls.sls_perd_id = l_next_perd_id AND dstrbtd_mrkt_sls.sls_typ_id = supply_est_id
               THEN 2
               WHEN dstrbtd_mrkt_sls.sls_perd_id = l_next1_perd_id AND dstrbtd_mrkt_sls.sls_typ_id = supply_est_id
               THEN 3
               ELSE 4 END otr,               
               PA_MAPS_PUBLIC.GET_MSTR_FSC_CD(P_MRKT_ID => p_mrkt_id,P_SKU_ID => offr_sku_line.sku_id, P_PERD_ID => dstrbtd_mrkt_sls.sls_perd_id) fsc_cd,
               dstrbtd_mrkt_sls.sls_perd_id,
               dstrbtd_mrkt_sls.sls_typ_id,
               round(SUM(nvl(dstrbtd_mrkt_sls.unit_qty, 0))) units
          FROM dstrbtd_mrkt_sls,
               offr_sku_line,
               offr_prfl_prc_point,
               offr
         WHERE dstrbtd_mrkt_sls.mrkt_id = p_mrkt_id
           AND ((dstrbtd_mrkt_sls.sls_perd_id = l_actual_perd_id
                AND dstrbtd_mrkt_sls.sls_typ_id = supply_bst_id)
             OR (dstrbtd_mrkt_sls.sls_perd_id = l_next_perd_id
                AND dstrbtd_mrkt_sls.sls_typ_id = supply_est_id)   
             OR (dstrbtd_mrkt_sls.sls_perd_id = l_next1_perd_id
                AND dstrbtd_mrkt_sls.sls_typ_id = supply_est_id)   
             OR (dstrbtd_mrkt_sls.sls_perd_id = l_actual_perd_id
                AND dstrbtd_mrkt_sls.sls_typ_id = 2))   
           AND dstrbtd_mrkt_sls.offr_sku_line_id =
               offr_sku_line.offr_sku_line_id
           AND offr_sku_line.dltd_ind <> 'Y'
           AND offr_sku_line.offr_prfl_prcpt_id =
               offr_prfl_prc_point.offr_prfl_prcpt_id
           AND offr_prfl_prc_point.offr_id = offr.offr_id
           and OFFR.OFFR_TYP = 'CMP'
--           AND offr.ver_id = 0
         GROUP BY PA_MAPS_PUBLIC.GET_MSTR_FSC_CD(P_MRKT_ID => p_mrkt_id, P_SKU_ID => offr_sku_line.sku_id, P_PERD_ID => dstrbtd_mrkt_sls.sls_perd_id),
                  dstrbtd_mrkt_sls.sls_perd_id,
                  dstrbtd_mrkt_sls.sls_typ_id)
    SELECT 
     D1.FSC_CD,
     PA_MAPS_PUBLIC.GET_FSC_DESC(P_MRKT_ID => P_MRKT_ID, P_OFFR_PERD_ID => D1.SLS_PERD_ID, P_FSC_CD =>D1.FSC_CD) FSC_NM,
     D1.UNITS D1_UNITS,
     D2.UNITS D2_UNITS,
     CASE WHEN (SELECT count(1) FROM details WHERE otr=3)=0 THEN d4.sls_typ_id ELSE d3.sls_typ_id END d3_sls_typ_id
    from DETAILS D1
    left JOIN details d2 ON d1.fsc_cd=d2.fsc_cd AND d2.otr=2
    left JOIN details d4 ON d1.fsc_cd=d4.fsc_cd AND d4.otr=4
    left JOIN details d3 on d1.fsc_cd=d3.fsc_cd and d3.otr=3
    where d1.otr=1;
  begin
    select max(PRCSNG_DT)
     into l_last_accs_dt 
     from DLY_BILNG_TRND
     where MRKT_ID=P_MRKT_ID and OFFR_PERD_ID=P_CAMPGN_SLS_PERD_ID;

    for REC in CC LOOP
      PIPE row(
        SCT_TREND_CHECK_RPT_LINE(
          L_LAST_ACCS_DT,
          REC.FSC_CD,
          REC.FSC_NM,
          L_ACTUAL_PERD_ID,
          L_ACTUAL_PERD_ID,
          REC.D1_UNITS,
          L_next_PERD_ID,
          L_PREV_PERD_ID,
          REC.D2_UNITS,
          L_NEXT1_PERD_ID,
          L_ACTUAL_PERD_ID,
          REC.D2_UNITS
        ));
    END LOOP;

  end sct_trend_check_rpt;

  function sct_dly_updt_rpt (p_mrkt_id             in dstrbtd_mrkt_sls.mrkt_id%type,
                             p_campgn_sls_perd_id  in dstrbtd_mrkt_sls.sls_perd_id%type) 
   return sct_dly_updt_rpt_table 
   PIPELINED as
   
   l_actual_perd_id NUMBER := p_campgn_sls_perd_id;
   l_next_perd_id   NUMBER := PA_MAPS_PUBLIC.PERD_PLUS(p_mrkt_id,p_campgn_sls_perd_id,1);
   l_next1_perd_id  NUMBER := PA_MAPS_PUBLIC.PERD_PLUS(p_mrkt_id,p_campgn_sls_perd_id,2);
   l_prev_perd_id   NUMBER := PA_MAPS_PUBLIC.PERD_PLUS(p_mrkt_id,p_campgn_sls_perd_id,-1);
   
   L_LAST_ACCS_DT   date;
   L_CASH_VALUE     number;
   l_r_factor       number;
   
   cursor CC is
     select dstrbtd_mrkt_sls.sls_perd_id,
            dstrbtd_mrkt_sls.sls_typ_id,
            ROUND(SUM(NVL(DSTRBTD_MRKT_SLS.UNIT_QTY, 0) *
                      nvl(offr_prfl_prc_point.sls_prc_amt, 0) /
                      decode(nvl(offr_prfl_prc_point.nr_for_qty, 0),
                             0,
                             1,
                             offr_prfl_prc_point.nr_for_qty) *
                      decode(nvl(offr_prfl_prc_point.net_to_avon_fct, 0),
                             0,
                             1,
                             offr_prfl_prc_point.net_to_avon_fct))) sales
          FROM dstrbtd_mrkt_sls,
               offr_sku_line,
               offr_prfl_prc_point,
               offr
         WHERE dstrbtd_mrkt_sls.mrkt_id = p_mrkt_id
           AND ((dstrbtd_mrkt_sls.sls_perd_id = l_actual_perd_id
                AND dstrbtd_mrkt_sls.sls_typ_id = supply_bst_id)
             OR (dstrbtd_mrkt_sls.sls_perd_id = l_next_perd_id
                AND dstrbtd_mrkt_sls.sls_typ_id = supply_est_id)   
             OR (dstrbtd_mrkt_sls.sls_perd_id = l_next1_perd_id
                AND dstrbtd_mrkt_sls.sls_typ_id = supply_est_id))   
           AND dstrbtd_mrkt_sls.offr_sku_line_id =
               offr_sku_line.offr_sku_line_id
           AND offr_sku_line.dltd_ind <> 'Y'
           AND offr_sku_line.offr_prfl_prcpt_id =
               offr_prfl_prc_point.offr_prfl_prcpt_id
           AND offr_prfl_prc_point.offr_id = offr.offr_id
           and OFFR.OFFR_TYP = 'CMP'
--           and OFFR.VER_ID = 0
         GROUP BY dstrbtd_mrkt_sls.sls_perd_id,
                  DSTRBTD_MRKT_SLS.SLS_TYP_ID
     ;
     
         -- for LOG
    l_run_id         NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id        VARCHAR(35) :=  USER();
    l_module_name    VARCHAR2(30) := 'sct_dly_updt_rpt';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       TO_CHAR(P_MRKT_ID) || ', ' ||
                                       'p_campgn_SLS_perd_id: ' ||
                                       to_char(p_campgn_SLS_perd_id) || ')';
    --
  BEGIN
    --
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    APP_PLSQL_LOG.SET_CONTEXT(L_USER_ID, G_PACKAGE_NAME, L_RUN_ID);
    app_plsql_log.info(l_module_name || ' start' || l_parameter_list);

    select max(PRCSNG_DT)
     into l_last_accs_dt 
     from DLY_BILNG_TRND
     where MRKT_ID=P_MRKT_ID and OFFR_PERD_ID=P_CAMPGN_SLS_PERD_ID;

      for REC in CC LOOP
        app_plsql_log.info(l_module_name ||
                           ' 1 for mrkt_id=' ||
                           TO_CHAR(P_MRKT_ID) ||
                           ' and >>> sls_perd_id=' ||
                           TO_CHAR(REC.SLS_PERD_ID) ||
                           ' and sls_typ_id=' ||
                           TO_CHAR(rec.sls_typ_id) || ' <<<' ||
                           l_parameter_list);
        begin
          select distinct 
             FIRST_VALUE(CASH_VAL) 
                over (partition by MRKT_ID,SLS_PERD_ID,SLS_TYP_ID order by LAST_UPDT_TS desc),
             FIRST_VALUE(R_FACTOR) 
                over (partition by MRKT_ID,SLS_PERD_ID,SLS_TYP_ID order by LAST_UPDT_TS desc)
           into l_cash_value, l_r_factor
           from CASH_VAL_RF_HIST
           where MRKT_ID=P_MRKT_ID
             and SLS_PERD_ID = rec.sls_perd_id
             and SLS_TYP_ID = REC.SLS_typ_ID
          ;
        EXCEPTION when NO_DATA_FOUND then
          APP_PLSQL_LOG.INFO(L_MODULE_NAME ||
                           ' NO_DATA_FOUND ' ||
                           '  >>> sls_perd_id=' ||
                           TO_CHAR(REC.SLS_PERD_ID) ||
                           ' and sls_typ_id=' ||
                           L_PARAMETER_LIST);
          L_CASH_VALUE := null;
          l_r_factor := null;
        end;
        PIPE row(
          SCT_DLY_UPDT_RPT_LINE(
            l_last_accs_dt,
            case rec.sls_perd_id
              when L_NEXT_PERD_ID then L_PREV_PERD_ID
              else l_actual_perd_id end,
            REC.SLS_TYP_ID,
            l_cash_value,
            REC.SALES,
            l_r_factor
        ));
      END LOOP;
  end sct_dly_updt_rpt;

end pa_sims_mantnc;
/
