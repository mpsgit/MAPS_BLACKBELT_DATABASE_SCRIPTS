CREATE OR REPLACE package pa_sims_mantnc as 

  g_package_name CONSTANT VARCHAR2(30) := 'PA_SIMS_MANTNC';
  --
  -- sales types for dbt
  estimate       CONSTANT NUMBER := 1;
  demand_actuals CONSTANT NUMBER := 6;
  BILLED_ACTUALS CONSTANT number := 7;
  --
  --sales type ids
  marketing_bst_id CONSTANT NUMBER := 4;
  marketing_est_id CONSTANT NUMBER := 3;
  marketing_fst_id CONSTANT NUMBER := 5;

  supply_bst_id CONSTANT NUMBER := 104;
  supply_est_id CONSTANT NUMBER := 103;
  supply_fst_id CONSTANT NUMBER := 105;

----- for testing (not enough data generated for 103 and 104 sls_type)
--  supply_bst_id CONSTANT NUMBER := 4;
--  supply_est_id CONSTANT NUMBER := 3;
--  supply_fst_id CONSTANT NUMBER := 5;
  
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
             and DSTRBTD_MRKT_SLS.VER_ID = 0
           AND dstrbtd_mrkt_sls.offr_sku_line_id =
               offr_sku_line.offr_sku_line_id
           AND offr_sku_line.dltd_ind <> 'Y'
           AND offr_sku_line.offr_prfl_prcpt_id =
               offr_prfl_prc_point.offr_prfl_prcpt_id
           AND offr_prfl_prc_point.offr_id = offr.offr_id
           AND offr.offr_typ = 'CMP'
           AND offr.ver_id = 0
         GROUP BY PA_MAPS_PUBLIC.GET_MSTR_FSC_CD(P_MRKT_ID => p_mrkt_id, P_SKU_ID => offr_sku_line.sku_id, P_PERD_ID => dstrbtd_mrkt_sls.sls_perd_id),
                  dstrbtd_mrkt_sls.sls_perd_id,
                  dstrbtd_mrkt_sls.sls_typ_id)
    SELECT
     D1.FSC_CD,
     PA_MAPS_PUBLIC.GET_FSC_DESC(P_MRKT_ID => P_MRKT_ID, P_OFFR_PERD_ID => D1.SLS_PERD_ID, P_FSC_CD =>D1.FSC_CD) FSC_NM,
     D1.SLS_TYP_ID D1_SLS_TYP_ID,
     D1.UNITS D1_UNITS,
     d2.sls_typ_id d2_sls_typ_id,
     D2.UNITS D2_UNITS,
     CASE WHEN (SELECT count(1) FROM details WHERE otr=3)=0 THEN d4.sls_typ_id ELSE d3.sls_typ_id END d3_sls_typ_id,
     case when (select count(1) from details where otr=3)=0 then d4.units else d3.units end d3_units
    FROM details d1
    JOIN details d2 ON d1.fsc_cd=d2.fsc_cd AND d2.otr=2
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
      with perd_data as(
        select distinct
          LR.MRKT_ID,
          LR.SLS_PERD_ID,
          LR.SLS_TYP_ID,
          LR.PRCSNG_DT,
          FIRST_VALUE(CV.CASH_VAL)
            over (partition by cv.MRKT_ID,cv.SLS_PERD_ID,cv.SLS_TYP_ID order by cv.LAST_UPDT_TS desc) CASH_VAL,
          FIRST_VALUE(CV.R_FACTOR)
            over (partition by CV.MRKT_ID,CV.SLS_PERD_ID,CV.SLS_TYP_ID order by CV.LAST_UPDT_TS desc) R_FACTOR
        from
          (select 
              MRKT_ID,
              SLS_PERD_ID,
              SLS_TYP_ID,
              max(TRUNC(LAST_UPDT_TS)) PRCSNG_DT
            from CASH_VAL_RF_HIST
            where MRKT_ID=P_MRKT_ID
              and ((SLS_PERD_ID =L_ACTUAL_PERD_ID and SLS_TYP_ID  = SUPPLY_BST_ID)
                or (SLS_PERD_ID =l_next_perd_id and SLS_TYP_ID  = supply_bst_id)
                or (SLS_PERD_ID = l_next1_perd_id and SLS_TYP_ID  = supply_bst_id))
            group by MRKT_ID,SLS_PERD_ID,SLS_TYP_ID) LR
          join CASH_VAL_RF_HIST CV on CV.MRKT_ID=LR.MRKT_ID and CV.SLS_PERD_ID=LR.SLS_PERD_ID and CV.SLS_TYP_ID=LR.SLS_TYP_ID),
      LAST_RUN as(
        select PD.*,
           pa_maps_public.perd_plus(P_MRKT_ID =>pd.mrkt_id,P_PERD1 => pd.sls_perd_id, P_PERD_DIFF => nvl(tof.offst,0)) offr_perd_id
        from PERD_DATA PD
          left join TREND_OFFST TOF
            on PD.MRKT_ID=TOF.MRKT_ID and PD.SLS_TYP_ID=TOF.SLS_TYP_ID)
      SELECT sls_perd_id,
            sls_typ_id,
            ROUND(SUM(NVL(UNIT_QTY, 0) * NVL(sls_prc_amt, 0) / DECODE(NVL(nr_for_qty, 0), 0, 1, nr_for_qty) * DECODE(NVL(net_to_avon_fct, 0), 0, 1, net_to_avon_fct))) sales,
            MAX(cash_val) cash_val,
            MAX(r_factor) r_factor,
            max(offr_perd_id) offr_perd_id,
            max(prcsng_dt) prcsng_dt
          FROM
            (SELECT dly_bilng_trnd.dly_bilng_id,
              DLY_BILNG_TRND.TRND_SLS_PERD_ID SLS_PERD_ID,
              last_run.sls_typ_id,
              MAX(dly_bilng_trnd.unit_qty) unit_qty,
              MAX(offr_prfl_prc_point.sls_prc_amt) sls_prc_amt,
              MAX(offr_prfl_prc_point.nr_for_qty) nr_for_qty,
              MAX(offr_prfl_prc_point.net_to_avon_fct) net_to_avon_fct,
              MAX(last_run.cash_val) cash_val,
              MAX(last_run.r_factor) r_factor,
              max(dly_bilng_trnd.offr_perd_id) offr_perd_id,
              max(dly_bilng_trnd.prcsng_dt) prcsng_dt
            FROM dly_bilng_trnd,
              dly_bilng_trnd_offr_sku_line,
              dstrbtd_mrkt_sls,
              offr_sku_line,
              offr_prfl_prc_point,
              LAST_RUN
            where DLY_BILNG_TRND.MRKT_ID                      = LAST_RUN.MRKT_ID
            and DLY_BILNG_TRND.TRND_SLS_PERD_ID               = LAST_RUN.SLS_PERD_ID
            AND dly_bilng_trnd.offr_perd_id                   = LAST_RUN.offr_PERD_ID
            AND dly_bilng_trnd.dly_bilng_id                   = dly_bilng_trnd_offr_sku_line.dly_bilng_id
            AND dly_bilng_trnd_offr_sku_line.offr_sku_line_id = dstrbtd_mrkt_sls.offr_sku_line_id
            AND dstrbtd_mrkt_sls.offr_Sku_line_id             = offr_sku_line.offr_sku_line_id
            and OFFR_SKU_LINE.OFFR_PRFL_PRCPT_ID              = OFFR_PRFL_PRC_POINT.OFFR_PRFL_PRCPT_ID
            and DSTRBTD_MRKT_SLS.SLS_TYP_ID                   = estimate
            AND dly_bilng_trnd_offr_sku_line.sls_typ_id       = demand_actuals
            AND dly_bilng_trnd.mrkt_id                        = last_run.mrkt_id(+)
            AND dly_bilng_trnd.trnd_sls_perd_id               = last_run.sls_perd_id(+)
            AND TRUNC(dly_bilng_trnd.prcsng_dt)              <= NVL(last_run.prcsng_dt,sysdate)
            group by DLY_BILNG_TRND.DLY_BILNG_ID,
              DLY_BILNG_TRND.TRND_SLS_PERD_ID,last_run.sls_typ_id
            )
          group by SLS_PERD_ID,
            SLS_TYP_ID
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

      for REC in CC LOOP
        PIPE row(
          SCT_DLY_UPDT_RPT_LINE(
            rec.prcsng_dt,
            rec.offr_perd_id,
            REC.SLS_TYP_ID,
            rec.cash_val,
            REC.SALES,
            rec.r_factor
        ));

      END LOOP;
  end sct_dly_updt_rpt;

end pa_sims_mantnc;
/
