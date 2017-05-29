CREATE OR REPLACE PACKAGE "CPY_OFFR"
AS

  /*******************************************************************************************
  * Modification History
  *
  * Date         : 01/02/2010
  * Developer    : Unknown
  * Description  : For QC2747 - Copy to Market (batch) - include units based on UPO by Category
  *                FOR QC 2940 -  CEE_Supply Chain_FB  Asynchronous Batch Jobs
  *                (Create Versions, Copy to Local Market, Copy Units)
  *
  * Date         : 30/03/2011
  * Developer    : Sivakumar. D.R
  * Description  : Removed APP_PLSQL logs
  *                Added analytical function for srce_offr_id
  *                outer join fix in the main query sku_reg_prc and sku_cost for source and target
  *                changed srce_<information> trgt_information in osl info in main query
  *                Do the missing skus only at the last execution of offr loop; -- Add l_job_typ check
  *
  * Date         : 29/04/2011
  * Developer    : Ramkumar V
  * Description  : QC3305-CEE - Forecast Boost - change to Status Messages in Job results output
  *                (i.e. Completed with Warnings, Completed with Errors)
  *
  * Date         : 05/05/2011
  * Developer    : Ramkumar V
  * Description  : QC3321-CEE - Forecast Boost (from Versions screen) does not appear -
  *                due to wrongly checking source Market Campaign is On,
  *                and wrongly checking Market Relationship has End Period
  *
  * Date         : 11/05/2011
  * Developer    : Ramkumar V
  * Description  : QC3336 -CEE - Copy Vehicle to Local Vehicle - not clearing Section/ Sub-Section
  *                (when copying an empty source Vehicle)
  *
  * Date         : 29/04/2011
  * Developer    : Sivakumar. D.R
  * Description  : QC3329 - Handled the Linenr copy Issue raised at UAT
  *
  * Date         : 17/05/2011
  * Developer    : Sivakumar. D.R
  * Description  : QC3337 - To avoid the disable item creation when retain selling price is ON
  *                QC3347 - To populate rpt_sbtl_typ_id as '2' always in OFFR table
  *
  * Date         : 23/05/2011
  * Developer    : Ramkumar V, Sivakumar. D.R
  * Description  : QC3359 CEE - Forecast Boost - calciulation sum of Boosted target Units are wrong
  *                (Target Sales is mulitplied by number of source Vehicles?)
  *
  * Date         : 24/05/2011
  * Developer    : Sivakumar. D.R, Lakshmana Karri
  * Description  : QC3361 - To handle items  in Forecastboost exclusion
  *                QC3362 - To handle items to be boosted and items not boosted
  *
  * Date         : 01/07/2011
  * Developer    : Sivakumar. D.R
  * Description  : QC3422 - To handle the price point creation issue identified
  *
  * Date         : 11/07/2011
  * Developer    : Sivakumar. D.R
  * Description  : QC3422 - Set Cmpnt_QTY fix in Forecast boost version
  *
  * Date         : 23/07/2011
  * Developer    : Sivakumar. D.R
  * Description  : Modified for the fix QC3422
  *
  * Date         : 02/12/2011
  * Developer    : Lakshmana Karri
  * Description  :
  *
  * Date         : 02/12/2011
  * Developer    : Lakshmana Karri
  * Description  : QC3735 - To remove the usage of wm_concat()
  *
  * Date         : 20/03/2012
  * Developer    : James Allingham
  * Description  : Modified to fix a bug in Product Sales Screen caused by cmpgn_ver_creat_dt being set to sysdate
  *                for Version 0, this should always be set to null.
  *
  * Date         : 13/09/2012
  * Developer    : Sivakumar. D.R
  * Description  : QC3555 - Added mltpl_ind and cmltv_ind for Edit item CEV enhancement
  *
  * Date         : 22/03/2013
  * Developer    : Fiona Lindsay
  * Description  : Populate DMS with trgt_ver_id when creating a new version
  *
  * Date         : 01/10/2013
  * Developer    : Fiona Lindsay
  * Description  : Handle null SLS_STUS_CD line 10298 (ask James!)
  *
  * Date         : 30/10/2013
  * Developer    : Fiona Lindsay
  * Description  : Populate DMS with trgt_ver_id for second insert in create_version
  *
  * Date         : 15/01/2014
  * Developer    : Fiona Lindsay
  * Description  : INC 2547884 Forecast boost not populating units - UPDATE_TRGT_UNITS procedure
  *                main query in forecast boost section did not include grp_typ in the group by of
  *                the srce section, resulting in ORA-00979 not a GROUP BY expression
  *
  * Date         : 15/05/2014
  * Developer    : Fiona Lindsay
  * Description  : Issue in batch copy process where missing MRKT_PRFL records cause copy process
  *                to crash out rather than continuing to copy remaining offers - fix made to
  *                app_cpy_offr_trgt_prfls_info package and comments changed here to clarify
  *                reasons for errors.
  *                Also removed large chunk of commented out code.
  *
  * Date         : 06/08/2014
  * Developer    : Julie Scotney
  * Description  : INC000002865470 - Issue in in Create Version functionality which
  *                wrongly deletes mrkt_veh_perd_plcmt rows as source and target market,
  *                offer period and vehicle are the same
  *                Changed code is around DELETE FROM mrkt_veh_perd_brchr_plcmt ...
  *
  * Date         : 15/01/2015
  * Developer    : Fiona Lindsay
  * Description  : Added code to handle WSL_IND to create_version - value is copied if target version is not WiP
  *
  * Date         : 28/01/2015
  * Developer    : Fiona Lindsay
  * Description  : Added support for additional Pricing Corridors OPP columns to create_version
  *
  * Date         : 09/02/2015
  * Developer    : Fiona Lindsay
  * Description  : Changes to selling price calculations for Pricing Corridors
  *
  * Date         : 10/03/2015
  * Developer    : Fiona Lindsay
  * Description  : copy_offer - fix select which determines whether price point already exists or not
  *                             changed to use rec.oppp_sls_prc_amt instead of rec.calct_trgt_sls_prc_amt
  *
  * Date         : 24/04/2015
  * Developer    : Fiona Lindsay
  * Description  : Added plnd_promtn_id to OPP insert and update for copy offer and create version
  *
  * Date         : 24/04/2015
  * Developer    : Fiona Lindsay
  * Description  : Changed copy offer to include number for in Pricing Corridors logic
  *
  * Date         : 11/05/2016
  * Developer    : Fiona Lindsay
  * Description  : Changed unit quantity calculation for Forecast Boost to include logic for the
  *                new Extrapolation Parameter
  *
  *********************************************************************************************/

   SUBTYPE single_char IS char(1);

   g_run_id      NUMBER         ;
   g_user_id     NUMBER         := 99999;
   g_module_nm   VARCHAR2 (30) := 'CPY_OFFR';

   TYPE trgt_mrkt_details IS RECORD (
      srce_mrkt_id        mrkt.mrkt_id%TYPE,
      srce_mrkt_nm        mrkt.mrkt_nm%TYPE,
      srce_perd_id        mrkt_perd_cmpgn_mapg.srce_perd_id%TYPE,
      trgt_mrkt_id        mrkt.mrkt_id%TYPE,
      trgt_mrkt_nm        mrkt.mrkt_nm%TYPE,
      trgt_perd_id        mrkt_perd_cmpgn_mapg.trgt_perd_id%TYPE,
      veh_id              mrkt_veh.veh_id%TYPE,
      veh_nm              mrkt_veh.lcl_veh_desc_txt%TYPE,
      mrkt_acs_ind        CHAR (1),
      veh_acs_ind         CHAR (1),
      calc_ind            CHAR (1),
      est_typ_ind         CHAR (1),
      used_vehicles_ind   CHAR (1),
      forecastboost_ind   CHAR (1),
      camp_lock_ind       CHAR (1),
      offr_lock_ind       CHAR (1),
      mrkt_rel_ind        CHAR (1),
      veh_acs_ind1        CHAR (1)
   );

   TYPE ttable_result IS TABLE OF trgt_mrkt_details;

   TYPE trec_parm_info IS RECORD (
      parm_id                          mps_parm.mps_parm_id%TYPE,
      srce_mrkt_id                     mps_parm.srce_mrkt_id%TYPE,
      srce_ver_id                      mps_parm.srce_ver_id%TYPE,
      srce_veh_id                      mps_parm_srce_veh.veh_id%TYPE,
      srce_offr_perd_id                mps_parm_srce_offr_perd.offr_perd_id%TYPE,
      srce_offr_perd_refmrkt           mps_parm_srce_offr_perd.mrkt_id%TYPE,
      srce_sls_perd_id                 mps_parm_srce_sls_perd.sls_perd_id%TYPE,
      srce_sls_perd_refmrkt            mps_parm_srce_sls_perd.mrkt_id%TYPE,
      srce_offr_id                     mps_parm_offr.offr_id%TYPE,
------------------------------------------
      set_to_reqrng_plnrs_rev_ind      mps_parm.set_to_reqrng_plnrs_rev_ind%TYPE,
      offr_hdr_only_ind                mps_parm.offr_hdr_only_ind%TYPE,
      rtain_prc_pnts_when_rplcng_ind   mps_parm.rtain_prc_pnts_when_rplcng_ind%TYPE,
      updt_what_if_ind                 mps_parm.updt_what_if_ind%TYPE,
      dlt_srce_offr_ind                mps_parm.dlt_srce_offr_ind%TYPE,
      trgt_mrkt_veh_perd_sctn_id       mps_parm.trgt_mrkt_veh_perd_sctn_id%TYPE,
      replctn_mov_mrkt_id              mps_parm_replctn_mov_mrkt.mrkt_id%TYPE,
      replctn_mov_seq_nr               mps_parm_replctn_mov_mrkt.seq_nr%TYPE,
-------------------------------------------
      trgt_mrkt_id                     mps_parm_trgt_mrkt.mrkt_id%TYPE,
      trgt_ver_id                      mps_parm_trgt_ver.ver_id%TYPE,
      trgt_veh_id                      mps_parm_trgt_veh.veh_id%TYPE,
      trgt_offr_perd_id                mps_parm_trgt_offr_perd.offr_perd_id%TYPE,
      trgt_offr_perd_refmrkt           mps_parm_trgt_offr_perd.mrkt_id%TYPE,
      trgt_sls_perd_id                 mps_parm_trgt_sls_perd.sls_perd_id%TYPE,
      trgt_sls_perd_refmrkt            mps_parm_trgt_sls_perd.mrkt_id%TYPE,
      retain_price                     mps_parm_trgt_mrkt.rtain_sls_prc_ind%TYPE,
      mrkt_unit_calc_typ_id            mps_parm_trgt_mrkt.copy_units_rul_id%TYPE,
      veh_unit_calc_typ_id             mps_parm_trgt_veh.copy_units_rul_id%TYPE,
      perd_unit_calc_typ_id            mps_parm_trgt_offr_perd.copy_units_rul_id%TYPE
   );

   TYPE t_tbl_parm_info IS TABLE OF trec_parm_info;

   TYPE t_rec_key_vals_for_update IS RECORD (
      parm_id             mps_parm.mps_parm_id%type,
      srce_mrkt_id        offr.mrkt_id%TYPE,
      srce_veh_id         offr.veh_id%TYPE,
      srce_offr_perd_id   offr.offr_perd_id%TYPE,
      srce_ver_id         offr.ver_id%TYPE,
      srce_offr_id        offr.offr_id%TYPE,
      srce_oppp_id        offr_prfl_prc_point.offr_prfl_prcpt_id%TYPE,
      srce_osl_id         offr_sku_line.offr_sku_line_id%TYPE,
      trgt_mrkt_id        offr.mrkt_id%TYPE,
      trgt_veh_id         offr.veh_id%TYPE,
      trgt_offr_perd_id   offr.offr_perd_id%TYPE,
      trgt_ver_id         offr.ver_id%TYPE,
      trgt_offr_id        offr.offr_id%TYPE,
      trgt_oppp_id        offr_prfl_prc_point.offr_prfl_prcpt_id%TYPE,
      trgt_osl_id         offr_sku_line.offr_sku_line_id%TYPE,
      prfl_cd             prfl.prfl_cd%TYPE,
      sku_id              sku.sku_id%TYPE,
      trgt_sku_id              sku.sku_id%TYPE,
      sls_cls_cd          offr_prfl_prc_point.sls_cls_cd%TYPE,
      ret_price           VARCHAR2 (1),
      unit_calc_ind       VARCHAR2 (1),
      srce_brchr_plcmt_id  offr.brchr_plcmt_id%type,
      srce_mrkt_veh_perd_sctn_id offr.mrkt_veh_perd_sctn_id%type ,
      srce_comsn_typ        offr_prfl_prc_point.comsn_typ%type ,
      dms_sls_perd_id     dstrbtd_mrkt_sls.sls_perd_id%type,
      dms_sls_typ_id      dstrbtd_mrkt_sls.sls_typ_id%type,
      trgt_sls_prc_amt    offr_prfl_prc_point.sls_prc_amt%type,
      chrty_amt           offr_prfl_prc_point.chrty_amt%type,
      roylt_pct           offr_prfl_prc_point.roylt_pct%type,
      awrd_sls_prc_amt    offr_prfl_prc_point.awrd_sls_prc_amt%type,
      comsn_typ           offr_prfl_prc_point.comsn_typ%type,
      tax_typ             offr_prfl_prc_point.tax_type_id%type
   );

   TYPE t_tbl_key_vals_for_update IS TABLE OF t_rec_key_vals_for_update ;


   TYPE t_oss_line_nr IS RECORD (
      offr_id               offr_sku_line.offr_id%TYPE,
      osl_id                offr_sku_line.offr_sku_line_id%TYPE,
      oss_id                offr_sku_line.offr_sku_set_id%TYPE,
      line_nr               offr_sku_set.line_nr%TYPE,
      line_nr_typ_id        offr_sku_set.line_nr_typ_id%TYPE,
      fsc_cd                offr_sku_set.fsc_cd%TYPE
   );

   TYPE ttable_oss_line_nr IS TABLE OF t_oss_line_nr;

   TYPE t_dlt_invld_recs IS RECORD (
      offr_id               offr.offr_id%TYPE,
      sls_cls_cd            offr_prfl_sls_cls_plcmt.sls_cls_cd%TYPE,
      prfl_cd               offr_prfl_sls_cls_plcmt.prfl_cd%TYPE,
      pg_ofs_nr             offr_prfl_sls_cls_plcmt.pg_ofs_nr%TYPE,
      featrd_side_cd        offr_prfl_sls_cls_plcmt.featrd_side_cd%TYPE,
      offr_prfl_prcpt_id    offr_prfl_prc_point.offr_prfl_prcpt_id%TYPE,
      offr_sku_line_id      offr_sku_line.offr_sku_line_id%TYPE,
      sku_id                offr_sls_cls_sku.sku_id%TYPE
   );

   TYPE ttable_dlt_invld_recs IS TABLE OF t_dlt_invld_recs;

  type trec_sku_data is record(
prfl_cd            prfl.prfl_cd%TYPE,
sku_id             sku.sku_id%TYPE,
reg_prc_amt    sku_reg_prc.reg_prc_amt%TYPE,
sls_cls_cd        sls_cls.sls_cls_cd%TYPE,
cost_amt         sku_cost.wghtd_avg_cost_amt%TYPE,
cost_typ          sku_cost.cost_typ%TYPE
);


type ttbl_sku_data is table of trec_sku_data;

   FUNCTION F_GET_KEY_VALS_FOR_UPDATE --(p_key_vals_for_update IN t_tbl_key_vals_for_update)
      RETURN t_tbl_key_vals_for_update PIPELINED;

   FUNCTION f_get_parms (p_parm_id IN NUMBER)
      RETURN t_tbl_parm_info PIPELINED;

   FUNCTION get_tgrt_mrkt_dtl (
      p_clsrt_id       mps_user_mrkt_veh_acs.clstr_id%TYPE,
      p_srce_mrkt_id   mrkt_veh_perd_ver.mrkt_id%TYPE,
      p_srce_veh_id    VARCHAR2,
      p_srce_ver_id    mrkt_veh_perd_ver.ver_id%TYPE,
      p_srce_perd_id   offr.offr_perd_id%TYPE,
      p_user_nm        VARCHAR2
   )
      RETURN ttable_result PIPELINED;

   PROCEDURE insert_btch_task_log (
      p_btch_task_id           btch_task.btch_task_id%TYPE,
      p_btch_task_log_typ_id   btch_task_log_typ.btch_task_log_typ_id%TYPE,
      p_btch_task_key          NUMBER,
      p_btch_err_lvl_txt       btch_task_log.btch_task_log_err_lvl_txt%TYPE,
      p_btch_log_txt           btch_task_log.btch_task_log_desc_txt%TYPE,
      p_boosted_qty            btch_task_log.btch_task_log_to_be_boostd_qty%TYPE,
      p_not_boosted_qty        btch_task_log.btch_task_log_not_boostd_qty%TYPE,
      p_trgt_mrkt_id           NUMBER
   );

   PROCEDURE parms (
      cpy_offr_parms   IN       tbl_mps_parm,
      p_user_nm        IN       VARCHAR2,
      parm_id          OUT      mps_parm.mps_parm_id%TYPE
   );


   PROCEDURE UPDATE_TRGT_UNITS(
        p_parm_id                 IN mps_parm.mps_parm_id%type,
        p_srce_mrkt_id            IN offr.mrkt_id%type,
        p_srce_veh_id             IN offr.veh_id%type,
        p_srce_offr_perd_id       IN offr.offr_perd_id%type,
        p_srce_ver_id             IN offr.ver_id%type,
        p_trgt_mrkt_id            IN offr.mrkt_id%type,
        p_trgt_veh_id             IN offr.veh_id%type,
        p_trgt_offr_perd_id       IN offr.offr_perd_id%type,
        p_trgt_ver_id             IN offr.ver_id%type,
        p_unit_calc_typ_id        IN COPY_UNITS_RUL.COPY_UNITS_RUL_ID%type,
        p_fb_ind                  IN CHAR,
        p_status                  IN OUT NUMBER,
        p_veh_id                  IN VARCHAR2 ,
        p_fb_xclud_sls_cls_cd     IN VARCHAR2
   );

   PROCEDURE copy_offr (
      p_parm_id   IN   mps_parm.mps_parm_id%TYPE,
      p_user_nm   IN   VARCHAR2,
      p_task_id   IN   NUMBER DEFAULT NULL,
      p_status    OUT   NUMBER -- '0' -> Success; '1' -> Error;
   );

   PROCEDURE cpy_offr_scrn (
      cpy_offr_parms   IN       tbl_mps_parm,
      p_user_nm        IN       VARCHAR2,
      p_parm_id        OUT      mps_parm.mps_parm_id%TYPE
   );

   FUNCTION is_mapg_exists (
      p_src_mrkt_id    IN   mrkt.mrkt_id%TYPE,
      p_src_offr_perd_id    IN   offr.offr_perd_id%TYPE,
      p_trgt_mrkt_id   IN   mrkt.mrkt_id%TYPE,
      p_trgt_offr_perd_id   IN   offr.offr_perd_id%TYPE
   )
      RETURN CHAR;

   procedure P_ADD_ADD_REG_PRC_ITEMS (
    p_mrkt_id number,
    p_offr_perd_id number
   );

  PROCEDURE set_pricing_flags
  ( p_mrkt_id         in     mrkt.mrkt_id%TYPE,
    p_offr_perd_id    in     mrkt_perd.perd_id%TYPE,
    p_prc_enabled     in out single_char,
    p_trgt_strtgy_ind in out single_char
  );

END cpy_offr;
/
CREATE OR REPLACE PACKAGE BODY "CPY_OFFR"
AS

   type tbl_sku_data_processed is table of TREC_CPY_OFFR_SKU_DATA index by varchar2(1024);


   TYPE tbl_key_value IS TABLE OF VARCHAR (32000)
      INDEX BY VARCHAR (32000);
    v_tbl_key_vals_for_update   t_tbl_key_vals_for_update;

   procedure set_gta_dms(
    p_trgt_mrkt_id number,
    p_trgt_offr_perd_id number,
    p_trgt_veh_id number
   ) AS
   begin


    for rec in (
      select
        offr_sku_line.offr_sku_line_id,
        offr_prfl_prc_point.net_to_avon_fct,
        offr_prfl_prc_point.comsn_amt,
        offr_prfl_prc_point.tax_amt
      from
        offr,offr_prfl_prc_point,offr_sku_line
      where
        offr.mrkt_id = p_trgt_mrkt_id
        and offr.veh_id = p_trgt_veh_id
        and offr.ver_id =0
        and offr.offr_perd_id = p_trgt_offr_perd_id
        and offr.offr_typ= 'CMP'
        and offr.offr_id = offr_prfl_prc_point.offr_id
        and offr_prfl_prc_point.offr_prfl_prcpt_id = offr_sku_line.offr_prfl_prcpt_id
    ) loop

      begin

        update dstrbtd_mrkt_sls
        set
          dstrbtd_mrkt_sls.net_to_avon_fct = rec.net_to_avon_fct,
          dstrbtd_mrkt_sls.comsn_amt = rec.comsn_amt,
          dstrbtd_mrkt_sls.tax_amt = rec.tax_amt
        where
          dstrbtd_mrkt_sls.mrkt_id = p_trgt_mrkt_id
          and dstrbtd_mrkt_sls.offr_sku_line_id = rec.offr_sku_line_id;

      exception when others then
        null;
      end;

    end loop;


   end;

   procedure set_pso_ind (
    p_trgt_mrkt_id number,
    p_trgt_offr_perd_id number,
    p_trgt_veh_id number
   ) AS
   begin

    begin

    update offr_sku_line
      SET prmry_sku_offr_ind = 'N'
    WHERE
      mrkt_id = p_trgt_mrkt_id
      and offr_perd_id = p_trgt_offr_perd_id
      and veh_id = p_trgt_veh_id
      and offr_sku_line_id IN (
                    SELECT offr_sku_line_id
                      FROM (SELECT offr_sku_line.offr_sku_line_id
                              FROM offr, offr_sku_line, offr_prfl_prc_point
                             WHERE offr.mrkt_id = p_trgt_mrkt_id
                               AND offr.offr_perd_id = p_trgt_offr_perd_id
                               AND offr.ver_id = 0
                               AND offr.offr_typ = 'CMP'
                               AND offr_prfl_prc_point.offr_id = offr.offr_id
                               AND offr_sku_line.offr_prfl_prcpt_id =
                                            offr_prfl_prc_point.offr_prfl_prcpt_id));



    UPDATE offr_sku_line
      SET prmry_sku_offr_ind = 'Y'
    WHERE
        mrkt_id = p_trgt_mrkt_id
        and offr_perd_id = p_trgt_offr_perd_id
        and veh_id = p_trgt_veh_id
        and offr_sku_line_id IN (
                    SELECT offr_sku_line_id
                      FROM (SELECT offr_sku_line.offr_sku_line_id,
                                   offr_sku_line.sku_id,
                                   MIN
                                      (  offr_prfl_prc_point.sls_prc_amt
                                       / offr_prfl_prc_point.nr_for_qty
                                      ) OVER (PARTITION BY offr_sku_line.sku_id)
                                                                  min_sls_prc_amt,
                                     offr_prfl_prc_point.sls_prc_amt
                                   / offr_prfl_prc_point.nr_for_qty sls_prc_amt
                              FROM offr, offr_sku_line, offr_prfl_prc_point
                             WHERE offr.mrkt_id = p_trgt_mrkt_id
                               AND offr.offr_perd_id = p_trgt_offr_perd_id
                               AND offr.ver_id = 0
                               AND offr.offr_typ = 'CMP'
                               AND offr_prfl_prc_point.offr_id = offr.offr_id
                               AND offr_sku_line.offr_prfl_prcpt_id =
                                            offr_prfl_prc_point.offr_prfl_prcpt_id)
                     WHERE min_sls_prc_amt = sls_prc_amt);


    update
      offr_prfl_prc_point
    set
      offr_prfl_prc_point.prmry_offr_ind = 'N'
    where
      mrkt_id = p_trgt_mrkt_id
      and offr_perd_id = p_trgt_offr_perd_id
      and veh_id = p_trgt_veh_id
      and exists (
        select * from offr where
          offr.ver_id =0
          and offr.offr_id = offr_prfl_prc_point.offr_id
      );

    update
      offr_prfl_prc_point
    set
      offr_prfl_prc_point.prmry_offr_ind = 'Y'
    where
      mrkt_id = p_trgt_mrkt_id
      and offr_perd_id = p_trgt_offr_perd_id
      and veh_id = p_trgt_veh_id
      and exists (
        select * from offr_sku_line
        where
          offr_sku_line.offr_prfl_prcpt_id = offr_prfl_prc_point.offr_prfl_prcpt_id
          and offr_sku_line.prmry_sku_offr_ind = 'Y'
      )
      and exists (
        select * from offr where
          offr.ver_id =0
          and offr.offr_id = offr_prfl_prc_point.offr_id
      );

   exception when others then
    null;
   end;

   end;

   PROCEDURE SET_UNIT_SPLIT_STAT (
    p_src_mrkt_id number,
    p_src_offr_perd_id number,
    p_src_veh_id number,
    p_trgt_mrkt_id number,
    p_trgt_offr_perd_id number,
    p_trgt_veh_id number
   )
   AS
    usplit      number;
    total_usplit number;
    static_skus varchar(4000);
    count_static_skus number;
   begin

    for zero_oppp_rec in (
      SELECT
        offr_prfl_prc_point.offr_prfl_prcpt_id,
        count(*) item_count
        FROM offr,
          offr_prfl_prc_point,
          offr_sku_line,
          dstrbtd_mrkt_sls
        WHERE offr.mrkt_id                         = p_trgt_mrkt_id
        AND offr.ver_id                            = 0
        AND offr.veh_id                            = p_trgt_veh_id
        AND offr.offr_perd_id                      = p_trgt_offr_perd_id
        AND offr.offr_id                           = offr_prfl_prc_point.offr_id
        AND offr_prfl_prc_point.offr_prfl_prcpt_id = offr_sku_line.offr_prfl_prcpt_id
        AND offr_sku_line.offr_sku_line_id         = dstrbtd_mrkt_sls.offr_sku_line_id
        AND dstrbtd_mrkt_sls.sls_typ_id            = 1
        and offr_Sku_line.dltd_ind <> 'Y'
        HAVING SUM(dstrbtd_mrkt_sls.unit_qty)      = 0
        GROUP BY offr_prfl_prc_point.offr_id,
          offr_prfl_prc_point.offr_prfl_prcpt_id
    ) loop

      static_skus := '-1';

      count_static_skus :=0;

      for zero_rec in (
        select
          offr_sku_line.offr_sku_line_id,
          offr_sku_line.sku_id,
          offr_sku_line.offr_perd_id
        from
          offr_sku_line
        where
          offr_sku_line.offr_prfl_prcpt_id = zero_oppp_rec.offr_prfl_prcpt_id
          and offr_Sku_line.dltd_ind <> 'Y'
      ) loop

      usplit := -1;



      begin

      SELECT statc_unit_splt_pct into usplit
        FROM mrkt_sku_unit_splt_pct
        WHERE sku_id         = zero_rec.sku_id
        AND mrkt_id          = p_trgt_mrkt_id
        AND eff_offr_perd_id =
          (SELECT MAX (eff_offr_perd_id)
          FROM mrkt_sku_unit_splt_pct
          WHERE sku_id          = zero_rec.sku_id
          AND mrkt_id           = p_trgt_mrkt_id
          AND eff_offr_perd_id <= p_trgt_offr_perd_id
          );

      exception when others then

        usplit := -1;

      end;

      if usplit > -1 then

      dbms_output.put_line('Split update: ' || zero_rec.offr_sku_line_id || '->' || usplit);

      static_skus := static_skus || ',' || zero_rec.offr_sku_line_id;
      count_static_skus := count_static_skus +1;


      update offr_sku_line set offr_sku_line.unit_splt_pct = usplit where offr_sku_line.offr_sku_line_id = zero_rec.offr_sku_line_id;

      end if;

    end loop;

    static_skus := static_skus || ',-1';

    dbms_output.put_line('Price point ids:: ' || zero_oppp_rec.offr_prfl_prcpt_id);
    dbms_output.put_line('Offr Sku Line count:: ' || zero_oppp_rec.item_count);
    dbms_output.put_line('Static offr_sku_line ids:: ' || static_skus);
    dbms_output.put_line('Static offr_sku_line count:: ' || count_static_skus);


    for rec in (
      SELECT offr_sku_line_id,
        ROUND(unit_splt_pct/total_unit_split_pct * count_static_skus/zero_oppp_rec.item_count*100,2) unit_splt_pct,
        unit_splt_pct os,
        total_unit_split_pct ts
      FROM
        (SELECT offr_sku_line.offr_sku_line_id,
          offr_sku_line.unit_splt_pct,
          SUM(offr_sku_line.unit_splt_pct) over (partition BY offr_sku_line.offr_prfl_prcpt_id) total_unit_split_pct
        FROM offr_sku_line
        WHERE offr_sku_line.offr_prfl_prcpt_id = zero_oppp_rec.offr_prfl_prcpt_id
        and offr_Sku_line.dltd_ind <> 'Y'
        and ( INSTR(static_skus, ','|| offr_sku_line.offr_sku_line_id ||',') > 0)
        )
      WHERE total_unit_split_pct>0
    ) loop


      update offr_sku_line set offr_sku_line.unit_splt_pct = rec.unit_splt_pct where
      offr_sku_line.offr_sku_line_id = rec.offr_sku_line_id;

      dbms_output.put_line('Split update: ' || rec.offr_sku_line_id || '->' || rec.unit_splt_pct || ' ----- ' || rec.os || ',' || rec.ts);


    end loop;
    end loop;

   end;

   PROCEDURE SET_UNIT_SPLIT_COPY (
    p_src_mrkt_id number,
    p_src_offr_perd_id number,
    p_src_veh_id number,
    p_trgt_mrkt_id number,
    p_trgt_offr_perd_id number,
    p_trgt_veh_id number
   )
   AS
    csplit      number;
    check_ai    number;
    diff        number;
   begin

    for zero_rec in (
      SELECT
        offr_sku_line.offr_sku_line_id,
        offr_sku_line.offr_sku_line_link_id,
        offr_sku_line.sku_id,
        offr_sku_line.offr_perd_id
      FROM offr_sku_line
      WHERE offr_Sku_line.offr_prfl_prcpt_id IN
        (SELECT offr_prfl_prc_point.offr_prfl_prcpt_id
        FROM offr,
          offr_prfl_prc_point,
          offr_sku_line,
          dstrbtd_mrkt_sls
        WHERE offr.mrkt_id                         = p_trgt_mrkt_id
        AND offr.ver_id                            = 0
        AND offr.veh_id                            = p_trgt_veh_id
        AND offr.offr_perd_id                      = p_trgt_offr_perd_id
        AND offr.offr_id                           = offr_prfl_prc_point.offr_id
        AND offr_prfl_prc_point.offr_prfl_prcpt_id = offr_sku_line.offr_prfl_prcpt_id
        AND offr_sku_line.offr_sku_line_id         = dstrbtd_mrkt_sls.offr_sku_line_id
        AND dstrbtd_mrkt_sls.sls_typ_id            = 1
        and offr_Sku_line.dltd_ind <> 'Y'
        HAVING SUM(dstrbtd_mrkt_sls.unit_qty)      = 0
        GROUP BY offr_prfl_prc_point.offr_id,
          offr_prfl_prc_point.offr_prfl_prcpt_id
        )
    ) loop


      check_ai := 0;
      diff     := 0;

      begin

      select
        count(*) into check_ai
      from (
      SELECT
        sum(osl_t.sku_id) tst,
        sum(decode(osl_t.dltd_ind,'Y',0,1)) tdt,
        sum(osl_s.sku_id) tss,
        sum(decode(osl_s.dltd_ind,'Y',0,1)) tds
      FROM offr_sku_line osl_t,offr_sku_line osl_s
      WHERE osl_t.offr_prfl_prcpt_id IN
        (SELECT offr_prfl_prcpt_id
        FROM offr_sku_line
        WHERE offr_sku_line_id = zero_rec.offr_sku_line_id
        )
        and osl_t.offr_sku_line_link_id = osl_s.offr_sku_line_id(+)
      )
      where
        tst=tss
        and tdt=tds;


      SELECT
        ((SELECT COUNT(*)
        FROM offr_sku_line
        WHERE offr_sku_line.offr_prfl_prcpt_id = target_osl.offr_prfl_prcpt_id
        AND offr_sku_line.dltd_ind            <> 'Y'
        )             -
        (SELECT COUNT(*)
        FROM offr_sku_line
        WHERE offr_sku_line.offr_prfl_prcpt_id = source_osl.offr_prfl_prcpt_id
        AND offr_sku_line.dltd_ind            <> 'Y'
        )) into diff
      FROM offr_sku_line source_osl,
        offr_sku_line target_osl
      WHERE target_osl.offr_sku_line_id = zero_rec.offr_sku_line_id
      AND source_osl.offr_sku_line_id   = target_osl.offr_sku_line_link_id;


      exception when others then
        check_ai := 0;
        diff := 0;
      end;

      if check_ai > 0 and diff = 0 then

        csplit := -1;

        begin

        SELECT unit_splt_pct into csplit
          FROM offr_Sku_line
          WHERE offr_Sku_line.offr_sku_line_id = zero_rec.offr_sku_line_link_id;

        exception when others then

          csplit := -1;

        end;

        if csplit > -1 then

        update offr_sku_line set offr_sku_line.unit_splt_pct = csplit where offr_sku_line.offr_sku_line_id = zero_rec.offr_sku_line_id;

        end if;

      end if;

    end loop;

    -------------------

    for rec in (
      SELECT offr_sku_line_id,
        ROUND(unit_splt_pct/total_unit_split_pct * 100,2) unit_splt_pct
      FROM
        (SELECT offr_sku_line.offr_sku_line_id,
          offr_sku_line.unit_splt_pct,
          SUM(offr_sku_line.unit_splt_pct) over (partition BY offr_sku_line.offr_prfl_prcpt_id) total_unit_split_pct
        FROM offr_sku_line,
          offr
        WHERE offr.mrkt_id    = p_trgt_mrkt_id
        AND offr.offr_perd_id = p_trgt_offr_perd_id
        AND offr.veh_id       = p_trgt_veh_id
        AND offr.ver_id       = 0
        and offr_Sku_line.dltd_ind <> 'Y'
        AND offr.offr_id      = offr_sku_line.offr_id
        )
      WHERE total_unit_split_pct>0
    ) loop


      update offr_sku_line set offr_sku_line.unit_splt_pct = rec.unit_splt_pct where
      offr_sku_line.offr_sku_line_id = rec.offr_sku_line_id;

    end loop;

   end;


   PROCEDURE SET_UNIT_SPLIT_AVG (
    p_src_mrkt_id number,
    p_src_offr_perd_id number,
    p_src_veh_id number,
    p_trgt_mrkt_id number,
    p_trgt_offr_perd_id number,
    p_trgt_veh_id number
   )
   AS
    hs      varchar(1);
    yn      varchar(1);
   begin

    for rec in (
      select
        offr_id,
        offr_prfl_prcpt_id,
        offr_sku_line_id,
        total_units,
        units,
        case
          when total_units > 0 then
            round(units / total_units*100,2)
          else
            round((1/total_cc)*100,2)
        end split_pct
      from (
      select
        offr.offr_id,
        offr_prfl_prc_point.offr_prfl_prcpt_id,
        offr_sku_line.offr_sku_line_id,
        sum(dstrbtd_mrkt_sls.unit_qty) over (partition by offr_prfl_prc_point.offr_prfl_prcpt_id) total_units,
        count(*) over (partition by offr_prfl_prc_point.offr_prfl_prcpt_id) total_cc,
        dstrbtd_mrkt_sls.unit_qty units
      from
      offr_prfl_prc_point,
      offr_sku_line,
      dstrbtd_mrkt_sls,
      offr
      where
      offr.mrkt_id = p_trgt_mrkt_id
      and offr.offr_perd_id= p_trgt_offr_perd_id
      and offr.veh_id = p_trgt_veh_id
      and offr.ver_id = 0
      and offr.offr_id = offr_prfl_prc_point.offr_id
      and offr_prfl_prc_point.offr_prfl_prcpt_id = offr_sku_line.offr_prfl_prcpt_id
      and dstrbtd_mrkt_sls.offr_sku_line_id = offr_sku_line.offr_sku_line_id
      and dstrbtd_mrkt_sls.offr_perd_id = dstrbtd_mrkt_sls.sls_perd_id
      and dstrbtd_mrkt_sls.sls_typ_id =1
      and offr_Sku_line.dltd_ind <> 'Y'
      )
    ) loop

      begin

      update
        offr_sku_line
      set
        offr_sku_line.unit_splt_pct = rec.split_pct
      where
        offr_sku_line.offr_sku_line_id = rec.offr_sku_line_id;

      exception when others then
        null;
      end;

    end loop;


    begin

      select mrkt_config_item_val_txt into hs from mrkt_config_item where config_item_id = 2352 and mrkt_id = p_trgt_mrkt_id;
      select mrkt_config_item_val_txt into yn from mrkt_config_item where config_item_id = 2351 and mrkt_id = p_trgt_mrkt_id;

      if hs = 'H' then

          dbms_output.put_line('Historical');

         if yn = 'Y' then

          dbms_output.put_line('Historical: On');


          SET_UNIT_SPLIT_COPY (
            p_src_mrkt_id,
            p_src_offr_perd_id,
            p_src_veh_id,
            p_trgt_mrkt_id,
            p_trgt_offr_perd_id,
            p_trgt_veh_id
           );


        else

          dbms_output.put_line('Historical: Off');

          null;

        end if;

      else

        dbms_output.put_line('Static');


        SET_UNIT_SPLIT_STAT (
            p_src_mrkt_id,
            p_src_offr_perd_id,
            p_src_veh_id,
            p_trgt_mrkt_id,
            p_trgt_offr_perd_id,
            p_trgt_veh_id
           );

      end if;


    exception when others then

      dbms_output.put_line('Exception');

    end;

   end;





   FUNCTION f_get_parms (p_parm_id IN NUMBER)
      RETURN t_tbl_parm_info PIPELINED
   AS
      CURSOR c_parm_info (p_parm_id IN NUMBER)
      IS
         SELECT   mps_parm.mps_parm_id parm_id,
                  mps_parm.srce_mrkt_id srce_mrkt_id,
                  mps_parm.srce_ver_id srce_ver_id,
                  mps_parm_srce_veh.veh_id srce_veh_id,
                  mps_parm_srce_offr_perd.offr_perd_id srce_offr_perd_id,
                  mps_parm_srce_offr_perd.mrkt_id srce_offr_perd_refmrkt,
                  mps_parm_srce_sls_perd.sls_perd_id srce_sls_perd_id,
                  mps_parm_srce_sls_perd.mrkt_id srce_sls_perd_refmrkt,
                  mps_parm_offr.offr_id srce_offr_id,
------------------------------------------
                  mps_parm.set_to_reqrng_plnrs_rev_ind,
                  mps_parm.offr_hdr_only_ind,
                  mps_parm.rtain_prc_pnts_when_rplcng_ind,
                  mps_parm.updt_what_if_ind, mps_parm.dlt_srce_offr_ind,
                  mps_parm.trgt_mrkt_veh_perd_sctn_id,
                  mps_parm_replctn_mov_mrkt.mrkt_id replctn_mov_mrkt_id,
                  mps_parm_replctn_mov_mrkt.seq_nr replctn_mov_seq_nr,
-------------------------------------------
                  mps_parm_trgt_mrkt.mrkt_id trgt_mrkt_id,
                  mps_parm_trgt_ver.ver_id trgt_ver_id,
                  mps_parm_trgt_veh.veh_id trgt_veh_id,
                  mps_parm_trgt_offr_perd.offr_perd_id trgt_offr_perd_id,
                  mps_parm_trgt_offr_perd.mrkt_id trgt_offr_perd_refmrkt,
                  mps_parm_trgt_sls_perd.sls_perd_id trgt_sls_perd_id,
                  mps_parm_trgt_sls_perd.mrkt_id trgt_sls_perd_refmrkt,
                  CASE
                     WHEN mps_parm_trgt_mrkt.rtain_sls_prc_ind =
                                                             'Y'
                      OR mps_parm_trgt_veh.rtain_sls_prc_ind = 'Y'
                      OR mps_parm_trgt_offr_perd.rtain_sls_prc_ind = 'Y'
                        THEN 'Y'
                     ELSE 'N'
                  END retain_price,
                  mps_parm_trgt_mrkt.copy_units_rul_id mrkt_unit_calc_typ_id,
                  mps_parm_trgt_veh.copy_units_rul_id veh_unit_calc_typ_id,
                  mps_parm_trgt_offr_perd.copy_units_rul_id
                                                       perd_unit_calc_typ_id
             FROM mps_parm,
                  mps_parm_offr,
                  mps_parm_replctn_mov_mrkt,
                  mps_parm_srce_offr_perd,
                  mps_parm_srce_sls_perd,
                  mps_parm_srce_veh,
                  mps_parm_trgt_mrkt,
                  mps_parm_trgt_offr_perd,
                  mps_parm_trgt_sls_perd,
                  mps_parm_trgt_veh,
                  mps_parm_trgt_ver
            WHERE mps_parm.mps_parm_id = p_parm_id
              AND (mps_parm.mps_parm_id = mps_parm_offr.mps_parm_id(+))
              AND (mps_parm.mps_parm_id = mps_parm_replctn_mov_mrkt.mps_parm_id(+))
              AND (mps_parm.mps_parm_id = mps_parm_srce_offr_perd.mps_parm_id(+))
              AND (mps_parm.mps_parm_id = mps_parm_srce_sls_perd.mps_parm_id(+))
              AND (mps_parm.mps_parm_id = mps_parm_srce_veh.mps_parm_id(+))
              AND (mps_parm.mps_parm_id = mps_parm_trgt_mrkt.mps_parm_id(+))
              AND (mps_parm.mps_parm_id = mps_parm_trgt_offr_perd.mps_parm_id(+))
              AND (mps_parm.mps_parm_id = mps_parm_trgt_sls_perd.mps_parm_id(+))
              AND (mps_parm.mps_parm_id = mps_parm_trgt_veh.mps_parm_id(+))
              AND (mps_parm.mps_parm_id = mps_parm_trgt_ver.mps_parm_id(+))
         ORDER BY mps_parm_srce_veh.seq_nr,
                  mps_parm_srce_offr_perd.seq_nr,
                  mps_parm_srce_sls_perd.seq_nr,
                  mps_parm_offr.seq_nr,
                  mps_parm_trgt_mrkt.seq_nr,
                  mps_parm_trgt_ver.seq_nr,
                  mps_parm_trgt_veh.seq_nr,
                  mps_parm_trgt_offr_perd.seq_nr,
                  mps_parm_trgt_sls_perd.seq_nr;
      parm_results   c_parm_info%ROWTYPE;
   BEGIN
      OPEN c_parm_info (p_parm_id);
      LOOP
         FETCH c_parm_info
          INTO parm_results;
         EXIT WHEN c_parm_info%NOTFOUND OR c_parm_info%NOTFOUND IS NULL;
         PIPE ROW (parm_results);
      END LOOP;
      CLOSE c_parm_info;
   END f_get_parms;
   FUNCTION get_tgrt_mrkt_dtl (
      p_clsrt_id       mps_user_mrkt_veh_acs.clstr_id%TYPE,
      p_srce_mrkt_id   mrkt_veh_perd_ver.mrkt_id%TYPE,
      p_srce_veh_id    VARCHAR2,
      p_srce_ver_id    mrkt_veh_perd_ver.ver_id%TYPE,
      p_srce_perd_id   offr.offr_perd_id%TYPE,
      p_user_nm        VARCHAR2
   )
      RETURN ttable_result PIPELINED
   AS
      RESULT   ttable_result;
   BEGIN
      SELECT   p_srce_mrkt_id,
               (SELECT mrkt_nm
                  FROM mrkt
                 WHERE mrkt_id = p_srce_mrkt_id) src_mrkt_nm,
               p_srce_perd_id,
               m.mrkt_id trgt_mrkt_id,
               (SELECT mrkt_nm
                  FROM mrkt
                 WHERE mrkt_id = m.mrkt_id) trgt_mrkt_nm,
               mpcm.trgt_perd_id,
               mv.veh_id,
               mv.lcl_veh_desc_txt,
               DECODE ((SELECT 'Y'
                          FROM mps_user_mrkt_acs muma
                         WHERE muma.mrkt_id = m.mrkt_id
                           AND muma.clstr_id = p_clsrt_id
                           AND muma.user_nm = p_user_nm
                           AND muma.sys_id = 1),
                       'Y', 'Y',
                       'N'
                      ) mrkt_acs_ind,
               DECODE ((SELECT 'N'
                          FROM mrkt_veh mv1
                         WHERE veh_id = mv.veh_id
                           AND NOT EXISTS (
                                  SELECT 'x'
                                    FROM mps_user_mrkt_veh_acs va
                                   WHERE va.user_nm = p_user_nm
                                     AND va.sys_id = 1
                                     AND va.veh_id = mv1.veh_id)
                           AND ROWNUM = 1),
                       'N', 'N',
                       'Y'
                      ) veh_acs_ind,
               DECODE ((SELECT 'Y'
                          FROM mrkt_config_item
                         WHERE mrkt_id = m.mrkt_id AND config_item_id = 2100),
                       'Y', 'Y',
                       'N'
                      ) calculation_ind,
               DECODE ((SELECT count(1)
                          FROM mrkt_veh_perd_ver mvpv
                         WHERE mrkt_id = m.mrkt_id
                           AND offr_perd_id = mpcm.trgt_perd_id
                           AND veh_id = mv.veh_id
                           AND ver_id = p_srce_ver_id
                           AND mps_sls_typ_id > 1
                       ),
                       0, 'Y',
                       'N'
                      ) est_typ_ind,
               DECODE ((SELECT 'Y'
                          FROM offr o
                         WHERE o.mrkt_id = mv.mrkt_id
                           AND o.veh_id = mv.veh_id
                           AND o.offr_perd_id = mpcm.trgt_perd_id
                           AND o.ver_id = p_srce_ver_id
                           AND ROWNUM = 1),
                       'Y', 'Y',
                       'N'
                      ) used_vehicles_ind,
               decode((select mrkt_config_item_val_txt
                         from mrkt_config_item
                        where config_item_id= 5001
                          and mrkt_id =  mpcm.trgt_mrkt_id
                      ),'N','N', decode( (SELECT  frcst_boost_trgt_actv_ind
                                            FROM mrkt_perd
                                            WHERE mrkt_id =  mpcm.trgt_mrkt_id
                                            AND perd_id =  mpcm.trgt_perd_id
                                            AND perd_typ = 'SC'),'N','N','Y'
                                       )
                     )  forecastboost_ind,
               DECODE ((SELECT l.cmpgn_ver_lock_ind
                          FROM mrkt_veh_perd_ver l
                         WHERE l.mrkt_id = mv.mrkt_id
                           AND l.offr_perd_id = mpcm.trgt_perd_id
                           AND l.veh_id = mv.veh_id
                           AND l.ver_id = p_srce_ver_id
                                                       --AND l.cmpgn_ver_lock_ind = 'Y'
                       ),
                       'Y', 'Y',
                       'N'
                      ) camp_lock_ind,
               DECODE ((SELECT 'Y'
                          FROM offr, offr_lock
                         WHERE offr.offr_id = offr_lock.offr_id
                           AND offr.mrkt_id = mv.mrkt_id
                           AND offr.offr_perd_id = mpcm.trgt_perd_id
                           AND offr.veh_id = mv.veh_id
                           AND ROWNUM = 1),
                       'Y', 'Y',
                       'N'
                      ) offr_lock_ind,
               DECODE ((SELECT 'Y'
                          FROM mrkt_rltnshp
                         WHERE mrkt_id = mpcm.trgt_mrkt_id
                           AND srce_mrkt_id = p_srce_mrkt_id
                           AND mpcm.trgt_perd_id BETWEEN strt_offr_perd_id
                                                     AND Nvl(end_offr_perd_id, 99990399)),--QC3321
                       'Y', 'Y',
                       'N'
                      ) mrkt_rel_ind,
               DECODE ((SELECT 'X'                          --DISTINCT mrkt_id
                          FROM mrkt_veh mv_in
                        WHERE  mrkt_id NOT IN (
                                  SELECT DISTINCT mrkt_id
                                             FROM mps_user_mrkt_veh_acs
                                            WHERE veh_id = mv_in.veh_id
                                              AND user_nm = p_user_nm)
                           AND veh_id = mv.veh_id
                           AND mrkt_id = mv.mrkt_id
                           AND ROWNUM = 1),
                       'X', 'N',
                       'Y'
                      ) veh_acs_ind1
      BULK COLLECT INTO RESULT
          FROM mrkt m, mrkt_veh mv, mrkt_perd_cmpgn_mapg mpcm
         WHERE m.mrkt_typ = 'MRKT'
           AND m.parnt_mrkt_id = p_clsrt_id
           AND m.mrkt_id = mv.mrkt_id
           AND m.mrkt_id <> p_srce_mrkt_id
           AND mpcm.srce_mrkt_id(+) = p_srce_mrkt_id
           AND mpcm.trgt_mrkt_id(+) = mv.mrkt_id
           AND mpcm.srce_perd_id(+) = p_srce_perd_id
           AND (   (p_srce_veh_id = '-1' AND 1 = 1)
                OR (    p_srce_veh_id <> '-1'
                    AND INSTR (',' || p_srce_veh_id || ',',
                               ',' || mv.veh_id || ','
                              ) > 0
                   )
               )
      ORDER BY m.seq_nr, trgt_mrkt_id, mv.veh_id, trgt_perd_id;
      IF RESULT.COUNT > 0
      THEN
         FOR i IN RESULT.FIRST .. RESULT.LAST
         LOOP
            PIPE ROW (RESULT (i));
         END LOOP;
      END IF;
      RETURN;
   END get_tgrt_mrkt_dtl;
   PROCEDURE insert_btch_task_log (
      p_btch_task_id           btch_task.btch_task_id%TYPE,
      p_btch_task_log_typ_id   btch_task_log_typ.btch_task_log_typ_id%TYPE,
      p_btch_task_key          NUMBER,
      p_btch_err_lvl_txt       btch_task_log.btch_task_log_err_lvl_txt%TYPE,
      p_btch_log_txt           btch_task_log.btch_task_log_desc_txt%TYPE,
      p_boosted_qty            btch_task_log.btch_task_log_to_be_boostd_qty%TYPE,
      p_not_boosted_qty        btch_task_log.btch_task_log_not_boostd_qty%TYPE,
      p_trgt_mrkt_id           NUMBER --LAKSHMANA, 6 SEP 2011
   )
   IS
      PRAGMA AUTONOMOUS_TRANSACTION;
      l_btch_seq   NUMBER;
   BEGIN
      SELECT seq_btch_framwork.NEXTVAL
        INTO l_btch_seq
        FROM DUAL;
-----------------------------------
-- P_BTCH_TASK_LOG_TYP_ID
-- 1 -> Vehicle Level Error
-- 2 -> Profile Level Error
-- 3 -> Sku item Level Error
-- 4 -> Target Sales parameter Error
-- 5 -> Market Level Error
-- 6 --> Offr_perd_id_level
-----------------------------------
      INSERT INTO btch_task_log
                  (btch_task_log_id, btch_task_id, btch_task_log_typ_id,
                   btch_task_log_mrkt_id,
                   btch_task_log_veh_id,
                   btch_task_log_offr_perd_id, btch_task_log_ver_id,
                   btch_task_log_err_lvl_txt,
                   btch_task_log_prfl_cd,
                   btch_task_log_sku_id,
                   btch_task_log_fsc_cd, btch_task_log_desc_txt,
                   btch_task_log_to_be_boostd_qty,
                   btch_task_log_not_boostd_qty
                  )
           VALUES (l_btch_seq, p_btch_task_id, p_btch_task_log_typ_id,
                   p_trgt_mrkt_id,
                   DECODE (p_btch_task_log_typ_id, 1, p_btch_task_key, NULL),
                   DECODE (p_btch_task_log_typ_id, 6, p_btch_task_key, NULL),
                   NULL,
                   p_btch_err_lvl_txt,
                   DECODE (p_btch_task_log_typ_id, 2, p_btch_task_key, NULL),
                   DECODE (p_btch_task_log_typ_id, 3, p_btch_task_key, NULL),
                   NULL, p_btch_log_txt,
                   p_boosted_qty,
                   p_not_boosted_qty
                  );
      COMMIT;
   END insert_btch_task_log;




    PROCEDURE log_review_job_rslts (
       p_task_id           IN       btch_task.btch_task_id%TYPE,
       p_parm_id           IN       mps_parm.mps_parm_id%TYPE,
       p_status            IN OUT   NUMBER,
       p_partial_success   IN OUT   NUMBER
    )
    AS
       ln_cnt_itm_to_be_boostd    NUMBER;
       l_err_prfl_lvl             NUMBER                 := 2;
       l_err_item_lvl             NUMBER                 := 3;
       ls_btch_log_sub            NUMBER;
       ls_btch_log_key            NUMBER;
       ls_btch_err_svrty          VARCHAR (30);
       ls_btch_log_txt            VARCHAR2 (2000);
       prvs_prfl_cd               NUMBER                 := 0;
       ls_fb_xcluded_sls_cls_cd   VARCHAR2 (4000);
       ln_srce_mrkt_id            NUMBER;
       ln_srce_offr_perd_id       NUMBER;
       ln_trgt_mrkt_id            NUMBER;
       ln_trgt_offr_perd_id       NUMBER;
       ls_veh_ids                 VARCHAR2 (4000);
       ln_job_typ                 NUMBER;

       TYPE t_not_boosted_list IS RECORD (
          not_boosted_prfl_cd    NUMBER,
          not_boosted_sku_list   NUMBER,
          act_trgt_sku_cnt       NUMBER,
          is_prfl_lvl_err        CHAR (1)
       );

       TYPE t_tbl_not_boosted_list IS TABLE OF t_not_boosted_list;

       not_boosted_rs             t_tbl_not_boosted_list;


       TYPE t_not_copied_list IS RECORD (
          not_copied_prfl_cd    NUMBER,
          not_copied_sku_id     NUMBER,
          actv_trgt_prfl_cd     NUMBER,
          actv_trgt_sku_id      NUMBER,
          trgt_sls_cls_cd       NUMBER,
          act_trgt_sku_cnt      NUMBER,
          prfl_sku_err_ind      CHAR (1)
       );

       TYPE t_tbl_not_copied_list IS TABLE OF t_not_copied_list;

       not_copied_rs             t_tbl_not_copied_list;

       TYPE tbl_number IS TABLE OF NUMBER;

       TYPE tbl_char IS TABLE OF VARCHAR (4000);

       l_tbl_srce_mrkt_id         tbl_number;
       l_tbl_srce_offr_perd_id    tbl_number;
       l_tbl_trgt_mrkt_id         tbl_number;
       l_tbl_trgt_offr_perd_id    tbl_number;
       l_tbl_veh_ids              tbl_char;
    BEGIN
       IF P_TASK_ID IS NOT NULL THEN

           BEGIN
              SELECT bt.btch_job_typ_id
                INTO ln_job_typ
                FROM btch_job bt, btch_task btask
               WHERE bt.btch_job_id = btask.btch_job_id AND btch_task_id = p_task_id;
           EXCEPTION
              WHEN OTHERS  THEN
                 ln_job_typ := 9999;
           END;

---------------------------------------------------------------------------------------------------------------------
-- Logging review job results for Forecast boost
---------------------------------------------------------------------------------------------------------------------
           IF ln_job_typ = 1 THEN
              BEGIN
                --Added for QC 3735 on 2nd December,2011 to avoid wm_concat usage
				SELECT DISTINCT sd1.srce_mrkt_id, sd1.srce_offr_perd_id, sd1.trgt_mrkt_id,
								sd1.trgt_offr_perd_id,
								util.tbl_to_str
									   (CURSOR (SELECT sd2.srce_veh_id
												  FROM TABLE (cpy_offr.f_get_parms (p_parm_id)
															 ) sd2
												 WHERE sd2.srce_veh_id = sd2.trgt_veh_id
												   AND sd2.srce_mrkt_id =
																	sd2.srce_offr_perd_refmrkt
												   AND sd2.trgt_mrkt_id =
																	sd2.trgt_offr_perd_refmrkt
												   AND sd2.srce_mrkt_id = sd1.srce_mrkt_id
												   AND sd2.srce_offr_perd_id =
																		 sd1.srce_offr_perd_id
												   AND sd2.trgt_mrkt_id = sd1.trgt_mrkt_id
												   AND sd2.trgt_offr_perd_id =
																		 sd1.trgt_offr_perd_id
											   )
									   ) srce_veh_id
						   INTO ln_srce_mrkt_id, ln_srce_offr_perd_id, ln_trgt_mrkt_id,
								ln_trgt_offr_perd_id,
								ls_veh_ids
						   FROM TABLE (cpy_offr.f_get_parms (p_parm_id)) sd1
						  WHERE sd1.srce_veh_id = sd1.trgt_veh_id
							AND sd1.srce_mrkt_id = sd1.srce_offr_perd_refmrkt
							AND sd1.trgt_mrkt_id = sd1.trgt_offr_perd_refmrkt;

              EXCEPTION
                 WHEN OTHERS THEN
                    app_plsql_log.info
                       ( 'Error in log_review_job_rslts in getting parm info forcastboost'|| SQLERRM (SQLCODE) );
              END;

              --logging the item to be boosted count for the task.
              BEGIN
                SELECT count(distinct osl.sku_id)
                  INTO ln_cnt_itm_to_be_boostd
                  FROM mrkt_veh mv,
                       mrkt_veh_perd mvp,
                       mrkt_veh_perd_ver mvpv,
                       offr o,
                       offr_prfl_prc_point oppp,
                       offr_sku_line osl,
                       prfl pf
                 WHERE mv.mrkt_id = ln_srce_mrkt_id
                   AND mv.veh_id IN (
                          SELECT a.COLUMN_VALUE veh_id
                            FROM TABLE
                                    (util.str_to_tbl
                                                 (ls_veh_ids)
                                    ) a)
                   AND mvp.mrkt_id = mv.mrkt_id
                   AND mvp.veh_id = mv.veh_id
                   AND mvp.offr_perd_id = ln_srce_offr_perd_id
                   AND mvpv.mrkt_id = mvp.mrkt_id
                   AND mvpv.veh_id = mvp.veh_id
                   AND mvpv.offr_perd_id = mvp.offr_perd_id
                   AND mvpv.ver_id = 0
                   AND o.mrkt_id = mvpv.mrkt_id
                   AND o.offr_perd_id = mvpv.offr_perd_id
                   AND o.ver_id = mvpv.ver_id
                   AND o.offr_typ = 'CMP'
                   AND o.veh_id = mvpv.veh_id
                   AND oppp.offr_id = o.offr_id
                   AND osl.offr_prfl_prcpt_id = oppp.offr_prfl_prcpt_id
                   AND osl.dltd_ind <> 'Y'
                   AND osl.prfl_cd = pf.prfl_cd
                   AND NOT EXISTS
                       (SELECT 1
                          FROM frcst_boost_xclusn_mrkt_perd fx
                         WHERE fx.mrkt_id = ln_trgt_mrkt_id
                           AND fx.trgt_offr_perd_id = ln_trgt_offr_perd_id
                           AND INSTR(',' || nvl(catgry_id_list, '-1') ||' ,', ',' || nvl(pf.catgry_id, -1) ||',') > 0
                           AND INSTR(',' || nvl(sls_cls_cd_list, '-1') || ',', ',' || nvl(osl.sls_cls_cd, '-1') ||',') > 0
                           AND INSTR(',' || nvl(sgmt_id_list, '-1') || ',', ',' || nvl(pf.sgmt_id, -1) ||',') > 0);
              EXCEPTION
                 WHEN OTHERS THEN
                    app_plsql_log.info
                       (   'Error in log_review_job_rslts in getting ln_cnt_itm_to_be_boostd'
                        || SQLERRM (SQLCODE)
                       );
              END;

             ls_btch_log_sub := l_err_prfl_lvl;
             ls_btch_log_txt :=
                     'not Boosted because it does not exist in the target Market';
             ls_btch_err_svrty := 'Warning';
             ls_btch_log_key := -1;

             IF p_status <> 1 THEN
                --inserting a dedicated row for the items to be boosted count for the task with log type prfl.
                insert_btch_task_log (p_task_id,
                                      ls_btch_log_sub,
                                      ls_btch_log_key,
                                      ls_btch_err_svrty,
                                      ls_btch_log_txt,
                                      ln_cnt_itm_to_be_boostd,
                                      0,
                                      ln_trgt_mrkt_id
                                     );
             END IF;

              --logging not boosted sku's or profile information.
              BEGIN
                 WITH not_boosted_master_list AS
                      (SELECT   offr.mrkt_id, offr.offr_perd_id, prfl_cd, sku_id
                           FROM mrkt_veh,
                                mrkt_veh_perd,
                                mrkt_veh_perd_ver,
                                offr,
                                offr_sku_line
                          WHERE mrkt_veh.mrkt_id = ln_srce_mrkt_id
                            --source market id
                            AND mrkt_veh.veh_id IN (
                                            SELECT a.COLUMN_VALUE veh_id
                                              FROM TABLE (util.str_to_tbl (ls_veh_ids)) a)
                            -- source vehicle id
                            AND mrkt_veh.mrkt_id = mrkt_veh_perd.mrkt_id
                            AND mrkt_veh.veh_id = mrkt_veh_perd.veh_id
                            AND mrkt_veh_perd.offr_perd_id = ln_srce_offr_perd_id
                            -- source offer period id
                            AND mrkt_veh_perd_ver.mrkt_id = mrkt_veh_perd.mrkt_id
                            AND mrkt_veh_perd_ver.veh_id = mrkt_veh_perd.veh_id
                            AND mrkt_veh_perd_ver.offr_perd_id =
                                                            mrkt_veh_perd.offr_perd_id
                            AND mrkt_veh_perd_ver.ver_id = 0
                            -- work in progress
                            AND offr.mrkt_id = mrkt_veh_perd_ver.mrkt_id
                            AND offr.offr_perd_id = mrkt_veh_perd_ver.offr_perd_id
                            AND offr.veh_id = mrkt_veh_perd_ver.veh_id
                            AND offr.ver_id = mrkt_veh_perd_ver.ver_id
                            AND offr.offr_typ = 'CMP'
                            AND offr_sku_line.offr_id = offr.offr_id
                            AND offr_sku_line.dltd_ind <> 'Y'
                            AND offr_sku_line.offr_sku_line_id IN (
                                   SELECT offr_sku_line.offr_sku_line_id
                                     FROM mrkt_veh,
                                          mrkt_veh_perd,
                                          mrkt_veh_perd_ver,
                                          offr,
                                          offr_sku_line
                                    WHERE mrkt_veh.mrkt_id = ln_srce_mrkt_id
                                      --source market id
                                      AND mrkt_veh.veh_id IN (
                                             SELECT a.COLUMN_VALUE veh_id
                                               FROM TABLE (util.str_to_tbl (ls_veh_ids)
                                                          ) a)
                                      -- source vehicle id
                                      AND mrkt_veh.mrkt_id = mrkt_veh_perd.mrkt_id
                                      AND mrkt_veh.veh_id = mrkt_veh_perd.veh_id
                                      AND mrkt_veh_perd.offr_perd_id =
                                                                  ln_srce_offr_perd_id
                                      -- source offer period id
                                      AND mrkt_veh_perd_ver.mrkt_id =
                                                                 mrkt_veh_perd.mrkt_id
                                      AND mrkt_veh_perd_ver.veh_id =
                                                                  mrkt_veh_perd.veh_id
                                      AND mrkt_veh_perd_ver.offr_perd_id =
                                                            mrkt_veh_perd.offr_perd_id
                                      AND mrkt_veh_perd_ver.ver_id = 0
                                      -- work in progress
                                      AND offr.mrkt_id = mrkt_veh_perd_ver.mrkt_id
                                      AND offr.offr_perd_id =
                                                        mrkt_veh_perd_ver.offr_perd_id
                                      AND offr.veh_id = mrkt_veh_perd_ver.veh_id
                                      AND offr.ver_id = mrkt_veh_perd_ver.ver_id
                                      AND offr.offr_typ = 'CMP'
                                      AND offr_sku_line.offr_id = offr.offr_id
                                      AND offr_sku_line.dltd_ind <> 'Y'
                                   MINUS
                                   SELECT offr_sku_line.offr_sku_line_link_id
                                     FROM mrkt_veh,
                                          mrkt_veh_perd,
                                          mrkt_veh_perd_ver,
                                          offr,
                                          offr_sku_line
                                    WHERE mrkt_veh.mrkt_id = ln_trgt_mrkt_id
                                      --target market id
                                      AND mrkt_veh.veh_id IN (
                                             SELECT a.COLUMN_VALUE veh_id
                                               FROM TABLE (util.str_to_tbl (ls_veh_ids)
                                                          ) a)
                                      -- target vehicle id
                                      AND mrkt_veh.mrkt_id = mrkt_veh_perd.mrkt_id
                                      AND mrkt_veh.veh_id = mrkt_veh_perd.veh_id
                                      AND mrkt_veh_perd.offr_perd_id =
                                                                  ln_trgt_offr_perd_id
                                      -- target offer period id
                                      AND mrkt_veh_perd_ver.mrkt_id =
                                                                 mrkt_veh_perd.mrkt_id
                                      AND mrkt_veh_perd_ver.veh_id =
                                                                  mrkt_veh_perd.veh_id
                                      AND mrkt_veh_perd_ver.offr_perd_id =
                                                            mrkt_veh_perd.offr_perd_id
                                      AND mrkt_veh_perd_ver.ver_id = 0
                                      -- work in progress
                                      AND offr.mrkt_id = mrkt_veh_perd_ver.mrkt_id
                                      AND offr.offr_perd_id =
                                                        mrkt_veh_perd_ver.offr_perd_id
                                      AND offr.veh_id = mrkt_veh_perd_ver.veh_id
                                      AND offr.ver_id = mrkt_veh_perd_ver.ver_id
                                      AND offr.offr_typ = 'CMP'
                                      AND offr_sku_line.offr_id = offr.offr_id
                                      AND offr_sku_line.dltd_ind <> 'Y'
                                )
                       GROUP BY offr.mrkt_id, offr.offr_perd_id, prfl_cd, sku_id)
                 SELECT   prfl_cd not_boosted_prfl_cd,
                          not_boosted_sku_list,
                          act_trgt_sku_cnt,
                          is_prfl_lvl_err
                 BULK COLLECT INTO not_boosted_rs
                     FROM (SELECT NVL (not_boosted_list.mrkt_id,
                                       ln_srce_mrkt_id
                                      ) mrkt_id,
                                  NVL (not_boosted_list.offr_perd_id,
                                       ln_srce_offr_perd_id
                                      ) offr_perd_id,
                                  s.prfl_cd,
                                  CASE
                                     WHEN not_boosted_list.sku_id =
                                                       s.sku_id
                                        THEN not_boosted_list.sku_id
                                     ELSE NULL
                                  END not_boosted_sku_list,
                                  COUNT
                                     (CASE
                                         WHEN not_boosted_list.sku_id = s.sku_id
                                            THEN not_boosted_list.sku_id
                                         ELSE NULL
                                      END
                                     ) OVER (PARTITION BY NVL
                                                            (not_boosted_list.prfl_cd,
                                                             s.prfl_cd
                                                            )) act_trgt_sku_cnt,
                                  s.sku_id total_trgt_sku_list,
                                  COUNT (distinct s.sku_id) OVER (PARTITION BY NVL
                                                                         (not_boosted_list.prfl_cd,
                                                                          s.prfl_cd
                                                                         ))
                                                                         trgt_sku_cnt,
                                  CASE
                                     WHEN COUNT
                                            (CASE
                                                WHEN not_boosted_list.sku_id =
                                                                             s.sku_id
                                                   THEN not_boosted_list.sku_id
                                                ELSE NULL
                                             END
                                            ) OVER (PARTITION BY NVL
                                                                   (not_boosted_list.prfl_cd,
                                                                    s.prfl_cd
                                                                   )) =
                                            COUNT (distinct s.sku_id) OVER (PARTITION BY NVL
                                                                                   (not_boosted_list.prfl_cd,
                                                                                    s.prfl_cd
                                                                                   ))
                                        THEN 'Y'
                                     ELSE 'N'
                                  END is_prfl_lvl_err
                             FROM not_boosted_master_list not_boosted_list,
                                  sku s
                            WHERE s.prfl_cd = not_boosted_list.prfl_cd(+))
                    WHERE not_boosted_sku_list IS NOT NULL
                 ORDER BY prfl_cd, not_boosted_sku_list;
              EXCEPTION
                 WHEN OTHERS THEN
                    app_plsql_log.info
                        (   'Error in log_review_job_rslts in getting not_boosted_rs'
                         || SQLERRM (SQLCODE)
                        );
              END;

              IF not_boosted_rs.COUNT > 0
              THEN
                IF p_status <> 1  THEN
                   p_status := 2;

                   FOR i IN not_boosted_rs.FIRST .. not_boosted_rs.LAST
                   LOOP
                      --logging sku level errors.
                      IF not_boosted_rs (i).is_prfl_lvl_err = 'N'
                      THEN
                         ls_btch_log_sub := l_err_item_lvl;
                         ls_btch_log_txt :=
                            'not Boosted because it does not exist in the target Market';
                         ls_btch_err_svrty := 'Warning';
                         ls_btch_log_key :=
                                          not_boosted_rs (i).not_boosted_sku_list;
                         insert_btch_task_log (p_task_id,
                                               ls_btch_log_sub,
                                               ls_btch_log_key,
                                               ls_btch_err_svrty,
                                               ls_btch_log_txt,
                                               0,
                                               1,
                                               ln_trgt_mrkt_id
                                              );
                         prvs_prfl_cd := not_boosted_rs (i).not_boosted_prfl_cd;
                      --logging prfl level errors.
                      ELSE
                         IF not_boosted_rs (i).not_boosted_prfl_cd <> prvs_prfl_cd THEN
                            ls_btch_log_sub := l_err_prfl_lvl;
                            ls_btch_log_txt :=
                               'not Boosted because it does not exist in the target Market';
                            ls_btch_err_svrty := 'Warning';
                            ls_btch_log_key :=
                                           not_boosted_rs (i).not_boosted_prfl_cd;
                            insert_btch_task_log
                                             (p_task_id,
                                              ls_btch_log_sub,
                                              ls_btch_log_key,
                                              ls_btch_err_svrty,
                                              ls_btch_log_txt,
                                              0,
                                              not_boosted_rs (i).act_trgt_sku_cnt,
                                              ln_trgt_mrkt_id
                                             );
                            prvs_prfl_cd := not_boosted_rs (i).not_boosted_prfl_cd;
                         END IF;
                      END IF;
                   END LOOP;
                END IF;
              END IF;
---------------------------------------------------------------------------------------------------------------------
-- Logging review job results for Reboost
---------------------------------------------------------------------------------------------------------------------
           ELSIF ln_job_typ = 8 THEN
              BEGIN
				--Added for QC 3735 on 2nd December,2011 to avoid wm_concat usage
					SELECT DISTINCT sd1.srce_mrkt_id, sd1.srce_offr_perd_id,
									sd1.trgt_mrkt_id, sd1.trgt_offr_perd_id,
									util.tbl_to_str
										   (CURSOR (SELECT sd2.srce_veh_id
													  FROM TABLE (cpy_offr.f_get_parms (p_parm_id)
																 ) sd2
													 WHERE sd2.srce_veh_id = sd2.trgt_veh_id
													   AND sd2.srce_mrkt_id =
																		sd2.srce_offr_perd_refmrkt
													   AND sd2.trgt_mrkt_id =
																		sd2.trgt_offr_perd_refmrkt
													   AND sd2.srce_mrkt_id = sd1.srce_mrkt_id
													   AND sd2.srce_offr_perd_id =
																			 sd1.srce_offr_perd_id
													   AND sd2.trgt_mrkt_id = sd1.trgt_mrkt_id
													   AND sd2.trgt_offr_perd_id =
																			 sd1.trgt_offr_perd_id
												   )
										   ) srce_veh_id
					BULK COLLECT INTO l_tbl_srce_mrkt_id, l_tbl_srce_offr_perd_id,
									l_tbl_trgt_mrkt_id, l_tbl_trgt_offr_perd_id,
									l_tbl_veh_ids
							   FROM TABLE (cpy_offr.f_get_parms (p_parm_id)) sd1
							  WHERE sd1.srce_veh_id = sd1.trgt_veh_id
								AND sd1.srce_mrkt_id = sd1.srce_offr_perd_refmrkt
								AND sd1.trgt_mrkt_id = sd1.trgt_offr_perd_refmrkt;


				--logging the item to be boosted count for the reboost task.
                 ln_cnt_itm_to_be_boostd := 0;

                 FOR i IN
                    NVL (l_tbl_trgt_mrkt_id.FIRST, 1) .. NVL (l_tbl_trgt_mrkt_id.LAST,
                                                              0
                                                             )
                 LOOP
                    BEGIN
                      SELECT count(distinct osl.sku_id)
                        INTO ln_cnt_itm_to_be_boostd      
                        FROM mrkt_veh mv,
                             mrkt_veh_perd mvp,
                             mrkt_veh_perd_ver mvpv,
                             offr o,
                             offr_prfl_prc_point oppp,
                             offr_sku_line osl,
                             prfl pf
                       WHERE mv.mrkt_id = l_tbl_srce_mrkt_id (i)
                         AND mv.veh_id IN (
                                SELECT a.COLUMN_VALUE
                                                     veh_id
                                  FROM TABLE
                                          (util.str_to_tbl
                                              (l_tbl_veh_ids
                                                         (i)
                                              )
                                          ) a)
                         AND mvp.mrkt_id = mv.mrkt_id
                         AND mvp.veh_id = mv.veh_id
                         AND mvp.offr_perd_id = l_tbl_srce_offr_perd_id (i)
                         AND mvpv.mrkt_id = mvp.mrkt_id
                         AND mvpv.veh_id = mvp.veh_id
                         AND mvpv.offr_perd_id = mvp.offr_perd_id
                         AND mvpv.ver_id = 0
                         AND o.mrkt_id = mvpv.mrkt_id
                         AND o.offr_perd_id = mvpv.offr_perd_id
                         AND o.ver_id = mvpv.ver_id
                         AND o.offr_typ = 'CMP'
                         AND o.veh_id = mvpv.veh_id
                         AND oppp.offr_id = o.offr_id
                         AND osl.offr_prfl_prcpt_id = oppp.offr_prfl_prcpt_id
                         AND osl.dltd_ind <> 'Y'
                         AND osl.prfl_cd = pf.prfl_cd
                         AND NOT EXISTS
                             (SELECT 1
                                FROM frcst_boost_xclusn_mrkt_perd fx
                               WHERE fx.mrkt_id = l_tbl_trgt_mrkt_id (i)
                                 AND fx.trgt_offr_perd_id = l_tbl_trgt_offr_perd_id (i)
                                 AND INSTR(',' || nvl(catgry_id_list, '-1') ||' ,', ',' || nvl(pf.catgry_id, -1) ||',') > 0
                                 AND INSTR(',' || nvl(sls_cls_cd_list, '-1') || ',', ',' || nvl(osl.sls_cls_cd, '-1') ||',') > 0
                                 AND INSTR(',' || nvl(sgmt_id_list, '-1') || ',', ',' || nvl(pf.sgmt_id, -1) ||',') > 0);

                      ls_btch_log_sub := l_err_prfl_lvl;
                      ls_btch_log_txt :=
                         'not Boosted because it does not exist in the target Market';
                      ls_btch_err_svrty := 'Warning';
                      ls_btch_log_key := -1;

                      IF p_status <> 1
                      THEN
                         --inserting a dedicated row for the items to be boosted count for the task with log type prfl for reboost.
                         insert_btch_task_log (p_task_id,
                                               ls_btch_log_sub,
                                               ls_btch_log_key,
                                               ls_btch_err_svrty,
                                               ls_btch_log_txt,
                                               ln_cnt_itm_to_be_boostd,
                                               0,
                                               l_tbl_trgt_mrkt_id (i)
                                              );
                      END IF;
                    EXCEPTION
                       WHEN OTHERS
                       THEN
                          app_plsql_log.info
                             (   'Error in log_review_job_rslts in getting ln_cnt_itm_to_be_boostd'
                              || SQLERRM (SQLCODE)
                             );
                    END;

                    --logging not boosted sku's or profile information for reboost
                    BEGIN
                       WITH not_boosted_master_list AS
                            (SELECT   offr.mrkt_id, offr.offr_perd_id, prfl_cd,
                                      sku_id
                                 FROM mrkt_veh,
                                      mrkt_veh_perd,
                                      mrkt_veh_perd_ver,
                                      offr,
                                      offr_sku_line
                                WHERE mrkt_veh.mrkt_id = l_tbl_srce_mrkt_id (i)
                                  --source market id
                                  AND mrkt_veh.veh_id IN (
                                         SELECT a.COLUMN_VALUE veh_id
                                           FROM TABLE
                                                    (util.str_to_tbl (l_tbl_veh_ids
                                                                                   (i)
                                                                     )
                                                    ) a)
                                  -- source vehicle id
                                  AND mrkt_veh.mrkt_id = mrkt_veh_perd.mrkt_id
                                  AND mrkt_veh.veh_id = mrkt_veh_perd.veh_id
                                  AND mrkt_veh_perd.offr_perd_id =
                                                           l_tbl_srce_offr_perd_id (i)
                                  -- source offer period id
                                  AND mrkt_veh_perd_ver.mrkt_id =
                                                                 mrkt_veh_perd.mrkt_id
                                  AND mrkt_veh_perd_ver.veh_id = mrkt_veh_perd.veh_id
                                  AND mrkt_veh_perd_ver.offr_perd_id =
                                                            mrkt_veh_perd.offr_perd_id
                                  AND mrkt_veh_perd_ver.ver_id = 0
                                  -- work in progress
                                  AND offr.mrkt_id = mrkt_veh_perd_ver.mrkt_id
                                  AND offr.offr_perd_id =
                                                        mrkt_veh_perd_ver.offr_perd_id
                                  AND offr.veh_id = mrkt_veh_perd_ver.veh_id
                                  AND offr.ver_id = mrkt_veh_perd_ver.ver_id
                                  AND offr.offr_typ = 'CMP'
                                  AND offr_sku_line.offr_id = offr.offr_id
                                  AND offr_sku_line.dltd_ind <> 'Y'
                                  AND offr_sku_line.offr_sku_line_id IN (
                                         SELECT offr_sku_line.offr_sku_line_id
                                           FROM mrkt_veh,
                                                mrkt_veh_perd,
                                                mrkt_veh_perd_ver,
                                                offr,
                                                offr_sku_line
                                          WHERE mrkt_veh.mrkt_id =
                                                                l_tbl_srce_mrkt_id (i)
                                            --source market id
                                            AND mrkt_veh.veh_id IN (
                                                   SELECT a.COLUMN_VALUE veh_id
                                                     FROM TABLE
                                                             (util.str_to_tbl
                                                                     (l_tbl_veh_ids
                                                                                   (i)
                                                                     )
                                                             ) a)
                                            -- source vehicle id
                                            AND mrkt_veh.mrkt_id =
                                                                 mrkt_veh_perd.mrkt_id
                                            AND mrkt_veh.veh_id = mrkt_veh_perd.veh_id
                                            AND mrkt_veh_perd.offr_perd_id =
                                                           l_tbl_srce_offr_perd_id (i)
                                            -- source offer period id
                                            AND mrkt_veh_perd_ver.mrkt_id =
                                                                 mrkt_veh_perd.mrkt_id
                                            AND mrkt_veh_perd_ver.veh_id =
                                                                  mrkt_veh_perd.veh_id
                                            AND mrkt_veh_perd_ver.offr_perd_id =
                                                            mrkt_veh_perd.offr_perd_id
                                            AND mrkt_veh_perd_ver.ver_id = 0
                                            -- work in progress
                                            AND offr.mrkt_id =
                                                             mrkt_veh_perd_ver.mrkt_id
                                            AND offr.offr_perd_id =
                                                        mrkt_veh_perd_ver.offr_perd_id
                                            AND offr.veh_id = mrkt_veh_perd_ver.veh_id
                                            AND offr.ver_id = mrkt_veh_perd_ver.ver_id
                                            AND offr.offr_typ = 'CMP'
                                            AND offr_sku_line.offr_id = offr.offr_id
                                            AND offr_sku_line.dltd_ind <> 'Y'
                                         MINUS
                                         SELECT offr_sku_line.offr_sku_line_link_id
                                           FROM mrkt_veh,
                                                mrkt_veh_perd,
                                                mrkt_veh_perd_ver,
                                                offr,
                                                offr_sku_line
                                          WHERE mrkt_veh.mrkt_id =
                                                                l_tbl_trgt_mrkt_id (i)
                                            --target market id
                                            AND mrkt_veh.veh_id IN (
                                                   SELECT a.COLUMN_VALUE veh_id
                                                     FROM TABLE
                                                             (util.str_to_tbl
                                                                     (l_tbl_veh_ids
                                                                                   (i)
                                                                     )
                                                             ) a)
                                            -- target vehicle id
                                            AND mrkt_veh.mrkt_id =
                                                                 mrkt_veh_perd.mrkt_id
                                            AND mrkt_veh.veh_id = mrkt_veh_perd.veh_id
                                            AND mrkt_veh_perd.offr_perd_id =
                                                           l_tbl_trgt_offr_perd_id (i)
                                            -- target offer period id
                                            AND mrkt_veh_perd_ver.mrkt_id =
                                                                 mrkt_veh_perd.mrkt_id
                                            AND mrkt_veh_perd_ver.veh_id =
                                                                  mrkt_veh_perd.veh_id
                                            AND mrkt_veh_perd_ver.offr_perd_id =
                                                            mrkt_veh_perd.offr_perd_id
                                            AND mrkt_veh_perd_ver.ver_id = 0
                                            -- work in progress
                                            AND offr.mrkt_id =
                                                             mrkt_veh_perd_ver.mrkt_id
                                            AND offr.offr_perd_id =
                                                        mrkt_veh_perd_ver.offr_perd_id
                                            AND offr.veh_id = mrkt_veh_perd_ver.veh_id
                                            AND offr.ver_id = mrkt_veh_perd_ver.ver_id
                                            AND offr.offr_typ = 'CMP'
                                            AND offr_sku_line.offr_id = offr.offr_id
                                            AND offr_sku_line.dltd_ind <> 'Y')
                             GROUP BY offr.mrkt_id, offr.offr_perd_id, prfl_cd,
                                      sku_id)
                       SELECT   prfl_cd not_boosted_prfl_cd,
                                not_boosted_sku_list,
                                act_trgt_sku_cnt,
                                is_prfl_lvl_err
                       BULK COLLECT INTO not_boosted_rs
                           FROM (SELECT NVL (not_boosted_list.mrkt_id,
                                             l_tbl_srce_mrkt_id (i)
                                            ) mrkt_id,
                                        NVL
                                           (not_boosted_list.offr_perd_id,
                                            l_tbl_srce_offr_perd_id (i)
                                           ) offr_perd_id,
                                        s.prfl_cd,
                                        CASE
                                           WHEN not_boosted_list.sku_id =
                                                       s.sku_id
                                              THEN not_boosted_list.sku_id
                                           ELSE NULL
                                        END not_boosted_sku_list,
                                        COUNT
                                           (CASE
                                               WHEN not_boosted_list.sku_id =
                                                                             s.sku_id
                                                  THEN not_boosted_list.sku_id
                                               ELSE NULL
                                            END
                                           ) OVER (PARTITION BY NVL
                                                                  (not_boosted_list.prfl_cd,
                                                                   s.prfl_cd
                                                                  )) act_trgt_sku_cnt,
                                        s.sku_id total_trgt_sku_list,
                                        COUNT (distinct s.sku_id) OVER (PARTITION BY NVL
                                                                               (not_boosted_list.prfl_cd,
                                                                                s.prfl_cd
                                                                               ))
                                                                         trgt_sku_cnt,
                                        CASE
                                           WHEN COUNT
                                                  (CASE
                                                      WHEN not_boosted_list.sku_id =
                                                                             s.sku_id
                                                         THEN not_boosted_list.sku_id
                                                      ELSE NULL
                                                   END
                                                  ) OVER (PARTITION BY NVL
                                                                         (not_boosted_list.prfl_cd,
                                                                          s.prfl_cd
                                                                         )) =
                                                  COUNT (distinct s.sku_id) OVER (PARTITION BY NVL
                                                                                         (not_boosted_list.prfl_cd,
                                                                                          s.prfl_cd
                                                                                         ))
                                              THEN 'Y'
                                           ELSE 'N'
                                        END is_prfl_lvl_err
                                   FROM not_boosted_master_list not_boosted_list,
                                        sku s
                                  WHERE s.prfl_cd = not_boosted_list.prfl_cd(+))
                          WHERE not_boosted_sku_list IS NOT NULL
                       ORDER BY prfl_cd, not_boosted_sku_list;
                    EXCEPTION
                       WHEN OTHERS
                       THEN
                          app_plsql_log.info
                             (   'Error in log_review_job_rslts in getting not_boosted_rs'
                              || SQLERRM (SQLCODE)
                             );
                    END;

                    IF not_boosted_rs.COUNT > 0
                    THEN
                       IF p_task_id IS NOT NULL
                       THEN
                          IF p_status <> 1
                          THEN
                             p_status := 2;
                             prvs_prfl_cd := 0;

                             FOR j IN not_boosted_rs.FIRST .. not_boosted_rs.LAST
                             LOOP
                                --logging sku level errors.
                                IF not_boosted_rs (j).is_prfl_lvl_err = 'N'
                                THEN
                                   ls_btch_log_sub := l_err_item_lvl;
                                   ls_btch_log_txt :=
                                      'not Boosted because it does not exist in the target Market';
                                   ls_btch_err_svrty := 'Warning';
                                   ls_btch_log_key :=
                                              not_boosted_rs (j).not_boosted_sku_list;
                                   insert_btch_task_log (p_task_id,
                                                         ls_btch_log_sub,
                                                         ls_btch_log_key,
                                                         ls_btch_err_svrty,
                                                         ls_btch_log_txt,
                                                         0,
                                                         1,
                                                         l_tbl_trgt_mrkt_id (i)
                                                        );
                                   prvs_prfl_cd :=
                                                not_boosted_rs (j).not_boosted_prfl_cd;
                                --logging prfl level errors.
                                ELSE
                                   IF not_boosted_rs (j).not_boosted_prfl_cd <>
                                                                         prvs_prfl_cd
                                   THEN
                                      ls_btch_log_sub := l_err_prfl_lvl;
                                      ls_btch_log_txt :=
                                         'not Boosted because it does not exist in the target Market';
                                      ls_btch_err_svrty := 'Warning';
                                      ls_btch_log_key :=
                                               not_boosted_rs (j).not_boosted_prfl_cd;
                                      insert_btch_task_log
                                                 (p_task_id,
                                                  ls_btch_log_sub,
                                                  ls_btch_log_key,
                                                  ls_btch_err_svrty,
                                                  ls_btch_log_txt,
                                                  0,
                                                  not_boosted_rs (j).act_trgt_sku_cnt,
                                                  l_tbl_trgt_mrkt_id (i)
                                                 );
                                      prvs_prfl_cd :=
                                                not_boosted_rs (j).not_boosted_prfl_cd;
                                   END IF;
                                END IF;
                             END LOOP;
                          END IF;
                       ELSE
                          p_partial_success := 2;
                       END IF;
                    END IF;
                 END LOOP;
              EXCEPTION
                 WHEN OTHERS
                 THEN
                    app_plsql_log.info
                       (   'Error in log_review_job_rslts in getting parm info forcastboost'
                        || SQLERRM (SQLCODE)
                       );
              END;
---------------------------------------------------------------------------------------------------------------------
-- Logging review job results for Copy to Local Market Job
---------------------------------------------------------------------------------------------------------------------
           ELSIF ln_job_typ = 4 THEN
              FOR parm_rec IN (SELECT sd.srce_mrkt_id, sd.srce_offr_perd_id,
                                      sd.srce_veh_id, sd.trgt_mrkt_id,
                                      sd.trgt_offr_perd_id, sd.trgt_veh_id
                               FROM   TABLE (cpy_offr.f_get_parms (p_parm_id)) sd
                                WHERE srce_mrkt_id = srce_offr_perd_refmrkt
                                  AND trgt_mrkt_id = trgt_offr_perd_refmrkt)
              LOOP


                 BEGIN

                    SELECT COUNT(DISTINCT osl.sku_id)
                      INTO ln_cnt_itm_to_be_boostd
                               FROM mrkt_veh mv,
                                    mrkt_veh_perd mvp,
                                    mrkt_veh_perd_ver mvpv,
                                    offr o,
                                    offr_prfl_prc_point oppp,
                                    offr_sku_line osl
                              WHERE mv.mrkt_id = parm_rec.srce_mrkt_id
                                AND mv.veh_id = parm_rec.srce_veh_id
                                AND mvp.mrkt_id = mv.mrkt_id
                                AND mvp.veh_id = mv.veh_id
                                AND mvp.offr_perd_id = parm_rec.srce_offr_perd_id
                                AND mvpv.mrkt_id = mvp.mrkt_id
                                AND mvpv.veh_id = mvp.veh_id
                                AND mvpv.offr_perd_id =
                                                   mvp.offr_perd_id
                                AND mvpv.ver_id = 0
                                AND o.mrkt_id = mvpv.mrkt_id
                                AND o.offr_perd_id =
                                                  mvpv.offr_perd_id
                                AND o.ver_id = mvpv.ver_id
                                AND o.offr_typ = 'CMP'
                                AND o.veh_id = mvpv.veh_id
                                AND oppp.offr_id = o.offr_id
                                AND osl.offr_prfl_prcpt_id = oppp.offr_prfl_prcpt_id
                                AND osl.dltd_ind <> 'Y';


                    ls_btch_log_sub := l_err_prfl_lvl;
                    ls_btch_log_txt :=
                       'not Copied because it does not exist in the target Market';
                    ls_btch_err_svrty := 'Warning';
                    ls_btch_log_key := -1;

                    IF p_status <> 1  THEN
                      --inserting a dedicated row for the items to be boosted count for the task with log type prfl for reboost.
                      insert_btch_task_log (p_task_id,
                                            ls_btch_log_sub,
                                            ls_btch_log_key,
                                            ls_btch_err_svrty,
                                            ls_btch_log_txt,
                                            ln_cnt_itm_to_be_boostd,
                                            0,
                                            parm_rec.trgt_mrkt_id
                                           );
                    END IF;

                 EXCEPTION
                    WHEN OTHERS THEN
                       app_plsql_log.info
                          (   'Error in log_review_job_rslts in getting ln_cnt_itm_to_be_boostd'
                           || SQLERRM (SQLCODE)
                          );
                 END;

                 --logging not boosted sku's or profile information.
                 BEGIN
                    WITH not_boosted_master_list AS
                         (SELECT   offr.mrkt_id, offr.offr_perd_id, prfl_cd, sku_id
                              FROM mrkt_veh,
                                   mrkt_veh_perd,
                                   mrkt_veh_perd_ver,
                                   offr,
                                   offr_sku_line
                             WHERE mrkt_veh.mrkt_id = parm_rec.srce_mrkt_id
                               --source market id
                               AND mrkt_veh.veh_id = parm_rec.srce_veh_id
                               -- source vehicle id
                               AND mrkt_veh.mrkt_id = mrkt_veh_perd.mrkt_id
                               AND mrkt_veh.veh_id = mrkt_veh_perd.veh_id
                               AND mrkt_veh_perd.offr_perd_id = parm_rec.srce_offr_perd_id
                               -- source offer period id
                               AND mrkt_veh_perd_ver.mrkt_id = mrkt_veh_perd.mrkt_id
                               AND mrkt_veh_perd_ver.veh_id = mrkt_veh_perd.veh_id
                               AND mrkt_veh_perd_ver.offr_perd_id =
                                                            mrkt_veh_perd.offr_perd_id
                               AND mrkt_veh_perd_ver.ver_id = 0
                               -- work in progress
                               AND offr.mrkt_id = mrkt_veh_perd_ver.mrkt_id
                               AND offr.offr_perd_id = mrkt_veh_perd_ver.offr_perd_id
                               AND offr.veh_id = mrkt_veh_perd_ver.veh_id
                               AND offr.ver_id = mrkt_veh_perd_ver.ver_id
                               AND offr.offr_typ = 'CMP'
                               AND offr_sku_line.offr_id = offr.offr_id
                               AND offr_sku_line.offr_sku_line_id IN (
                                      SELECT offr_sku_line.offr_sku_line_id
                                        FROM mrkt_veh,
                                             mrkt_veh_perd,
                                             mrkt_veh_perd_ver,
                                             offr,
                                             offr_sku_line
                                       WHERE mrkt_veh.mrkt_id = parm_rec.srce_mrkt_id
                                         --source market id
                                         AND mrkt_veh.veh_id = parm_rec.srce_veh_id
                                         -- source vehicle id
                                         AND mrkt_veh.mrkt_id = mrkt_veh_perd.mrkt_id
                                         AND mrkt_veh.veh_id = mrkt_veh_perd.veh_id
                                         AND mrkt_veh_perd.offr_perd_id =
                                                            parm_rec.srce_offr_perd_id
                                         -- source offer period id
                                         AND mrkt_veh_perd_ver.mrkt_id =
                                                                 mrkt_veh_perd.mrkt_id
                                         AND mrkt_veh_perd_ver.veh_id =
                                                                  mrkt_veh_perd.veh_id
                                         AND mrkt_veh_perd_ver.offr_perd_id =
                                                            mrkt_veh_perd.offr_perd_id
                                         AND mrkt_veh_perd_ver.ver_id = 0
                                         -- work in progress
                                         AND offr.mrkt_id = mrkt_veh_perd_ver.mrkt_id
                                         AND offr.offr_perd_id =
                                                        mrkt_veh_perd_ver.offr_perd_id
                                         AND offr.veh_id = mrkt_veh_perd_ver.veh_id
                                         AND offr.ver_id = mrkt_veh_perd_ver.ver_id
                                         AND offr.offr_typ = 'CMP'
                                         AND offr_sku_line.offr_id = offr.offr_id
                                      MINUS
                                      SELECT offr_sku_line.offr_sku_line_link_id
                                        FROM mrkt_veh,
                                             mrkt_veh_perd,
                                             mrkt_veh_perd_ver,
                                             offr,
                                             offr_sku_line
                                       WHERE mrkt_veh.mrkt_id = parm_rec.trgt_mrkt_id
                                         --target market id
                                         AND mrkt_veh.veh_id = parm_rec.trgt_veh_id
                                         -- target vehicle id
                                         AND mrkt_veh.mrkt_id = mrkt_veh_perd.mrkt_id
                                         AND mrkt_veh.veh_id = mrkt_veh_perd.veh_id
                                         AND mrkt_veh_perd.offr_perd_id =
                                                            parm_rec.trgt_offr_perd_id
                                                     -- ln_trgt_offr_perd_id 06Sep2011
                                         -- target offer period id
                                         AND mrkt_veh_perd_ver.mrkt_id =
                                                                 mrkt_veh_perd.mrkt_id
                                         AND mrkt_veh_perd_ver.veh_id =
                                                                  mrkt_veh_perd.veh_id
                                         AND mrkt_veh_perd_ver.offr_perd_id =
                                                            mrkt_veh_perd.offr_perd_id
                                         AND mrkt_veh_perd_ver.ver_id = 0
                                         -- work in progress
                                         AND offr.mrkt_id = mrkt_veh_perd_ver.mrkt_id
                                         AND offr.offr_perd_id =
                                                        mrkt_veh_perd_ver.offr_perd_id
                                         AND offr.veh_id = mrkt_veh_perd_ver.veh_id
                                         AND offr.ver_id = mrkt_veh_perd_ver.ver_id
                                         AND offr.offr_typ = 'CMP'
                                         AND offr_sku_line.offr_id = offr.offr_id
                                         )
                          GROUP BY offr.mrkt_id, offr.offr_perd_id, prfl_cd, sku_id)
                       select nbml.prfl_cd, nbml.sku_id, mp.prfl_cd trgt_prfl_cd, ms.sku_id trgt_ms_sku_id,  ms.trgt_sls_cls_cd,
                              count(nbml.sku_id) over (partition by nbml.prfl_cd, mp.prfl_cd), decode(mp.prfl_cd, null, 'P', 'S') prfl_sku_err_ind
                         BULK COLLECT INTO not_copied_rs
                         from not_boosted_master_list nbml,
                              (select mrkt_sku.*, pa_maps.get_sls_cls_cd
                                                   (parm_rec.trgt_offr_perd_id,
                                                    parm_rec.trgt_mrkt_id,
                                                    mrkt_sku.avlbl_perd_id,
                                                    mrkt_sku.intrdctn_perd_id,
                                                    mrkt_sku.demo_ofs_nr,
                                                    mrkt_sku.demo_durtn_nr,
                                                    mrkt_sku.new_durtn_nr,
                                                    mrkt_sku.stus_perd_id,
                                                    mrkt_sku.dspostn_perd_id,
                                                    mrkt_sku.on_stus_perd_id
                                                   ) trgt_sls_cls_cd
                                 from mrkt_sku where mrkt_id = parm_rec.trgt_mrkt_id
                               ) ms,
                               (select * from mrkt_perd_prfl where mrkt_id = parm_rec.trgt_mrkt_id and offr_perd_id = parm_rec.trgt_offr_perd_id) mp
                        where nbml.sku_id  = ms.sku_id  (+)
                          and nbml.prfl_cd = mp.prfl_cd (+)
                        order by nbml.mrkt_id, nbml.offr_perd_id, nbml.prfl_cd;


                    IF not_copied_rs.COUNT > 0
                    THEN
                          IF p_status <> 1  THEN
                             p_status := 2;
                             prvs_prfl_cd := 0;

                             FOR j IN not_copied_rs.FIRST .. not_copied_rs.LAST
                             LOOP
                                --logging sku level errors.
                                IF not_copied_rs (j).prfl_sku_err_ind = 'S'
                                THEN
                                   ls_btch_log_sub := l_err_item_lvl;

                                   if not_copied_rs (j).trgt_sls_cls_cd = -1 then
                                      ls_btch_log_txt :=  'not Copied, Invalid Sales Class in the target Market';
                                   --elsif not_copied_rs (j).actv_trgt_sku_id is null then
                                   else
                                      ls_btch_log_txt :=  'not Copied, it does not exist in the target Market';
                                   end if;

                                   ls_btch_err_svrty := 'Warning';
                                   ls_btch_log_key := not_copied_rs (j).not_copied_sku_id;
                                   insert_btch_task_log (p_task_id,
                                                         ls_btch_log_sub,
                                                         ls_btch_log_key,
                                                         ls_btch_err_svrty,
                                                         ls_btch_log_txt,
                                                         0,
                                                         1,
                                                         parm_rec.trgt_mrkt_id
                                                        );
                                   prvs_prfl_cd :=  not_copied_rs (j).not_copied_prfl_cd;
                                --logging prfl level errors.
                                ELSE
                                   IF not_copied_rs (j).not_copied_prfl_cd <> prvs_prfl_cd
                                   THEN
                                      ls_btch_log_sub   := l_err_prfl_lvl;
                                      ls_btch_log_txt   := 'not Copied, it does not exist in the target Market';
                                      ls_btch_err_svrty := 'Warning';
                                      ls_btch_log_key   := not_copied_rs (j).not_copied_prfl_cd;
                                      insert_btch_task_log
                                                 (p_task_id,
                                                  ls_btch_log_sub,
                                                  ls_btch_log_key,
                                                  ls_btch_err_svrty,
                                                  ls_btch_log_txt,
                                                  0,
                                                  not_copied_rs (j).act_trgt_sku_cnt,
                                                  parm_rec.trgt_mrkt_id
                                                 );
                                      prvs_prfl_cd :=  not_copied_rs (j).not_copied_prfl_cd;
                                   END IF;
                                END IF;
                             END LOOP;
                          END IF;
                    END IF;

                 EXCEPTION
                    WHEN OTHERS THEN
                       app_plsql_log.info
                          (   'Error in log_review_job_rslts in getting not_copied_rs query'
                           || SQLERRM (SQLCODE)
                          );
                 END;

                 BEGIN

                    NULL;

                 EXCEPTION
                    WHEN OTHERS THEN
                       app_plsql_log.info
                          (   'Error in log_review_job_rslts in processing not_copied_rs query'
                           || SQLERRM (SQLCODE)
                          );
                 END;


              END LOOP;
           END IF;
       end if;
    EXCEPTION
       WHEN OTHERS
       THEN
          app_plsql_log.info ('Error in log_review_job_rslts '
                              || SQLERRM (SQLCODE)
                             );
    END;



   procedure P_ADD_ADD_REG_PRC_ITEMS (
    p_mrkt_id number,
    p_offr_perd_id number
   )
   IS
    ln_osl_id number;
   BEGIN

APP_PLSQL_LOG.info('Start of P_ADD_ADD_REG_PRC_ITEMS');
   for rec in (
    SELECT cd.offr_prfl_prcpt_id,
      cd.prfl_cd,
      cd.sls_cls_Cd,
      cd.c copied_items_count,
      sd.c standard_data_items_count
    FROM
      (SELECT offr_prfl_prc_point.offr_prfl_prcpt_id,
        offr_prfl_prc_point.prfl_cd,
        offr_prfl_prc_point.sls_cls_cd,
        COUNT(*) c
      FROM offr_prfl_prc_point,offr_sku_line,
        offr
      WHERE offr.mrkt_id    = p_mrkt_id
      AND offr.offr_perd_id = p_offr_perd_id
      AND offr.ver_id       =0
      AND offr.offr_id      = offr_prfl_prc_point.offr_id
      and offr_prfl_prc_point.offr_prfl_prcpt_id = offr_sku_line.offr_prfl_prcpt_id
      GROUP BY offr_prfl_prc_point.offr_prfl_prcpt_id,
        offr_prfl_prc_point.sls_cls_cd,
        offr_prfl_prc_point.prfl_cd
      ) cd,
      (SELECT prfl_Cd,
        sls_cls_cd,
        COUNT(*) c
      FROM
        (SELECT sku.prfl_cd,
          pa_maps.get_sls_cls_cd (p_offr_perd_id, p_mrkt_id, mrkt_sku.avlbl_perd_id, mrkt_sku.intrdctn_perd_id, mrkt_sku.demo_ofs_nr, mrkt_sku.demo_durtn_nr, mrkt_sku.new_durtn_nr, mrkt_sku.stus_perd_id, mrkt_sku.dspostn_perd_id, mrkt_sku.on_stus_perd_id) sls_cls_cd
        FROM sku,
          mrkt_sku
        WHERE sku.sku_id       = mrkt_sku.sku_id
        AND mrkt_sku.mrkt_id   = p_mrkt_id
        AND mrkt_sku.dltd_ind <> 'Y'
        )
      WHERE sls_cls_Cd > -1
      GROUP BY prfl_Cd,
        sls_cls_cd
      ) sd
    WHERE sd.prfl_Cd  = cd.prfl_cd
    AND sd.sls_cls_cd = cd.sls_Cls_Cd
    AND sd.c          > cd.c
    ) loop

      null;

      app_plsql_log.info(rec.offr_prfl_prcpt_id || ' | ' || rec.prfl_cd || ' | ' || rec.sls_cls_cd || ' | ' || rec.copied_items_count || ' | ' || rec.standard_data_items_count);

      for rec_sku in (
        select
          sku_id
        from (
        SELECT
          mrkt_sku.sku_id,
          pa_maps.get_sls_cls_cd (p_offr_perd_id, p_mrkt_id, mrkt_sku.avlbl_perd_id, mrkt_sku.intrdctn_perd_id, mrkt_sku.demo_ofs_nr, mrkt_sku.demo_durtn_nr, mrkt_sku.new_durtn_nr, mrkt_sku.stus_perd_id, mrkt_sku.dspostn_perd_id, mrkt_sku.on_stus_perd_id) sls_cls_cd
        FROM
          sku,mrkt_sku
        WHERE
          mrkt_sku.mrkt_id = p_mrkt_id
          and mrkt_sku.sku_id = sku.sku_id
          and sku.prfl_Cd = rec.prfl_cd
        )
        where
          sls_cls_cd = rec.sls_cls_cd
          and sku_id not in (
            select sku_id from offr_sku_line where offr_sku_line.offr_prfl_prcpt_id = rec.offr_prfl_prcpt_id
          )
      ) loop

        app_plsql_log.info(rec_sku.sku_id);



        for rec_osl in (
          select
            seq.NEXTVAL offr_sku_line_id,
            offr_id,
            veh_id,
            featrd_side_cd,
            offr_perd_id,
            mrkt_id,
            rec_sku.sku_id sku_id,
            pg_ofs_nr,
            crncy_cd,
            prfl_cd,
            sls_cls_cd,
            offr_prfl_prcpt_id,
            promtn_desc_txt,
            sls_prc_amt,
            cost_typ,
            prmry_sku_offr_ind,
            'Y' dltd_ind,
            unit_splt_pct,
            'N' demo_avlbl_ind,
            null offr_sku_line_link_id,
            'N' set_cmpnt_ind,
            0 set_cmpnt_qty,
            null offr_sku_set_id,
            unit_prc_amt
          from
            offr_sku_line
          where
            offr_sku_line.offr_prfl_prcpt_id = rec.offr_prfl_prcpt_id
            and rownum < 2
        ) loop


          for rec_scps in (
            select
              offr_sls_cls_sku.pg_ofs_nr,
              offr_sls_cls_sku.featrd_side_cd
            from offr_sls_cls_sku
              where
              offr_sls_cls_sku.offr_id = rec_osl.offr_id
              and offr_sls_cls_sku.sls_cls_cd = rec_osl.sls_cls_cd
              and offr_sls_cls_sku.prfl_cd = rec_osl.prfl_cd
              and offr_sls_cls_sku.sku_id = rec_osl.sku_id
              and rownum < 2 ) loop

              app_plsql_log.info (  'sls_cls_plcmt_sku: ' || rec_scps.pg_ofs_nr || '/' || rec_scps.featrd_side_cd );

              begin

                INSERT INTO offr_sku_line (
                  offr_sku_line_id,
                  offr_id,
                  veh_id,
                  featrd_side_cd,
                  offr_perd_id,
                  mrkt_id,
                  sku_id,
                  pg_ofs_nr,
                  crncy_cd,
                  prfl_cd,
                  sls_cls_cd,
                  offr_prfl_prcpt_id,
                  promtn_desc_txt,
                  sls_prc_amt,
                  cost_typ,
                  prmry_sku_offr_ind,
                  dltd_ind,
                  unit_splt_pct,
                  demo_avlbl_ind,
                  offr_sku_line_link_id,
                  set_cmpnt_ind,
                  set_cmpnt_qty,
                  offr_sku_set_id,
                  unit_prc_amt
                )
                VALUES
                (
                  rec_osl.offr_sku_line_id,
                  rec_osl.offr_id,
                  rec_osl.veh_id,
                  rec_scps.featrd_side_cd,
                  rec_osl.offr_perd_id,
                  rec_osl.mrkt_id,
                  rec_osl.sku_id,
                  rec_scps.pg_ofs_nr,
                  rec_osl.crncy_cd,
                  rec_osl.prfl_cd,
                  rec_osl.sls_cls_cd,
                  rec_osl.offr_prfl_prcpt_id,
                  rec_osl.promtn_desc_txt,
                  rec_osl.sls_prc_amt,
                  rec_osl.cost_typ,
                  rec_osl.prmry_sku_offr_ind,
                  rec_osl.dltd_ind,
                  rec_osl.unit_splt_pct,
                  rec_osl.demo_avlbl_ind,
                  rec_osl.offr_sku_line_link_id,
                  rec_osl.set_cmpnt_ind,
                  rec_osl.set_cmpnt_qty,
                  rec_osl.offr_sku_set_id,
                  rec_osl.unit_prc_amt
              );


              app_plsql_log.info (  'New item was created! ' || rec_osl.offr_sku_line_id || '/' || rec_osl.sku_id );




              ---DMS INSERTS
              begin
                        INSERT INTO dstrbtd_mrkt_sls
                          (
                            mrkt_id,
                            offr_perd_id,
                            veh_id,
                            sls_perd_id,
                            offr_sku_line_id,
                            sls_typ_id,
                            unit_qty,
                            sls_stus_cd,
                            sls_srce_id,
                            prev_unit_qty,
                            prev_sls_srce_id,
                            comsn_amt,
                            tax_amt,
                            net_to_avon_fct ,
                            unit_ovrrd_ind  ,
                            currnt_est_ind
                          )
                        VALUES
                          (
                            rec_osl.mrkt_id,
                            rec_osl.offr_perd_id,
                            rec_osl.veh_id,
                            rec_osl.offr_perd_id,
                            rec_osl.offr_sku_line_id,
                            1,
                            0,
                            2, --rec.dms_sls_stus_cd     ,
                            1, --rec.dms_sls_srce_id     ,
                            0, --rec.dms_prev_unit_qty   , -- Changed on 17Mar2011
                            1, --rec.dms_prev_sls_srce_id,
                            0, --rec.dms_comsn_amt       ,
                            0, --rec.dms_tax_amt         ,
                            0,--ln_gta, --rec.dms_net_to_avon_fct ,
                            'N', --rec.dms_unit_ovrrd_ind  ,
                            'N'
                        );

                app_plsql_log.info (  'New dms was created! ' || rec_osl.offr_sku_line_id || '/' || rec_osl.sku_id );

              EXCEPTION
                WHEN OTHERS THEN
                  APP_PLSQL_LOG.info(  'ERROR!!! New DMS was not created! '||SQLERRM);
              end;

            EXCEPTION
              WHEN OTHERS   THEN

              app_plsql_log.info (  'ERROR!!! New item was not created!' || rec_osl.offr_sku_line_id || '/' || rec_osl.sku_id );

              app_plsql_log.info (  'INSERT INTO offr_sku_line ( '
              || '    offr_sku_line_id, '
              || '    offr_id, '
              || '    veh_id, '
              || '    featrd_side_cd,'
              || '    offr_perd_id, '
              || '    mrkt_id,'
              || '    sku_id, '
              || '    pg_ofs_nr,'
              || '    crncy_cd, '
              || '    prfl_cd,'
              || '    sls_cls_cd,'
              || '    offr_prfl_prcpt_id,'
              || '    promtn_desc_txt,'
              || '    sls_prc_amt, '
              || '    cost_typ,'
              || '    prmry_sku_offr_ind,'
              || '    dltd_ind,'
              || '    unit_splt_pct,'
              || '    demo_avlbl_ind,'
              || '    offr_sku_line_link_id,'
              || '    set_cmpnt_ind,'
              || '    set_cmpnt_qty,'
              || '    offr_sku_set_id,'
              || '    unit_prc_amt'
              || '  )'
              || '  VALUES'
              || '  ('
              ||    rec_osl.offr_sku_line_id || ','
              ||    rec_osl.offr_id || ','
              ||    rec_osl.veh_id || ','
              ||    rec_scps.featrd_side_cd || ','
              ||    rec_osl.offr_perd_id || ','
              ||    rec_osl.mrkt_id || ','
              ||    rec_osl.sku_id || ','
              ||    rec_scps.pg_ofs_nr || ','
              ||    rec_osl.crncy_cd || ','
              ||    rec_osl.prfl_cd || ','
              ||    rec_osl.sls_cls_cd || ','
              ||    rec_osl.offr_prfl_prcpt_id || ','
              ||    rec_osl.promtn_desc_txt || ','
              ||    rec_osl.sls_prc_amt || ','
              ||    rec_osl.cost_typ || ','
              ||    rec_osl.prmry_sku_offr_ind || ','
              ||    rec_osl.dltd_ind || ','
              ||    rec_osl.unit_splt_pct || ','
              ||    rec_osl.demo_avlbl_ind || ','
              ||    rec_osl.offr_sku_line_link_id || ','
              ||    rec_osl.set_cmpnt_ind || ','
              ||    rec_osl.set_cmpnt_qty || ','
              ||    rec_osl.offr_sku_set_id || ','
              ||    rec_osl.unit_prc_amt || ' '
              || ');');

            END;

          end loop;

          null;

          end loop;

      end loop;

    end loop;


   END;


   FUNCTION F_GET_KEY_VALS_FOR_UPDATE --(p_key_vals_for_update IN t_tbl_key_vals_for_update)
      RETURN t_tbl_key_vals_for_update PIPELINED
   AS
   BEGIN
      FOR i in v_tbl_key_vals_for_update.first..v_tbl_key_vals_for_update.last
      LOOP
         PIPE ROW (v_tbl_key_vals_for_update(i));
      END LOOP;
   END F_GET_KEY_VALS_FOR_UPDATE;
   PROCEDURE parms (
      cpy_offr_parms   IN       tbl_mps_parm,
      p_user_nm        IN       VARCHAR2,
      parm_id          OUT      mps_parm.mps_parm_id%TYPE
   )
   AS
      l_parm_id   NUMBER;
      l_run_id    NUMBER;
   BEGIN
      SELECT seq_mps_parm.NEXTVAL
        INTO l_parm_id
        FROM DUAL;
      BEGIN
         INSERT INTO mps_parm
                     (mps_parm_id, srce_mrkt_id, srce_ver_id,
                      --set_units_to_zero_ind,
                      set_to_reqrng_plnrs_rev_ind, offr_hdr_only_ind,
                      rtain_prc_pnts_when_rplcng_ind, updt_what_if_ind,
                      dlt_srce_offr_ind, trgt_mrkt_veh_perd_sctn_id,
                      creat_user_id, last_updt_user_id)
            (SELECT l_parm_id, srce_mrkt_id, srce_ver_id,
                    --nvl(set_units_to_zero_ind,'N'),
                    NVL (set_to_reqrng_plnrs_rev_ind, 'N'),
                    NVL (offr_hdr_only_ind, 'N'),
                    NVL (rtain_prc_pnts_when_rplcng_ind, 'N'),
                    NVL (updt_what_if_ind, 'N'),
                    NVL (dlt_srce_offr_ind, 'N'), trgt_mrkt_veh_perd_sctn_id,
                    p_user_nm, p_user_nm
               FROM TABLE (CAST (cpy_offr_parms AS tbl_mps_parm))parm
              WHERE 0 < (SELECT COUNT (1)
                           FROM TABLE (CAST (cpy_offr_parms AS tbl_mps_parm)))
                AND parm.srce_mrkt_id IS NOT NULL);
      EXCEPTION
         WHEN OTHERS
         THEN
            app_plsql_log.info (   'ERROR - while inserting MPS_PARM'
                                || SQLERRM (SQLCODE)
                               );
      END;
      BEGIN
         INSERT INTO mps_parm_srce_veh
                     (mps_parm_id, veh_id, seq_nr, creat_user_id,
                      last_updt_user_id)
            (SELECT l_parm_id, veh_id, seq_nr, p_user_nm, p_user_nm
               FROM TABLE
                        (CAST (cpy_offr_parms (1).srce_vehs AS tbl_mps_parm_veh
                              )
                        )srce_vehs
               WHERE srce_vehs.veh_id <> -9999
            );
      EXCEPTION
         WHEN OTHERS
         THEN
            app_plsql_log.info
                              (   'ERROR - while inserting MPS_PARM_SRCE_VEH'
                               || SQLERRM (SQLCODE)
                              );
      END;
      BEGIN
         INSERT INTO mps_parm_srce_offr_perd
                     (mps_parm_id, mrkt_id, offr_perd_id, seq_nr,
                      creat_user_id, last_updt_user_id)
            (SELECT l_parm_id, ref_mrkt_id, perd_id, seq_nr, p_user_nm,
                    p_user_nm
               FROM TABLE
                       (CAST
                           (cpy_offr_parms (1).srce_offr_perds AS tbl_mps_parm_perd
                           )
                       )srce_offr_perds
              WHERE srce_offr_perds.perd_id <> -9999
            )
                 ;
      EXCEPTION
         WHEN OTHERS
         THEN
            app_plsql_log.info
                        (   'ERROR - while inserting MPS_PARM_SRCE_OFFR_PERD'
                         || SQLERRM (SQLCODE)
                        );
      END;
      BEGIN
         INSERT INTO mps_parm_srce_sls_perd
                     (mps_parm_id, mrkt_id, sls_perd_id, seq_nr,
                      creat_user_id, last_updt_user_id)
            (SELECT distinct  l_parm_id, ref_mrkt_id, perd_id, seq_nr, p_user_nm,
                    p_user_nm
               FROM TABLE
                       (CAST
                           (cpy_offr_parms (1).srce_sls_perds AS tbl_mps_parm_perd
                           )
                       )srce_sls_perds
              WHERE srce_sls_perds.perd_id <> -9999
            );
      EXCEPTION
         WHEN OTHERS
         THEN
            app_plsql_log.info
                         (   'ERROR - while inserting MPS_PARM_SRCE_SLS_PERD'
                          || SQLERRM (SQLCODE)
                         );
      END;
      BEGIN
         INSERT INTO mps_parm_offr
                     (mps_parm_id, offr_id, seq_nr, creat_user_id,
                      last_updt_user_id)
            (SELECT l_parm_id, offr_id, seq_nr, p_user_nm, p_user_nm
               FROM TABLE
                       (CAST
                            (cpy_offr_parms (1).srce_offrs AS tbl_mps_parm_offr
                            )
                       )srce_offrs
              WHERE srce_offrs.offr_id <> -9999
                    );
      EXCEPTION
         WHEN OTHERS
         THEN
            app_plsql_log.info (   'ERROR - while inserting MPS_PARM_OFFR'
                                || SQLERRM (SQLCODE)
                               );
      END;
      BEGIN
         INSERT INTO mps_parm_trgt_mrkt
                     (mps_parm_id, mrkt_id, rtain_sls_prc_ind,
                      copy_units_rul_id, seq_nr, creat_user_id,
                      last_updt_user_id)
            (SELECT l_parm_id, mrkt_id, rtain_sls_prc_ind, copy_units_rul_id,
                    seq_nr, p_user_nm, p_user_nm
               FROM TABLE
                       (CAST
                            (cpy_offr_parms (1).trgt_mrkts AS tbl_mps_parm_mrkt
                            )
                       )trgt_mrkts
              WHERE trgt_mrkts.mrkt_id <> -9999
             );
      EXCEPTION
         WHEN OTHERS
         THEN
            app_plsql_log.info
                             (   'ERROR - while inserting MPS_PARM_TRGT_MRKT'
                              || SQLERRM (SQLCODE)
                             );
      END;
      BEGIN
         INSERT INTO mps_parm_trgt_ver
                     (mps_parm_id, ver_id, seq_nr, creat_user_id,
                      last_updt_user_id)
            (SELECT l_parm_id, ver_id, seq_nr, p_user_nm, p_user_nm
               FROM TABLE
                        (CAST (cpy_offr_parms (1).trgt_vers AS tbl_mps_parm_ver
                              )
                        )trgt_vers
              WHERE trgt_vers.ver_id <> -9999
            );
      EXCEPTION
         WHEN OTHERS
         THEN
            app_plsql_log.info
                              (   'ERROR - while inserting MPS_PARM_TRGT_VER'
                               || SQLERRM (SQLCODE)
                              );
      END;
      BEGIN
         INSERT INTO mps_parm_trgt_veh
                     (mps_parm_id, veh_id, rtain_sls_prc_ind,
                      copy_units_rul_id, seq_nr, creat_user_id,
                      last_updt_user_id)
            (SELECT distinct l_parm_id, veh_id, rtain_sls_prc_ind, copy_units_rul_id,
                    seq_nr, p_user_nm, p_user_nm
               FROM TABLE
                        (CAST (cpy_offr_parms (1).trgt_vehs AS tbl_mps_parm_veh
                              )
                        )trgt_vehs
             WHERE trgt_vehs.veh_id <> -9999
              );
      EXCEPTION
         WHEN OTHERS
         THEN
            app_plsql_log.info
                              (   'ERROR - while inserting MPS_PARM_TRGT_VEH'
                               || SQLERRM (SQLCODE)
                              );
      END;
      BEGIN
         INSERT INTO mps_parm_trgt_offr_perd
                     (mps_parm_id, mrkt_id, offr_perd_id, rtain_sls_prc_ind,
                      copy_units_rul_id, seq_nr, creat_user_id,
                      last_updt_user_id)
            (SELECT l_parm_id, ref_mrkt_id, perd_id, rtain_sls_prc_ind,
                    copy_units_rul_id, seq_nr, p_user_nm, p_user_nm
               FROM TABLE
                       (CAST
                           (cpy_offr_parms (1).trgt_offr_perds AS tbl_mps_parm_perd
                           )
                       )trgt_offr_perds
              WHERE trgt_offr_perds.perd_id <> -9999
            );
      EXCEPTION
         WHEN OTHERS
         THEN
            app_plsql_log.info
                        (   'ERROR - while inserting MPS_PARM_TRGT_OFFR_PERD'
                         || SQLERRM (SQLCODE)
                        );
      END;
      BEGIN
         INSERT INTO mps_parm_trgt_sls_perd
                     (mps_parm_id, mrkt_id, sls_perd_id, seq_nr,
                      creat_user_id, last_updt_user_id)
            (SELECT distinct l_parm_id, ref_mrkt_id, perd_id, seq_nr, p_user_nm,
                    p_user_nm
               FROM TABLE
                       (CAST
                           (cpy_offr_parms (1).trgt_sls_perds AS tbl_mps_parm_perd
                           )
                       )trgt_sls_perds
              WHERE trgt_sls_perds.perd_id <> -9999
            );
      EXCEPTION
         WHEN OTHERS
         THEN
            app_plsql_log.info
                         (   'ERROR - while inserting MPS_PARM_TRGT_SLS_PERD'
                          || SQLERRM (SQLCODE)
                         );
      END;
      BEGIN
         INSERT INTO mps_parm_replctn_mov_mrkt
                     (mps_parm_id, mrkt_id, seq_nr, creat_user_id,
                      last_updt_user_id)
            (SELECT l_parm_id, mrkt_id, seq_nr, p_user_nm, p_user_nm
               FROM TABLE
                       (CAST
                           (cpy_offr_parms (1).replctn_mov_mrkts AS tbl_mps_parm_mrkt
                           )
                       )replctn_mov_mrkts
              WHERE replctn_mov_mrkts.mrkt_id <> -9999
            );
      EXCEPTION
         WHEN OTHERS
         THEN
            app_plsql_log.info
                      (   'ERROR - while inserting MPS_PARM_REPLCTN_MOV_MRKT'
                       || SQLERRM (SQLCODE)
                      );
      END;
      parm_id := l_parm_id;
   EXCEPTION
      WHEN OTHERS
      THEN
         app_plsql_log.info ('Error while inserting PARM TABLES');
   END;
   FUNCTION is_mapg_exists (
      p_src_mrkt_id    IN   mrkt.mrkt_id%TYPE,
      p_src_offr_perd_id    IN   offr.offr_perd_id%TYPE,
      p_trgt_mrkt_id   IN   mrkt.mrkt_id%TYPE,
      p_trgt_offr_perd_id   IN   offr.offr_perd_id%TYPE
   )
      RETURN CHAR
   AS
    l_mapg_exists number;
   BEGIN
        SELECT
            count(*)
                -- if  db = 0 --> not equal
                -- if  db > 0 --> equal
        INTO l_mapg_exists
        FROM
            (
                select
                    MRKT_PERD_CMPGN_MAPG.SRCE_MRKT_ID,
                    MRKT_PERD_CMPGN_MAPG.SRCE_PERD_ID,
                    MRKT_PERD_CMPGN_MAPG.TRGT_MRKT_ID,
                    MRKT_PERD_CMPGN_MAPG.TRGT_PERD_ID
                from
                    MRKT_PERD_CMPGN_MAPG
                where
                    SRCE_MRKT_ID = p_src_mrkt_id
                    and TRGT_MRKT_ID = p_trgt_mrkt_id
                    and TRGT_PERD_ID = p_trgt_offr_perd_id
                    and SRCE_PERD_ID = p_src_offr_perd_id
                union all
                select
                    p_src_mrkt_id,
                    p_src_offr_perd_id,
                    p_trgt_mrkt_id,
                    p_trgt_offr_perd_id
                from dual
                    where
                        p_src_mrkt_id = p_trgt_mrkt_id
                        and p_src_offr_perd_id = p_trgt_offr_perd_id
        );
      IF (l_mapg_exists > 0)
      THEN
         RETURN ('Y');                                                -- Same
      ELSE
         RETURN ('N');                                           -- Different
      END IF;
   END is_mapg_exists;

   PROCEDURE UPDATE_TRGT_UNITS(
        p_parm_id                 IN mps_parm.mps_parm_id%type,
        p_srce_mrkt_id            IN offr.mrkt_id%type,
        p_srce_veh_id             IN offr.veh_id%type,
        p_srce_offr_perd_id       IN offr.offr_perd_id%type,
        p_srce_ver_id             IN offr.ver_id%type,
        p_trgt_mrkt_id            IN offr.mrkt_id%type,
        p_trgt_veh_id             IN offr.veh_id%type,
        p_trgt_offr_perd_id       IN offr.offr_perd_id%type,
        p_trgt_ver_id             IN offr.ver_id%type,
        p_unit_calc_typ_id        IN COPY_UNITS_RUL.COPY_UNITS_RUL_ID%type,
        p_fb_ind                  IN CHAR,
        p_status                  IN OUT NUMBER,
        p_veh_id                  IN VARCHAR2, -- Added by Ramkumar. V for the QC3359 on 23May2011
        p_fb_xclud_sls_cls_cd     IN VARCHAR2  -- Added by Sivakumar D.R for the QC 3361 on 24May2011
   ) AS

   type tbl_numbers  is table of NUMBER INDEX BY BINARY_INTEGER;
   type tbl_varchars is table of varchar2(250) INDEX BY BINARY_INTEGER;
   v_trgt_mrkt_ids          tbl_numbers;
   v_trgt_sls_perd_ids      tbl_numbers;
   v_trgt_osl_ids           tbl_numbers;
   v_trgt_sls_typ_ids       tbl_numbers;
   v_trgt_unit_qtys         tbl_numbers;
   v_trgt_calc_unit_qtys    tbl_numbers;
   v_unit_calc_inds         tbl_varchars;
   v_trgt_perd_ofs_nr       tbl_numbers;
   v_trgt_ofs_dstrbtn_pct   tbl_numbers;
   v_trgt_offr_perd_ids     tbl_numbers;
   v_xtrpltn_pcts           tbl_numbers;
   l_fb_xclud_sls_cls_cd    varchar2(1000) := p_fb_xclud_sls_cls_cd;
   l_procedure_name         pa_maps_public.proc_name := 'UPDATE_TRGT_UNITS: ';
   BEGIN

    app_plsql_log.info(l_procedure_name||'start');

        app_plsql_log.info (l_procedure_name||'source data: '
          || '(U1)'  || p_parm_id
          || '(U2)'  || p_srce_mrkt_id
          || '(U3)'  || p_srce_veh_id
          || '(U4)'  || p_srce_offr_perd_id
          || '(U5)'  || p_srce_ver_id
          || '(U6)'  || p_trgt_mrkt_id
          || '(U7)'  || p_trgt_veh_id
          || '(U8)'  || p_trgt_offr_perd_id
          || '(U9)'  || p_trgt_ver_id
          || '(U10)' || p_unit_calc_typ_id
          || '(U11)' || p_fb_ind
          || '(U12)' || p_status
          || '(U13)' || p_veh_id
          || '(U14)' || p_fb_xclud_sls_cls_cd
         );

    app_plsql_log.info(l_procedure_name||'Forecast Boost = '||p_fb_ind);
    app_plsql_log.info(l_procedure_name||'Units Calc Type = '||p_unit_calc_typ_id);

    if p_fb_ind = 'Y' then

        if l_fb_xclud_sls_cls_cd is null then
           l_fb_xclud_sls_cls_cd := '9999999';
        end if;

        app_plsql_log.info(l_procedure_name||'Boost or reboost unit calculation enters... !!!');
        app_plsql_log.info(l_procedure_name||'Excluded sales classes: (not used anymore): '||l_fb_xclud_sls_cls_cd);
        for i in (SELECT nvl(fx.catgry_id_list, 'NULL') as category_list,
                         nvl(fx.sls_cls_cd_list, 'NULL') as salesclass_list,
                         nvl(fx.sgmt_id_list, 'NULL') as segment_list
                    FROM frcst_boost_xclusn_mrkt_perd fx
                   WHERE fx.mrkt_id = p_trgt_mrkt_id
                     AND fx.trgt_offr_perd_id = p_trgt_offr_perd_id
                   ORDER BY fx.catgry_id_list, fx.sls_cls_cd_list, fx.sgmt_id_list) loop
          app_plsql_log.info(l_procedure_name
                             ||'Excluded Categories: '''||i.category_list
                             ||''' and Sales Classes: '''||i.salesclass_list
                             ||''' and Segments: '''||i.segment_list
                             ||''' (by FRCST_BOOST_XCLUSN_MRKT_PERD table)');
        end loop;
        --
        SELECT trgt_mrkt_id, trgt_sls_perd_id, trgt_items, trgt_sls_typ_id, trgt_unit_qty, unit_calc_ind,
               case
                 when xtrpltn_pct is null then
                   src_units * decode(nvl(SUM (temp_sales) OVER (PARTITION BY grp_b),0), 0, 0,
                                    (fb_trgt_sls_amt / SUM (temp_sales) OVER (PARTITION BY grp_b)))
                 else
                   src_units * xtrpltn_pct / 100
                 end trgt_units,
               trgt_offr_perd_id,
               xtrpltn_pct
          BULK COLLECT
          INTO v_trgt_mrkt_ids, v_trgt_sls_perd_ids, v_trgt_osl_ids, v_trgt_sls_typ_ids, v_trgt_unit_qtys,
               v_unit_calc_inds, v_trgt_calc_unit_qtys, v_trgt_offr_perd_ids, v_xtrpltn_pcts
          FROM (SELECT 'b' grp_b, src_items, osl_link_id, src_sales, src_units,
                       total_gs, trgt_items, trgt_selprc, trgt_nofor, trgt_nta,
                       src_units * decode(nvl(trgt_nofor,0),0, 0,
                                          (trgt_selprc / trgt_nofor)) * trgt_nta temp_sales,
                       trgt_mrkt_id, trgt_sls_perd_id, trgt_sls_typ_id, fb_trgt_sls_amt, unit_calc_ind, trgt_unit_qty,
                       trgt_offr_perd_id,
                       xtrpltn_pct
                  FROM (SELECT   src_items, src_sales, src_units,
                                 SUM (src_sales) OVER (PARTITION BY grp_typ) total_gs,
                                 src_offr_id,
                                 src_prfl_cd,
                                 src_sls_cls_cd,
                                 src_pg_ofs_nr,
                                 src_FEATRD_SIDE_CD,
                                 src_SLS_PRC_AMT ,
                                 src_NR_FOR_QTY   ,
                                 src_COMSN_TYP,
                                 src_REG_PRC_AMT,
                                 src_sku_id
                            FROM (SELECT 'a' grp_typ, dms.offr_sku_line_id src_items,
                                         unit_qty src_units,
                                         ROUND
                                            (DECODE (dms.unit_qty,
                                                     NULL, 0,
                                                     decode(nvl(oppp.nr_for_qty,0),0,0,
                                                            (dms.unit_qty * oppp.sls_prc_amt * dms.net_to_avon_fct / oppp.nr_for_qty))
                                                    ),
                                             2
                                            ) src_sales,
                                         OPPP.OFFR_ID           src_offr_id,
                                         OPPP.PRFL_CD           src_prfl_cd,
                                         OPPP.SLS_CLS_CD        src_sls_cls_cd,
                                         OPPP.PG_OFS_NR         src_pg_ofs_nr,
                                         OPPP.FEATRD_SIDE_CD    src_FEATRD_SIDE_CD,
                                         OPPP.SLS_PRC_AMT       src_SLS_PRC_AMT ,
                                         OPPP.NR_FOR_QTY        src_NR_FOR_QTY   ,
                                         OPPP.COMSN_TYP         src_COMSN_TYP,
                                         SRP.REG_PRC_AMT        src_REG_PRC_AMT,
                                         OSL.SKU_ID             src_sku_id,
                                         dms.net_to_avon_fct src_nta
                                    FROM offr o,
                                         dstrbtd_mrkt_sls dms,
                                         offr_sku_line osl,
                                         offr_prfl_prc_point oppp,
                                         SKU_REG_PRC SRP,
                                         prfl pf,
                                         sku s,
                                         mrkt_sku ms,
                                         sku_cost sc,
                                         mrkt_veh_perd_ver mvpv
                                   WHERE o.mrkt_id = p_srce_mrkt_id
                                     AND o.offr_perd_id = p_srce_offr_perd_id
                                     AND INSTR(',' || p_veh_id || ',' , ',' || o.veh_id || ',' ) > 0
                                     AND o.ver_id = p_srce_ver_id
                                     AND o.offr_id = oppp.offr_id
                                     AND oppp.offr_prfl_prcpt_id = osl.offr_prfl_prcpt_id
                                     AND osl.offr_sku_line_id = dms.offr_sku_line_id
                                     AND dms.offr_perd_id = dms.sls_perd_id
                                     AND osl.dltd_ind <> 'Y'
                                     and pf.prfl_cd = osl.prfl_cd
                                     and ms.mrkt_id = o.mrkt_id
                                     and ms.sku_id = osl.sku_id
                                     AND ms.sku_id = s.sku_id
                                     and s.prfl_cd = pf.prfl_cd
                                     and s.sku_id = osl.sku_id
                                     and mvpv.mrkt_id = o.mrkt_id
                                     and mvpv.veh_id = o.veh_id
                                     and mvpv.ver_id= o.ver_id
                                     and mvpv.offr_perd_id = o.offr_perd_id
                                     and mvpv.mps_sls_typ_id = dms.sls_typ_id
                                     AND sc.mrkt_id = o.mrkt_id
                                     AND sc.offr_perd_id = dms.sls_perd_id
                                     AND sc.sku_id = s.sku_id
                                     AND sc.cost_typ = osl.cost_typ
                                     AND SRP.OFFR_PERD_ID = OSL.OFFR_PERD_ID
                                     AND SRP.MRKT_ID      = OSL.MRKT_ID
                                     AND SRP.SKU_ID       = OSL.SKU_ID
                                     )
                        GROUP BY src_items, src_units, src_sales, grp_typ,
                                 src_offr_id,
                                 src_prfl_cd,
                                 src_sls_cls_cd,
                                 src_pg_ofs_nr,
                                 src_FEATRD_SIDE_CD,
                                 src_SLS_PRC_AMT ,
                                 src_NR_FOR_QTY   ,
                                 src_COMSN_TYP,
                                 src_REG_PRC_AMT,
                                 src_sku_id
                        )srce_col
                        JOIN
    --------------------------------------------------------------------------------
                       (SELECT dms.mrkt_id trgt_mrkt_id, dms.sls_perd_id trgt_sls_perd_id,
                               dms.offr_perd_id trgt_offr_perd_id ,
                               osl.offr_sku_line_link_id osl_link_id,
                               dms.offr_sku_line_id trgt_items, dms.sls_typ_id trgt_sls_typ_id,
                               oppp.sls_prc_amt trgt_selprc,
                               oppp.nr_for_qty trgt_nofor,
                               dms.net_to_avon_fct trgt_nta,
                               dms.unit_qty trgt_unit_qty,
                               3 unit_calc_ind,
                               frcst_boost_trgt_sls_amt fb_trgt_sls_amt,
                               O.OFFR_LINK_ID          trgt_offr_id,
                               OPPP.PRFL_CD           trgt_prfl_cd,
                               OPPP.SLS_CLS_CD        trgt_sls_cls_cd,
                               OPPP.PG_OFS_NR         trgt_pg_ofs_nr,
                               OPPP.FEATRD_SIDE_CD    trgt_FEATRD_SIDE_CD,
                               OPPP.SLS_PRC_AMT       trgt_SLS_PRC_AMT ,
                               OPPP.NR_FOR_QTY        trgt_NR_FOR_QTY   ,
                               OPPP.COMSN_TYP         trgt_COMSN_TYP,
                               SRP.REG_PRC_AMT        trgt_REG_PRC_AMT,
                               OSL.SKU_ID             trgt_sku_id,
                               pf.catgry_id,
                               f.form_grp_id
                          FROM offr o,
                               offr_prfl_prc_point oppp,
                               offr_sku_line osl,
                               dstrbtd_mrkt_sls dms,
                               mrkt_perd mp,
                               prfl pf,
                               sku s,
                               mrkt_sku ms,
                               sku_cost sc,
                               mrkt_veh_perd_ver mvpv,
                               SKU_REG_PRC SRP,
                               form f
                         WHERE o.mrkt_id = p_trgt_mrkt_id
                           AND o.offr_perd_id = p_trgt_offr_perd_id
                           AND INSTR(',' || p_veh_id || ',' , ',' || o.veh_id || ',' ) > 0
                           AND o.ver_id = p_trgt_ver_id
                           AND o.offr_id = oppp.offr_id
                           AND oppp.offr_prfl_prcpt_id = osl.offr_prfl_prcpt_id
                           AND osl.offr_sku_line_id = dms.offr_sku_line_id
                           AND mp.mrkt_id = o.mrkt_id
                           AND mp.perd_id = o.offr_perd_id
                           and pf.prfl_cd = osl.prfl_cd
                           and dms.sls_perd_id = dms.offr_perd_id
                           AND osl.dltd_ind <> 'Y'
                           AND NOT EXISTS
                                   (SELECT 1
                                      FROM frcst_boost_xclusn_mrkt_perd fx
                                     WHERE fx.mrkt_id = p_trgt_mrkt_id
                                       AND fx.trgt_offr_perd_id = p_trgt_offr_perd_id
                                       AND INSTR(',' || nvl(catgry_id_list, '-1') ||' ,', 
                                                 ',' || nvl(pf.catgry_id, -1) ||',') > 0
                                       AND INSTR(',' || nvl(sls_cls_cd_list, '-1') || ',', 
                                                 ',' || nvl(osl.sls_cls_cd, '-1') ||',') > 0
                                       AND INSTR(',' || nvl(sgmt_id_list, '-1') || ',', 
                                                 ',' || nvl(pf.sgmt_id, -1) ||',') > 0)
                           and oppp.sls_cls_cd = osl.sls_cls_cd
                           and ms.mrkt_id = o.mrkt_id
                           and ms.sku_id = osl.sku_id
                           AND ms.sku_id = s.sku_id
                           and s.prfl_cd = pf.prfl_cd
                           and s.sku_id = osl.sku_id
                           and mvpv.mrkt_id = o.mrkt_id
                           and mvpv.veh_id = o.veh_id
                           and mvpv.ver_id= o.ver_id
                           and mvpv.offr_perd_id = o.offr_perd_id
                           and mvpv.mps_sls_typ_id = dms.sls_typ_id
                           and ms.dltd_ind <> 'Y'
                           and s.dltd_ind <> 'Y'
                           and pf.dltd_ind <> 'Y'
                           AND sc.mrkt_id = o.mrkt_id
                           AND sc.offr_perd_id = dms.sls_perd_id
                           AND sc.sku_id = s.sku_id
                           AND sc.cost_typ = osl.cost_typ
                           AND SRP.OFFR_PERD_ID = OSL.OFFR_PERD_ID
                           AND SRP.MRKT_ID      = OSL.MRKT_ID
                           AND SRP.SKU_ID       = OSL.SKU_ID
                           AND f.form_id       = pf.form_id
                           ) trgt_col
                           ON (srce_col.src_items = trgt_col.osl_link_id)
                           LEFT JOIN frcst_boost_xtrpltn_pct xp
                           ON (xp.mrkt_id    = trgt_mrkt_id and
                               xp.catgry_id   = trgt_col.catgry_id and
                               xp.form_grp_id = trgt_col.form_grp_id and
                               trgt_offr_perd_id between xp.strt_perd_id and nvl(xp.end_perd_id, 99999999))
               );

           update dstrbtd_mrkt_sls
              set unit_qty = 0
            where offr_sku_line_id in (select osl.offr_sku_line_id
                                          from offr o,
                                               offr_prfl_prc_point oppp,
                                               offr_sku_line osl,
                                               dstrbtd_mrkt_sls dms,
                                               mrkt_veh_perd_ver mvpv
                                         where mvpv.mrkt_id = p_trgt_mrkt_id
                                           and mvpv.offr_perd_id = p_trgt_offr_perd_id
                                           and mvpv.ver_id = p_trgt_ver_id
                                           and o.mrkt_id = mvpv.mrkt_id
                                           and o.offr_perd_id = mvpv.offr_perd_id
                                           and o.veh_id = mvpv.veh_id
                                           and o.ver_id = mvpv.ver_id
                                           and o.offr_id = oppp.offr_id
                                           and oppp.offr_prfl_prcpt_id = osl.offr_prfl_prcpt_id
                                           and osl.offr_sku_line_id = dms.offr_sku_line_id
                                           and osl.mrkt_id = dms.mrkt_id
                                           and osl.offr_perd_id = dms.offr_perd_id
                                           and osl.veh_id = dms.veh_id
                                           and dms.sls_typ_id = mvpv.mps_sls_typ_id
                                           and osl.dltd_ind <> 'Y'
                                        );
            for i in nvl(v_trgt_osl_ids.first,1)..nvl(v_trgt_osl_ids.last,0)
            LOOP
                BEGIN
                   app_plsql_log.info(l_procedure_name||'OSL_ID: ' || v_trgt_osl_ids(i) || '   # Unit_qty: ' || v_trgt_calc_unit_qtys(i));
                   if v_xtrpltn_pcts(i) is not null then
                     app_plsql_log.info(l_procedure_name||'Extrapolation Parameter '||v_xtrpltn_pcts(i)||' applied');
                   end if;

                    update dstrbtd_mrkt_sls
                       set unit_qty         = round(decode(p_unit_calc_typ_id,
                                                                          1, v_trgt_unit_qtys(i),
                                                                          2, 0,
                                                                          3, v_trgt_calc_unit_qtys(i),
                                                                          4, v_trgt_unit_qtys(i),
                                                                          0))
                     where mrkt_id           = v_trgt_mrkt_ids(i)
                       and offr_perd_id      = v_trgt_offr_perd_ids(i)
                       and offr_perd_id      = sls_perd_id
                       and offr_sku_line_id  = v_trgt_osl_ids(i)
                       and sls_typ_id        = v_trgt_sls_typ_ids(i);
                EXCEPTION
                WHEN OTHERS THEN
                    APP_PLSQL_LOG.info(l_procedure_name||'Error while updating Units1' || sqlerrm(sqlcode));
                END;
            END LOOP;
            COMMIT;

    else  -- not forecast boost mode

      if p_unit_calc_typ_id = 2 then

           update dstrbtd_mrkt_sls
              set unit_qty = 0
            where offr_sku_line_id in(select osl.offr_sku_line_id
                                          from offr o,
                                               offr_prfl_prc_point oppp,
                                               offr_sku_line osl,
                                               dstrbtd_mrkt_sls dms,
                                               mrkt_veh_perd_ver mvpv
                                         where mvpv.mrkt_id = p_trgt_mrkt_id
                                           and mvpv.offr_perd_id = p_trgt_offr_perd_id
                                           and mvpv.ver_id = p_trgt_ver_id
                                           and mvpv.veh_id = p_trgt_veh_id
                                           and o.mrkt_id = mvpv.mrkt_id
                                           and o.offr_perd_id = mvpv.offr_perd_id
                                           and o.veh_id = mvpv.veh_id
                                           and o.ver_id = mvpv.ver_id
                                           and o.offr_id = oppp.offr_id
                                           and oppp.offr_prfl_prcpt_id = osl.offr_prfl_prcpt_id
                                           and osl.offr_sku_line_id = dms.offr_sku_line_id
                                           and dms.sls_typ_id = 1 --mvpv.mps_sls_typ_id
                                           and osl.dltd_ind <> 'Y'
                                        );
      else

          WITH srce AS
                     (SELECT o.mrkt_id srce_mrkt_id, o.offr_perd_id srce_offr_perd_id,
                             osl.offr_sku_line_link_id srce_osl_link_id,
                             dms.offr_sku_line_id srce_items, dms.unit_qty srce_unit_qty,
                             mp.ord_cnt srce_ord_cnt, osl.offr_sku_set_id srce_oss_id,
                             mvpc.catgry_id srce_catgry_id,
                             mvpc.trgt_units_per_ord_qty srce_upo_ord_cnt,
                             DECODE (NVL (esg.gift_ind, 'S'),
                                     'Y', 'G',
                                     'N', 'P',
                                     'S'
                                    ) gift_ind,
                             esg.est_link_id est_link_id, o.offr_id srce_offr_id,
                             ---------------------------
                             OPPP.OFFR_ID           src_offr_id,
                             OPPP.PRFL_CD           src_prfl_cd,
                             OPPP.SLS_CLS_CD        src_sls_cls_cd,
                             OPPP.PG_OFS_NR         src_pg_ofs_nr,
                             OPPP.FEATRD_SIDE_CD    src_FEATRD_SIDE_CD,
                             OPPP.SLS_PRC_AMT       src_SLS_PRC_AMT ,
                             OPPP.NR_FOR_QTY        src_NR_FOR_QTY   ,
                             OPPP.COMSN_TYP         src_COMSN_TYP,
                             SRP.REG_PRC_AMT        src_REG_PRC_AMT,
                             OSL.SKU_ID             src_sku_id
                             ---------------------------
                        FROM offr o,
                             dstrbtd_mrkt_sls dms,
                             offr_sku_line osl,
                             offr_prfl_prc_point oppp,
                             mrkt_veh_perd_ver mp,
                             mrkt_prfl mprfl,
                             prfl p,
                             mrkt_veh_perd_catgry mvpc,
                             est_lnkg esg,
                             SKU_REG_PRC SRP
                       WHERE o.mrkt_id = p_srce_mrkt_id
                         AND o.veh_id = p_srce_veh_id
                         AND o.offr_perd_id = p_srce_offr_perd_id
                         AND o.ver_id = p_srce_ver_id
                         AND o.offr_id = oppp.offr_id
                         AND oppp.offr_prfl_prcpt_id = osl.offr_prfl_prcpt_id
                         AND osl.offr_sku_line_id = dms.offr_sku_line_id
                         AND osl.dltd_ind <> 'Y'
                         AND mp.mrkt_id = o.mrkt_id
                         AND mp.offr_perd_id = o.offr_perd_id
                         AND mp.veh_id = o.veh_id
                         AND mp.ver_id = o.ver_id
                         AND mp.mps_sls_typ_id = dms.sls_typ_id
                         AND mprfl.mrkt_id = o.mrkt_id
                         AND mprfl.prfl_cd = oppp.prfl_cd
                         AND mprfl.prfl_cd = p.prfl_cd
                         AND mvpc.mrkt_id = oppp.mrkt_id
                         AND mvpc.veh_id = oppp.veh_id
                         AND mvpc.offr_perd_id = oppp.offr_perd_id
                         AND mvpc.catgry_id = p.catgry_id
                         AND dms.offr_perd_id = dms.sls_perd_id
                         AND esg.offr_sku_line_id(+) = osl.offr_sku_line_id
                         AND esg.offr_id(+) = osl.offr_id
                         AND SRP.OFFR_PERD_ID = OSL.OFFR_PERD_ID
                         AND SRP.MRKT_ID      = OSL.MRKT_ID
                         AND SRP.SKU_ID       = OSL.SKU_ID
                         ),
                     trgt AS
                     (SELECT o.mrkt_id trgt_mrkt_id, o.offr_perd_id trgt_offr_perd_id,
                             dms.sls_perd_id trgt_sls_perd_id, dms.sls_typ_id trgt_sls_typ_id,
                             osl.offr_sku_line_link_id osl_link_id,
                             dms.offr_sku_line_id trgt_items, mp.ord_cnt trgt_ord_cnt,
                             osl.offr_sku_set_id trgt_oss_id, mvpc.catgry_id trgt_catgry_id,
                             mvpc.trgt_units_per_ord_qty trgt_upo_ord_cnt,
                             dms.unit_qty trgt_unit_qty,-- odp.perd_ofs_nr, odp.dstrbtn_pct,
                             DECODE (NVL (esg.gift_ind, 'S'),
                                     'Y', 'G',
                                     'N', 'P',
                                     'S'
                                    ) gift_ind,
                             esg.est_link_id est_link_id, o.offr_id trgt_offr_id,
                             ----------------------------------------
                             O.OFFR_LINK_ID          trgt_offr_link_id,
                             OPPP.PRFL_CD           trgt_prfl_cd,
                             OPPP.SLS_CLS_CD        trgt_sls_cls_cd,
                             OPPP.PG_OFS_NR         trgt_pg_ofs_nr,
                             OPPP.FEATRD_SIDE_CD    trgt_FEATRD_SIDE_CD,
                             OPPP.SLS_PRC_AMT       trgt_SLS_PRC_AMT ,
                             OPPP.NR_FOR_QTY        trgt_NR_FOR_QTY   ,
                             OPPP.COMSN_TYP         trgt_COMSN_TYP,
                             SRP.REG_PRC_AMT        trgt_REG_PRC_AMT,
                             OSL.SKU_ID             trgt_sku_id
                             ----------------------------------------
                        FROM offr o,
                             dstrbtd_mrkt_sls dms,
                             offr_sku_line osl,
                             offr_prfl_prc_point oppp,
                             mrkt_veh_perd_ver mp,
                             mrkt_prfl mprfl,
                             prfl p,
                             mrkt_veh_perd_catgry mvpc,
                             est_lnkg esg,
                             SKU_REG_PRC SRP
                       WHERE o.mrkt_id = p_trgt_mrkt_id
                         AND o.veh_id = p_trgt_veh_id
                         AND o.offr_perd_id = p_trgt_offr_perd_id
                         AND o.ver_id = p_trgt_ver_id
                         AND o.offr_id = oppp.offr_id
                         AND oppp.offr_prfl_prcpt_id = osl.offr_prfl_prcpt_id
                         AND osl.offr_sku_line_id = dms.offr_sku_line_id
                         AND osl.dltd_ind <> 'Y'
                         AND mp.mrkt_id = o.mrkt_id
                         AND mp.offr_perd_id = o.offr_perd_id
                         AND mp.veh_id = o.veh_id
                         AND mp.ver_id = o.ver_id
                         AND mprfl.mrkt_id = o.mrkt_id
                         AND mprfl.prfl_cd = oppp.prfl_cd
                         AND mprfl.prfl_cd = p.prfl_cd
                         AND mvpc.mrkt_id = o.mrkt_id
                         AND mvpc.veh_id = o.veh_id
                         AND mvpc.offr_perd_id = o.offr_perd_id
                         AND mvpc.catgry_id = p.catgry_id
                         AND dms.offr_perd_id = dms.sls_perd_id
                         AND esg.offr_sku_line_id(+) = osl.offr_sku_line_id
                         AND esg.offr_id(+) = osl.offr_id
                         --------------------------------------
                         AND SRP.OFFR_PERD_ID = OSL.OFFR_PERD_ID
                         AND SRP.MRKT_ID      = OSL.MRKT_ID
                         AND SRP.SKU_ID           = OSL.SKU_ID
                         --------------------------------------
                         AND dms.sls_typ_id = 1
                         )
                SELECT trgt_mrkt_id, trgt_sls_perd_id, trgt_items, trgt_sls_typ_id,
                       trgt_unit_qty, p_unit_calc_typ_id,
                       (  DECODE (NVL (srce_ord_cnt, 0),
                                  0, 0, --trgt_ord_cnt, -- Added by Sivakumar. D.R on 13Jun2011
                                  (trgt_ord_cnt / srce_ord_cnt)
                                 )
                        * DECODE (NVL (max_srce_upo_ord_cnt, 0),
                                  0, 0, --max_trgt_upo_ord_cnt, -- Added by Sivakumar. D.R on 13Jun2011
                                  (max_trgt_upo_ord_cnt / max_srce_upo_ord_cnt
                                  )
                                 )
                        * srce_unit_qty
                       ) trgt_item_units,
                       trgt_offr_perd_id
                    bulk collect
                    into v_trgt_mrkt_ids, v_trgt_sls_perd_ids, v_trgt_osl_ids, v_trgt_sls_typ_ids,
                         v_trgt_unit_qtys, v_unit_calc_inds, v_trgt_calc_unit_qtys, v_trgt_offr_perd_ids
                FROM   (SELECT trgt_mrkt_id, trgt_sls_perd_id, trgt_items, trgt_sls_typ_id,
                               trgt_oss_id, trgt_catgry_id, srce_ord_cnt, trgt_ord_cnt,
                               srce_upo_ord_cnt,
                               MAX (srce_upo) OVER (PARTITION BY NVL
                                                                   (srce_oss_id,
                                                                    srce_items
                                                                   )) max_srce_upo_ord_cnt,
                               trgt_upo_ord_cnt,
                               MAX (trgt_upo) OVER (PARTITION BY NVL
                                                                   (trgt_oss_id,
                                                                    trgt_items
                                                                   )) max_trgt_upo_ord_cnt,
                               srce_unit_qty, trgt_unit_qty,
                               trgt_offr_perd_id
                          FROM (SELECT CASE
                                          WHEN srce_outr.gift_ind IN ('G', 'P')
                                             THEN (SELECT MAX (srce_upo_ord_cnt)
                                                     FROM srce innr
                                                    WHERE innr.srce_mrkt_id =
                                                              srce_outr.srce_mrkt_id
                                                      AND innr.gift_ind = 'P'
                                                      AND innr.srce_offr_id =
                                                                        srce_outr.srce_offr_id
                                                      AND innr.est_link_id =
                                                                         srce_outr.est_link_id)
                                          ELSE srce_upo_ord_cnt
                                       END srce_upo,
                                       srce_outr.*,
                                       CASE
                                          WHEN trgt_outr.gift_ind IN ('G', 'P')
                                             THEN (SELECT MAX (trgt_upo_ord_cnt)
                                                     FROM trgt innr
                                                    WHERE innr.trgt_mrkt_id =
                                                              trgt_outr.trgt_mrkt_id
                                                      AND innr.gift_ind = 'P'
                                                      AND innr.trgt_offr_id =
                                                                        trgt_outr.trgt_offr_id
                                                      AND innr.est_link_id =
                                                                         trgt_outr.est_link_id)
                                          ELSE trgt_upo_ord_cnt
                                       END trgt_upo,
                                       trgt_outr.*
                                  FROM srce srce_outr, trgt trgt_outr
                                 WHERE srce_outr.srce_items = trgt_outr.osl_link_id
                                 ));

            for i in nvl(v_trgt_osl_ids.first,1)..nvl(v_trgt_osl_ids.last,0)
            LOOP
               BEGIN
                update dstrbtd_mrkt_sls
                   set unit_qty         = round(decode(p_unit_calc_typ_id, 1, v_trgt_unit_qtys(i),
                                                                      2, 0,
                                                                      3, v_trgt_calc_unit_qtys(i),
                                                                      4, v_trgt_unit_qtys(i)))
                 where mrkt_id          = v_trgt_mrkt_ids(i)
                   and sls_perd_id      = v_trgt_sls_perd_ids(i)
                   and offr_sku_line_id = v_trgt_osl_ids(i)
                   and offr_perd_id      = sls_perd_id
                   and sls_typ_id       = 1;
                EXCEPTION
                WHEN OTHERS THEN
                    APP_PLSQL_LOG.info(l_procedure_name||'Error while updating Units3' || sqlerrm(sqlcode));
                END;
            END LOOP;
            COMMIT;

      end if;

    end if;

   EXCEPTION
   WHEN OTHERS THEN
        --p_status := 1;
        APP_PLSQL_LOG.INFO(l_procedure_name||'Error while updating Units.. UPDATE_TRGT_UNITS '|| SQLERRM(SQLCODE));
   END;


function get_cost_amt
(p_mrkt_id         dstrbtd_mrkt_sls.mrkt_id%type,
 p_sls_perd_id    dstrbtd_mrkt_sls.offr_perd_id%type,
 p_sku_id          offr_sku_line.sku_id%type,
 p_cost_typ        offr_sku_line.cost_typ%type
)
return number
as
 ln_cost_amt number;
begin
  select sc.wghtd_avg_cost_amt
    into ln_cost_amt
    from sku_cost sc
   where mrkt_id   = p_mrkt_id
     and offr_perd_id = p_sls_perd_id
     and sku_id = p_sku_id
     and cost_typ = p_cost_typ;
     return  ln_cost_amt;

exception
when others then
    return(0);
end;



---------------------------------------------------------------------------------------------------------------------------------------------
--*******************************************************************************************************************************************
---------------------------------------------------------------------------------------------------------------------------------------------
   PROCEDURE create_version(p_srce_mrkt_id          IN     offr.mrkt_id%type,
                               p_srce_veh_id        IN     offr.veh_id%type,
                               p_srce_offr_perd_id  IN     offr.offr_perd_id%type,
                               p_srce_ver_id        IN     offr.ver_id%type,
                               p_trgt_mrkt_id       IN     offr.mrkt_id%type,
                               p_trgt_veh_id        IN     offr.veh_id%type,
                               p_trgt_offr_perd_id  IN     offr.offr_perd_id%type,
                               p_trgt_ver_id        IN     offr.ver_id%type,
                               p_parm_id            IN     mps_parm.mps_parm_id%type,
                               p_user_nm            IN     varchar2,
                               p_task_id            IN     number,
                               p_status             IN OUT varchar2
                              )
   AS
      ls_offr_key                   VARCHAR (1000);
      ls_offr_trgt_key              VARCHAR (1000);
      ls_opscp_key                  VARCHAR2 (32000);
      ls_oppp_key                   VARCHAR (32000);
      ls_oscs_key                   VARCHAR2 (32000);
      ls_oss_key                    VARCHAR2 (32000);
      ls_osl_key                    VARCHAR2 (32000);
      ls_dms_key                    VARCHAR2 (255);
      ls_el_key                     VARCHAR2 (255);

      ln_offr_id                  NUMBER;
      ln_oppp_id                  NUMBER;
      ln_oss_id                   NUMBER;
      ln_osl_id                   NUMBER;

      ls_trgt_deleted_key           VARCHAR2(250);
      ls_err_key                    VARCHAR2 (4000);


      ls_stage                      VARCHAR2 (2000);
      v_start                       NUMBER;
      v_end                         NUMBER;
      l_job_typ                     NUMBER;
      cnt                           NUMBER; -- COUNT FOR KEY VALUE COLLECTION
      ls_trgt_deleted               VARCHAR2(1) := 'N';
      l_continue_loop               CHAR(1) :='Y';
      l_partial_success             NUMBER := 0;

      t_tbl_trgt_deleted_keys       TBL_KEY_VALUE;
      t_tbl_err_keys                TBL_KEY_VALUE;

      ln_src_mrkt_id                MRKT.MRKT_ID%TYPE;
      ln_src_offr_perd_id           NUMBER;
      ln_trgt_mrkt_id               MRKT.MRKT_ID%TYPE;
      l_trgt_veh_avlbl              CHAR (1);
      l_process                     NUMBER;

      l_mps_sls_typ_id              number;
      l_ver_typ_id                number;
      ls_dms_surr_key             varchar2(255);

      type tbl_numbers  is table of NUMBER INDEX BY PLS_INTEGER;
      type tbl_varchars is table of varchar2(250) INDEX BY PLS_INTEGER;


      t_tbl_offr_keys             tbl_key_value;
      t_tbl_offr_trgt_keys        tbl_key_value;
      t_tbl_opscp_keys            tbl_key_value;
      t_tbl_oppp_keys             tbl_key_value;
      t_tbl_oscs_keys             tbl_key_value;
      t_tbl_oss_keys              tbl_key_value;
      t_tbl_osl_keys              tbl_key_value;
      t_tbl_el_keys               tbl_key_value;
      t_tbl_dms_keys              tbl_key_value;
      t_tbl_dms_surr_keys        tbl_key_value;
      t_tbl_osl_sur_keys          tbl_key_value;

      ls_crncy_cd                 VARCHAR2 (3);
      ln_est_links number;

      ln_trgt_oss_id              number;
      ls_cost_typ                 varchar2(1);

      ln_cost_amt                 number;
      ln_sls_typ_id               number;
      ln_wip_sls_typ_id           number;
      ln_max_sls_typ_id           number;
      ln_rep_cnt                  number;
      ln_ord_cnt                  number;
      ln_stencil_cnt              number;
      ln_actvty_pct               number;
      ln_cust_cnt                 number;
      ln_appt_cnt                 number;

   BEGIN

             app_plsql_log.info('Entered into Create Version Block');


             FOR rec IN
                (
    ---------------------------------------------------------------------------------------------------------------------------------
                  SELECT  ROW_NUMBER( ) OVER (PARTITION BY o.offr_id
                                                      ORDER BY o.offr_id,
                                                               otrg.trgt_id,
                                                               ood.oppp_prfl_cd,
                                                               ood.offr_prfl_prcpt_id,
                                                               ood.offr_sku_line_id
                                                     ) SRCE_OFFR_RN,
                          COUNT(o.offr_id) OVER (PARTITION BY o.offr_id) TOTAL_OFFR_IDS,
                          DECODE (NVL (offr_target.offr_id, -1),
                                  -1, 'OFFR_NEW',
                                  'OFFR_EXISTS'
                                 ) offr_rec_typ,
                          DECODE
                               (NVL (opp_target.offr_prfl_prcpt_id, -1),
                                -1, 'OPPP_NEW',
                                'OPPP_EXISTS'
                               ) oppp_rec_typ,
                          DECODE (NVL (osl_target.offr_sku_line_id, -1),
                                  -1, 'OSL_NEW',
                                  'OSL_EXISTS'
                                 ) osl_rec_typ,
                          --------------------------------------------
                          -- OFFR Records -- Create Version
                          --------------------------------------------
                          o.offr_id                         srce_offr_id,
                          o.mrkt_id                         srce_mrkt_id,
                          o.veh_id                          srce_veh_id,
                          o.offr_perd_id                    srce_offr_perd_id,
                          o.ver_id                          srce_ver_id,
                          ----------------------------------
                          offr_target.offr_id               trgt_offr_id,
                          p_trgt_mrkt_id             trgt_mrkt_id,
                          p_trgt_veh_id              trgt_veh_id,
                          p_trgt_offr_perd_id        trgt_offr_perd_id,
                          p_trgt_ver_id              trgt_ver_id,
                          ----------------------------------
                          o.offr_desc_txt,
                          o.offr_stus_cd,
                          o.offr_stus_rsn_desc_txt,
                          o.brchr_plcmt_id,
                          o.pg_wght_pct,
                          o.shpng_perd_id,
                          o.bilng_perd_id,
                          o.micr_ncpsltn_desc_txt,
                          o.micr_ncpsltn_ind,
                          o.mrkt_veh_perd_sctn_id,
                          o.sctn_page_ofs_nr,
                          o.featrd_side_cd,
                          o.enrgy_chrt_postn_id,
                          o.enrgy_chrt_offr_desc_txt,
                          o.offr_ntes_txt,
                          o.rpt_sbtl_typ_id,
                          o.unit_rptg_lvl_id,
                          o.offr_lyot_cmnts_txt,
                          o.est_srce_id,
                          o.est_stus_cd,
                          o.offr_typ,
                          o.std_offr_id,
                          o.offr_link_ind,
                          o.prfl_offr_strgth_pct,
                          o.prfl_cnt, o.sku_cnt,
                          o.ssnl_evnt_id,
                          o.brchr_postn_id,
                          o.frnt_cvr_ind,
                          o.pg_typ_id,
                          o.offr_prsntn_strnth_id,
                          o.flap_pg_wght_pct,
                          o.offr_cls_id,
                          o.offr_link_id,
                          o.cust_pull_id, -- DRS 22Aug2011
                          o.flap_ind,     -- DRS 22Aug2011
                          ----------------------------------
                          offr_target.mrkt_veh_perd_sctn_id trgt_mrkt_veh_perd_sctn_id,
                          offr_target.brchr_plcmt_id        trgt_brchr_plcmt_id,
                          offr_target.sctn_page_ofs_nr      trgt_sctn_page_ofs_nr,
                          offr_target.crncy_cd              trgt_crncy_cd,
                          --------------------------------------------
                          -- OFFR_TRGT Records -- Create Version
                          --------------------------------------------
                          otrg.trgt_id                      otrg_trgt_id,
                          otrg.catgry_id                    otrg_catgry_id,
                          otrg.brnd_fmly_id                 otrg_brnd_fmly_id,
                          otrg.brnd_id                      otrg_brnd_id,
                          otrg.form_id                      otrg_form_id,
                          otrg.crncy_cd                     otrg_crncy_cd,
                          otrg.sgmt_id                      otrg_sgmt_id,
                          otrg.suplr_id                     otrg_suplr_id,
                          otrg.trgt_unit_qty                otrg_trgt_unit_qty,
                          otrg.trgt_sls_amt                 otrg_trgt_sls_amt,
                          otrg.trgt_cost_amt                otrg_trgt_cost_amt,
                          otrg.prft_amt                     otrg_prft_amt,
                          otrg.prft_pct                     otrg_prft_pct,
                          otrg.net_per_unit_amt             otrg_net_per_unit_amt,
                          otrg.units_per_rep_qty            otrg_units_per_rep_qty,
                          otrg.units_per_ord_qty            otrg_units_per_ord_qty,
                          otrg.net_per_rep_amt              otrg_net_per_rep_amt,
                          --------------------------------------------
                          -- OFFR_PRFL_SLS_CLS_CD Records -- Create Version
                          --------------------------------------------
                          ood.opscp_sls_cls_cd,
                          ood.mpp_prfl_cd,
                          ood.opscp_prfl_cd,
                          ood.opscp_pg_ofs_nr,
                          ood.opscp_featrd_side_cd,
                          ood.opscp_prod_endrsmt_id,
                          ood.opscp_pg_wght_pct,
                          ood.opscp_fxd_pg_wght_ind,
                          ood.opscp_featrd_prfl_ind,
                          ood.opscp_use_instrctns_ind,
                          ood.opscp_sku_cnt,
                          ood.opscp_sku_offr_strgth_pct, -- DRS 22Aug2011
                          --------------------------------------------
                          -- OFFR_PRFL_PRC_POINT Records -- Create Version
                          --------------------------------------------
                          ood.offr_prfl_prcpt_id            srce_oppp_id,
                          opp_target.offr_prfl_prcpt_id     trgt_oppp_id,
                          ood.oppp_offr_id,
                          ood.oppp_veh_id,
                          ood.oppp_mrkt_id,
                          ood.oppp_sls_cls_cd,
                          ood.oppp_prfl_cd,
                          ood.oppp_offr_perd_id,
                          ood.oppp_pg_ofs_nr,
                          ood.oppp_featrd_side_cd,
                          ood.oppp_crncy_cd,
                          ood.oppp_impct_catgry_id,
                          ood.oppp_cnsmr_invstmt_bdgt_id,
                          ood.oppp_promtn_id,
                          ood.oppp_promtn_clm_id,
                          ----------------------------------
                          ood.oppp_sls_prc_amt,
                          ----------------------------------
                          ood.oppp_net_to_avon_fct,
                          ood.oppp_ssnl_evnt_id,
                          ood.oppp_sls_stus_cd,
                          ood.oppp_sku_cnt,
                          ood.oppp_sku_offr_strgth_pct,
                          ood.oppp_est_unit_qty,
                          ood.oppp_est_sls_amt,
                          ood.oppp_est_cost_amt,
                          ood.oppp_prfl_stus_rsn_desc_txt,
                          ood.oppp_prfl_stus_cd,
                          ood.oppp_tax_amt,
                          ood.oppp_comsn_amt,
                          ood.oppp_sls_srce_id,
                          ood.oppp_pymt_typ,
                          ood.oppp_prc_point_desc_txt,
                          ood.oppp_prmry_offr_ind,
                          ood.oppp_sls_promtn_ind,
                          ood.oppp_impct_prfl_cd,
                          ood.oppp_awrd_sls_prc_amt,
                          ood.oppp_nr_for_qty,
                          ood.oppp_chrty_amt,
                          ood.oppp_chrty_ovrrd_ind,
                          ood.oppp_roylt_pct,
                          ood.oppp_roylt_ovrrd_ind,
                          ood.oppp_tax_type_id,
                          ood.oppp_comsn_typ,
                          ood.oppp_offr_prfl_prcpt_link_id,
                          ood.oppp_unit_calc_ind,
                          ood.oppp_demo_discnt_id,
                          ood.oppp_frc_mtch_mthd_id,
                          ood.oppp_prc_lvl_typ_id,
                          ood.oppp_prc_lvl_typ_cd,
                          ood.oppp_min_prc_amt,
                          ood.oppp_opt_prc_amt,
                          ood.oppp_max_prc_amt,
                          ood.oppp_plnd_prc_lvl_typ_id,
                          ood.oppp_plnd_prc_lvl_typ_cd,
                          ood.oppp_plnd_min_prc_amt,
                          ood.oppp_plnd_opt_prc_amt,
                          ood.oppp_plnd_max_prc_amt,
                          ood.oppp_plnd_promtn_id,
                          ood.oppp_key_prfl_ind,
                          ood.oppp_grail_min_prc_amt,
                          ood.oppp_grail_max_prc_amt,
                          --------------------------------------------
                          -- OFFR_SLS_CLS_SKU Records
                          --------------------------------------------
                          ood.oscs_sls_cls_cd,
                          ood.oscs_prfl_cd,
                          ood.oscs_pg_ofs_nr,
                          ood.oscs_featrd_side_cd,
                          ood.oscs_sku_id,
                          ood.oscs_mrkt_id,
                          ood.oscs_hero_ind,
                          ood.oscs_micr_ncpsltn_ind,
                          ood.oscs_smplg_ind,
                          ood.oscs_mltpl_ind,
                          ood.oscs_cmltv_ind,
                          ood.oscs_incntv_id,
                          ood.oscs_reg_prc_amt,
                          ood.oscs_cost_amt,
                          ood.oscs_wsl_ind,
                          --------------------------------------------
                          -- OFFR_SKU_LINE Records -- Create Version
                          --------------------------------------------
                          ood.offr_sku_line_id              srce_osl_id,
                          osl_target.offr_sku_line_id       trgt_osl_id,
                          ood.osl_offr_id,
                          ood.osl_veh_id,
                          ood.osl_featrd_side_cd,
                          ood.osl_offr_perd_id,
                          ood.osl_mrkt_id,
                          ood.osl_sku_id,
                          ood.osl_pg_ofs_nr,
                          ood.osl_crncy_cd,
                          ood.osl_prfl_cd,
                          ood.osl_sls_cls_cd,
                          ood.osl_offr_prfl_prcpt_id,
                          ood.osl_promtn_desc_txt,
                          ood.osl_sls_prc_amt,
                          ood.osl_cost_typ,
                          ood.osl_prmry_sku_offr_ind,
                          ood.osl_dltd_ind,
                          ood.osl_line_nr,
                          ood.osl_unit_splt_pct,
                          ood.osl_demo_avlbl_ind,
                          ood.osl_offr_sku_line_link_id,
                          ood.osl_set_cmpnt_ind,
                          ood.osl_set_cmpnt_qty,
                          ood.osl_offr_sku_set_id,
                          ood.osl_unit_prc_amt,
                          ood.osl_line_nr_typ_id,
                          --------------------------------------------
                          -- OFFR_SKU_SET Records-- Create Version
                          --------------------------------------------
                          ood.oss_offr_sku_set_id,
                          ood.oss_mrkt_id,
                          ood.oss_veh_id,
                          ood.oss_offr_perd_id,
                          ood.oss_offr_id,
                          ood.oss_offr_sku_set_nm,
                          ood.oss_set_prc_amt,
                          ood.oss_set_prc_typ_id,
                          ood.oss_pg_ofs_nr,
                          ood.oss_featrd_side_cd,
                          ood.oss_set_cmpnt_cnt,
                          ood.oss_crncy_cd,
                          ood.oss_promtn_desc_txt,
                          ood.oss_line_nr,
                          ood.oss_line_nr_typ_id,
                          ood.oss_offr_sku_set_sku_id,
                          ood.oss_fsc_cd,
                          --------------------------------------------
                          -- EST_LNKG Records -- Create Version
                          --------------------------------------------
                          ood.el_est_link_id,
                          ood.el_offr_id,
                          ood.el_offr_sku_line_id,
                          ood.el_offr_gift_sku_line_id,
                          ood.el_gift_ind,
                          ood.el_unit_splt_pct,
                          ood.el_offr_sku_id,
                          ood.el_gift_sku_id,
                          --------------------------------------------
                          -- DSTRBTD_MRKT_SLS Records -- Create Version
                          --------------------------------------------
                          ood.dms_mrkt_id,
                          ood.dms_offr_perd_id,
                          ood.dms_veh_id,
                          ood.dms_sls_perd_id,
                          ood.dms_offr_sku_line_id,
                          ood.dms_sls_typ_id,
                          ood.dms_unit_qty,
                          ----------------------------------
                          osl_target.unit_qty               trgt_dms_unit_qty,
                          ----------------------------------
                          ood.dms_sls_stus_cd,
                          ood.dms_sls_srce_id,
                          ood.dms_prev_unit_qty,
                          ood.dms_prev_sls_srce_id,
                          ood.dms_comsn_amt,
                          ood.dms_tax_amt,
                          ood.dms_net_to_avon_fct,
                          ood.dms_unit_ovrrd_ind,
                          ood.dms_currnt_est_ind,
                          ood.dms_sls_perd_ofs_nr,
                          --------------------------------------------
                          -- Other Values  -- Create Version
                          --------------------------------------------
                          ood.srce_reg_prc_amt,
                          ood.trgt_reg_prc_amt
                     FROM offr o,
                          (SELECT dstrbtd_mrkt_sls.offr_perd_id,
                                  dstrbtd_mrkt_sls.unit_qty,
                                  offr_sku_set.offr_sku_set_id,
                                  -----------------------------------------
                                  offr_prfl_prc_point.offr_id,
                                  offr_prfl_prc_point.offr_prfl_prcpt_id,
                                  offr_prfl_prc_point.offr_id               oppp_offr_id,
                                  offr_prfl_prc_point.veh_id                oppp_veh_id,
                                  offr_prfl_prc_point.mrkt_id               oppp_mrkt_id,
                                  offr_prfl_prc_point.sls_cls_cd            oppp_sls_cls_cd,
                                  offr_prfl_prc_point.prfl_cd               oppp_prfl_cd,
                                  offr_prfl_prc_point.offr_perd_id          oppp_offr_perd_id,
                                  offr_prfl_prc_point.pg_ofs_nr             oppp_pg_ofs_nr,
                                  offr_prfl_prc_point.featrd_side_cd        oppp_featrd_side_cd,
                                  offr_prfl_prc_point.crncy_cd              oppp_crncy_cd,
                                  offr_prfl_prc_point.impct_catgry_id       oppp_impct_catgry_id,
                                  offr_prfl_prc_point.cnsmr_invstmt_bdgt_id oppp_cnsmr_invstmt_bdgt_id,
                                  offr_prfl_prc_point.promtn_id             oppp_promtn_id,
                                  offr_prfl_prc_point.promtn_clm_id         oppp_promtn_clm_id,
                                  offr_prfl_prc_point.sls_prc_amt           oppp_sls_prc_amt,
                                  offr_prfl_prc_point.net_to_avon_fct       oppp_net_to_avon_fct,
                                  offr_prfl_prc_point.ssnl_evnt_id          oppp_ssnl_evnt_id,
                                  offr_prfl_prc_point.sls_stus_cd           oppp_sls_stus_cd,
                                  offr_prfl_prc_point.sku_cnt               oppp_sku_cnt,
                                  offr_prfl_prc_point.sku_offr_strgth_pct   oppp_sku_offr_strgth_pct,
                                  offr_prfl_prc_point.est_unit_qty          oppp_est_unit_qty,
                                  offr_prfl_prc_point.est_sls_amt           oppp_est_sls_amt,
                                  offr_prfl_prc_point.est_cost_amt          oppp_est_cost_amt,
                                  offr_prfl_prc_point.prfl_stus_rsn_desc_txt oppp_prfl_stus_rsn_desc_txt,
                                  offr_prfl_prc_point.prfl_stus_cd          oppp_prfl_stus_cd,
                                  offr_prfl_prc_point.tax_amt               oppp_tax_amt,
                                  offr_prfl_prc_point.comsn_amt             oppp_comsn_amt,
                                  offr_prfl_prc_point.pymt_typ              oppp_pymt_typ,
                                  offr_prfl_prc_point.prc_point_desc_txt    oppp_prc_point_desc_txt,
                                  offr_prfl_prc_point.prmry_offr_ind        oppp_prmry_offr_ind,
                                  offr_prfl_prc_point.sls_promtn_ind        oppp_sls_promtn_ind,
                                  offr_prfl_prc_point.impct_prfl_cd         oppp_impct_prfl_cd,
                                  offr_prfl_prc_point.awrd_sls_prc_amt      oppp_awrd_sls_prc_amt,
                                  offr_prfl_prc_point.nr_for_qty            oppp_nr_for_qty,
                                  offr_prfl_prc_point.chrty_amt             oppp_chrty_amt,
                                  offr_prfl_prc_point.chrty_ovrrd_ind       oppp_chrty_ovrrd_ind,
                                  offr_prfl_prc_point.roylt_pct             oppp_roylt_pct,
                                  offr_prfl_prc_point.roylt_ovrrd_ind       oppp_roylt_ovrrd_ind,
                                  offr_prfl_prc_point.tax_type_id           oppp_tax_type_id,
                                  offr_prfl_prc_point.comsn_typ             oppp_comsn_typ,
                                  offr_prfl_prc_point.offr_prfl_prcpt_link_id oppp_offr_prfl_prcpt_link_id,
                                  offr_prfl_prc_point.sls_srce_id           oppp_sls_srce_id,
                                  offr_prfl_prc_point.unit_calc_ind         oppp_unit_calc_ind,
                                  offr_prfl_prc_point.demo_discnt_id        oppp_demo_discnt_id,
                                  offr_prfl_prc_point.frc_mtch_mthd_id      oppp_frc_mtch_mthd_id,
                                  offr_prfl_prc_point.prc_lvl_typ_id        oppp_prc_lvl_typ_id,
                                  offr_prfl_prc_point.prc_lvl_typ_cd        oppp_prc_lvl_typ_cd,
                                  offr_prfl_prc_point.min_prc_amt           oppp_min_prc_amt,
                                  offr_prfl_prc_point.opt_prc_amt           oppp_opt_prc_amt,
                                  offr_prfl_prc_point.max_prc_amt           oppp_max_prc_amt,
                                  offr_prfl_prc_point.plnd_prc_lvl_typ_id   oppp_plnd_prc_lvl_typ_id,
                                  offr_prfl_prc_point.plnd_prc_lvl_typ_cd   oppp_plnd_prc_lvl_typ_cd,
                                  offr_prfl_prc_point.plnd_min_prc_amt      oppp_plnd_min_prc_amt,
                                  offr_prfl_prc_point.plnd_opt_prc_amt      oppp_plnd_opt_prc_amt,
                                  offr_prfl_prc_point.plnd_max_prc_amt      oppp_plnd_max_prc_amt,
                                  offr_prfl_prc_point.plnd_promtn_id        oppp_plnd_promtn_id,
                                  offr_prfl_prc_point.key_prfl_ind          oppp_key_prfl_ind,
                                  offr_prfl_prc_point.grail_min_prc_amt     oppp_grail_min_prc_amt,
                                  offr_prfl_prc_point.grail_max_prc_amt     oppp_grail_max_prc_amt,
                                  ------------------------------------
                                  offr_prfl_sls_cls_plcmt.sls_cls_cd        opscp_sls_cls_cd,
                                  mrkt_perd_prfl.prfl_cd                    mpp_prfl_cd,
                                  offr_prfl_sls_cls_plcmt.prfl_cd           opscp_prfl_cd,
                                  offr_prfl_sls_cls_plcmt.pg_ofs_nr         opscp_pg_ofs_nr,
                                  offr_prfl_sls_cls_plcmt.featrd_side_cd    opscp_featrd_side_cd,
                                  offr_prfl_sls_cls_plcmt.prod_endrsmt_id   opscp_prod_endrsmt_id,
                                  offr_prfl_sls_cls_plcmt.pg_wght_pct       opscp_pg_wght_pct,
                                  offr_prfl_sls_cls_plcmt.fxd_pg_wght_ind   opscp_fxd_pg_wght_ind,
                                  offr_prfl_sls_cls_plcmt.featrd_prfl_ind   opscp_featrd_prfl_ind,
                                  offr_prfl_sls_cls_plcmt.use_instrctns_ind opscp_use_instrctns_ind,
                                  offr_prfl_sls_cls_plcmt.sku_cnt           opscp_sku_cnt,
                                  offr_prfl_sls_cls_plcmt.sku_offr_strgth_pct opscp_sku_offr_strgth_pct,
                                  ------------------------------------
                                  offr_sls_cls_sku.sls_cls_cd               oscs_sls_cls_cd,
                                  offr_sls_cls_sku.prfl_cd                  oscs_prfl_cd,
                                  offr_sls_cls_sku.pg_ofs_nr                oscs_pg_ofs_nr,
                                  offr_sls_cls_sku.featrd_side_cd           oscs_featrd_side_cd,
                                  offr_sls_cls_sku.sku_id                   oscs_sku_id,
                                  offr_sls_cls_sku.mrkt_id                  oscs_mrkt_id,
                                  offr_sls_cls_sku.hero_ind                 oscs_hero_ind,
                                  offr_sls_cls_sku.micr_ncpsltn_ind         oscs_micr_ncpsltn_ind,
                                  offr_sls_cls_sku.smplg_ind                oscs_smplg_ind,
                                  offr_sls_cls_sku.mltpl_ind                oscs_mltpl_ind,
                                  offr_sls_cls_sku.cmltv_ind                oscs_cmltv_ind,
                                  offr_sls_cls_sku.incntv_id                oscs_incntv_id,
                                  offr_sls_cls_sku.reg_prc_amt              oscs_reg_prc_amt,
                                  offr_sls_cls_sku.cost_amt                 oscs_cost_amt,
                                  -- copy previous wsl_ind for non-Wip offers, for WiP triggers will derive correct value
                                  decode(p_trgt_ver_id,
                                         0, null,
                                         offr_sls_cls_sku.wsl_ind)          oscs_wsl_ind,
                                  ------------------------------------
                                  offr_sku_line.offr_sku_line_id,
                                  offr_sku_line.offr_id                     osl_offr_id,
                                  offr_sku_line.veh_id                      osl_veh_id,
                                  offr_sku_line.featrd_side_cd              osl_featrd_side_cd,
                                  offr_sku_line.offr_perd_id                osl_offr_perd_id,
                                  offr_sku_line.mrkt_id                     osl_mrkt_id,
                                  offr_sku_line.sku_id                      osl_sku_id,
                                  offr_sku_line.pg_ofs_nr                   osl_pg_ofs_nr,
                                  offr_sku_line.crncy_cd                    osl_crncy_cd,
                                  offr_sku_line.prfl_cd                     osl_prfl_cd,
                                  offr_sku_line.sls_cls_cd                  osl_sls_cls_cd,
                                  offr_sku_line.offr_prfl_prcpt_id          osl_offr_prfl_prcpt_id,
                                  offr_sku_line.promtn_desc_txt             osl_promtn_desc_txt,
                                  offr_sku_line.sls_prc_amt                 osl_sls_prc_amt,
                                  offr_sku_line.cost_typ                    osl_cost_typ,
                                  offr_sku_line.prmry_sku_offr_ind          osl_prmry_sku_offr_ind,
                                  offr_sku_line.dltd_ind                    osl_dltd_ind,
                                  offr_sku_line.line_nr                     osl_line_nr,
                                  offr_sku_line.unit_splt_pct               osl_unit_splt_pct,
                                  offr_sku_line.demo_avlbl_ind              osl_demo_avlbl_ind,
                                  offr_sku_line.offr_sku_line_link_id       osl_offr_sku_line_link_id,
                                  offr_sku_line.set_cmpnt_ind               osl_set_cmpnt_ind,
                                  offr_sku_line.set_cmpnt_qty               osl_set_cmpnt_qty,
                                  offr_sku_line.offr_sku_set_id             osl_offr_sku_set_id,
                                  offr_sku_line.unit_prc_amt                osl_unit_prc_amt,
                                  offr_sku_line.line_nr_typ_id              osl_line_nr_typ_id,
                                  ------------------------------------
                                  offr_sku_set.offr_sku_set_id              oss_offr_sku_set_id,
                                  offr_sku_set.mrkt_id                      oss_mrkt_id,
                                  offr_sku_set.veh_id                       oss_veh_id,
                                  offr_sku_set.offr_perd_id                 oss_offr_perd_id,
                                  offr_sku_set.offr_id                      oss_offr_id,
                                  offr_sku_set.offr_sku_set_nm              oss_offr_sku_set_nm,
                                  offr_sku_set.set_prc_amt                  oss_set_prc_amt,
                                  offr_sku_set.set_prc_typ_id               oss_set_prc_typ_id,
                                  offr_sku_set.pg_ofs_nr                    oss_pg_ofs_nr,
                                  offr_sku_set.featrd_side_cd               oss_featrd_side_cd,
                                  offr_sku_set.set_cmpnt_cnt                oss_set_cmpnt_cnt,
                                  offr_sku_set.crncy_cd                     oss_crncy_cd,
                                  offr_sku_set.promtn_desc_txt              oss_promtn_desc_txt,
                                  offr_sku_set.offr_sku_set_sku_id          oss_offr_sku_set_sku_id,
                                  offr_sku_set.line_nr                      oss_line_nr,
                                  offr_sku_set.line_nr_typ_id               oss_line_nr_typ_id,
                                  offr_sku_set.fsc_cd                       oss_fsc_cd,
                                  ------------------------------------
                                  est_lnkg.est_link_id                      el_est_link_id,
                                  est_lnkg.offr_id                          el_offr_id,
                                  est_lnkg.offr_sku_line_id                 el_offr_sku_line_id,
                                  est_lnkg.offr_gift_sku_line_id            el_offr_gift_sku_line_id,
                                  est_lnkg.gift_ind                         el_gift_ind,
                                  est_lnkg.unit_splt_pct                    el_unit_splt_pct,
                                  est_lnkg.offr_sku_id                      el_offr_sku_id,
                                  est_lnkg.gift_sku_id                      el_gift_sku_id,
                                  ------------------------------------
                                  dstrbtd_mrkt_sls.mrkt_id                  dms_mrkt_id,
                                  dstrbtd_mrkt_sls.offr_perd_id             dms_offr_perd_id,
                                  dstrbtd_mrkt_sls.veh_id                   dms_veh_id,
                                  dstrbtd_mrkt_sls.sls_perd_id              dms_sls_perd_id,
                                  dstrbtd_mrkt_sls.offr_sku_line_id         dms_offr_sku_line_id,
                                  dstrbtd_mrkt_sls.sls_typ_id               dms_sls_typ_id,
                                  dstrbtd_mrkt_sls.unit_qty                 dms_unit_qty,
                                  dstrbtd_mrkt_sls.sls_stus_cd              dms_sls_stus_cd,
                                  dstrbtd_mrkt_sls.sls_srce_id              dms_sls_srce_id,
                                  dstrbtd_mrkt_sls.prev_unit_qty            dms_prev_unit_qty,
                                  dstrbtd_mrkt_sls.prev_sls_srce_id         dms_prev_sls_srce_id,
                                  dstrbtd_mrkt_sls.comsn_amt                dms_comsn_amt,
                                  dstrbtd_mrkt_sls.tax_amt                  dms_tax_amt,
                                  dstrbtd_mrkt_sls.net_to_avon_fct          dms_net_to_avon_fct,
                                  dstrbtd_mrkt_sls.unit_ovrrd_ind           dms_unit_ovrrd_ind,
                                  dstrbtd_mrkt_sls.currnt_est_ind           dms_currnt_est_ind,
                                  dstrbtd_mrkt_sls.sls_perd_ofs_nr          dms_sls_perd_ofs_nr,
                                  --------------------------------------------
                                  srce_reg_prc.reg_prc_amt                  srce_reg_prc_amt,
                                  trgt_reg_prc.reg_prc_amt                  trgt_reg_prc_amt
                             FROM mrkt,
                                  mrkt_perd,
                                  mrkt_veh_perd,
                                  --mrkt_veh_perd_ver,
                                  offr,
                                  offr_prfl_prc_point,
                                  offr_sku_line,
                                  offr_prfl_sls_cls_plcmt,
                                  offr_sls_cls_sku,
                                  dstrbtd_mrkt_sls,
                                  mrkt_sku,
                                  mrkt_perd_prfl,
                                  (select * from sku_reg_prc
                                    where  mrkt_id        = p_srce_mrkt_id
                                      and offr_perd_id    = p_srce_offr_perd_id
                                   ) srce_reg_prc,
                                  (select * from sku_reg_prc
                                    where  mrkt_id        = p_trgt_mrkt_id
                                      and offr_perd_id    = p_trgt_offr_perd_id
                                  ) trgt_reg_prc,
                                  prfl,
                                  offr_sku_set,
                                  est_lnkg
                            WHERE mrkt.mrkt_id                              = p_srce_mrkt_id
                              AND mrkt_perd.mrkt_id                         = mrkt.mrkt_id
                              AND mrkt_perd.perd_id                         = p_srce_offr_perd_id
                              AND mrkt_veh_perd.mrkt_id                     = mrkt_perd.mrkt_id
                              AND mrkt_veh_perd.veh_id                      = p_srce_veh_id
                              AND mrkt_veh_perd.offr_perd_id                = mrkt_perd.perd_id
                              AND offr.mrkt_id                              = mrkt_veh_perd.mrkt_id
                              AND offr.veh_id                               = mrkt_veh_perd.veh_id
                              AND offr.offr_perd_id                         = mrkt_veh_perd.offr_perd_id
                              AND offr.ver_id                               = p_srce_ver_id
                              AND offr.offr_typ                             = 'CMP'
                              AND offr.offr_id                              = offr_prfl_prc_point.offr_id
                              AND offr_prfl_prc_point.offr_prfl_prcpt_id    = offr_sku_line.offr_prfl_prcpt_id
                              AND offr_prfl_sls_cls_plcmt.offr_id           = offr_prfl_prc_point.offr_id
                              AND offr_prfl_sls_cls_plcmt.sls_cls_cd        = offr_prfl_prc_point.sls_cls_cd
                              AND offr_prfl_sls_cls_plcmt.prfl_cd           = offr_prfl_prc_point.prfl_cd
                              AND offr_prfl_sls_cls_plcmt.pg_ofs_nr         = offr_prfl_prc_point.pg_ofs_nr
                              AND offr_prfl_sls_cls_plcmt.featrd_side_cd    = offr_prfl_prc_point.featrd_side_cd
                              AND offr_prfl_sls_cls_plcmt.mrkt_id           = offr_prfl_prc_point.mrkt_id
                              AND offr_prfl_sls_cls_plcmt.veh_id            = offr_prfl_prc_point.veh_id
                              AND offr_prfl_sls_cls_plcmt.offr_perd_id      = offr_prfl_prc_point.offr_perd_id
                              AND offr_sls_cls_sku.offr_id                  = offr_sku_line.offr_id
                              AND offr_sls_cls_sku.sls_cls_cd               = offr_sku_line.sls_cls_cd
                              AND offr_sls_cls_sku.prfl_cd                  = offr_sku_line.prfl_cd
                              AND offr_sls_cls_sku.pg_ofs_nr                = offr_sku_line.pg_ofs_nr
                              AND offr_sls_cls_sku.featrd_side_cd           = offr_sku_line.featrd_side_cd
                              AND offr_sls_cls_sku.sku_id                   = offr_sku_line.sku_id
                              AND offr_sls_cls_sku.mrkt_id                  = offr_sku_line.mrkt_id
                              AND dstrbtd_mrkt_sls.offr_sku_line_id         = offr_sku_line.offr_sku_line_id
    ----------------------------------------------------------------------------------
                              AND mrkt_sku.mrkt_id                          = p_srce_mrkt_id
                              AND mrkt_sku.sku_id                           = offr_sku_line.sku_id
                              AND mrkt_perd_prfl.mrkt_id                    = p_srce_mrkt_id
                              AND mrkt_perd_prfl.offr_perd_id               =p_srce_offr_perd_id
                              AND mrkt_perd_prfl.prfl_cd                    = offr_prfl_prc_point.prfl_cd
    ----------------------------------------------------------------------------------
    -- Regular Price Join
                              AND srce_reg_prc.sku_id                       = offr_sku_line.sku_id
                              AND srce_reg_prc.sku_id                       = trgt_reg_prc.sku_id (+)
    ----------------------------------------------------------------------------------
                              AND prfl.prfl_cd                              = offr_prfl_prc_point.prfl_cd
                              AND offr_sku_line.mrkt_id                     = offr_sku_set.mrkt_id(+)
                              AND offr_sku_line.offr_id                     = offr_sku_set.offr_id(+)
                              AND offr_sku_line.offr_perd_id                = offr_sku_set.offr_perd_id(+)
                              AND offr_sku_line.veh_id                      = offr_sku_set.veh_id(+)
                              AND offr_sku_line.offr_sku_set_id             = offr_sku_set.offr_sku_set_id(+)
                              AND offr_sku_line.offr_sku_line_id            = est_lnkg.offr_sku_line_id(+)
                          ) ood,
                          offr_trgt otrg,
                          mrkt m,
                          mrkt_perd mp,
                          mrkt_veh_perd mvp,
                          mrkt_veh_perd_ver mvpv,
                          (SELECT *
                             FROM mrkt_veh_perd_sctn
                            WHERE mrkt_veh_perd_sctn.mrkt_id                = p_srce_mrkt_id
                              AND mrkt_veh_perd_sctn.offr_perd_id           = p_srce_offr_perd_id
                              AND mrkt_veh_perd_sctn.veh_id                 = p_srce_veh_id
                              AND mrkt_veh_perd_sctn.ver_id                 = p_srce_ver_id
                          ) mvps,
    ----------------------------------------------------------------------------------
                          (SELECT offr.offr_id,
                                  offr.offr_link_id,
                                  offr.mrkt_veh_perd_sctn_id,
                                  mrkt_perd.crncy_cd,
                                  offr.brchr_plcmt_id,
                                  offr.sctn_page_ofs_nr
                             FROM mrkt,
                                  mrkt_perd,
                                  mrkt_veh_perd,
                                  mrkt_veh_perd_ver,
                                  offr
                            WHERE mrkt.mrkt_id                              = p_trgt_mrkt_id
                              AND mrkt_perd.mrkt_id                         = mrkt.mrkt_id
                              AND mrkt_perd.perd_id                         = p_trgt_offr_perd_id
                              AND mrkt_veh_perd.mrkt_id                     = mrkt_perd.mrkt_id
                              AND mrkt_veh_perd.veh_id                      = p_trgt_veh_id
                              AND mrkt_veh_perd.offr_perd_id                = mrkt_perd.perd_id
                              AND mrkt_veh_perd_ver.mrkt_id                 = mrkt_veh_perd.mrkt_id
                              AND mrkt_veh_perd_ver.veh_id                  = mrkt_veh_perd.veh_id
                              AND mrkt_veh_perd_ver.offr_perd_id            = mrkt_veh_perd.offr_perd_id
                              AND mrkt_veh_perd_ver.ver_id                  = p_trgt_ver_id
                              AND offr.mrkt_id                              = mrkt_veh_perd_ver.mrkt_id
                              AND offr.veh_id                               = mrkt_veh_perd_ver.veh_id
                              AND offr.offr_perd_id                         = mrkt_veh_perd_ver.offr_perd_id
                              AND offr.ver_id                               = mrkt_veh_perd_ver.ver_id
                              AND offr.offr_typ                             = 'CMP'
                          ) offr_target,
                          (SELECT *
                             FROM mrkt_veh_perd_sctn
                            WHERE mrkt_veh_perd_sctn.mrkt_id                =p_trgt_mrkt_id
                              AND mrkt_veh_perd_sctn.offr_perd_id           =p_trgt_offr_perd_id
                              AND mrkt_veh_perd_sctn.veh_id                 = p_trgt_veh_id
                              AND mrkt_veh_perd_sctn.ver_id                 = p_trgt_ver_id) trgt_mvps,
                          (SELECT offr_prfl_prc_point.offr_prfl_prcpt_id,
                                  offr_prfl_prc_point.offr_prfl_prcpt_link_id,
                                  offr_prfl_prc_point.sls_prc_amt,
                                  offr_prfl_prc_point.nr_for_qty
                             FROM mrkt,
                                  mrkt_perd,
                                  mrkt_veh_perd,
                                  mrkt_veh_perd_ver,
                                  offr,
                                  offr_prfl_prc_point
                            WHERE mrkt.mrkt_id                              = p_trgt_mrkt_id
                              AND mrkt_perd.mrkt_id                         = mrkt.mrkt_id
                              AND mrkt_perd.perd_id                         = p_trgt_offr_perd_id
                              AND mrkt_veh_perd.mrkt_id                     = mrkt_perd.mrkt_id
                              AND mrkt_veh_perd.veh_id                      = p_trgt_veh_id
                              AND mrkt_veh_perd.offr_perd_id                = mrkt_perd.perd_id
                              AND mrkt_veh_perd_ver.mrkt_id                 = mrkt_veh_perd.mrkt_id
                              AND mrkt_veh_perd_ver.veh_id                  = mrkt_veh_perd.veh_id
                              AND mrkt_veh_perd_ver.offr_perd_id            = mrkt_veh_perd.offr_perd_id
                              AND mrkt_veh_perd_ver.ver_id                  = p_trgt_ver_id
                              AND offr.mrkt_id                              = mrkt_veh_perd_ver.mrkt_id
                              AND offr.veh_id                               = mrkt_veh_perd_ver.veh_id
                              AND offr.offr_perd_id                         = mrkt_veh_perd_ver.offr_perd_id
                              AND offr.ver_id                               = mrkt_veh_perd_ver.ver_id
                              AND offr.offr_typ                             = 'CMP'
                              AND offr.offr_id                              = offr_prfl_prc_point.offr_id
                              ) opp_target,
                          (SELECT offr_sku_line.offr_sku_line_link_id,
                                  offr_sku_line.offr_sku_line_id,
                                  dstrbtd_mrkt_sls.unit_qty,
                                  offr_sku_line.offr_id
                             FROM mrkt,
                                  mrkt_perd,
                                  mrkt_veh_perd,
                                  offr,
                                  offr_sku_line,
                                  dstrbtd_mrkt_sls
                            WHERE mrkt.mrkt_id                              = p_trgt_mrkt_id
                              AND mrkt_perd.mrkt_id                         = mrkt.mrkt_id
                              AND mrkt_perd.perd_id                         = p_trgt_offr_perd_id
                              AND mrkt_veh_perd.mrkt_id                     = mrkt_perd.mrkt_id
                              AND mrkt_veh_perd.veh_id                      = p_trgt_veh_id
                              AND mrkt_veh_perd.offr_perd_id                = mrkt_perd.perd_id
                              AND offr.mrkt_id                              = mrkt_veh_perd.mrkt_id
                              AND offr.veh_id                               = mrkt_veh_perd.veh_id
                              AND offr.offr_perd_id                         = mrkt_veh_perd.offr_perd_id
                              AND offr.ver_id                               = p_trgt_ver_id
                              AND offr.offr_typ                             = 'CMP'
                              AND offr.offr_id                              = offr_sku_line.offr_id
                              AND offr_sku_line.offr_sku_line_id           = dstrbtd_mrkt_sls.offr_sku_line_id
                              AND dstrbtd_mrkt_sls.sls_perd_id              = dstrbtd_mrkt_sls.offr_perd_id
                             ) osl_target
                    WHERE m.mrkt_id = p_srce_mrkt_id
                      AND mp.mrkt_id = m.mrkt_id
                      AND mp.perd_id = p_srce_offr_perd_id
                      AND mvp.mrkt_id = mp.mrkt_id
                      AND mvp.veh_id = p_srce_veh_id
                      AND mvp.offr_perd_id = mp.perd_id
                      AND mvpv.mrkt_id = mvp.mrkt_id
                      AND mvpv.veh_id = mvp.veh_id
                      AND mvpv.offr_perd_id = mvp.offr_perd_id
                      AND mvpv.ver_id = p_srce_ver_id
                      AND o.mrkt_id = mvpv.mrkt_id
                      AND o.veh_id = mvpv.veh_id
                      AND o.offr_perd_id = mvpv.offr_perd_id
                      AND o.ver_id = mvpv.ver_id
                      AND o.offr_typ = 'CMP'
                      AND o.offr_id = otrg.offr_id(+)
                      AND o.offr_id = ood.offr_id(+)
                      AND o.mrkt_veh_perd_sctn_id = mvps.mrkt_veh_perd_sctn_id(+)
                      AND offr_target.mrkt_veh_perd_sctn_id = trgt_mvps.mrkt_veh_perd_sctn_id(+)
                      AND o.offr_id = offr_target.offr_link_id(+)
                      AND ood.offr_prfl_prcpt_id = opp_target.offr_prfl_prcpt_link_id(+)
                      AND ood.offr_sku_line_id = osl_target.offr_sku_line_link_id(+)
                 ORDER BY o.offr_id,
                          otrg.trgt_id,
                          ood.oppp_prfl_cd,
                          ood.offr_prfl_prcpt_id,
                          ood.offr_sku_line_id
    ---------------------------------------------------------------------------------------------------------------------------------
                )
             LOOP


                    ls_offr_key :=      rec.srce_offr_id
                                     || '-'
                                     || rec.trgt_mrkt_id
                                     || '-'
                                     || rec.trgt_veh_id
                                     || '-'
                                     || rec.trgt_offr_perd_id;

                    IF t_tbl_offr_keys.EXISTS (ls_offr_key) THEN
                        ln_offr_id := t_tbl_offr_keys (ls_offr_key);
                    ELSE
                        IF rec.offr_rec_typ = 'OFFR_EXISTS' THEN
                           BEGIN
                            ln_offr_id := rec.trgt_offr_id;
                            UPDATE offr
                               SET offr_desc_txt               = rec.offr_desc_txt,
                                   offr_stus_cd                = rec.offr_stus_cd,
                                   offr_stus_rsn_desc_txt      = rec.offr_stus_rsn_desc_txt,
                                   brchr_plcmt_id              = rec.brchr_plcmt_id,
                                   pg_wght_pct                 = rec.pg_wght_pct,
                                   micr_ncpsltn_desc_txt       = rec.micr_ncpsltn_desc_txt,
                                   micr_ncpsltn_ind            = rec.micr_ncpsltn_ind,
                                   mrkt_veh_perd_sctn_id       = rec.mrkt_veh_perd_sctn_id,
                                   sctn_page_ofs_nr            = rec.sctn_page_ofs_nr,
                                   featrd_side_cd              = rec.featrd_side_cd,
                                   flap_ind                    = rec.flap_ind,
                                   enrgy_chrt_postn_id         = rec.enrgy_chrt_postn_id,
                                   enrgy_chrt_offr_desc_txt    = rec.enrgy_chrt_offr_desc_txt,
                                   shpng_perd_id               = rec.shpng_perd_id,
                                   bilng_perd_id               = rec.bilng_perd_id,
                                   offr_ntes_txt               = rec.offr_ntes_txt,
                                   rpt_sbtl_typ_id             = 2,
                                   unit_rptg_lvl_id            = rec.unit_rptg_lvl_id,
                                   offr_lyot_cmnts_txt         = rec.offr_lyot_cmnts_txt,
                                   est_srce_id                 = rec.est_srce_id,
                                   est_stus_cd                 = rec.est_stus_cd,
                                   offr_typ                    = rec.offr_typ,
                                   std_offr_id                 = rec.std_offr_id,
                                   offr_link_ind               = 'Y',
                                   offr_link_id                = rec.srce_offr_id,
                                   prfl_offr_strgth_pct        = rec.prfl_offr_strgth_pct,
                                   prfl_cnt                    = rec.prfl_cnt           ,
                                   sku_cnt                     = rec.sku_cnt            ,
                                   ssnl_evnt_id                = rec.ssnl_evnt_id,
                                   brchr_postn_id              = rec.brchr_postn_id,
                                   frnt_cvr_ind                = rec.frnt_cvr_ind,
                                   pg_typ_id                   = rec.pg_typ_id,
                                   offr_prsntn_strnth_id       = rec.offr_prsntn_strnth_id,
                                   flap_pg_wght_pct            = rec.flap_pg_wght_pct,
                                   offr_cls_id                 = rec.offr_cls_id,
                                   cust_pull_id                = rec.cust_pull_id,
                                   last_updt_user_id           = p_user_nm,
                                   last_updt_ts                = SYSDATE
                             WHERE offr_id = ln_offr_id;
                            t_tbl_offr_keys (ls_offr_key) := ln_offr_id;
                           EXCEPTION
                           WHEN OTHERS THEN
                               app_plsql_log.info (ls_stage || ': ' || 'ERROR While Updating Offr Table' || SQLERRM(SQLCODE));
                           END;
                        ELSIF rec.offr_rec_typ = 'OFFR_NEW' THEN
                             SELECT seq.NEXTVAL
                               INTO ln_offr_id
                               FROM DUAL;
                           BEGIN
                               INSERT INTO offr
                                           (offr_id,
                                            mrkt_id,
                                            offr_perd_id,
                                            veh_id,
                                            ver_id,
                                            offr_desc_txt,
                                            offr_stus_cd,
                                            offr_stus_rsn_desc_txt,
                                            brchr_plcmt_id,
                                            pg_wght_pct,
                                            micr_ncpsltn_desc_txt,
                                            micr_ncpsltn_ind,
                                            mrkt_veh_perd_sctn_id,
                                            sctn_page_ofs_nr,
                                            featrd_side_cd,
                                            flap_ind,
                                            enrgy_chrt_postn_id,
                                            enrgy_chrt_offr_desc_txt,
                                            shpng_perd_id,
                                            bilng_perd_id,
                                            offr_ntes_txt,
                                            rpt_sbtl_typ_id,
                                            unit_rptg_lvl_id,
                                            offr_lyot_cmnts_txt,
                                            est_srce_id,
                                            est_stus_cd,
                                            offr_typ,
                                            std_offr_id,
                                            offr_link_ind,
                                            offr_link_id,
                                            prfl_offr_strgth_pct,
                                            prfl_cnt,
                                            sku_cnt,
                                            ssnl_evnt_id,
                                            brchr_postn_id,
                                            frnt_cvr_ind,
                                            pg_typ_id,
                                            offr_prsntn_strnth_id,
                                            flap_pg_wght_pct,
                                            offr_cls_id,
                                            cust_pull_id,
                                            creat_user_id,
                                            creat_ts
                                           )
                                    VALUES (ln_offr_id,
                                            rec.trgt_mrkt_id,
                                            rec.trgt_offr_perd_id,
                                            rec.trgt_veh_id,
                                            rec.trgt_ver_id,
                                            rec.offr_desc_txt,
                                            rec.offr_stus_cd,
                                            rec.offr_stus_rsn_desc_txt,
                                            rec.brchr_plcmt_id,
                                            rec.pg_wght_pct,
                                            rec.micr_ncpsltn_desc_txt,
                                            rec.micr_ncpsltn_ind,
                                            rec.mrkt_veh_perd_sctn_id,
                                            rec.sctn_page_ofs_nr,
                                            rec.featrd_side_cd,
                                            rec.flap_ind,
                                            rec.enrgy_chrt_postn_id,
                                            rec.enrgy_chrt_offr_desc_txt,
                                            rec.shpng_perd_id,
                                            rec.bilng_perd_id,
                                            rec.offr_ntes_txt,
                                            2,
                                            rec.unit_rptg_lvl_id,
                                            rec.offr_lyot_cmnts_txt,
                                            rec.est_srce_id,
                                            rec.est_stus_cd,
                                            rec.offr_typ,
                                            rec.std_offr_id,
                                            'Y',
                                            rec.srce_offr_id,
                                            rec.prfl_offr_strgth_pct,
                                            rec.prfl_cnt,
                                            rec.sku_cnt ,
                                            rec.ssnl_evnt_id,
                                            rec.brchr_postn_id,
                                            rec.frnt_cvr_ind,
                                            rec.pg_typ_id,
                                            rec.offr_prsntn_strnth_id,
                                            rec.flap_pg_wght_pct,
                                            rec.offr_cls_id,
                                            rec.cust_pull_id,
                                            p_user_nm,
                                            SYSDATE
                                           );
                               t_tbl_offr_keys (ls_offr_key) := ln_offr_id;

                           EXCEPTION
                           WHEN OTHERS THEN
                                p_status := 1;
                                app_plsql_log.info (  ls_stage || ': ' || 'ERROR While Inserting Offr Table' || SQLERRM(SQLCODE));
                           END;
                        END IF;


                        BEGIN

                          delete from offr_trgt where offr_id = ln_offr_id;

                          INSERT INTO offr_trgt
                                     (offr_id,
                                      trgt_id,
                                      crncy_cd,
                                      catgry_id,
                                      brnd_fmly_id,
                                      brnd_id,
                                      form_id,
                                      sgmt_id,
                                      suplr_id,
                                      trgt_unit_qty,
                                      trgt_sls_amt,
                                      trgt_cost_amt,
                                      prft_amt,
                                      prft_pct,
                                      net_per_unit_amt,
                                      units_per_rep_qty,
                                      units_per_ord_qty,
                                      net_per_rep_amt,
                                      creat_user_id,
                                      creat_ts
                                     )
                              SELECT  ln_offr_id,
                                      seq.NEXTVAL,
                                      rec.otrg_crncy_cd,
                                      rec.otrg_catgry_id,
                                      rec.otrg_brnd_fmly_id,
                                      rec.otrg_brnd_id,
                                      rec.otrg_form_id,
                                      rec.otrg_sgmt_id,
                                      rec.otrg_suplr_id,
                                      rec.otrg_trgt_unit_qty,
                                      rec.otrg_trgt_sls_amt,
                                      rec.otrg_trgt_cost_amt,
                                      rec.otrg_prft_amt,
                                      rec.otrg_prft_pct,
                                      rec.otrg_net_per_unit_amt,
                                      rec.otrg_units_per_rep_qty,
                                      rec.otrg_units_per_ord_qty,
                                      rec.otrg_net_per_rep_amt,
                                      p_user_nm, SYSDATE
                                 FROM OFFR_TRGT
                                WHERE OFFR_ID =  rec.srce_offr_id;
                        EXCEPTION
                        WHEN OTHERS THEN
                           p_status := 1;
                           app_plsql_log.info (  ls_stage || ': ' || 'ERROR While Inserting Offr Table' || SQLERRM(SQLCODE));
                        END;

                    END IF;


                   if (rec.opscp_sls_cls_cd is not null
                            and rec.opscp_prfl_cd is not null
                            and rec.opscp_pg_ofs_nr  is not null
                            and rec.opscp_featrd_side_cd  is not null) then
                           ls_opscp_key :=   ln_offr_id
                                          || '-'
                                          || rec.opscp_sls_cls_cd
                                          || '-'
                                          || rec.opscp_prfl_cd
                                          || '-'
                                          || rec.opscp_pg_ofs_nr
                                          || '-'
                                          || rec.opscp_featrd_side_cd
                                          ;
                           IF t_tbl_opscp_keys.EXISTS (ls_opscp_key) THEN                                   --record already exists;
                              ls_opscp_key := t_tbl_opscp_keys (ls_opscp_key);
                           ELSE                                              --new record;
                              t_tbl_opscp_keys (ls_opscp_key) := ls_opscp_key;
                                BEGIN
                                   ls_stage := 'Stage29: '|| p_parm_id;
                                   INSERT INTO offr_prfl_sls_cls_plcmt
                                               (offr_id,
                                                mrkt_id,
                                                veh_id,
                                                offr_perd_id,
                                                prfl_cd,
                                                sls_cls_cd,
                                                pg_ofs_nr,
                                                featrd_side_cd,
                                                sku_cnt,
                                                pg_wght_pct,
                                                fxd_pg_wght_ind,
                                                featrd_prfl_ind,
                                                use_instrctns_ind,
                                                prod_endrsmt_id,
                                                sku_offr_strgth_pct
                                               )
                                        VALUES (ln_offr_id,
                                                rec.trgt_mrkt_id,
                                                rec.trgt_veh_id,
                                                rec.trgt_offr_perd_id,
                                                rec.opscp_prfl_cd,
                                                rec.opscp_sls_cls_cd,
                                                rec.opscp_pg_ofs_nr,
                                                rec.opscp_featrd_side_cd,
                                                rec.opscp_sku_cnt,
                                                rec.opscp_pg_wght_pct,
                                                rec.opscp_fxd_pg_wght_ind,
                                                rec.opscp_featrd_prfl_ind,
                                                rec.opscp_use_instrctns_ind,
                                                rec.opscp_prod_endrsmt_id,
                                                rec.opscp_sku_offr_strgth_pct
                                               );
                                EXCEPTION
                                   WHEN DUP_VAL_ON_INDEX
                                   THEN
                                      UPDATE offr_prfl_sls_cls_plcmt
                                         SET sku_cnt = rec.opscp_sku_cnt,
                                             pg_wght_pct        = rec.opscp_pg_wght_pct,
                                             fxd_pg_wght_ind    = rec.opscp_fxd_pg_wght_ind,
                                             featrd_prfl_ind    = rec.opscp_featrd_prfl_ind,
                                             use_instrctns_ind  = rec.opscp_use_instrctns_ind,
                                             prod_endrsmt_id    = rec.opscp_prod_endrsmt_id,
                                             sku_offr_strgth_pct= rec.opscp_sku_offr_strgth_pct
                                       WHERE offr_id            = ln_offr_id
                                         AND sls_cls_cd         = rec.opscp_sls_cls_cd
                                         AND prfl_cd            = rec.opscp_prfl_cd
                                         AND pg_ofs_nr          = rec.opscp_pg_ofs_nr
                                         AND featrd_side_cd     = rec.opscp_featrd_side_cd;
                                   WHEN OTHERS
                                   THEN
                                      null;
                                END;
                              END IF;

                           ls_oppp_key :=
                                 ln_offr_id
                              || '-'
                              || rec.srce_oppp_id
                           ;
                           IF t_tbl_oppp_keys.EXISTS (ls_oppp_key)  THEN
                                 NULL;
                           ELSE
                                IF rec.oppp_rec_typ = 'OPPP_EXISTS' THEN
                                     BEGIN
                                        ls_stage := 'Stage31: '|| p_parm_id;
                                        ln_oppp_id := rec.trgt_oppp_id;
                                        UPDATE offr_prfl_prc_point
                                           SET offr_id                  = ln_offr_id,
                                               mrkt_id                  = rec.trgt_mrkt_id,
                                               veh_id                   = rec.trgt_veh_id,
                                               offr_perd_id             = rec.trgt_offr_perd_id,
                                               sls_cls_cd               = rec.oppp_sls_cls_cd,
                                               prfl_cd                  = rec.oppp_prfl_cd,
                                               pg_ofs_nr                = rec.oppp_pg_ofs_nr,
                                               featrd_side_cd           = rec.oppp_featrd_side_cd,
                                               crncy_cd                 = rec.oppp_crncy_cd,
                                               impct_catgry_id          = rec.oppp_impct_catgry_id,
                                               cnsmr_invstmt_bdgt_id    = rec.oppp_cnsmr_invstmt_bdgt_id,
                                               promtn_id                = rec.oppp_promtn_id,
                                               promtn_clm_id            = rec.oppp_promtn_clm_id,
                                               sls_prc_amt              = rec.oppp_sls_prc_amt,
                                               nr_for_qty               = rec.oppp_nr_for_qty,
                                               net_to_avon_fct          = rec.oppp_net_to_avon_fct,
                                               comsn_typ                = rec.oppp_comsn_typ,
                                               ssnl_evnt_id             = rec.oppp_ssnl_evnt_id,
                                               sls_stus_cd              = rec.oppp_sls_stus_cd,
                                               sku_offr_strgth_pct      = rec.oppp_sku_offr_strgth_pct,
                                               est_unit_qty             = rec.oppp_est_unit_qty,
                                               est_sls_amt              = rec.oppp_est_sls_amt,
                                               est_cost_amt             = rec.oppp_est_cost_amt,
                                               prfl_stus_rsn_desc_txt   = rec.oppp_prfl_stus_rsn_desc_txt,
                                               prfl_stus_cd             = rec.oppp_prfl_stus_cd,
                                               tax_amt                  = rec.oppp_tax_amt,
                                               comsn_amt                = rec.oppp_comsn_amt,
                                               pymt_typ                 = nvl(rec.oppp_pymt_typ,1),
                                               prc_point_desc_txt       = rec.oppp_prc_point_desc_txt,
                                               prmry_offr_ind           = rec.oppp_prmry_offr_ind,
                                               sls_promtn_ind           = rec.oppp_sls_promtn_ind,
                                               impct_prfl_cd            = rec.oppp_impct_prfl_cd,
                                               awrd_sls_prc_amt         = rec.oppp_awrd_sls_prc_amt,
                                               chrty_amt                = rec.oppp_chrty_amt,
                                               chrty_ovrrd_ind          = rec.oppp_chrty_ovrrd_ind,
                                               roylt_pct                = rec.oppp_roylt_pct,
                                               roylt_ovrrd_ind          = rec.oppp_roylt_ovrrd_ind,
                                               tax_type_id              = rec.oppp_tax_type_id,
                                               sls_srce_id              = rec.oppp_sls_srce_id,
                                               unit_calc_ind            = 'Y',
                                               demo_discnt_id           = rec.oppp_demo_discnt_id,
                                               frc_mtch_mthd_id         = null,
                                               prc_lvl_typ_id           = rec.oppp_prc_lvl_typ_id,
                                               prc_lvl_typ_cd           = rec.oppp_prc_lvl_typ_cd,
                                               min_prc_amt              = rec.oppp_min_prc_amt,
                                               opt_prc_amt              = rec.oppp_opt_prc_amt,
                                               max_prc_amt              = rec.oppp_max_prc_amt,
                                               plnd_prc_lvl_typ_id      = rec.oppp_plnd_prc_lvl_typ_id,
                                               plnd_prc_lvl_typ_cd      = rec.oppp_plnd_prc_lvl_typ_cd,
                                               plnd_min_prc_amt         = rec.oppp_plnd_min_prc_amt,
                                               plnd_opt_prc_amt         = rec.oppp_plnd_opt_prc_amt,
                                               plnd_max_prc_amt         = rec.oppp_plnd_max_prc_amt,
                                               plnd_promtn_id           = rec.oppp_plnd_promtn_id,
                                               key_prfl_ind             = rec.oppp_key_prfl_ind,
                                               grail_min_prc_amt        = rec.oppp_grail_min_prc_amt,
                                               grail_max_prc_amt        = rec.oppp_grail_max_prc_amt
                                         WHERE offr_prfl_prcpt_id = ln_oppp_id;
                                         t_tbl_oppp_keys (ls_oppp_key) := ln_oppp_id;
                                     EXCEPTION
                                        WHEN OTHERS THEN
                                           null;
                                     END;
                                ELSIF rec.oppp_rec_typ = 'OPPP_NEW' THEN
                                       BEGIN
                                         SELECT seq.NEXTVAL
                                          INTO ln_oppp_id
                                          FROM DUAL;
                                          ls_stage := 'Stage33: '|| p_parm_id;
                                          INSERT INTO offr_prfl_prc_point
                                                      (offr_prfl_prcpt_id, offr_id,
                                                       veh_id,
                                                       mrkt_id,
                                                       sls_cls_cd,
                                                       prfl_cd,
                                                       offr_perd_id,
                                                       pg_ofs_nr,
                                                       featrd_side_cd,
                                                       crncy_cd,
                                                       impct_catgry_id,
                                                       cnsmr_invstmt_bdgt_id,
                                                       promtn_id,
                                                       promtn_clm_id,
                                                       sls_prc_amt,
                                                       net_to_avon_fct,
                                                       ssnl_evnt_id,
                                                       sls_stus_cd,
                                                       sku_cnt,
                                                       sku_offr_strgth_pct,
                                                       est_unit_qty,
                                                       est_sls_amt,
                                                       est_cost_amt,
                                                       prfl_stus_rsn_desc_txt,
                                                       prfl_stus_cd,
                                                       tax_amt,
                                                       comsn_amt,
                                                       pymt_typ               ,
                                                       prc_point_desc_txt,
                                                       prmry_offr_ind,
                                                       sls_promtn_ind,
                                                       impct_prfl_cd,
                                                       awrd_sls_prc_amt,
                                                       nr_for_qty,
                                                       chrty_amt,
                                                       chrty_ovrrd_ind,
                                                       roylt_pct,
                                                       roylt_ovrrd_ind,
                                                       tax_type_id,
                                                       comsn_typ,
                                                       offr_prfl_prcpt_link_id,
                                                       sls_srce_id,
                                                       unit_calc_ind ,
                                                       demo_discnt_id ,
                                                       frc_mtch_mthd_id,
                                                       prc_lvl_typ_id,
                                                       prc_lvl_typ_cd,
                                                       min_prc_amt,
                                                       opt_prc_amt,
                                                       max_prc_amt,
                                                       plnd_prc_lvl_typ_id,
                                                       plnd_prc_lvl_typ_cd,
                                                       plnd_min_prc_amt,
                                                       plnd_opt_prc_amt,
                                                       plnd_max_prc_amt,
                                                       plnd_promtn_id,
                                                       key_prfl_ind,
                                                       grail_min_prc_amt,
                                                       grail_max_prc_amt
                                                      )
                                               VALUES (ln_oppp_id, ln_offr_id,
                                                       rec.trgt_veh_id,
                                                       rec.trgt_mrkt_id,
                                                       rec.oppp_sls_cls_cd,
                                                       rec.oppp_prfl_cd,
                                                       rec.trgt_offr_perd_id,
                                                       rec.oppp_pg_ofs_nr,
                                                       rec.oppp_featrd_side_cd,
                                                       rec.oppp_crncy_cd,
                                                       rec.oppp_impct_catgry_id,
                                                       rec.oppp_cnsmr_invstmt_bdgt_id,
                                                       rec.oppp_promtn_id,
                                                       rec.oppp_promtn_clm_id,
                                                       rec.oppp_sls_prc_amt,
                                                       rec.oppp_net_to_avon_fct,
                                                       rec.oppp_ssnl_evnt_id,
                                                       rec.oppp_sls_stus_cd,
                                                       rec.oppp_sku_cnt,
                                                       rec.oppp_sku_offr_strgth_pct,
                                                       rec.oppp_est_unit_qty,
                                                       rec.oppp_est_sls_amt,
                                                       rec.oppp_est_cost_amt,
                                                       rec.oppp_prfl_stus_rsn_desc_txt,
                                                       rec.oppp_prfl_stus_cd,
                                                       rec.oppp_tax_amt,
                                                       rec.oppp_comsn_amt,
                                                       rec.oppp_pymt_typ,
                                                       rec.oppp_prc_point_desc_txt,
                                                       rec.oppp_prmry_offr_ind,
                                                       rec.oppp_sls_promtn_ind,
                                                       rec.oppp_impct_prfl_cd,
                                                       rec.oppp_awrd_sls_prc_amt,
                                                       rec.oppp_nr_for_qty,
                                                       rec.oppp_chrty_amt,
                                                       rec.oppp_chrty_ovrrd_ind,
                                                       rec.oppp_roylt_pct,
                                                       rec.oppp_roylt_ovrrd_ind,
                                                       rec.oppp_tax_type_id,
                                                       rec.oppp_comsn_typ,
                                                       rec.srce_oppp_id,
                                                       rec.oppp_sls_srce_id,
                                                       'Y',
                                                       rec.oppp_demo_discnt_id,
                                                       null,
                                                       rec.oppp_prc_lvl_typ_id,
                                                       rec.oppp_prc_lvl_typ_cd,
                                                       rec.oppp_min_prc_amt,
                                                       rec.oppp_opt_prc_amt,
                                                       rec.oppp_max_prc_amt,
                                                       rec.oppp_plnd_prc_lvl_typ_id,
                                                       rec.oppp_plnd_prc_lvl_typ_cd,
                                                       rec.oppp_plnd_min_prc_amt,
                                                       rec.oppp_plnd_opt_prc_amt,
                                                       rec.oppp_plnd_max_prc_amt,
                                                       rec.oppp_plnd_promtn_id,
                                                       rec.oppp_key_prfl_ind,
                                                       rec.oppp_grail_min_prc_amt,
                                                       rec.oppp_grail_max_prc_amt
                                                      );
                                                 t_tbl_oppp_keys (ls_oppp_key) := ln_oppp_id;
                                       EXCEPTION
                                          WHEN OTHERS THEN
                                           app_plsql_log.info('Error in Create Version OPPP Insert: ' || rec.srce_oppp_id || sqlerrm(sqlcode));
                                       END;
                                END IF;
                           END IF;

                           ls_oscs_key :=
                               ln_offr_id
                              || '-'
                              || rec.oscs_sls_cls_cd
                              || '-'
                              || rec.oscs_prfl_cd
                              || '-'
                              || rec.oscs_pg_ofs_nr
                              || '-'
                              || rec.oscs_featrd_side_cd
                              || '-'
                              || rec.oscs_sku_id;

                           IF t_tbl_oscs_keys.EXISTS (ls_oscs_key)
                           THEN
                              NULL;
                           ELSE
                                 ls_stage := 'Stage35: '|| p_parm_id;
                                 BEGIN
                                    INSERT INTO offr_sls_cls_sku
                                                (offr_id,
                                                 sls_cls_cd,
                                                 prfl_cd,
                                                 pg_ofs_nr,
                                                 featrd_side_cd,
                                                 sku_id,
                                                 mrkt_id,
                                                 hero_ind,
                                                 micr_ncpsltn_ind,
                                                 smplg_ind,
                                                 mltpl_ind,
                                                 cmltv_ind,
                                                 incntv_id,
                                                 reg_prc_amt,
                                                 cost_amt,
                                                 wsl_ind
                                                )
                                         VALUES (ln_offr_id,
                                                 rec.oscs_sls_cls_cd,
                                                 rec.oscs_prfl_cd,
                                                 rec.oscs_pg_ofs_nr,
                                                 rec.oscs_featrd_side_cd,
                                                 rec.oscs_sku_id,
                                                 rec.trgt_mrkt_id,
                                                 rec.oscs_hero_ind,
                                                 rec.oscs_micr_ncpsltn_ind,
                                                 rec.oscs_smplg_ind,
                                                 rec.oscs_mltpl_ind,
                                                 rec.oscs_cmltv_ind,
                                                 rec.oscs_incntv_id,
                                                 rec.trgt_reg_prc_amt,
                                                 null,
                                                 rec.oscs_wsl_ind
                                                );
                                    t_tbl_oscs_keys (ls_oscs_key) := ls_oscs_key;
                                 EXCEPTION
                                    WHEN DUP_VAL_ON_INDEX THEN
                                       BEGIN
                                          UPDATE offr_sls_cls_sku
                                             SET hero_ind           = rec.oscs_hero_ind,
                                                 micr_ncpsltn_ind   = rec.oscs_micr_ncpsltn_ind,
                                                 smplg_ind          = rec.oscs_smplg_ind,
                                                 mltpl_ind          = rec.oscs_mltpl_ind,
                                                 cmltv_ind          = rec.oscs_cmltv_ind,
                                                 incntv_id          = rec.oscs_incntv_id,
                                                 reg_prc_amt        = rec.trgt_reg_prc_amt,
                                                 cost_amt           = null,
                                                 wsl_ind            = rec.oscs_wsl_ind
                                          WHERE  offr_id            = ln_offr_id
                                             AND sls_cls_cd         = rec.oscs_sls_cls_cd
                                             AND prfl_cd            = rec.oscs_prfl_cd
                                             AND pg_ofs_nr          = rec.oscs_pg_ofs_nr
                                             AND featrd_side_cd     = rec.oscs_featrd_side_cd
                                             AND sku_id             = rec.oscs_sku_id;

                                             t_tbl_oscs_keys (ls_oscs_key) := ls_oscs_key;
                                       EXCEPTION
                                          WHEN OTHERS THEN
                                             app_plsql_log.info('Error in Create Version offr_sls_cls_sku: ' || ls_oscs_key || sqlerrm(sqlcode));
                                       END;
                                    WHEN OTHERS THEN
                                       app_plsql_log.info('Error in Create Version offr_sls_cls_sku: ' || ls_oscs_key || sqlerrm(sqlcode));
                                 END;
                           END IF;


                           if rec.oss_offr_sku_set_id is not null then
                             ls_oss_key :=
                                         ln_offr_id
                                      || '-'
                                      || rec.oss_offr_sku_set_id
                                      || '-'
                                      || rec.trgt_mrkt_id
                                      || '-'
                                      || rec.trgt_veh_id
                                      || '-'
                                      || rec.trgt_offr_perd_id
                                      ;

                               IF t_tbl_oss_keys.EXISTS (ls_oss_key) THEN
                                    NULL;
                               ELSE
                                  SELECT seq.NEXTVAL
                                    INTO ln_oss_id
                                    FROM DUAL;

                                    BEGIN
                                         ls_stage := 'Stage38: '|| p_parm_id;

                                         INSERT INTO offr_sku_set
                                                     (offr_sku_set_id, mrkt_id,
                                                      veh_id, offr_perd_id,
                                                      offr_id, offr_sku_set_nm,
                                                      set_prc_amt,
                                                      set_prc_typ_id, pg_ofs_nr,
                                                      featrd_side_cd,
                                                      set_cmpnt_cnt, crncy_cd,
                                                      promtn_desc_txt, line_nr,
                                                      line_nr_typ_id,
                                                      fsc_cd
                                                     )
                                              VALUES (ln_oss_id, rec.trgt_mrkt_id,
                                                      rec.trgt_veh_id, rec.trgt_offr_perd_id,
                                                      ln_offr_id, rec.oss_offr_sku_set_nm,
                                                      rec.oss_set_prc_amt,
                                                      rec.oss_set_prc_typ_id, rec.oss_pg_ofs_nr,
                                                      rec.oss_featrd_side_cd,
                                                      rec.oss_set_cmpnt_cnt, rec.oss_crncy_cd,
                                                      rec.oss_promtn_desc_txt, rec.oss_line_nr,
                                                      rec.oss_line_nr_typ_id,
                                                      rec.oss_fsc_cd
                                                     );
                                         t_tbl_oss_keys (ls_oss_key) := ln_oss_id;

                                    EXCEPTION
                                    WHEN OTHERS THEN
                                        null;
                                    END;
                               END IF;
                           end if;

                          if rec.trgt_ver_id = 11 then
                              ls_cost_typ := 'A';
                           else
                              ls_cost_typ := 'P';
                           end if;

                           ls_osl_key := rec.srce_osl_id;


                           IF rec.osl_offr_sku_set_id IS NOT NULL THEN
                              ln_trgt_oss_id := t_tbl_oss_keys (    ln_offr_id
                                                                  || '-'
                                                                  || rec.osl_offr_sku_set_id
                                                                  || '-'
                                                                  || rec.trgt_mrkt_id
                                                                  || '-'
                                                                  || rec.trgt_veh_id
                                                                  || '-'
                                                                  || rec.trgt_offr_perd_id
                                                                );
                           ELSE
                              ln_trgt_oss_id := NULL;
                           END IF;

                           IF t_tbl_osl_keys.EXISTS (ls_osl_key) THEN
                              NULL;
                           ELSE
                               IF rec.osl_rec_typ = 'OSL_EXISTS' THEN
                                  ln_osl_id := rec.trgt_osl_id;
                                  t_tbl_osl_keys (ls_osl_key) := ln_osl_id;
                                  begin
                                      UPDATE offr_sku_line
                                         SET featrd_side_cd = rec.osl_featrd_side_cd,
                                             pg_ofs_nr              = rec.osl_pg_ofs_nr,
                                             crncy_cd               = rec.osl_crncy_cd,
                                             prfl_cd                = rec.osl_prfl_cd,
                                             sls_cls_cd             = rec.osl_sls_cls_cd,
                                             promtn_desc_txt        = rec.osl_promtn_desc_txt,
                                             sls_prc_amt            = rec.osl_sls_prc_amt,
                                             cost_typ               = ls_cost_typ,
                                             prmry_sku_offr_ind     = rec.osl_prmry_sku_offr_ind,
                                             dltd_ind               = rec.osl_dltd_ind,
                                             line_nr                = rec.osl_line_nr,
                                             unit_splt_pct          = rec.osl_unit_splt_pct,
                                             demo_avlbl_ind         = rec.osl_demo_avlbl_ind,
                                             offr_sku_line_link_id  = rec.srce_osl_id,
                                             set_cmpnt_ind          = rec.osl_set_cmpnt_ind,
                                             set_cmpnt_qty          = rec.osl_set_cmpnt_qty,
                                             offr_sku_set_id        = ln_trgt_oss_id,
                                             unit_prc_amt           = rec.osl_unit_prc_amt,
                                             line_nr_typ_id         = rec.osl_line_nr_typ_id
                                       WHERE offr_sku_line_id       = ln_osl_id;
                                  exception
                                   when others then
                                    null;
                                  end;

                                   ls_dms_key :=   rec.srce_osl_id;
                                   IF t_tbl_dms_keys.exists(ls_dms_key) then
                                       NULL;
                                   ELSE
                                        ls_dms_surr_key := ln_osl_id;
                                        if t_tbl_dms_surr_keys.exists(ls_dms_surr_key) then
                                            null;
                                        else
                                            begin
                                                delete from  dstrbtd_mrkt_sls where offr_sku_line_id = ln_osl_id;

                                                for dms_data in (select * from dstrbtd_mrkt_sls where offr_sku_line_id = rec.srce_osl_id)
                                                loop
                                                 ln_cost_amt :=  get_cost_amt(rec.trgt_mrkt_id,
                                                                    dms_data.sls_perd_id,
                                                                    rec.osl_sku_id,
                                                                    ls_cost_typ
                                                                   );

                                                    INSERT INTO dstrbtd_mrkt_sls
                                                           (mrkt_id         ,
                                                            offr_perd_id    ,
                                                            veh_id          ,
                                                            sls_perd_id     ,
                                                            offr_sku_line_id,
                                                            sls_typ_id      ,
                                                            unit_qty        ,
                                                            sls_stus_cd     ,
                                                            sls_srce_id     ,
                                                            prev_unit_qty   ,
                                                            prev_sls_srce_id,
                                                            comsn_amt       ,
                                                            tax_amt         ,
                                                            net_to_avon_fct ,
                                                            unit_ovrrd_ind  ,
                                                            currnt_est_ind,
                                                            cost_amt,
                                                            sls_perd_ofs_nr,
                                                            ver_id
                                                           )
                                                      values
                                                           ( rec.trgt_mrkt_id         ,
                                                             rec.trgt_offr_perd_id    ,
                                                             rec.trgt_veh_id          ,
                                                             dms_data.sls_perd_id     ,
                                                             ln_osl_id                ,
                                                             dms_data.sls_typ_id      ,
                                                             dms_data.unit_qty        ,
                                                             dms_data.sls_stus_cd     ,
                                                             dms_data.sls_srce_id     ,
                                                             dms_data.prev_unit_qty   ,
                                                             dms_data.prev_sls_srce_id,
                                                             dms_data.comsn_amt       ,
                                                             dms_data.tax_amt         ,
                                                             dms_data.net_to_avon_fct ,
                                                             dms_data.unit_ovrrd_ind  ,
                                                             dms_data.currnt_est_ind  ,
                                                             ln_cost_amt              ,
                                                             dms_data.sls_perd_ofs_nr ,
                                                             rec.trgt_ver_id
                                                       );
                                                end loop;
                                               t_tbl_dms_keys(ls_dms_key):= ls_dms_key;
                                               t_tbl_dms_surr_keys(ls_dms_surr_key):= ls_dms_surr_key;
                                            exception
                                            when no_data_found then
                                                null;
                                            end;
                                         end if;
                                   END IF;
                               ELSIF rec.osl_rec_typ = 'OSL_NEW' THEN
                                  SELECT seq.NEXTVAL
                                    INTO ln_osl_id
                                    FROM DUAL;
                                    BEGIN
                                        begin

                                            INSERT INTO offr_sku_line
                                                 (offr_sku_line_id, offr_id,
                                                  veh_id, featrd_side_cd,
                                                  offr_perd_id, mrkt_id,
                                                  sku_id, pg_ofs_nr,
                                                  crncy_cd, prfl_cd,
                                                  sls_cls_cd,
                                                  offr_prfl_prcpt_id,
                                                  promtn_desc_txt,
                                                  sls_prc_amt, cost_typ,
                                                  prmry_sku_offr_ind,
                                                  dltd_ind,
                                                  line_nr,
                                                  unit_splt_pct,
                                                  demo_avlbl_ind, offr_sku_line_link_id,
                                                  set_cmpnt_ind,
                                                  set_cmpnt_qty,
                                                  offr_sku_set_id,
                                                  unit_prc_amt,
                                                  line_nr_typ_id
                                                 )
                                          VALUES (ln_osl_id, ln_offr_id,
                                                  p_trgt_veh_id, rec.osl_featrd_side_cd,
                                                  p_trgt_offr_perd_id, p_trgt_mrkt_id,
                                                  rec.osl_sku_id, rec.osl_pg_ofs_nr,
                                                  rec.osl_crncy_cd, rec.osl_prfl_cd,
                                                  rec.osl_sls_cls_cd,
                                                  ln_oppp_id,
                                                  rec.osl_promtn_desc_txt,
                                                  rec.osl_sls_prc_amt,
                                                  ls_cost_typ,
                                                  rec.osl_prmry_sku_offr_ind,
                                                  rec.osl_dltd_ind,
                                                  rec.osl_line_nr,
                                                  rec.osl_unit_splt_pct,
                                                  rec.osl_demo_avlbl_ind, rec.srce_osl_id,
                                                  -- Changes DRS
                                                  rec.osl_set_cmpnt_ind,
                                                  rec.osl_set_cmpnt_qty,
                                                  ln_trgt_oss_id,
                                                  rec.osl_unit_prc_amt,
                                                  rec.osl_line_nr_typ_id
                                                 );
                                            t_tbl_osl_keys (ls_osl_key) := ln_osl_id;
                                        exception
                                        when others then
                                          app_plsql_log.info('Error in Create Version Offer SkuLine: ' || rec.srce_osl_id || sqlerrm(sqlcode));
                                        end;

                                       ls_dms_key :=   rec.srce_osl_id;

                                       IF t_tbl_dms_keys.exists(ls_dms_key) then
                                           NULL;
                                       ELSE
                                            BEGIN
                                                ls_stage := 'Stage43: '|| p_parm_id;
                                                ls_dms_surr_key := ln_osl_id;
                                                if t_tbl_dms_surr_keys.exists(ls_dms_surr_key) then
                                                    null;
                                                else
                                                    delete from  dstrbtd_mrkt_sls where offr_sku_line_id = ln_osl_id;
                                                    for dms_data in (select * from dstrbtd_mrkt_sls where offr_sku_line_id = rec.srce_osl_id)
                                                    loop
                                                     ln_cost_amt :=  get_cost_amt(rec.trgt_mrkt_id,
                                                                        dms_data.sls_perd_id,
                                                                        rec.osl_sku_id,
                                                                        ls_cost_typ
                                                                       );

                                                        INSERT INTO dstrbtd_mrkt_sls
                                                               (mrkt_id         ,
                                                                offr_perd_id    ,
                                                                veh_id          ,
                                                                sls_perd_id     ,
                                                                offr_sku_line_id,
                                                                sls_typ_id      ,
                                                                unit_qty        ,
                                                                sls_stus_cd     ,
                                                                sls_srce_id     ,
                                                                prev_unit_qty   ,
                                                                prev_sls_srce_id,
                                                                comsn_amt       ,
                                                                tax_amt         ,
                                                                net_to_avon_fct ,
                                                                unit_ovrrd_ind  ,
                                                                currnt_est_ind ,
                                                                cost_amt,
                                                                sls_perd_ofs_nr,
                                                                ver_id
                                                               )
                                                          values
                                                              (rec.trgt_mrkt_id        ,
                                                               rec.trgt_offr_perd_id   ,
                                                               rec.trgt_veh_id         ,
                                                               dms_data.sls_perd_id    ,
                                                               ln_osl_id               ,
                                                               dms_data.sls_typ_id          ,
                                                               dms_data.unit_qty            ,
                                                               dms_data.sls_stus_cd         ,
                                                               dms_data.sls_srce_id         ,
                                                               dms_data.prev_unit_qty       ,
                                                               dms_data.prev_sls_srce_id,
                                                               dms_data.comsn_amt       ,
                                                               dms_data.tax_amt         ,
                                                               dms_data.net_to_avon_fct ,
                                                               dms_data.unit_ovrrd_ind  ,
                                                               dms_data.currnt_est_ind ,
                                                               ln_cost_amt,
                                                               dms_data.sls_perd_ofs_nr,
                                                               rec.trgt_ver_id
                                                               )
                                                               ;

                                                    end loop;
                                                     t_tbl_dms_keys(ls_dms_key):= ls_dms_key;
                                                     t_tbl_dms_surr_keys(ls_dms_surr_key):= ls_dms_surr_key;
                                            end if;
                                        EXCEPTION
                                        WHEN OTHERS THEN
                                            app_plsql_log.info('Error in Create Version dstrbtd_mrkt_sls: ' || rec.srce_osl_id || sqlerrm(sqlcode));
                                        END;
                                       END IF;
                                    exception
                                    when others then
                                      null;
                                    END;
                               END IF;
                           END IF;

                           IF REC.SRCE_OFFR_RN = REC.TOTAL_OFFR_IDS THEN

                               begin
                                    ln_est_links := pa_maps_copy.copy_estimate_links (rec.srce_offr_id, ln_offr_id, p_user_nm);
                               exception
                               when others then
                                    null;
                               end;

                           END IF;

                   end if;
      END LOOP;

      BEGIN

        SELECT NVL(MAX(mps_sls_typ_id),99999)
         INTO ln_max_sls_typ_id
         FROM mrkt_veh_perd_ver
        WHERE mrkt_id = p_srce_mrkt_id
          AND offr_perd_id = p_srce_offr_perd_id
          AND veh_id = p_srce_veh_id
          AND ver_id = p_srce_ver_id;

        begin

        SELECT sls_typ_id into ln_sls_typ_id from (
        SELECT sls_typ_id
        FROM
          (SELECT sls_typ_id,
            seq_nr
          FROM sls_typ
          WHERE sls_typ_id IN
            (SELECT max_sls_typ_id FROM ver WHERE ver_id = p_trgt_ver_id
            )
          OR sls_typ_id = ln_max_sls_typ_id
          ORDER BY seq_nr
          )
        WHERE rownum < 2
        );

        exception when others then
          ln_sls_typ_id := -1;
        end;

        SELECT rep_cnt, ord_cnt,
               stencil_cnt,
               actvty_pct,
               cust_cnt,
               appt_cnt
          INTO ln_rep_cnt,
               ln_ord_cnt,
               ln_stencil_cnt,
               ln_actvty_pct,
               ln_cust_cnt,
               ln_appt_cnt
          FROM MRKT_VEH_PERD_VER
         WHERE mrkt_id = p_srce_mrkt_id
           and offr_perd_id = p_srce_offr_perd_id
           and veh_id = p_srce_veh_id
           and ver_id = 0;



        UPDATE MRKT_VEH_PERD_VER
           SET mps_sls_typ_id           = ln_sls_typ_id,
               last_updt_user_id        = p_user_nm,
               last_updt_ts             = sysdate,
               cmpgn_ver_creat_user_id  = p_user_nm,
               cmpgn_ver_creat_dt       = decode(p_trgt_ver_id, 0 , null, sysdate),
               rep_cnt                  = ln_rep_cnt,
               ord_cnt                  = ln_ord_cnt,
               stencil_cnt              = ln_stencil_cnt,
               actvty_pct               = ln_actvty_pct,
               cust_cnt                 = ln_cust_cnt,
               appt_cnt                 = ln_appt_cnt
         where mrkt_id = p_trgt_mrkt_id
           and offr_perd_id = p_trgt_offr_perd_id
           and veh_id = p_trgt_veh_id
           and ver_id = p_trgt_ver_id;

      EXCEPTION
      WHEN OTHERS THEN
        NULL;
      END;

   EXCEPTION
   WHEN OTHERS THEN
        p_status := 1;
        APP_PLSQL_LOG.INFO('Error in Create Version..!!');

   END CREATE_VERSION;

  PROCEDURE set_pricing_flags
  ( p_mrkt_id         in     mrkt.mrkt_id%TYPE,
    p_offr_perd_id    in     mrkt_perd.perd_id%TYPE,
    p_prc_enabled     in out single_char,
    p_trgt_strtgy_ind in out single_char
  )
  IS

    prc_enabled_ind  single_char;
    prc_strt_perd_id mrkt_perd.perd_id%TYPE;

  BEGIN

    -- work out if market is a pricing market in the specified campaign
    BEGIN
      SELECT mrkt_config_item_val_txt INTO prc_enabled_ind
      FROM   mrkt_config_item
      WHERE  mrkt_id = p_mrkt_id and
             config_item_id = PA_MAPS_PRICING.CFG_PRICING_MARKET;

      SELECT mrkt_config_item_val_txt INTO prc_strt_perd_id
      FROM   mrkt_config_item
      WHERE  mrkt_id = p_mrkt_id and
             config_item_id = PA_MAPS_PRICING.CFG_PRICING_START_PERD;

      IF prc_enabled_ind = 'Y' and p_offr_perd_id >= prc_strt_perd_id THEN
        p_prc_enabled := 'Y';
      ELSE
        p_prc_enabled := 'N';
      END IF;

    EXCEPTION
      WHEN OTHERS THEN
        p_prc_enabled  := 'N';
    END;

    -- check whether Pricing Strategy is to be applied during copy offer
    BEGIN
      SELECT mrkt_config_item_val_txt INTO p_trgt_strtgy_ind
      FROM   mrkt_config_item
      WHERE  mrkt_id = p_mrkt_id and
             config_item_id = PA_MAPS_PRICING.CFG_TARGET_STRATEGY;
    EXCEPTION
      WHEN OTHERS THEN
        p_trgt_strtgy_ind  := 'N';
    END;

  END set_pricing_flags;

---------------------------------------------------------------------------------------------------------------------------------------------
--*******************************************************************************************************************************************
---------------------------------------------------------------------------------------------------------------------------------------------

   PROCEDURE copy_offr (
      p_parm_id   IN   mps_parm.mps_parm_id%TYPE,
      p_user_nm   IN   VARCHAR2,
      p_task_id   IN   NUMBER DEFAULT NULL,
      p_status    OUT   NUMBER -- '0' -> Success; '1' -> Error;
   )
   AS
      ls_mvpv_key                   VARCHAR2 (1000);
      ls_offr_key                   VARCHAR (1000);
      ls_offr_trgt_key              VARCHAR (1000);
      ls_opscp_key                  VARCHAR2 (32000);
      ls_oppp_key                   VARCHAR (32000);
      ls_oscs_key                   VARCHAR2 (32000);
      ls_oss_key                    VARCHAR2 (32000);
      ls_osl_key                    VARCHAR2 (32000);
      ls_dms_key                    VARCHAR2 (255);

      ln_offr_id                  NUMBER;
      ln_oppp_id                  NUMBER;
      ln_oss_id                   NUMBER;
      ln_osl_id                   NUMBER;

      ls_trgt_deleted_key           VARCHAR2(250);
      ls_err_key                    VARCHAR2 (4000);

      ls_stage                      VARCHAR2 (2000);
      v_start                       NUMBER;
      v_end                         NUMBER;
      l_job_typ                     NUMBER;
      cnt                           NUMBER; -- COUNT FOR KEY VALUE COLLECTION
      ls_trgt_deleted               VARCHAR2(1) := 'N';
      l_continue_loop               CHAR(1) :='Y';
      l_partial_success             NUMBER := 0;

      t_tbl_trgt_deleted_keys       TBL_KEY_VALUE;
      t_tbl_err_keys                TBL_KEY_VALUE;

      ln_src_mrkt_id                MRKT.MRKT_ID%TYPE;
      ln_src_offr_perd_id           NUMBER;
      ln_trgt_mrkt_id               MRKT.MRKT_ID%TYPE;
      l_trgt_veh_avlbl              CHAR (1);
      l_process                     NUMBER;

      l_err_veh_lvl                 NUMBER                    := 1;
      l_err_trgt_sls_lvl            NUMBER                    := 4;
      l_err_mrkt_lvl                NUMBER                    := 5;
      l_err_perd_lvl                NUMBER                    := 6;
      ls_btch_log_sub               NUMBER;
      ls_btch_log_txt               VARCHAR2 (2000);
      ls_btch_err_svrty             VARCHAR (30);
      ls_btch_log_key               NUMBER;
      l_trgt_veh_camp_avlbl         CHAR (1);
      l_trgt_no_est_sls_typ         NUMBER;
      l_trgt_cmpgn_locked           CHAR (1);
      l_fb_trgt_sls_amt             NUMBER;
      l_fb_trgt_actv_ind            CHAR (1);
      l_mps_sls_typ_id              number;
      l_ver_typ_id                number;
      ls_dms_surr_key             varchar2(255);

      type tbl_numbers  is table of NUMBER INDEX BY PLS_INTEGER;
      type tbl_varchars is table of varchar2(250) INDEX BY PLS_INTEGER;

      v_dltd_osl_id                 tbl_numbers;
      v_dltd_oppp_id                tbl_numbers;
      v_dltd_offr_id                tbl_numbers;
      v_dltd_sls_cls_cd             tbl_numbers;
      v_dltd_prfl_cd                tbl_numbers;
      v_dltd_pg_ofs_nr              tbl_numbers;
      v_dltd_featrd_side_cd         tbl_numbers;
      v_dltd_sku_id                 tbl_numbers;
      v_dltd_oss_id                 tbl_numbers;

      t_tbl_oss_line_nr  ttable_oss_line_nr;

      t_tbl_mvpv_keys             tbl_key_value;
      t_tbl_offr_keys             tbl_key_value;
      t_tbl_offr_trgt_keys        tbl_key_value;
      t_tbl_opscp_keys            tbl_key_value;
      t_tbl_oppp_keys             tbl_key_value;
      t_tbl_oscs_keys             tbl_key_value;
      t_tbl_oss_keys              tbl_key_value;
      t_tbl_osl_keys              tbl_key_value;
      t_tbl_el_keys               tbl_key_value;
      t_tbl_dms_keys              tbl_key_value;
      t_tbl_invalid_tax_comsn_keys tbl_key_value;
      t_tbl_dms_surr_keys        tbl_key_value;
      t_tbl_osl_sur_keys          tbl_key_value;

      ls_crncy_cd                 VARCHAR2 (3);
      ln_no_of_cncpts             NUMBER;

      ln_min_ofs number;
      ln_max_ofs number;

      ls_fb_xcluded_sls_cls_cd    VARCHAR2(4000);
      ls_valid_veh_ids            VARCHAR2(4000);
      ln_est_links number;

      ln_trgt_oss_id              number;

      sku_info                    TTBL_CPY_OFFR_SKU_DATA;

      processed_skus              tbl_sku_data_processed;

      new_price_point             boolean;
      new_item                    boolean;

      ln_brchr_plcmt_id           number;
      ln_mrkt_veh_perd_sctn_id    number;

      SRC_OFFR_CNT                number;
      l_err_veh_status            number;

      offset_min                  number;
      offset_max                  number;

      srce_mrkt_id               mrkt.mrkt_id%type;
      srce_offr_per_id             offr.offr_perd_id%type;
      srce_ver_id                 ver.VER_ID%type;
      trg_mrkt_id                mrkt.mrkt_id%type;
      trg_offr_per_id             offr.offr_perd_id%type;
      trg_ver_id                 ver.vER_ID%type;
      trg_veh_id                 varchar2(32000);

      ln_parm_cnt number;
      ln_mvpv_cnt number;

      ln_process_cnt number := 0;

      trgt_pricing_mrkt_ind single_char;
      trgt_strtgy_ind       single_char;
      trgt_pricing_details  PA_MAPS_PRICING.t_pricing := PA_MAPS_PRICING.t_pricing();
      strategy_exists_idx   number;
      strategy_used_idx     number;
      price_level_idx       number;

   BEGIN
    --**********************************************************************
    -- Copy Offer Starts Here;
      p_status      := 0;
      ls_stage      := 'Start: ' || p_parm_id ;
      v_start       := DBMS_UTILITY.get_time;
      g_run_id      := app_plsql_output.generate_new_run_id;

      app_plsql_output.set_run_id (g_run_id);
      app_plsql_output.add_key('CPY_OFFR', p_parm_id);

      app_plsql_log.set_context (g_user_id, g_module_nm, g_run_id);
      app_plsql_log.info (p_parm_id || 'Started at: ' || SYSTIMESTAMP);
      app_plsql_log.info ('Copy Offer procedure');

        begin
            select bt.btch_job_typ_id
              into l_job_typ
              from btch_job bt,
                   btch_task btask
             where bt.btch_job_id   = btask.btch_job_id
               and btch_task_id     = p_task_id;
        exception
        when others then
            l_job_typ := 9999;
        end;

      FOR parm_rec IN (SELECT DISTINCT sd.srce_mrkt_id, sd.srce_veh_id,
                                       sd.srce_offr_perd_id, sd.srce_ver_id,
                                       sd.trgt_mrkt_id, sd.trgt_veh_id,
                                       sd.trgt_offr_perd_id, sd.trgt_ver_id,
                                       sd.retain_price,
                                       sd.mrkt_unit_calc_typ_id
                                  FROM TABLE (f_get_parms (p_parm_id)) sd
                                 WHERE ((l_job_typ IN (1, 8) AND srce_veh_id = trgt_veh_id)
                                        OR
                                        (l_job_typ NOT IN (1, 8) AND 1 = 1)
                                       )
                                   AND srce_mrkt_id = srce_offr_perd_refmrkt
                                   AND trgt_mrkt_id = trgt_offr_perd_refmrkt
                      )
      LOOP
         app_plsql_log.info (ls_stage || 'srce_mrkt_id: '           || parm_rec.srce_mrkt_id
                                      || '; srce_veh_id: '          || parm_rec.srce_veh_id
                                      || '; srce_offr_perd_id: '    || parm_rec.srce_offr_perd_id
                                      || '; srce_ver_id: '          || parm_rec.srce_ver_id
                                      || '; trgt_mrkt_id: '         || parm_rec.trgt_mrkt_id
                                      || '; trgt_veh_id: '          || parm_rec.trgt_veh_id
                                      || '; trgt_offr_perd_id: '    || parm_rec.trgt_offr_perd_id
                                      || '; trgt_ver_id: '          || parm_rec.trgt_ver_id
                                      || '; retain_price: '         || parm_rec.retain_price
                                      || '; mrkt_unit_calc_typ_id: '|| parm_rec.mrkt_unit_calc_typ_id
                                      || '; l_job_typ : '           || l_job_typ
                                      || '; p_task_id : '           || p_task_id
                                      || '; p_user_nm : '           || p_user_nm
                            );

           -- Initialize variables...
           ls_offr_key       := '-1';
           ls_offr_trgt_key  := '-1';
           ls_opscp_key      := '-1';
           ls_oppp_key       := '-1';
           ls_oscs_key       := '-1';
           ls_oss_key        := '-1';
           ls_osl_key        := '-1';
           ls_dms_key        := '-1';

           ln_offr_id        := -1;
           ln_oppp_id        := -1;
           ln_oss_id         := -1;
           ln_osl_id         := -1;
           cnt               := 1;

           v_tbl_key_vals_for_update := t_tbl_key_vals_for_update(null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null);


           -- getting the source mrkt_id and offr_perd_id which will be used in determining the Items to be Boosted
           ln_src_mrkt_id       := parm_rec.srce_mrkt_id;
           ln_src_offr_perd_id  := parm_rec.srce_offr_perd_id;
           ln_trgt_mrkt_id      := parm_rec.trgt_mrkt_id;


           if  (parm_rec.srce_mrkt_id = parm_rec.trgt_mrkt_id AND
                parm_rec.srce_veh_id  = parm_rec.trgt_veh_id AND
                parm_rec.srce_offr_perd_id = parm_rec.trgt_offr_perd_id AND
                parm_rec.srce_ver_id <> parm_rec.trgt_ver_id
               )
           then
               l_process := 1; -- Create Version
           elsif (parm_rec.srce_mrkt_id         <>  parm_rec.trgt_mrkt_id AND
                  parm_rec.srce_veh_id          =   parm_rec.trgt_veh_id AND
                  parm_rec.srce_ver_id          =   parm_rec.trgt_ver_id AND
                  is_mapg_exists(parm_rec.srce_mrkt_id , parm_rec.srce_offr_perd_id, parm_rec.trgt_mrkt_id, parm_rec.trgt_offr_perd_id) = 'Y'
                 ) then
               l_process := 2; -- Copy to Local Market
           elsif (parm_rec.srce_mrkt_id = parm_rec.trgt_mrkt_id AND
                  parm_rec.srce_veh_id <> parm_rec.trgt_veh_id AND
                  parm_rec.srce_offr_perd_id = parm_rec.trgt_offr_perd_id AND
                  parm_rec.srce_ver_id = parm_rec.trgt_ver_id
                 )  then
               l_process := 3; -- Copy to Local Vehicle
           elsif (parm_rec.srce_mrkt_id = parm_rec.trgt_mrkt_id AND
                  parm_rec.srce_veh_id = parm_rec.trgt_veh_id AND
                  parm_rec.srce_offr_perd_id <> parm_rec.trgt_offr_perd_id AND
                  parm_rec.srce_ver_id = parm_rec.trgt_ver_id
                 )  then
               l_process := 4; -- Copy Vehicle to Campaign
           elsif (parm_rec.srce_mrkt_id = parm_rec.trgt_mrkt_id AND
                  parm_rec.srce_veh_id <> parm_rec.trgt_veh_id AND
                  parm_rec.srce_offr_perd_id = parm_rec.trgt_offr_perd_id AND
                  parm_rec.srce_ver_id = parm_rec.trgt_ver_id
                 )  then
               l_process := 3; -- Copy Vehicle to Local Vehicle
           else
               l_process := 0;
               l_continue_loop :=  'N';
           end if;

           -- Load the Target Static Data

           IF l_continue_loop <> 'N' THEN
            ------------------------------------------------------------------------------------------------
            -- Log the error informations...
            ------------------------------------------------------------------------------------------------
            -- Check Vehicle access in target market
             ls_stage := 'Stage1: '|| p_parm_id;
             BEGIN
                SELECT NVL ('Y', 'N')
                  INTO l_trgt_veh_avlbl
                  FROM mrkt_veh mv
                 WHERE mrkt_id = parm_rec.trgt_mrkt_id
                   AND veh_id = parm_rec.trgt_veh_id;
             EXCEPTION
                WHEN NO_DATA_FOUND
                THEN
                   l_trgt_veh_avlbl := 'N';
             END;
             IF l_trgt_veh_avlbl = 'N'
             THEN
                ls_btch_log_sub := l_err_veh_lvl;
                ls_btch_log_txt :=
                     'not Boosted because it does not exist in the target Market';
                ls_btch_err_svrty := 'Error';
                ls_btch_log_key := parm_rec.trgt_veh_id;
                ls_err_key :=
                      p_task_id
                   || '-'
                   || ls_btch_log_sub
                   || '-'
                   || ls_btch_log_key
                   || '-'
                   || ls_btch_log_txt;
                IF t_tbl_err_keys.EXISTS (ls_err_key)
                THEN         -- If this offr_id already updated; don't do it again
                   NULL;
                ELSE
                   t_tbl_err_keys (ls_err_key) := parm_rec.trgt_veh_id;
                   p_status := 1;
                   l_continue_loop := 'N';
                   IF p_task_id IS NOT NULL
                   THEN
                      insert_btch_task_log (p_task_id,
                                            ls_btch_log_sub,
                                            ls_btch_log_key,
                                            ls_btch_err_svrty,
                                            ls_btch_log_txt,
                                            0,
                                            0,
                                            parm_rec.trgt_mrkt_id
                                           );
                   ELSE
                      app_plsql_output.add_msg('VEHICLE', ls_btch_log_key );
                      app_plsql_log.info (   ls_stage || ': ' || ls_btch_err_svrty
                                          || ': '
                                          || 'Vehicle: '
                                          || ls_btch_log_key
                                          || ' '
                                          || ls_btch_log_txt
                                         );
                   END IF;
                END IF;
             END IF;
            ------------------------------------------------------------------------------------------------
            -- Check Vehicle access in target market and period
             ls_stage := 'Stage1: '|| p_parm_id;
             BEGIN
                SELECT NVL ('Y', 'N')
                  INTO l_trgt_veh_camp_avlbl
                  FROM mrkt_veh_perd mv
                 WHERE mrkt_id = parm_rec.trgt_mrkt_id
                   AND veh_id = parm_rec.trgt_veh_id
                   AND offr_perd_id = parm_rec.trgt_offr_perd_id;
             EXCEPTION
                WHEN NO_DATA_FOUND
                THEN
                   l_trgt_veh_camp_avlbl := 'N';
             END;
             IF l_trgt_veh_camp_avlbl = 'N'
             THEN
                ls_btch_log_sub := l_err_veh_lvl;
                ls_btch_log_txt :=
                     'not Boosted because it does not exist in the target Market/Veh/Campaign - ' || parm_rec.trgt_mrkt_id || '/' || parm_rec.trgt_veh_id || '/' || parm_rec.trgt_offr_perd_id;
                ls_btch_err_svrty := 'Error';
                ls_btch_log_key := parm_rec.trgt_offr_perd_id;
                ls_err_key :=
                      p_task_id
                   || '-'
                   || ls_btch_log_sub
                   || '-'
                   || ls_btch_log_key
                   || '-'
                   || ls_btch_log_txt;
                l_continue_loop := 'N';
                IF t_tbl_err_keys.EXISTS (ls_err_key)
                THEN         -- If this offr_id already updated; don't do it again
                   NULL;
                ELSE
                      t_tbl_err_keys (ls_err_key) := parm_rec.trgt_veh_id || parm_rec.trgt_offr_perd_id;
                      if l_job_typ not in (1, 8, 4) then --modified on 11 Nov 2011 for QC 3660
                          p_status := 3;
                          app_plsql_output.add_msg('VEHICLE', parm_rec.trgt_veh_id );
                          app_plsql_log.info (   ls_stage || ': ' || ls_btch_err_svrty
                                              || ': '
                                              || 'Market/Campaign: '
                                              || ls_btch_log_key
                                              || ' '
                                              || ls_btch_log_txt
                                             );
                      elsif l_job_typ in (1, 8, 4) then --modified on 11 Nov 2011 for QC 3660
                         p_status := 1;
                         insert_btch_task_log (p_task_id,
                                                ls_btch_log_sub,
                                                parm_rec.trgt_veh_id,
                                                ls_btch_err_svrty,
                                                ls_btch_log_txt,
                                                0,
                                                0,
                                                parm_rec.trgt_mrkt_id
                                               );

                      end if;

                END IF;
             END IF;
             ------------------------------------------------------------------------------------------------
            -- Check One or more Target Market Campaign Vehicle, WIP Version has a Sales Type that is not Estimate
             ls_stage := 'Stage2: '|| p_parm_id;
             if  l_process not in (1, 4) then  -- Check this validation for all the functionality exception create version.
                     SELECT COUNT (1)
                       INTO l_trgt_no_est_sls_typ
                       FROM mrkt_veh_perd_ver mvpv
                      WHERE mrkt_id = parm_rec.trgt_mrkt_id
                        AND veh_id = parm_rec.trgt_veh_id
                        AND offr_perd_id = parm_rec.trgt_offr_perd_id
                        AND ver_id = 0
                        AND mps_sls_typ_id > 1;
                     IF l_trgt_no_est_sls_typ > 0
                     -- Some records non estimated sales type available
                     THEN
                        ls_btch_log_sub := l_err_mrkt_lvl;
                        ls_btch_log_txt :=
                                       'not Boosted because a Sales Type is not Estimate';
                        ls_btch_err_svrty := 'Error';
                        ls_btch_log_key := parm_rec.trgt_mrkt_id;
                        ls_err_key :=
                              p_task_id
                           || '-'
                           || ls_btch_log_sub
                           || '-'
                           || ls_btch_log_key
                           || '-'
                           || ls_btch_log_txt;
                        IF t_tbl_err_keys.EXISTS (ls_err_key)
                        THEN         -- If this offr_id already updated; don't do it again
                           NULL;
                        ELSE
                           t_tbl_err_keys (ls_err_key) := parm_rec.trgt_mrkt_id;
                           IF p_task_id IS NOT NULL
                           THEN
                              p_status := 1;
                              l_continue_loop := 'N';
                              insert_btch_task_log (p_task_id,
                                                    ls_btch_log_sub,
                                                    ls_btch_log_key,
                                                    ls_btch_err_svrty,
                                                    ls_btch_log_txt,
                                                    0,
                                                    0,
                                                    parm_rec.trgt_mrkt_id
                                                   );
                           ELSE
                              p_status := 1;
                              l_continue_loop := 'N';
                              app_plsql_output.add_msg('MARKET', ls_btch_log_key );
                              app_plsql_log.info (  ls_stage || ': ' ||    ls_btch_err_svrty
                                                  || ': '
                                                  || 'Market: '
                                                  || ls_btch_log_key
                                                  || ' '
                                                  || ls_btch_log_txt
                                                 );
                           END IF;
                        END IF;
                     END IF;
             end if;

            -- Check: One or more Target Market, Campaign, Vehicle, WIP Versions is Locked
             ls_stage := 'Stage3: '|| p_parm_id;
             if  l_job_typ in (1, 8) and l_process <> 1  then
                BEGIN
                  SELECT DECODE ((SELECT cmpgn_ver_lock_ind
                                    FROM mrkt_veh_perd_ver mvpv
                                   WHERE mvpv.mrkt_id = parm_rec.trgt_mrkt_id
                                     AND mvpv.offr_perd_id = parm_rec.trgt_offr_perd_id
                                     AND mvpv.veh_id = parm_rec.trgt_veh_id
                                     AND mvpv.ver_id = 0
                                 ),
                                   'Y', 'Y',
                                   'N'
                                  )
                      INTO l_trgt_cmpgn_locked
                      FROM DUAL;
                EXCEPTION
                    WHEN NO_DATA_FOUND
                    THEN
                       l_trgt_cmpgn_locked := 'N';
                END;

                IF l_trgt_cmpgn_locked = 'Y'                  -- Some offrs are locked
                THEN
                    ls_btch_log_sub := l_err_mrkt_lvl;
                    ls_btch_log_txt :=
                                     'not Boosted because a target Vehicle is Locked';
                    ls_btch_err_svrty := 'Error';
                    ls_btch_log_key := parm_rec.trgt_mrkt_id;
                    ls_err_key :=
                          p_task_id
                       || '-'
                       || ls_btch_log_sub
                       || '-'
                       || ls_btch_log_key
                       || '-'
                       || ls_btch_log_txt;
                    IF t_tbl_err_keys.EXISTS (ls_err_key)
                    THEN         -- If this offr_id already updated; don't do it again
                       NULL;
                    ELSE
                       t_tbl_err_keys (ls_err_key) := parm_rec.trgt_mrkt_id;
                       p_status := 1;
                       l_continue_loop := 'N';
                       IF p_task_id IS NOT NULL
                       THEN
                          insert_btch_task_log (p_task_id,
                                                ls_btch_log_sub,
                                                ls_btch_log_key,
                                                ls_btch_err_svrty,
                                                ls_btch_log_txt,
                                                0,
                                                0,
                                                parm_rec.trgt_mrkt_id
                                               );
                       ELSE
                          app_plsql_output.add_msg('MARKET', ls_btch_log_key );
                          app_plsql_log.info (    ls_stage || ': ' ||  ls_btch_err_svrty
                                              || ': '
                                              || 'Market: '
                                              || ls_btch_log_key
                                              || ' '
                                              || ls_btch_log_txt
                                             );
                       END IF;
                    END IF;
                END IF;
             end if;
            ------------------------------------------------------------------------------------------------
            -- Check: One or more Target Market, Campaign, Vehicle, WIP Versions is Locked
             ls_stage := 'Stage4: '|| p_parm_id;
             ls_stage := 'Stage5: '|| p_parm_id;
             if l_job_typ in (1, 8) and l_process <> 1  then -- If it is forecast boost process do this validation  ; 1-> Forecast boost and 8 -> Reboost
                   BEGIN
                     SELECT NVL (frcst_boost_trgt_sls_amt, 0),
                             NVL (frcst_boost_trgt_actv_ind, 'N')
                        INTO l_fb_trgt_sls_amt,
                             l_fb_trgt_actv_ind
                        FROM mrkt_perd mp
                       WHERE mp.mrkt_id = parm_rec.trgt_mrkt_id
                         AND mp.perd_id = parm_rec.trgt_offr_perd_id;
                   EXCEPTION
                      WHEN NO_DATA_FOUND
                      THEN
                         l_fb_trgt_sls_amt := 0;
                         l_fb_trgt_actv_ind := 'N';
                   END;

                   ls_stage := 'Stage6: '|| p_parm_id;
                   IF l_fb_trgt_sls_amt = 0                 -- Some offrs are locked
                   THEN
                      ls_btch_log_sub := l_err_mrkt_lvl;
                      ls_btch_log_txt :=
                           'not Boosted because it does not have a Target Sales figure';
                      ls_btch_err_svrty := 'Error';
                      ls_btch_log_key := parm_rec.trgt_mrkt_id;
                      ls_err_key :=
                            p_task_id
                         || '-'
                         || ls_btch_log_sub
                         || '-'
                         || ls_btch_log_key
                         || '-'
                         || ls_btch_log_txt;
                     IF t_tbl_err_keys.EXISTS (ls_err_key)
                     THEN         -- If this offr_id already updated; don't do it again
                         NULL;
                     ELSE
                         t_tbl_err_keys (ls_err_key) := parm_rec.trgt_mrkt_id;
                         p_status := 1;
                         l_continue_loop := 'N';
                            insert_btch_task_log (p_task_id,
                                                  ls_btch_log_sub,
                                                  ls_btch_log_key,
                                                  ls_btch_err_svrty,
                                                  ls_btch_log_txt,
                                                  0,
                                                  0,
                                                  parm_rec.trgt_mrkt_id
                                                 );
                     END IF;
                   END IF;
             end if;

            ------------------------------------------------------------------------------------------------
            -- Delete the Existing Target Informations...
            ------------------------------------------------------------------------------------------------
             IF l_continue_loop <> 'N' THEN
             ls_stage := 'Stage7: '|| p_parm_id;
                ls_trgt_deleted_key :=
                      p_parm_id
                   || '-'
                   || parm_rec.trgt_mrkt_id
                   || '-'
                   || parm_rec.trgt_veh_id
                   || '-'
                   || parm_rec.trgt_offr_perd_id
                   || '-'
                   || parm_rec.trgt_ver_id;

                IF t_tbl_trgt_deleted_keys.EXISTS (ls_trgt_deleted_key)
                THEN         -- If this target already deleted
                   ls_trgt_deleted := 'Y';
                ELSE
                   t_tbl_trgt_deleted_keys (ls_trgt_deleted_key) := ls_trgt_deleted_key;
                    BEGIN
                       DELETE FROM est_lnkg
                             WHERE est_link_id IN (
                                                  SELECT est_lnkg.est_link_id
                                                    FROM offr_sku_line, est_lnkg, offr
                                                   WHERE offr_sku_line.offr_id = offr.offr_id
                                                     AND offr.mrkt_id = parm_rec.trgt_mrkt_id
                                                     AND offr.veh_id = parm_rec.trgt_veh_id
                                                     AND offr.offr_perd_id = parm_rec.trgt_offr_perd_id
                                                     AND offr.ver_id = parm_rec.trgt_ver_id
                                                    and offr_sku_line.offr_id = est_lnkg.offr_id);


                         ls_stage := 'Stage8: '|| p_parm_id;
                     EXCEPTION
                     WHEN OTHERS THEN
                        app_plsql_log.info('Error while deleting est_lnkg'|| SQLERRM(SQLCODE) );
                     END;
                     ls_stage := 'Stage8: '|| p_parm_id;
                     BEGIN
                            DELETE FROM DSTRBTD_MRKT_SLS WHERE OFFR_SKU_LINE_ID IN
                            (
                            SELECT osl.offr_sku_line_id
                              FROM offr_sku_line osl,
                                   offr_prfl_prc_point oppp,
                                   offr o
                             WHERE o.mrkt_id                = parm_rec.trgt_mrkt_id
                               AND o.veh_id                 = parm_rec.trgt_veh_id
                               AND o.offr_perd_id           = parm_rec.trgt_offr_perd_id
                               AND o.ver_id                 = parm_rec.trgt_ver_id
                               AND o.offr_id                = oppp.offr_id
                               and oppp.offr_prfl_prcpt_id  = osl.offr_prfl_prcpt_id
                            MINUS
                            SELECT osl.offr_sku_line_id
                              FROM offr_sku_line osl,
                                   offr_prfl_prc_point oppp,
                                   offr o
                             WHERE o.mrkt_id                = parm_rec.trgt_mrkt_id
                               AND o.veh_id                 = parm_rec.trgt_veh_id
                               AND o.offr_perd_id           = parm_rec.trgt_offr_perd_id
                               AND o.ver_id                 = parm_rec.trgt_ver_id
                               AND o.offr_id                = oppp.offr_id
                               AND oppp.offr_prfl_prcpt_id  = osl.offr_prfl_prcpt_id
                               AND EXISTS ( SELECT 'x'
                                              FROM offr_sku_line osl_in,
                                                   offr_prfl_prc_point oppp_in, offr o
                                             WHERE o.mrkt_id = parm_rec.srce_mrkt_id
                                               AND o.veh_id = parm_rec.srce_veh_id
                                               AND o.offr_perd_id = parm_rec.srce_offr_perd_id
                                               AND o.ver_id = parm_rec.srce_ver_id
                                               AND o.offr_id = oppp_in.offr_id
                                               AND oppp_in.offr_prfl_prcpt_id = osl_in.offr_prfl_prcpt_id
                                               AND osl_in.offr_sku_line_id = osl.offr_sku_line_link_id)
                            )
                            RETURNING offr_sku_line_id BULK COLLECT INTO v_dltd_osl_id;
                     EXCEPTION
                     WHEN OTHERS THEN
                            app_plsql_log.info('Error while deleting dstrbtd_mrkt_sls'|| SQLERRM(SQLCODE) );
                     END;
                     ls_stage := 'Stage9: '|| p_parm_id;
                     BEGIN
                          FORALL  I in  NVL(v_dltd_osl_id.FIRST,1) .. NVL(v_dltd_osl_id.LAST,0)
                                DELETE
                                  FROM OFFR_SKU_LINE
                                 WHERE OFFR_SKU_LINE_ID = v_dltd_osl_id(i)
                             RETURNING offr_sku_set_id, offr_prfl_prcpt_id, offr_id, sls_cls_cd, prfl_cd, pg_ofs_nr, featrd_side_cd, sku_id
                          BULK COLLECT
                                  INTO v_dltd_oss_id, v_dltd_oppp_id, v_dltd_offr_id, v_dltd_sls_cls_cd, v_dltd_prfl_cd, v_dltd_pg_ofs_nr,
                                       v_dltd_featrd_side_cd, v_dltd_sku_id;
                     EXCEPTION
                     WHEN OTHERS THEN
                        app_plsql_log.info('Error while deleting offr_sku_line'|| SQLERRM(SQLCODE) );
                     END;
                     ls_stage := 'Stage10: '|| p_parm_id;
                     -- Delete OFFR_SKU_LINE table;
                     BEGIN
                         select osl.offr_id, osl.offr_sku_line_id, osl.offr_sku_set_id, oss.line_nr, oss.line_nr_typ_id, oss.fsc_cd
                           bulk collect
                           into t_tbl_oss_line_nr
                           from offr_sku_line osl,
                                offr_sku_set oss
                          where osl.mrkt_id      = parm_rec.trgt_mrkt_id
                            AND osl.veh_id       = parm_rec.trgt_veh_id
                            AND osl.offr_perd_id = parm_rec.trgt_offr_perd_id
                            and osl.offr_sku_set_id = oss.offr_sku_set_id;
                        DELETE FROM offr_sku_set
                           WHERE mrkt_id =  parm_rec.trgt_mrkt_id
                             AND veh_id = parm_rec.trgt_veh_id
                             AND offr_perd_id = parm_rec.trgt_offr_perd_id
                             AND offr_id in
                                    (SELECT offr_id
                                       FROM offr
                                      WHERE mrkt_id = parm_rec.trgt_mrkt_id
                                        AND veh_id = parm_rec.trgt_veh_id
                                        AND offr_perd_id = parm_rec.trgt_offr_perd_id
                                        AND ver_id = parm_rec.trgt_ver_id
                                     MINUS
                                     SELECT trgt.offr_id
                                       FROM offr trgt, offr srce
                                      WHERE trgt.mrkt_id         = parm_rec.trgt_mrkt_id
                                        AND trgt.veh_id          = parm_rec.trgt_veh_id
                                        AND trgt.offr_perd_id    = parm_rec.trgt_offr_perd_id
                                        AND trgt.ver_id          = parm_rec.trgt_ver_id
                                        AND srce.mrkt_id         = parm_rec.srce_mrkt_id
                                        AND srce.veh_id          = parm_rec.srce_veh_id
                                        AND srce.offr_perd_id    = parm_rec.srce_offr_perd_id
                                        AND srce.ver_id          = parm_rec.srce_ver_id
                                        AND trgt.offr_link_id    = srce.offr_id
                                    );

                        DELETE FROM offr_sku_set
                           WHERE mrkt_id =  parm_rec.trgt_mrkt_id
                             AND veh_id = parm_rec.trgt_veh_id
                             AND offr_perd_id = parm_rec.trgt_offr_perd_id
                             AND offr_sku_set_id not in
                                    (SELECT offr_sku_set_id
                                       FROM offr_sku_line
                                      WHERE offr_sku_line.offr_id = offr_sku_set.offr_id);

                     EXCEPTION
                     WHEN OTHERS THEN
                        app_plsql_log.info('Error while deleting offr_sku_set'|| SQLERRM(SQLCODE) );
                     END;
                     ls_stage := 'Stage11: '|| p_parm_id;
                     begin
                          IF v_dltd_offr_id.COUNT > 0 THEN
                              FOR I IN NVL(v_dltd_offr_id.FIRST,1)..NVL(v_dltd_offr_id.LAST,0)
                              LOOP
                                  BEGIN
                                      DELETE FROM offr_sls_cls_sku
                                       WHERE offr_id        = v_dltd_offr_id(i)
                                         AND sls_cls_cd     = v_dltd_sls_cls_cd(i)
                                         AND prfl_cd        = v_dltd_prfl_cd(i)
                                         AND pg_ofs_nr      = v_dltd_pg_ofs_nr(i)
                                         AND featrd_side_cd = v_dltd_featrd_side_cd(i)
                                         AND sku_id         = v_dltd_sku_id(i) ;
                                  EXCEPTION
                                  WHEN OTHERS THEN
                                    NULL;--it is a valid case that we have an exception here !!! it is ok and we can continue!!!
                                  END;
                              END LOOP;
                          END IF;
                     exception
                     when others then
                      app_plsql_log.info('Error while deleting offr_sls_cls_sku'|| SQLERRM(SQLCODE) );
                     end;

                     -- Delete OFFR_PRFL_PRC_POINT table; done - 9
                     ls_stage := 'Stage12: '|| p_parm_id;
                     begin
                             begin
                                  IF v_dltd_oppp_id.COUNT > 0 THEN
                                      FOR I IN NVL(v_dltd_oppp_id.FIRST,1)..NVL(v_dltd_oppp_id.LAST,0)
                                      LOOP
                                          BEGIN
                                              DELETE FROM offr_prfl_prc_point
                                               WHERE offr_prfl_prcpt_id =  v_dltd_oppp_id(i);

                                          EXCEPTION
                                          WHEN OTHERS THEN
                                            NULL; -- it is ok if we have an exception here... we should continue...
                                          END;
                                      END LOOP;
                                  END IF;
                             exception
                             when others then
                              app_plsql_log.info('Error while deleting offr_sls_cls_sku'|| SQLERRM(SQLCODE) );
                             end;
                                DELETE FROM OFFR_PRFL_PRC_POINT WHERE OFFR_PRFL_PRCPT_ID IN
                                (
                                SELECT oppp.offr_prfl_prcpt_id
                                  FROM OFFR_PRFL_PRC_POINT oppp, offr o
                                 WHERE o.mrkt_id = parm_rec.trgt_mrkt_id
                                   AND o.veh_id = parm_rec.trgt_veh_id
                                   AND o.offr_perd_id = parm_rec.trgt_offr_perd_id
                                   AND o.offr_id = oppp.offr_id
                                   AND 0 = (select count(*) from offr_sku_line where offr_sku_line.offr_prfl_prcpt_id = oppp.offr_prfl_prcpt_id)
                                );
                     exception
                     when others then
                      app_plsql_log.info('Error while deleting offr_prfl_prc_point'|| SQLERRM(SQLCODE) );
                     end;
                     ls_stage := 'Stage13: '|| p_parm_id;
                     begin
                               BEGIN
                                    delete from offr_prfl_sls_cls_plcmt
                                     where (offr_id, sls_cls_cd, prfl_cd, pg_ofs_nr, featrd_side_cd) in
                                    (select opscp.offr_id, opscp.sls_cls_cd, opscp.prfl_cd, opscp.pg_ofs_nr, opscp.featrd_side_cd
                                              from offr_prfl_sls_cls_plcmt opscp, offr o
                                             where not exists-- (opscp.offr_id, opscp.sls_cls_cd, opscp.prfl_cd, opscp.pg_ofs_nr, opscp.featrd_side_cd)
                                                   (select oppp.offr_id, oppp.sls_cls_cd, oppp.prfl_cd, oppp.pg_ofs_nr, oppp.featrd_side_cd
                                                      FROM OFFR_PRFL_PRC_POINT oppp, offr o
                                                     WHERE o.mrkt_id = parm_rec.trgt_mrkt_id
                                                       AND o.veh_id = parm_rec.trgt_veh_id
                                                       AND o.offr_perd_id = parm_rec.trgt_offr_perd_id
                                                       AND o.ver_id = parm_rec.trgt_ver_id
                                                       AND o.offr_id = oppp.offr_id
                                                       and oppp.offr_id = opscp.offr_id
                                                       and oppp.sls_cls_cd = opscp.sls_cls_cd
                                                       and oppp.prfl_cd = opscp.prfl_cd
                                                       and oppp.pg_ofs_nr= opscp.pg_ofs_nr
                                                       and oppp.featrd_side_cd = opscp.featrd_side_cd
                                                   )
                                               and o.offr_id = opscp.offr_id
                                               and o.mrkt_id = parm_rec.trgt_mrkt_id
                                               AND o.veh_id = parm_rec.trgt_veh_id
                                               AND o.offr_perd_id = parm_rec.trgt_offr_perd_id
                                               AND o.ver_id = parm_rec.trgt_ver_id
                                    );
                               EXCEPTION
                               WHEN OTHERS THEN
                                 NULL; --it is ok... we can continue...
                               END;
                     exception
                     when others then
                      app_plsql_log.info('Error while deleting offr_prfl_sls_cls_plcmt' || sqlerrm(sqlcode));
                     end;
                     ls_stage := 'Stage14: '|| p_parm_id;
                     BEGIN
                        DELETE
                        --SELECT *
                         FROM offr_trgt
                        where OFFR_ID IN
                            (SELECT offr_id
                               FROM offr
                              WHERE mrkt_id = parm_rec.trgt_mrkt_id
                                AND veh_id = parm_rec.trgt_veh_id
                                AND offr_perd_id = parm_rec.trgt_offr_perd_id
                                AND ver_id = parm_rec.trgt_ver_id
                             MINUS
                             SELECT trgt.offr_id
                               FROM offr trgt, offr srce
                              WHERE trgt.mrkt_id         = parm_rec.trgt_mrkt_id
                                AND trgt.veh_id          = parm_rec.trgt_veh_id
                                AND trgt.offr_perd_id    = parm_rec.trgt_offr_perd_id
                                AND trgt.ver_id          = parm_rec.trgt_ver_id
                                AND srce.mrkt_id         = parm_rec.srce_mrkt_id
                                AND srce.veh_id          = parm_rec.srce_veh_id
                                AND srce.offr_perd_id    = parm_rec.srce_offr_perd_id
                                AND srce.ver_id          = parm_rec.srce_ver_id
                                AND trgt.offr_link_id    = srce.offr_id
                            );

                     exception
                     when others then
                       app_plsql_log.info('Error while deleting offr_trgt' || sqlerrm(sqlcode));
                     end;

                     -- Delete Offr Table; done - 12
                     ls_stage := 'Stage15: '|| p_parm_id;
                     BEGIN
                            DELETE
                              FROM OFFR
                            where OFFR_ID IN
                            (SELECT offr_id
                               FROM offr
                              WHERE mrkt_id = parm_rec.trgt_mrkt_id
                                AND veh_id = parm_rec.trgt_veh_id
                                AND offr_perd_id = parm_rec.trgt_offr_perd_id
                                AND ver_id = parm_rec.trgt_ver_id
                             MINUS
                             SELECT trgt.offr_id
                               FROM offr trgt, offr srce
                              WHERE trgt.mrkt_id         = parm_rec.trgt_mrkt_id
                                AND trgt.veh_id          = parm_rec.trgt_veh_id
                                AND trgt.offr_perd_id    = parm_rec.trgt_offr_perd_id
                                AND trgt.ver_id          = parm_rec.trgt_ver_id
                                AND srce.mrkt_id         = parm_rec.srce_mrkt_id
                                AND srce.veh_id          = parm_rec.srce_veh_id
                                AND srce.offr_perd_id    = parm_rec.srce_offr_perd_id
                                AND srce.ver_id          = parm_rec.srce_ver_id
                                AND trgt.offr_link_id    = srce.offr_id
                            );
                     exception
                     when others then
                       app_plsql_log.info('Error while deleting offr' || sqlerrm(sqlcode));
                     end;
                     select ver_typ_id
                       into l_ver_typ_id
                       from ver
                      where ver_id = parm_rec.trgt_ver_id;

                     if l_ver_typ_id = 1 then
                          select max(mps_sls_typ_id)
                            into l_mps_sls_typ_id
                            from mrkt_veh_perd_ver
                           where mrkt_id = parm_rec.trgt_mrkt_id
                             and veh_id = parm_rec.trgt_veh_id
                             and offr_perd_id = parm_rec.trgt_offr_perd_id
                             and ver_id = parm_rec.trgt_ver_id;
                         if l_mps_sls_typ_id is null then
                              select max(mps_sls_typ_id)
                                into l_mps_sls_typ_id
                                from mrkt_veh_perd_ver
                               where mrkt_id = parm_rec.trgt_mrkt_id
                                 and veh_id = parm_rec.trgt_veh_id
                                 and offr_perd_id = parm_rec.trgt_offr_perd_id
                                 and ver_id = 0;
                         end if;
                     else
                          select max(mps_sls_typ_id)
                            into l_mps_sls_typ_id
                            from mrkt_veh_perd_ver
                           where mrkt_id = parm_rec.trgt_mrkt_id
                             and veh_id = parm_rec.trgt_veh_id
                             and offr_perd_id = parm_rec.trgt_offr_perd_id
                             and ver_id = 0;
                     end if;

                     ls_stage := 'Stage16: '|| p_parm_id;
                     BEGIN
                        INSERT INTO MRKT_VEH_PERD_VER MVPV
                                   (MRKT_ID,
                                    VEH_ID,
                                    OFFR_PERD_ID,
                                    VER_ID,
                                    MPS_SLS_TYP_ID,
                                    CREAT_USER_ID,
                                    CREAT_TS,
                                    LAST_UPDT_USER_ID,
                                    LAST_UPDT_TS,
                                    CMPGN_VER_CREAT_USER_ID ,
                                    CMPGN_VER_CREAT_DT
                                    )
                            (select
                             parm_rec.trgt_mrkt_id,
                                    parm_rec.trgt_veh_id,
                                    parm_rec.trgt_offr_perd_id,
                                    parm_rec.trgt_ver_id,
                                    l_mps_sls_typ_id,
                                    p_user_nm,
                                    sysdate,
                                    null,
                                    null,
                                    decode(parm_rec.srce_ver_id, parm_rec.trgt_ver_id, null, p_user_nm),
                                    decode(parm_rec.srce_ver_id, parm_rec.trgt_ver_id, null, sysdate)
                               from mrkt_veh_perd_ver
                              where mrkt_id         = parm_rec.srce_mrkt_id
                                and offr_perd_id    = parm_rec.srce_offr_perd_id
                                and veh_id          = parm_rec.srce_veh_id
                                and ver_id          = parm_rec.srce_ver_id
                              );
                     EXCEPTION
                     WHEN DUP_VAL_ON_INDEX THEN
                            UPDATE MRKT_VEH_PERD_VER MVPV
                               SET LAST_UPDT_USER_ID         = p_user_nm,
                                   LAST_UPDT_TS              = sysdate,
                                   CMPGN_VER_CREAT_USER_ID   = p_user_nm,
                                   CMPGN_VER_CREAT_DT        = decode(parm_rec.trgt_ver_id, 0, null, sysdate),
                                   MPS_SLS_TYP_ID            = l_mps_sls_typ_id
                             WHERE mrkt_id = parm_rec.trgt_mrkt_id
                               and veh_id = parm_rec.trgt_veh_id
                               and offr_perd_id = parm_rec.trgt_offr_perd_id
                               and ver_id = parm_rec.trgt_ver_id;
                             ls_stage := 'Stage17: '|| p_parm_id;
                     WHEN OTHERS THEN
                        NULL;
                     END;
                    ---DELETE THE EXISTING VALUES.
                    BEGIN
                        DELETE FROM MRKT_VEH_PERD_SCTN
                         WHERE MRKT_ID           = parm_rec.trgt_mrkt_id
                           AND   OFFR_PERD_ID    = parm_rec.trgt_offr_perd_id
                           AND   VER_ID          = parm_rec.trgt_ver_id
                           AND   VEH_ID          = parm_rec.trgt_veh_id;
                    EXCEPTION WHEN OTHERS THEN
                        NULL;
                    END;

                    BEGIN
                        INSERT INTO MRKT_VEH_PERD_SCTN
                                                (MRKT_ID,
                                              OFFR_PERD_ID           ,
                                              VER_ID                 ,
                                              MRKT_VEH_PERD_SCTN_ID  ,
                                              SCTN_SEQ_NR            ,
                                              VEH_ID                 ,
                                              BRCHR_PLCMT_ID         ,
                                              PG_CNT                 ,
                                              SCTN_NM                ,
                                              STRTG_PAGE_NR          ,
                                              STRTG_PAGE_SIDE_NR     ,
                                              CREAT_USER_ID          ,
                                              CREAT_TS               ,
                                              LAST_UPDT_USER_ID      ,
                                              LAST_UPDT_TS)
                                    SELECT  parm_rec.trgt_mrkt_id,
                                            parm_rec.trgt_offr_perd_id,
                                            parm_rec.trgt_ver_id,
                                            MRKT_VEH_PERD_SCTN_ID,
                                            SCTN_SEQ_NR,
                                            parm_rec.trgt_veh_id,
                                            BRCHR_PLCMT_ID,
                                            PG_CNT,
                                            SCTN_NM,
                                            STRTG_PAGE_NR,
                                            STRTG_PAGE_SIDE_NR,
                                            p_user_nm,
                                            sysdate,
                                            p_user_nm,
                                            sysdate
                          FROM MRKT_VEH_PERD_SCTN
                         WHERE MRKT_ID      = parm_rec.srce_mrkt_id
                           AND OFFR_PERD_ID = parm_rec.srce_offr_perd_id
                           AND VER_ID       = parm_rec.srce_ver_id
                           AND VEH_ID       = parm_rec.srce_veh_id;
                    EXCEPTION WHEN OTHERS THEN
                        NULL;
                    END;

                    -- Create Version functionality (where source and target market, offer period and vehicle are the same)
                    -- should not delete mrkt_veh_perd_plcmt rows
                    IF parm_rec.trgt_mrkt_id      = parm_rec.srce_mrkt_id AND
                       parm_rec.trgt_offr_perd_id = parm_rec.srce_offr_perd_id AND
                       parm_rec.trgt_veh_id       = parm_rec.srce_veh_id
                    THEN
                      NULL;
                      -- Do nothing for create version functionality
                    ELSE
                      BEGIN
                          DELETE FROM mrkt_veh_perd_brchr_plcmt
                           WHERE MRKT_ID      = parm_rec.trgt_mrkt_id
                             AND OFFR_PERD_ID = parm_rec.trgt_offr_perd_id
                             AND VEH_ID       = parm_rec.trgt_veh_id;
                      EXCEPTION WHEN OTHERS THEN
                          NULL;
                      END;

                      BEGIN
                          INSERT INTO mrkt_veh_perd_brchr_plcmt
                                   (MRKT_ID,
                                    VEH_ID             ,
                                    OFFR_PERD_ID       ,
                                    BRCHR_PLCMT_ID     ,
                                    PG_CNT             ,
                                    CREAT_USER_ID      ,
                                    CREAT_TS           ,
                                    LAST_UPDT_USER_ID  ,
                                    LAST_UPDT_TS)
                           SELECT parm_rec.trgt_mrkt_id,
                                          parm_rec.trgt_veh_id,
                                          parm_rec.trgt_offr_perd_id,
                                          BRCHR_PLCMT_ID,
                                          PG_CNT,
                                          p_user_nm,
                                          sysdate,
                                          p_user_nm,
                                          sysdate
                                  FROM mrkt_veh_perd_brchr_plcmt
                                  WHERE MRKT_ID      = parm_rec.srce_mrkt_id
                                  AND   OFFR_PERD_ID = parm_rec.srce_offr_perd_id
                                  AND   VEH_ID       = parm_rec.srce_veh_id;
                      EXCEPTION WHEN OTHERS THEN
                          NULL;
                      END;
                    END IF;
                    ls_stage := 'Stage18: '|| p_parm_id;
                    ls_trgt_deleted := 'N';

                END IF;

                T_TBL_OFFR_KEYS.DELETE;
                T_TBL_OFFR_TRGT_KEYS.DELETE;
                T_TBL_OPSCP_KEYS.DELETE;
                T_TBL_OPPP_KEYS.DELETE;
                T_TBL_OSCS_KEYS.DELETE;
                T_TBL_OSS_KEYS.DELETE;
                T_TBL_OSL_KEYS.DELETE;
                T_TBL_EL_KEYS.DELETE;
                T_TBL_DMS_KEYS.DELETE;

                SELECT MIN(perd_ofs_nr) minoffset ,MAX(perd_ofs_nr) maxoffset
                  INTO ln_min_ofs, ln_max_ofs
                  FROM ofs_dstrbtn_pct
                 WHERE mrkt_id = parm_rec.trgt_mrkt_id
                   AND offr_perd_id = parm_rec.trgt_offr_perd_id
                   AND veh_id = parm_rec.trgt_veh_id;

                SELECT MRKT_PERD.CRNCY_CD
                  INTO ls_crncy_cd
                  FROM mrkt_perd
                 WHERE mrkt_id = parm_rec.trgt_mrkt_id
                   AND perd_id = parm_rec.trgt_offr_perd_id;

                SELECT util.tbl_to_str
                  (CURSOR (SELECT DISTINCT sls_cls_cd
                                      FROM frcst_boost_xclusn
                                     WHERE mrkt_id = parm_rec.trgt_mrkt_id --ln_src_mrkt_id
                                       AND parm_rec.trgt_offr_perd_id BETWEEN strt_offr_perd_id
                                                                  AND NVL
                                                                        (end_offr_perd_id,
                                                                         99990399
                                                                        )
                          )
                  ) fb_xcluded_sls_cls_cd
                INTO ls_fb_xcluded_sls_cls_cd
                FROM DUAL;

                --collect the list of valid vehicles
                IF ls_valid_veh_ids IS NULL
                THEN
                    ls_valid_veh_ids := parm_rec.srce_veh_id;
                ELSE
                    ls_valid_veh_ids := ls_valid_veh_ids||','||parm_rec.srce_veh_id;
                END IF;

               if l_process = 1 then
                create_version(parm_rec.srce_mrkt_id,
                               parm_rec.srce_veh_id,
                               parm_rec.srce_offr_perd_id,
                               parm_rec.srce_ver_id,
                               parm_rec.trgt_mrkt_id,
                               parm_rec.trgt_veh_id,
                               parm_rec.trgt_offr_perd_id,
                               parm_rec.trgt_ver_id,
                               p_parm_id,
                               p_user_nm,
                               p_task_id,
                               p_status
                              );
               else


               --- BATCH COPY -- KK START

               app_cpy_offr_trgt_prfls_info.init_trgt_sku_info(
                parm_rec.srce_mrkt_id,
                parm_rec.srce_offr_perd_id,
                parm_rec.trgt_mrkt_id,
                parm_rec.trgt_offr_perd_id
               );

               set_pricing_flags(parm_rec.trgt_mrkt_id, parm_rec.trgt_offr_perd_id, trgt_pricing_mrkt_ind, trgt_strtgy_ind);
               app_plsql_log.info('Target Market Pricing Enabled: '||trgt_pricing_mrkt_ind);
               app_plsql_log.info('Target Market Strategy Enabled: '||trgt_strtgy_ind);

                 FOR rec IN
                    (
        ---------------------------------------------------------------------------------------------------------------------------------
                      SELECT ROW_NUMBER( ) OVER (PARTITION BY
                      o.offr_id,
                      nvl(ood.oppp_prfl_cd,-1)
                                                      ORDER BY nvl(ood.oppp_prfl_cd,-1),
                                                               nvl(ood.offr_prfl_prcpt_id,-1),
                                                               nvl(ood.offr_sku_line_id,-1)
                                                 ) SRCE_PRFL_RN,
                              COUNT(ood.oppp_prfl_cd) OVER (PARTITION BY
                              o.offr_id,
                              nvl(ood.oppp_prfl_cd,-1)
                              ) TOTAL_PRFL_CD,
                              DECODE (NVL (offr_target.offr_id, -1),
                                      -1, 'OFFR_NEW',
                                      'OFFR_EXISTS'
                                     ) offr_rec_typ,
                              DECODE
                                   (NVL (opp_target.offr_prfl_prcpt_id, -1),
                                    -1, 'OPPP_NEW',
                                    'OPPP_EXISTS'
                                   ) oppp_rec_typ,
                              DECODE (NVL (osl_target.offr_sku_line_id, -1),
                                      -1, 'OSL_NEW',
                                      'OSL_EXISTS'
                                     ) osl_rec_typ,
                              --------------------------------------------
                              -- OFFR Records
                              --------------------------------------------
                              o.offr_id                         srce_offr_id,
                              o.mrkt_id                         srce_mrkt_id,
                              o.veh_id                          srce_veh_id,
                              o.offr_perd_id                    srce_offr_perd_id,
                              o.ver_id                          srce_ver_id,
                              ----------------------------------
                              offr_target.offr_id               trgt_offr_id,
                              parm_rec.trgt_mrkt_id             trgt_mrkt_id,
                              parm_rec.trgt_veh_id              trgt_veh_id,
                              parm_rec.trgt_offr_perd_id        trgt_offr_perd_id,
                              parm_rec.trgt_ver_id              trgt_ver_id,
                              ----------------------------------
                              o.offr_desc_txt,
                              o.offr_stus_cd,
                              o.offr_stus_rsn_desc_txt,
                              o.brchr_plcmt_id,
                              o.pg_wght_pct,
                              o.micr_ncpsltn_desc_txt,
                              o.micr_ncpsltn_ind,
                              o.mrkt_veh_perd_sctn_id,
                              o.sctn_page_ofs_nr,
                              o.featrd_side_cd,
                              o.enrgy_chrt_postn_id,
                              o.enrgy_chrt_offr_desc_txt,
                              o.offr_ntes_txt,
                              o.rpt_sbtl_typ_id,
                              o.unit_rptg_lvl_id,
                              o.offr_lyot_cmnts_txt,
                              o.est_srce_id,
                              o.est_stus_cd,
                              o.offr_typ,
                              o.std_offr_id,
                              o.offr_link_ind,
                              o.prfl_offr_strgth_pct,
                              o.prfl_cnt, o.sku_cnt,
                              o.ssnl_evnt_id,
                              o.brchr_postn_id,
                              o.frnt_cvr_ind,
                              o.pg_typ_id,
                              o.offr_prsntn_strnth_id,
                              o.flap_pg_wght_pct,
                              o.offr_cls_id,
                              o.offr_link_id,
                              o.bilng_perd_id, -- DRS 22Aug2011
                              o.shpng_perd_id, -- DRS 22Aug2011
                              o.cust_pull_id,  -- DRS 22Aug2011
                              o.flap_ind,      -- DRS 22Aug2011
                              ----------------------------------
                              offr_target.mrkt_veh_perd_sctn_id trgt_mrkt_veh_perd_sctn_id,
                              offr_target.brchr_plcmt_id        trgt_brchr_plcmt_id,
                              offr_target.sctn_page_ofs_nr      trgt_sctn_page_ofs_nr,
                              offr_target.crncy_cd              trgt_crncy_cd,
                              --------------------------------------------
                              -- OFFR_TRGT Records
                              --------------------------------------------
                              otrg.trgt_id                      otrg_trgt_id,
                              otrg.catgry_id                    otrg_catgry_id,
                              otrg.brnd_fmly_id                 otrg_brnd_fmly_id,
                              otrg.brnd_id                      otrg_brnd_id,
                              otrg.form_id                      otrg_form_id,
                              otrg.sgmt_id                      otrg_sgmt_id,
                              otrg.suplr_id                     otrg_suplr_id,
                              otrg.trgt_unit_qty                otrg_trgt_unit_qty,
                              otrg.trgt_sls_amt                 otrg_trgt_sls_amt,
                              otrg.trgt_cost_amt                otrg_trgt_cost_amt,
                              otrg.prft_amt                     otrg_prft_amt,
                              otrg.prft_pct                     otrg_prft_pct,
                              otrg.net_per_unit_amt             otrg_net_per_unit_amt,
                              otrg.units_per_rep_qty            otrg_units_per_rep_qty,
                              otrg.units_per_ord_qty            otrg_units_per_ord_qty,
                              otrg.net_per_rep_amt              otrg_net_per_rep_amt,
                              --------------------------------------------
                              -- OFFR_PRFL_SLS_CLS_CD Records
                              --------------------------------------------
                              --ood.opscp_sls_cls_cd,
                              ood.mpp_prfl_cd,
                              ood.opscp_prfl_cd,
                              ood.opscp_pg_ofs_nr,
                              ood.opscp_featrd_side_cd,
                              ood.opscp_prod_endrsmt_id,
                              ood.opscp_pg_wght_pct,
                              ood.opscp_fxd_pg_wght_ind,
                              ood.opscp_featrd_prfl_ind,
                              ood.opscp_use_instrctns_ind,
                              ood.opscp_sku_cnt,
                              --------------------------------------------
                              -- OFFR_PRFL_PRC_POINT Records
                              --------------------------------------------
                              ood.offr_prfl_prcpt_id            srce_oppp_id,
                              opp_target.offr_prfl_prcpt_id     trgt_oppp_id,
                              ood.oppp_offr_id,
                              ood.oppp_veh_id,
                              ood.oppp_mrkt_id,
                              ood.oppp_prfl_cd,
                              ood.oppp_offr_perd_id,
                              ood.oppp_pg_ofs_nr,
                              ood.oppp_featrd_side_cd,
                              ood.oppp_crncy_cd,
                              ood.oppp_impct_catgry_id,
                              ood.oppp_cnsmr_invstmt_bdgt_id,
                              ood.oppp_promtn_id,
                              ood.oppp_promtn_clm_id,
                              ood.trgt_prc_promtn_ind,
                              ood.prc_lvl_typ_cd,
                              ood.plnd_prc_lvl_typ_cd,
                              ----------------------------------
                              ood.oppp_sls_prc_amt,
                              decode(nvl(ood.srce_reg_prc_amt, 0),
                                     0,  0,
                                     round((ood.oppp_sls_prc_amt * ood.trgt_reg_prc_amt/ ood.srce_reg_prc_amt),2)
                                    )                           calct_trgt_sls_prc_amt,
                              opp_target.sls_prc_amt            exstg_trgt_sls_prc_amt,
                              opp_target.sls_cls_cd             trgt_sls_cls_cd,
                              ----------------------------------
                              ood.oppp_net_to_avon_fct,
                              ood.oppp_ssnl_evnt_id,
                              ood.oppp_sls_stus_cd,
                              ood.oppp_sku_cnt,
                              ood.oppp_sku_offr_strgth_pct,
                              ood.oppp_est_unit_qty,
                              ood.oppp_est_sls_amt,
                              ood.oppp_est_cost_amt,
                              ood.oppp_prfl_stus_rsn_desc_txt,
                              ood.oppp_prfl_stus_cd,
                              ood.oppp_tax_amt,
                              ood.oppp_comsn_amt,
                              ood.oppp_pymt_typ,
                              ood.oppp_prc_point_desc_txt,
                              ood.oppp_prmry_offr_ind,
                              ood.oppp_sls_promtn_ind,
                              ood.oppp_impct_prfl_cd,
                              ood.oppp_awrd_sls_prc_amt,
                              ood.oppp_nr_for_qty,
                              ood.oppp_chrty_amt,
                              ood.oppp_chrty_ovrrd_ind,
                              ood.oppp_roylt_pct,
                              ood.oppp_roylt_ovrrd_ind,
                              ood.oppp_tax_type_id,
                              ood.oppp_comsn_typ,
                              ood.oppp_offr_prfl_prcpt_link_id,
                              --------------------------------------------
                              -- Calculated Values
                              --------------------------------------------
                              ood.trgt_comsn_typ,
                              ood.trgt_tax_typ_id,
                              ood.charity,
                              ood.royalty,
                              ood.gta_mthd_id,
                              ----------------------------------
                              pa_maps_gta.get_commission_percentage
                              (
                                  parm_rec.trgt_mrkt_id,
                                  parm_rec.trgt_offr_perd_id,
                                  ood.trgt_comsn_typ
                              )  trgt_comsn_amt,
                              ----------------------------------
                              get_tax_rate
                              (parm_rec.trgt_mrkt_id,
                                             trgt_tax_typ_id,
                                             parm_rec.trgt_offr_perd_id
                              ) trgt_tax_amt,
                              --------------------------------------------
                              -- OFFR_SLS_CLS_SKU Records
                              --------------------------------------------
                              ood.oscs_prfl_cd,
                              ood.oscs_pg_ofs_nr,
                              ood.oscs_featrd_side_cd,
                              ood.oscs_sku_id,
                              ood.oscs_mrkt_id,
                              ood.oscs_hero_ind,
                              ood.oscs_micr_ncpsltn_ind,
                              ood.oscs_smplg_ind,
                              ood.oscs_mltpl_ind,
                              ood.oscs_cmltv_ind,
                              ood.oscs_incntv_id,
                              ood.oscs_reg_prc_amt,
                              ood.oscs_cost_amt,
                              --------------------------------------------
                              -- OFFR_SKU_LINE Records
                              --------------------------------------------
                              ood.offr_sku_line_id              srce_osl_id,
                              osl_target.offr_sku_line_id       trgt_osl_id,
                              ood.osl_offr_id,
                              ood.osl_veh_id,
                              ood.osl_featrd_side_cd,
                              ood.osl_offr_perd_id,
                              ood.osl_mrkt_id,
                              ood.osl_sku_id,
                              ood.osl_pg_ofs_nr,
                              ood.osl_crncy_cd,
                              ood.osl_prfl_cd,
                              ood.osl_offr_prfl_prcpt_id,
                              ood.osl_promtn_desc_txt,
                              ood.osl_sls_prc_amt,
                              ood.osl_cost_typ,
                              ood.osl_prmry_sku_offr_ind,
                              ood.osl_dltd_ind,
                              ood.osl_line_nr,
                              ood.osl_unit_splt_pct,
                              ood.osl_demo_avlbl_ind,
                              ood.osl_offr_sku_line_link_id,
                              ood.osl_set_cmpnt_ind,
                              ood.osl_set_cmpnt_qty,
                              ood.osl_offr_sku_set_id,
                              ood.osl_unit_prc_amt,
                              ood.osl_line_nr_typ_id,
                              --------------------------------------------
                              -- OFFR_SKU_SET Records
                              --------------------------------------------
                              ood.oss_offr_sku_set_id,
                              ood.oss_mrkt_id,
                              ood.oss_veh_id,
                              ood.oss_offr_perd_id,
                              ood.oss_offr_id,
                              ood.oss_offr_sku_set_nm,
                              ood.oss_set_prc_amt,
                              ood.oss_set_prc_typ_id,
                              ood.oss_pg_ofs_nr,
                              ood.oss_featrd_side_cd,
                              ood.oss_set_cmpnt_cnt,
                              ood.oss_crncy_cd,
                              ood.oss_promtn_desc_txt,
                              ood.oss_line_nr,
                              ood.oss_line_nr_typ_id,
                              ood.oss_offr_sku_set_sku_id,
                              --------------------------------------------
                              -- DSTRBTD_MRKT_SLS Records
                              --------------------------------------------
                              ood.dms_mrkt_id,
                              ood.dms_offr_perd_id,
                              ood.dms_veh_id,
                              ood.dms_sls_perd_id,
                              ood.dms_offr_sku_line_id,
                              ood.dms_sls_typ_id,
                              ood.dms_unit_qty,
                              ----------------------------------
                              osl_target.unit_qty               trgt_dms_unit_qty,
                              ----------------------------------
                              ood.dms_sls_stus_cd,
                              ood.dms_sls_srce_id,
                              ood.dms_prev_unit_qty,
                              ood.dms_prev_sls_srce_id,
                              ood.dms_comsn_amt,
                              ood.dms_tax_amt,
                              ood.dms_net_to_avon_fct,
                              ood.dms_unit_ovrrd_ind,
                              ood.dms_currnt_est_ind,
                              --------------------------------------------
                              -- Other Values
                              --------------------------------------------
                              ood.srce_reg_prc_amt,
                              ood.trgt_reg_prc_amt,
                              --------------------------------------------
                              -- Placeholders for Target price-level details
                              --------------------------------------------
                              null trgt_prc_lvl_typ_id,
                              null trgt_prc_lvl_typ_cd,
                              null trgt_min_prc_amt,
                              null trgt_opt_prc_amt,
                              null trgt_max_prc_amt,
                              null trgt_plnd_prc_lvl_typ_id,
                              null trgt_plnd_prc_lvl_typ_cd,
                              null trgt_plnd_min_prc_amt,
                              null trgt_plnd_opt_prc_amt,
                              null trgt_plnd_max_prc_amt,
                              null trgt_plnd_promtn_id
                         FROM offr o,
                              (SELECT dstrbtd_mrkt_sls.offr_perd_id,
                                      dstrbtd_mrkt_sls.unit_qty,
                                      offr_sku_set.offr_sku_set_id,
                                      -----------------------------------------
                                      offr_prfl_prc_point.offr_id,
                                      offr_prfl_prc_point.offr_prfl_prcpt_id,
                                      offr_prfl_prc_point.offr_id               oppp_offr_id,
                                      offr_prfl_prc_point.veh_id                oppp_veh_id,
                                      offr_prfl_prc_point.mrkt_id               oppp_mrkt_id,
                                      offr_prfl_prc_point.prfl_cd               oppp_prfl_cd,
                                      offr_prfl_prc_point.offr_perd_id          oppp_offr_perd_id,
                                      offr_prfl_prc_point.pg_ofs_nr             oppp_pg_ofs_nr,
                                      offr_prfl_prc_point.featrd_side_cd        oppp_featrd_side_cd,
                                      offr_prfl_prc_point.crncy_cd              oppp_crncy_cd,
                                      offr_prfl_prc_point.impct_catgry_id       oppp_impct_catgry_id,
                                      offr_prfl_prc_point.cnsmr_invstmt_bdgt_id oppp_cnsmr_invstmt_bdgt_id,
                                      offr_prfl_prc_point.promtn_id             oppp_promtn_id,
                                      offr_prfl_prc_point.promtn_clm_id         oppp_promtn_clm_id,
                                      offr_prfl_prc_point.sls_prc_amt           oppp_sls_prc_amt,
                                      offr_prfl_prc_point.net_to_avon_fct       oppp_net_to_avon_fct,
                                      offr_prfl_prc_point.ssnl_evnt_id          oppp_ssnl_evnt_id,
                                      offr_prfl_prc_point.sls_stus_cd           oppp_sls_stus_cd,
                                      offr_prfl_prc_point.sku_cnt               oppp_sku_cnt,
                                      offr_prfl_prc_point.sku_offr_strgth_pct   oppp_sku_offr_strgth_pct,
                                      offr_prfl_prc_point.est_unit_qty          oppp_est_unit_qty,
                                      offr_prfl_prc_point.est_sls_amt           oppp_est_sls_amt,
                                      offr_prfl_prc_point.est_cost_amt          oppp_est_cost_amt,
                                      offr_prfl_prc_point.prfl_stus_rsn_desc_txt oppp_prfl_stus_rsn_desc_txt,
                                      offr_prfl_prc_point.prfl_stus_cd          oppp_prfl_stus_cd,
                                      offr_prfl_prc_point.tax_amt               oppp_tax_amt,
                                      offr_prfl_prc_point.comsn_amt             oppp_comsn_amt,
                                      offr_prfl_prc_point.pymt_typ              oppp_pymt_typ,
                                      offr_prfl_prc_point.prc_point_desc_txt    oppp_prc_point_desc_txt,
                                      offr_prfl_prc_point.prmry_offr_ind        oppp_prmry_offr_ind,
                                      offr_prfl_prc_point.sls_promtn_ind        oppp_sls_promtn_ind,
                                      offr_prfl_prc_point.impct_prfl_cd         oppp_impct_prfl_cd,
                                      offr_prfl_prc_point.awrd_sls_prc_amt      oppp_awrd_sls_prc_amt,
                                      offr_prfl_prc_point.nr_for_qty            oppp_nr_for_qty,
                                      offr_prfl_prc_point.chrty_amt             oppp_chrty_amt,
                                      offr_prfl_prc_point.chrty_ovrrd_ind       oppp_chrty_ovrrd_ind,
                                      offr_prfl_prc_point.roylt_pct             oppp_roylt_pct,
                                      offr_prfl_prc_point.roylt_ovrrd_ind       oppp_roylt_ovrrd_ind,
                                      offr_prfl_prc_point.tax_type_id           oppp_tax_type_id,
                                      offr_prfl_prc_point.comsn_typ             oppp_comsn_typ,
                                      offr_prfl_prc_point.offr_prfl_prcpt_link_id oppp_offr_prfl_prcpt_link_id,
                                      nvl(mrkt_promtn.prc_promtn_ind, 'N')      trgt_prc_promtn_ind,
                                      offr_prfl_prc_point.prc_lvl_typ_cd,
                                      offr_prfl_prc_point.plnd_prc_lvl_typ_cd,
                                      ------------------------------------
                                      PA_MAPS_GTA.get_default_tax_type_id
                                      (parm_rec.trgt_mrkt_id,
                                       offr_prfl_prc_point.prfl_cd,
                                       (select pa_maps_public.get_sls_cls_cd
                                               (parm_rec.trgt_offr_perd_id,
                                                parm_rec.trgt_mrkt_id,
                                                ms_in.avlbl_perd_id ,
                                                ms_in.intrdctn_perd_id ,
                                                ms_in.demo_ofs_nr ,
                                                ms_in.demo_durtn_nr ,
                                                ms_in.new_durtn_nr ,
                                                ms_in.stus_perd_id  ,
                                                ms_in.dspostn_perd_id  ,
                                                ms_in.on_stus_perd_id
                                               )                                trgt_sales_cls_cd
                                          from mrkt_sku ms_in
                                         where ms_in.mrkt_id       = parm_rec.trgt_mrkt_id
                                           and ms_in.sku_id        = offr_sku_line.sku_id
                                       ),
                                       parm_rec.trgt_offr_perd_id,
                                       parm_rec.trgt_veh_id
                                      ) trgt_tax_typ_id,
                                      ------------------------------------
                                       pa_maps_gta.get_commission_type
                                       (parm_rec.trgt_mrkt_id,
                                        parm_rec.trgt_veh_id,
                                        parm_rec.trgt_offr_perd_id,
                                        offr_prfl_prc_point.prfl_cd,
                                        'Y',
                                        parm_rec.srce_mrkt_id,
                                        parm_rec.srce_veh_id,
                                        parm_rec.srce_offr_perd_id,
                                        offr_prfl_prc_point.comsn_typ,
                                        (select pa_maps_public.get_sls_cls_cd
                                               (parm_rec.trgt_offr_perd_id,
                                                parm_rec.trgt_mrkt_id,
                                                ms_in.avlbl_perd_id ,
                                                ms_in.intrdctn_perd_id ,
                                                ms_in.demo_ofs_nr ,
                                                ms_in.demo_durtn_nr ,
                                                ms_in.new_durtn_nr ,
                                                ms_in.stus_perd_id  ,
                                                ms_in.dspostn_perd_id  ,
                                                ms_in.on_stus_perd_id
                                               )                                trgt_sales_cls_cd
                                          from mrkt_sku ms_in
                                         where ms_in.mrkt_id       = parm_rec.trgt_mrkt_id
                                           and ms_in.sku_id        = offr_sku_line.sku_id
                                         )
                                       )                        trgt_comsn_typ,
                                       PA_MAPS_GTA.PRI_GET_CHARITY (
                                          parm_rec.trgt_mrkt_id,
                                          parm_rec.trgt_offr_perd_id,
                                          offr_prfl_prc_point.prfl_cd
                                       )  charity,                              --target
                                       PA_MAPS_GTA.PRI_GET_ROYALTY
                                       (
                                          parm_rec.trgt_mrkt_id,
                                          parm_rec.trgt_offr_perd_id,
                                          offr_prfl_prc_point.prfl_cd
                                       )  royalty,                              --target
                                      MRKT_PERD.GTA_MTHD_ID ,
                                      ------------------------------------
                                      offr_prfl_sls_cls_plcmt.sls_cls_cd        opscp_sls_cls_cd,
                                      mrkt_perd_prfl.prfl_cd                    mpp_prfl_cd,
                                      offr_prfl_sls_cls_plcmt.prfl_cd           opscp_prfl_cd,
                                      offr_prfl_sls_cls_plcmt.pg_ofs_nr         opscp_pg_ofs_nr,
                                      offr_prfl_sls_cls_plcmt.featrd_side_cd    opscp_featrd_side_cd,
                                      offr_prfl_sls_cls_plcmt.prod_endrsmt_id   opscp_prod_endrsmt_id,
                                      offr_prfl_sls_cls_plcmt.pg_wght_pct       opscp_pg_wght_pct,
                                      offr_prfl_sls_cls_plcmt.fxd_pg_wght_ind   opscp_fxd_pg_wght_ind,
                                      offr_prfl_sls_cls_plcmt.featrd_prfl_ind   opscp_featrd_prfl_ind,
                                      offr_prfl_sls_cls_plcmt.use_instrctns_ind opscp_use_instrctns_ind,
                                      offr_prfl_sls_cls_plcmt.sku_cnt           opscp_sku_cnt,
                                      ------------------------------------
                                      offr_sls_cls_sku.sls_cls_cd               oscs_sls_cls_cd,
                                      offr_sls_cls_sku.prfl_cd                  oscs_prfl_cd,
                                      offr_sls_cls_sku.pg_ofs_nr                oscs_pg_ofs_nr,
                                      offr_sls_cls_sku.featrd_side_cd           oscs_featrd_side_cd,
                                      offr_sls_cls_sku.sku_id                   oscs_sku_id,
                                      offr_sls_cls_sku.mrkt_id                  oscs_mrkt_id,
                                      offr_sls_cls_sku.hero_ind                 oscs_hero_ind,
                                      offr_sls_cls_sku.micr_ncpsltn_ind         oscs_micr_ncpsltn_ind,
                                      offr_sls_cls_sku.smplg_ind                oscs_smplg_ind,
                                      offr_sls_cls_sku.mltpl_ind                oscs_mltpl_ind,
                                      offr_sls_cls_sku.cmltv_ind                oscs_cmltv_ind,
                                      offr_sls_cls_sku.incntv_id                oscs_incntv_id,
                                      offr_sls_cls_sku.reg_prc_amt              oscs_reg_prc_amt,
                                      offr_sls_cls_sku.cost_amt                 oscs_cost_amt,
                                      ------------------------------------
                                      offr_sku_line.offr_sku_line_id,
                                      offr_sku_line.offr_id                     osl_offr_id,
                                      offr_sku_line.veh_id                      osl_veh_id,
                                      offr_sku_line.featrd_side_cd              osl_featrd_side_cd,
                                      offr_sku_line.offr_perd_id                osl_offr_perd_id,
                                      offr_sku_line.mrkt_id                     osl_mrkt_id,
                                      offr_sku_line.sku_id                      osl_sku_id,
                                      offr_sku_line.pg_ofs_nr                   osl_pg_ofs_nr,
                                      offr_sku_line.crncy_cd                    osl_crncy_cd,
                                      offr_sku_line.prfl_cd                     osl_prfl_cd,
                                      offr_sku_line.sls_cls_cd                  osl_sls_cls_cd,
                                      offr_sku_line.offr_prfl_prcpt_id          osl_offr_prfl_prcpt_id,
                                      offr_sku_line.promtn_desc_txt             osl_promtn_desc_txt,
                                      offr_sku_line.sls_prc_amt                 osl_sls_prc_amt,
                                      offr_sku_line.cost_typ                    osl_cost_typ,
                                      offr_sku_line.prmry_sku_offr_ind          osl_prmry_sku_offr_ind,
                                      offr_sku_line.dltd_ind                    osl_dltd_ind,
                                      offr_sku_line.line_nr                     osl_line_nr,
                                      offr_sku_line.unit_splt_pct               osl_unit_splt_pct,
                                      offr_sku_line.demo_avlbl_ind              osl_demo_avlbl_ind,
                                      offr_sku_line.offr_sku_line_link_id       osl_offr_sku_line_link_id,
                                      offr_sku_line.set_cmpnt_ind               osl_set_cmpnt_ind,
                                      offr_sku_line.set_cmpnt_qty               osl_set_cmpnt_qty,
                                      offr_sku_line.offr_sku_set_id             osl_offr_sku_set_id,
                                      offr_sku_line.unit_prc_amt                osl_unit_prc_amt,
                                      offr_sku_line.line_nr_typ_id              osl_line_nr_typ_id,
                                      ------------------------------------
                                      offr_sku_set.offr_sku_set_id              oss_offr_sku_set_id,
                                      offr_sku_set.mrkt_id                      oss_mrkt_id,
                                      offr_sku_set.veh_id                       oss_veh_id,
                                      offr_sku_set.offr_perd_id                 oss_offr_perd_id,
                                      offr_sku_set.offr_id                      oss_offr_id,
                                      offr_sku_set.offr_sku_set_nm              oss_offr_sku_set_nm,
                                      offr_sku_set.set_prc_amt                  oss_set_prc_amt,
                                      offr_sku_set.set_prc_typ_id               oss_set_prc_typ_id,
                                      offr_sku_set.pg_ofs_nr                    oss_pg_ofs_nr,
                                      offr_sku_set.featrd_side_cd               oss_featrd_side_cd,
                                      offr_sku_set.set_cmpnt_cnt                oss_set_cmpnt_cnt,
                                      offr_sku_set.crncy_cd                     oss_crncy_cd,
                                      offr_sku_set.promtn_desc_txt              oss_promtn_desc_txt,
                                      offr_sku_set.offr_sku_set_sku_id          oss_offr_sku_set_sku_id,
                                      offr_sku_set.line_nr                      oss_line_nr,
                                      offr_sku_set.line_nr_typ_id               oss_line_nr_typ_id,
                                      ------------------------------------
                                      dstrbtd_mrkt_sls.mrkt_id                  dms_mrkt_id,
                                      dstrbtd_mrkt_sls.offr_perd_id             dms_offr_perd_id,
                                      dstrbtd_mrkt_sls.veh_id                   dms_veh_id,
                                      dstrbtd_mrkt_sls.sls_perd_id              dms_sls_perd_id,
                                      dstrbtd_mrkt_sls.offr_sku_line_id         dms_offr_sku_line_id,
                                      dstrbtd_mrkt_sls.sls_typ_id               dms_sls_typ_id,
                                      dstrbtd_mrkt_sls.unit_qty                 dms_unit_qty,
                                      dstrbtd_mrkt_sls.sls_stus_cd              dms_sls_stus_cd,
                                      dstrbtd_mrkt_sls.sls_srce_id              dms_sls_srce_id,
                                      dstrbtd_mrkt_sls.prev_unit_qty            dms_prev_unit_qty,
                                      dstrbtd_mrkt_sls.prev_sls_srce_id         dms_prev_sls_srce_id,
                                      dstrbtd_mrkt_sls.comsn_amt                dms_comsn_amt,
                                      dstrbtd_mrkt_sls.tax_amt                  dms_tax_amt,
                                      dstrbtd_mrkt_sls.net_to_avon_fct          dms_net_to_avon_fct,
                                      dstrbtd_mrkt_sls.unit_ovrrd_ind           dms_unit_ovrrd_ind,
                                      dstrbtd_mrkt_sls.currnt_est_ind           dms_currnt_est_ind,
                                      --------------------------------------------
                                      srce_reg_prc.reg_prc_amt                  srce_reg_prc_amt,
                                      trgt_reg_prc.reg_prc_amt                  trgt_reg_prc_amt
                                      --------------------------------------------
                                 FROM mrkt,
                                      mrkt_perd,
                                      mrkt_veh_perd,
                                      mrkt_veh_perd_ver,
                                      offr,
                                      offr_prfl_prc_point,
                                      offr_sku_line,
                                      offr_prfl_sls_cls_plcmt,
                                      offr_sls_cls_sku,
                                      dstrbtd_mrkt_sls,
                                      mrkt_sku,
                                      mrkt_perd_prfl,
                                      (select * from sku_reg_prc
                                        where  mrkt_id        = parm_rec.srce_mrkt_id
                                          and offr_perd_id    = parm_rec.srce_offr_perd_id
                                       ) srce_reg_prc,
                                      (select * from sku_reg_prc
                                        where  mrkt_id        = parm_rec.trgt_mrkt_id
                                          and offr_perd_id    = parm_rec.trgt_offr_perd_id
                                      ) trgt_reg_prc,
                                      prfl,
                                      offr_sku_set,
                                      mrkt_promtn
                                WHERE mrkt.mrkt_id                              = parm_rec.srce_mrkt_id
                                  AND mrkt_perd.mrkt_id                         = mrkt.mrkt_id
                                  AND mrkt_perd.perd_id                         = parm_rec.srce_offr_perd_id
                                  AND mrkt_veh_perd.mrkt_id                     = mrkt_perd.mrkt_id
                                  AND mrkt_veh_perd.veh_id                      = parm_rec.srce_veh_id
                                  AND mrkt_veh_perd.offr_perd_id                = mrkt_perd.perd_id
                                  AND mrkt_veh_perd_ver.mrkt_id                 = mrkt_veh_perd.mrkt_id
                                  AND mrkt_veh_perd_ver.veh_id                  = mrkt_veh_perd.veh_id
                                  AND mrkt_veh_perd_ver.offr_perd_id            = mrkt_veh_perd.offr_perd_id
                                  AND mrkt_veh_perd_ver.ver_id                  = parm_rec.srce_ver_id
                                  AND offr.mrkt_id                              = mrkt_veh_perd_ver.mrkt_id
                                  AND offr.veh_id                               = mrkt_veh_perd_ver.veh_id
                                  AND offr.offr_perd_id                         = mrkt_veh_perd_ver.offr_perd_id
                                  AND offr.ver_id                               = mrkt_veh_perd_ver.ver_id
                                  AND offr.offr_typ                             = 'CMP'
                                  AND offr.offr_id                              = offr_prfl_prc_point.offr_id
                                  AND offr_prfl_prc_point.offr_prfl_prcpt_id    = offr_sku_line.offr_prfl_prcpt_id
                                  AND mrkt_promtn.mrkt_id(+)                    = parm_rec.trgt_mrkt_id
                                  AND mrkt_promtn.promtn_id(+)                  = offr_prfl_prc_point.promtn_id
                                  AND offr_prfl_sls_cls_plcmt.offr_id           = offr_prfl_prc_point.offr_id
                                  AND offr_prfl_sls_cls_plcmt.sls_cls_cd        = offr_prfl_prc_point.sls_cls_cd
                                  AND offr_prfl_sls_cls_plcmt.prfl_cd           = offr_prfl_prc_point.prfl_cd
                                  AND offr_prfl_sls_cls_plcmt.pg_ofs_nr         = offr_prfl_prc_point.pg_ofs_nr
                                  AND offr_prfl_sls_cls_plcmt.featrd_side_cd    = offr_prfl_prc_point.featrd_side_cd
                                  AND offr_prfl_sls_cls_plcmt.mrkt_id           = offr_prfl_prc_point.mrkt_id
                                  AND offr_prfl_sls_cls_plcmt.veh_id            = offr_prfl_prc_point.veh_id
                                  AND offr_prfl_sls_cls_plcmt.offr_perd_id      = offr_prfl_prc_point.offr_perd_id
                                  AND offr_sls_cls_sku.offr_id                  = offr_sku_line.offr_id
                                  AND offr_sls_cls_sku.sls_cls_cd               = offr_sku_line.sls_cls_cd
                                  AND offr_sls_cls_sku.prfl_cd                  = offr_sku_line.prfl_cd
                                  AND offr_sls_cls_sku.pg_ofs_nr                = offr_sku_line.pg_ofs_nr
                                  AND offr_sls_cls_sku.featrd_side_cd           = offr_sku_line.featrd_side_cd
                                  AND offr_sls_cls_sku.sku_id                   = offr_sku_line.sku_id
                                  AND offr_sls_cls_sku.mrkt_id                  = offr_sku_line.mrkt_id
                                  AND dstrbtd_mrkt_sls.offr_sku_line_id         = offr_sku_line.offr_sku_line_id
                                  AND dstrbtd_mrkt_sls.sls_perd_id              = dstrbtd_mrkt_sls.offr_perd_id
        ----------------------------------------------------------------------------------
                                  AND (l_process <> 1 and dstrbtd_mrkt_sls.sls_typ_id               = mrkt_veh_perd_ver.mps_sls_typ_id)
                                  AND mrkt_sku.mrkt_id                          = parm_rec.srce_mrkt_id
                                  AND mrkt_sku.sku_id                           = offr_sku_line.sku_id
                                  AND mrkt_perd_prfl.mrkt_id                    = parm_rec.srce_mrkt_id
                                  AND mrkt_perd_prfl.offr_perd_id               =parm_rec.srce_offr_perd_id
                                  AND mrkt_perd_prfl.prfl_cd                    = offr_prfl_prc_point.prfl_cd
        ----------------------------------------------------------------------------------
        -- Regular Price Join
                                  AND srce_reg_prc.sku_id                       = offr_sku_line.sku_id
                                  AND srce_reg_prc.sku_id                       = trgt_reg_prc.sku_id (+)
        ----------------------------------------------------------------------------------
                                  AND prfl.prfl_cd                              = offr_prfl_prc_point.prfl_cd
                                  AND offr_sku_line.mrkt_id                     = offr_sku_set.mrkt_id(+)
                                  AND offr_sku_line.offr_id                     = offr_sku_set.offr_id(+)
                                  AND offr_sku_line.offr_perd_id                = offr_sku_set.offr_perd_id(+)
                                  AND offr_sku_line.veh_id                      = offr_sku_set.veh_id(+)
                                  AND offr_sku_line.offr_sku_set_id             = offr_sku_set.offr_sku_set_id(+)
                              ) ood,
                              offr_trgt otrg,
                              mrkt m,
                              mrkt_perd mp,
                              mrkt_veh_perd mvp,
                              mrkt_veh_perd_ver mvpv,
                              (SELECT *
                                 FROM mrkt_veh_perd_sctn
                                WHERE mrkt_veh_perd_sctn.mrkt_id                = parm_rec.srce_mrkt_id
                                  AND mrkt_veh_perd_sctn.offr_perd_id           = parm_rec.srce_offr_perd_id
                                  AND mrkt_veh_perd_sctn.veh_id                 = parm_rec.srce_veh_id
                                  AND mrkt_veh_perd_sctn.ver_id                 = parm_rec.srce_ver_id
                              ) mvps,
        ----------------------------------------------------------------------------------
                              (SELECT offr.offr_id,
                                      offr.offr_link_id,
                                      offr.mrkt_veh_perd_sctn_id,
                                      mrkt_perd.crncy_cd,
                                      offr.brchr_plcmt_id,
                                      offr.sctn_page_ofs_nr
                                 FROM mrkt,
                                      mrkt_perd,
                                      mrkt_veh_perd,
                                      mrkt_veh_perd_ver,
                                      offr
                                WHERE mrkt.mrkt_id                              = parm_rec.trgt_mrkt_id
                                  AND mrkt_perd.mrkt_id                         = mrkt.mrkt_id
                                  AND mrkt_perd.perd_id                         = parm_rec.trgt_offr_perd_id
                                  AND mrkt_veh_perd.mrkt_id                     = mrkt_perd.mrkt_id
                                  AND mrkt_veh_perd.veh_id                      = parm_rec.trgt_veh_id
                                  AND mrkt_veh_perd.offr_perd_id                = mrkt_perd.perd_id
                                  AND mrkt_veh_perd_ver.mrkt_id                 = mrkt_veh_perd.mrkt_id
                                  AND mrkt_veh_perd_ver.veh_id                  = mrkt_veh_perd.veh_id
                                  AND mrkt_veh_perd_ver.offr_perd_id            = mrkt_veh_perd.offr_perd_id
                                  AND mrkt_veh_perd_ver.ver_id                  = parm_rec.trgt_ver_id
                                  AND offr.mrkt_id                              = mrkt_veh_perd_ver.mrkt_id
                                  AND offr.veh_id                               = mrkt_veh_perd_ver.veh_id
                                  AND offr.offr_perd_id                         = mrkt_veh_perd_ver.offr_perd_id
                                  AND offr.ver_id                               = mrkt_veh_perd_ver.ver_id
                                  AND offr.offr_typ                             = 'CMP'
                              ) offr_target,
                              (SELECT *
                                 FROM mrkt_veh_perd_sctn
                                WHERE mrkt_veh_perd_sctn.mrkt_id                = parm_rec.trgt_mrkt_id
                                  AND mrkt_veh_perd_sctn.offr_perd_id           = parm_rec.trgt_offr_perd_id
                                  AND mrkt_veh_perd_sctn.veh_id                 = parm_rec.trgt_veh_id
                                  AND mrkt_veh_perd_sctn.ver_id                 = parm_rec.trgt_ver_id) trgt_mvps,
                              (SELECT offr_prfl_prc_point.offr_prfl_prcpt_id,
                                      offr_prfl_prc_point.offr_prfl_prcpt_link_id,
                                      offr_prfl_prc_point.sls_prc_amt,
                                      offr_prfl_prc_point.nr_for_qty,
                                      offr_prfl_prc_point.sls_cls_cd
                                 FROM mrkt,
                                      mrkt_perd,
                                      mrkt_veh_perd,
                                      mrkt_veh_perd_ver,
                                      offr,
                                      offr_prfl_prc_point
                                WHERE mrkt.mrkt_id                              = parm_rec.trgt_mrkt_id
                                  AND mrkt_perd.mrkt_id                         = mrkt.mrkt_id
                                  AND mrkt_perd.perd_id                         = parm_rec.trgt_offr_perd_id
                                  AND mrkt_veh_perd.mrkt_id                     = mrkt_perd.mrkt_id
                                  AND mrkt_veh_perd.veh_id                      = parm_rec.trgt_veh_id
                                  AND mrkt_veh_perd.offr_perd_id                = mrkt_perd.perd_id
                                  AND mrkt_veh_perd_ver.mrkt_id                 = mrkt_veh_perd.mrkt_id
                                  AND mrkt_veh_perd_ver.veh_id                  = mrkt_veh_perd.veh_id
                                  AND mrkt_veh_perd_ver.offr_perd_id            = mrkt_veh_perd.offr_perd_id
                                  AND mrkt_veh_perd_ver.ver_id                  = parm_rec.trgt_ver_id
                                  AND offr.mrkt_id                              = mrkt_veh_perd_ver.mrkt_id
                                  AND offr.veh_id                               = mrkt_veh_perd_ver.veh_id
                                  AND offr.offr_perd_id                         = mrkt_veh_perd_ver.offr_perd_id
                                  AND offr.ver_id                               = mrkt_veh_perd_ver.ver_id
                                  AND offr.offr_typ                             = 'CMP'
                                  AND offr.offr_id                              = offr_prfl_prc_point.offr_id
                                  ) opp_target,
                              (SELECT offr_sku_line.offr_sku_line_link_id,
                                      offr_sku_line.offr_sku_line_id,
                                      dstrbtd_mrkt_sls.unit_qty,
                                      offr_sku_line.offr_id
                                 FROM mrkt,
                                      mrkt_perd,
                                      mrkt_veh_perd,
                                      mrkt_veh_perd_ver,
                                      offr,
                                      offr_sku_line,
                                      dstrbtd_mrkt_sls
                                WHERE mrkt.mrkt_id                              = parm_rec.trgt_mrkt_id
                                  AND mrkt_perd.mrkt_id                         = mrkt.mrkt_id
                                  AND mrkt_perd.perd_id                         = parm_rec.trgt_offr_perd_id
                                  AND mrkt_veh_perd.mrkt_id                     = mrkt_perd.mrkt_id
                                  AND mrkt_veh_perd.veh_id                      = parm_rec.trgt_veh_id
                                  AND mrkt_veh_perd.offr_perd_id                = mrkt_perd.perd_id
                                  AND mrkt_veh_perd_ver.mrkt_id                 = mrkt_veh_perd.mrkt_id
                                  AND mrkt_veh_perd_ver.veh_id                  = mrkt_veh_perd.veh_id
                                  AND mrkt_veh_perd_ver.offr_perd_id            = mrkt_veh_perd.offr_perd_id
                                  AND mrkt_veh_perd_ver.ver_id                  = parm_rec.trgt_ver_id
                                  AND offr.mrkt_id                              = mrkt_veh_perd_ver.mrkt_id
                                  AND offr.veh_id                               = mrkt_veh_perd_ver.veh_id
                                  AND offr.offr_perd_id                         = mrkt_veh_perd_ver.offr_perd_id
                                  AND offr.ver_id                               = mrkt_veh_perd_ver.ver_id
                                  AND offr.offr_typ                             = 'CMP'
                                  AND offr.offr_id                              = offr_sku_line.offr_id
                                  AND offr_sku_line.offr_sku_line_id            = dstrbtd_mrkt_sls.offr_sku_line_id
                                  AND dstrbtd_mrkt_sls.sls_perd_id              = dstrbtd_mrkt_sls.offr_perd_id
                                  AND dstrbtd_mrkt_sls.sls_typ_id               = mrkt_veh_perd_ver.mps_sls_typ_id
                                 ) osl_target
                        WHERE m.mrkt_id = parm_rec.srce_mrkt_id
                          AND mp.mrkt_id = m.mrkt_id
                          AND mp.perd_id = parm_rec.srce_offr_perd_id
                          AND mvp.mrkt_id = mp.mrkt_id
                          AND mvp.veh_id = parm_rec.srce_veh_id
                          AND mvp.offr_perd_id = mp.perd_id
                          AND mvpv.mrkt_id = mvp.mrkt_id
                          AND mvpv.veh_id = mvp.veh_id
                          AND mvpv.offr_perd_id = mvp.offr_perd_id
                          AND mvpv.ver_id = parm_rec.srce_ver_id
                          AND o.mrkt_id = mvpv.mrkt_id
                          AND o.veh_id = mvpv.veh_id
                          AND o.offr_perd_id = mvpv.offr_perd_id
                          AND o.ver_id = mvpv.ver_id
                          AND o.offr_typ = 'CMP'
                          AND o.offr_id = otrg.offr_id(+)
                          AND o.offr_id = ood.offr_id(+)
                          AND o.mrkt_veh_perd_sctn_id = mvps.mrkt_veh_perd_sctn_id(+)
                          AND offr_target.mrkt_veh_perd_sctn_id = trgt_mvps.mrkt_veh_perd_sctn_id(+)
                          AND o.offr_id = offr_target.offr_link_id(+)
                          AND ood.offr_prfl_prcpt_id = opp_target.offr_prfl_prcpt_link_id(+)
                          AND ood.offr_sku_line_id = osl_target.offr_sku_line_link_id(+)
                     ORDER BY o.offr_id,
                              otrg.trgt_id,
                              ood.oppp_prfl_cd,
                              ood.offr_prfl_prcpt_id,
                              ood.offr_sku_line_id
        ---------------------------------------------------------------------------------------------------------------------------------
                    )
                 LOOP

                    trgt_pricing_details.delete;
                    strategy_exists_idx := 0;
                    strategy_used_idx   := 0;
                    price_level_idx     := 0;

                    ls_mvpv_key :=      rec.trgt_mrkt_id
                                     || '-'
                                     || rec.trgt_veh_id
                                     || '-'
                                     || rec.trgt_offr_perd_id
                                     || '-'
                                     || rec.trgt_ver_id;

                    IF t_tbl_mvpv_keys.EXISTS (ls_mvpv_key) THEN
                        null;
                    ELSE
                        t_tbl_mvpv_keys (ls_mvpv_key) := ls_mvpv_key;
                        ln_process_cnt := ln_process_cnt +1;
                    END IF;


                    app_plsql_log.info ('source data: '
                      || '(1)' || rec.SRCE_PRFL_RN
                      || '(2)' || rec.TOTAL_PRFL_CD
                      || '(3)' || rec.offr_rec_typ
                      || '(4)' || rec.oppp_rec_typ
                      || '(5)' || rec.osl_rec_typ
                      || '(6)' || rec.srce_offr_id
                      || '(7)' || rec.srce_mrkt_id
                      || '(8)' || rec.srce_veh_id
                      || '(9)' || rec.srce_offr_perd_id
                      || '(10)' || rec.srce_ver_id
                      || '(11)' || rec.trgt_offr_id
                      || '(12)' || rec.trgt_mrkt_id
                      || '(13)' || rec.trgt_veh_id
                      || '(14)' || rec.trgt_offr_perd_id
                      || '(15)' || rec.trgt_ver_id
                      || '(16)' || rec.offr_desc_txt
                      || '(17)' || rec.offr_stus_cd
                      || '(18)' || rec.offr_stus_rsn_desc_txt
                      || '(19)' || rec.brchr_plcmt_id
                      || '(20)' || rec.pg_wght_pct
                      || '(21)' || rec.micr_ncpsltn_desc_txt
                      || '(22)' || rec.micr_ncpsltn_ind
                      || '(23)' || rec.mrkt_veh_perd_sctn_id
                      || '(24)' || rec.sctn_page_ofs_nr
                      || '(25)' || rec.featrd_side_cd
                      || '(26)' || rec.enrgy_chrt_postn_id
                      || '(27)' || rec.enrgy_chrt_offr_desc_txt
                      || '(28)' || rec.offr_ntes_txt
                      || '(29)' || rec.rpt_sbtl_typ_id
                      || '(30)' || rec.unit_rptg_lvl_id
                      || '(31)' || rec.offr_lyot_cmnts_txt
                      || '(32)' || rec.est_srce_id
                      || '(33)' || rec.est_stus_cd
                      || '(34)' || rec.offr_typ
                      || '(35)' || rec.std_offr_id
                      || '(36)' || rec.offr_link_ind
                      || '(37)' || rec.prfl_offr_strgth_pct
                      || '(38)' || rec.prfl_cnt
                      || '(39)' || rec.sku_cnt
                      || '(40)' || rec.ssnl_evnt_id
                      || '(41)' || rec.brchr_postn_id
                      || '(42)' || rec.frnt_cvr_ind
                      || '(43)' || rec.pg_typ_id
                      || '(44)' || rec.offr_prsntn_strnth_id
                      || '(45)' || rec.flap_pg_wght_pct
                      || '(46)' || rec.offr_cls_id
                      || '(47)' || rec.offr_link_id
                      || '(48)' || rec.trgt_mrkt_veh_perd_sctn_id
                      || '(49)' || rec.trgt_brchr_plcmt_id
                      || '(50)' || rec.trgt_sctn_page_ofs_nr
                      || '(51)' || rec.trgt_crncy_cd
                      || '(52)' || rec.otrg_trgt_id
                      || '(53)' || rec.otrg_catgry_id
                      || '(54)' || rec.otrg_brnd_fmly_id
                      || '(55)' || rec.otrg_brnd_id
                      || '(56)' || rec.otrg_form_id
                      || '(57)' || rec.otrg_sgmt_id
                      || '(58)' || rec.otrg_suplr_id
                      || '(59)' || rec.otrg_trgt_unit_qty
                      || '(60)' || rec.otrg_trgt_sls_amt
                      || '(61)' || rec.otrg_trgt_cost_amt
                      || '(62)' || rec.otrg_prft_amt
                      || '(63)' || rec.otrg_prft_pct
                      || '(64)' || rec.otrg_net_per_unit_amt
                      || '(65)' || rec.otrg_units_per_rep_qty
                      || '(66)' || rec.otrg_units_per_ord_qty
                      || '(67)' || rec.otrg_net_per_rep_amt
                      --|| '(68)' || rec.opscp_sls_cls_cd
                      || '(69)' || rec.mpp_prfl_cd
                      || '(70)' || rec.opscp_prfl_cd
                      || '(71)' || rec.opscp_pg_ofs_nr
                      || '(72)' || rec.opscp_featrd_side_cd
                      || '(73)' || rec.opscp_prod_endrsmt_id
                      || '(74)' || rec.opscp_pg_wght_pct
                      || '(75)' || rec.opscp_fxd_pg_wght_ind
                      || '(76)' || rec.opscp_featrd_prfl_ind
                      || '(77)' || rec.opscp_use_instrctns_ind
                      || '(78)' || rec.opscp_sku_cnt
                      || '(79)' || rec.srce_oppp_id
                      || '(80)' || rec.trgt_oppp_id
                      || '(81)' || rec.oppp_offr_id
                      || '(82)' || rec.oppp_veh_id
                      || '(83)' || rec.oppp_mrkt_id
                      --|| '(84)' || rec.oppp_sls_cls_cd
                      || '(85)' || rec.oppp_prfl_cd
                      || '(86)' || rec.oppp_offr_perd_id
                      || '(87)' || rec.oppp_pg_ofs_nr
                      || '(88)' || rec.oppp_featrd_side_cd
                      || '(89)' || rec.oppp_crncy_cd
                      || '(90)' || rec.oppp_impct_catgry_id
                      || '(91)' || rec.oppp_cnsmr_invstmt_bdgt_id
                      || '(92)' || rec.oppp_promtn_id
                      || '(93)' || rec.oppp_promtn_clm_id
                      || '(94)' || rec.oppp_sls_prc_amt
                      || '(95)' || rec.calct_trgt_sls_prc_amt
                      || '(96)' || rec.exstg_trgt_sls_prc_amt
                      || '(97)' || rec.oppp_net_to_avon_fct
                      || '(98)' || rec.oppp_ssnl_evnt_id
                      || '(99)' || rec.oppp_sls_stus_cd
                      || '(100)' || rec.oppp_sku_cnt
                      || '(101)' || rec.oppp_sku_offr_strgth_pct
                      || '(102)' || rec.oppp_est_unit_qty
                      || '(103)' || rec.oppp_est_sls_amt
                      || '(104)' || rec.oppp_est_cost_amt
                      || '(105)' || rec.oppp_prfl_stus_rsn_desc_txt
                      || '(106)' || rec.oppp_prfl_stus_cd
                      || '(107)' || rec.oppp_tax_amt
                      || '(108)' || rec.oppp_comsn_amt
                      || '(109)' || rec.oppp_pymt_typ
                      || '(110)' || rec.oppp_prc_point_desc_txt
                      || '(111)' || rec.oppp_prmry_offr_ind
                      || '(112)' || rec.oppp_sls_promtn_ind
                      || '(113)' || rec.oppp_impct_prfl_cd
                      || '(114)' || rec.oppp_awrd_sls_prc_amt
                      || '(115)' || rec.oppp_nr_for_qty
                      || '(116)' || rec.oppp_chrty_amt
                      || '(117)' || rec.oppp_chrty_ovrrd_ind
                      || '(118)' || rec.oppp_roylt_pct
                      || '(119)' || rec.oppp_roylt_ovrrd_ind
                      || '(120)' || rec.oppp_tax_type_id
                      || '(121)' || rec.oppp_comsn_typ
                      || '(122)' || rec.oppp_offr_prfl_prcpt_link_id
                      || '(123)' || rec.trgt_comsn_typ
                      || '(124)' || rec.trgt_tax_typ_id
                      || '(125)' || rec.charity
                      || '(126)' || rec.royalty
                      || '(127)' || rec.gta_mthd_id
                      || '(128)' || rec.trgt_comsn_amt
                      || '(129)' || rec.trgt_tax_amt
                      --|| '(130)' || rec.oscs_sls_cls_cd
                      || '(131)' || rec.oscs_prfl_cd
                      || '(132)' || rec.oscs_pg_ofs_nr
                      || '(133)' || rec.oscs_featrd_side_cd
                      || '(134)' || rec.oscs_sku_id
                      || '(135)' || rec.oscs_mrkt_id
                      || '(136)' || rec.oscs_hero_ind
                      || '(137)' || rec.oscs_micr_ncpsltn_ind
                      || '(138)' || rec.oscs_smplg_ind
                      || '(138a)' || rec.oscs_mltpl_ind
                      || '(138b)' || rec.oscs_cmltv_ind
                      || '(139)' || rec.oscs_incntv_id
                      || '(140)' || rec.oscs_reg_prc_amt
                      || '(141)' || rec.oscs_cost_amt
                      || '(142)' || rec.srce_osl_id
                      || '(143)' || rec.trgt_osl_id
                      || '(144)' || rec.osl_offr_id
                      || '(145)' || rec.osl_veh_id
                      || '(146)' || rec.osl_featrd_side_cd
                      || '(147)' || rec.osl_offr_perd_id
                      || '(148)' || rec.osl_mrkt_id
                      || '(149)' || rec.osl_sku_id
                      || '(150)' || rec.osl_pg_ofs_nr
                      || '(151)' || rec.osl_crncy_cd
                      || '(152)' || rec.osl_prfl_cd
                      || '(153)' || rec.trgt_sls_cls_cd
                      || '(154)' || rec.osl_offr_prfl_prcpt_id
                      || '(155)' || rec.osl_promtn_desc_txt
                      || '(156)' || rec.osl_sls_prc_amt
                      || '(157)' || rec.osl_cost_typ
                      || '(158)' || rec.osl_prmry_sku_offr_ind
                      || '(159)' || rec.osl_dltd_ind
                      || '(160)' || rec.osl_line_nr
                      || '(161)' || rec.osl_unit_splt_pct
                      || '(162)' || rec.osl_demo_avlbl_ind
                      || '(163)' || rec.osl_offr_sku_line_link_id
                      || '(164)' || rec.osl_set_cmpnt_ind
                      || '(165)' || rec.osl_set_cmpnt_qty
                      || '(166)' || rec.osl_offr_sku_set_id
                      || '(167)' || rec.osl_unit_prc_amt
                      || '(168)' || rec.osl_line_nr_typ_id
                      || '(169)' || rec.oss_offr_sku_set_id
                      || '(170)' || rec.oss_mrkt_id
                      || '(171)' || rec.oss_veh_id
                      || '(172)' || rec.oss_offr_perd_id
                      || '(173)' || rec.oss_offr_id
                      || '(174)' || rec.oss_offr_sku_set_nm
                      || '(175)' || rec.oss_set_prc_amt
                      || '(176)' || rec.oss_set_prc_typ_id
                      || '(177)' || rec.oss_pg_ofs_nr
                      || '(178)' || rec.oss_featrd_side_cd
                      || '(179)' || rec.oss_set_cmpnt_cnt
                      || '(180)' || rec.oss_crncy_cd
                      || '(181)' || rec.oss_promtn_desc_txt
                      || '(182)' || rec.oss_line_nr
                      || '(183)' || rec.oss_line_nr_typ_id
                      || '(184)' || rec.oss_offr_sku_set_sku_id
                      || '(185)' || rec.dms_mrkt_id
                      || '(186)' || rec.dms_offr_perd_id
                      || '(187)' || rec.dms_veh_id
                      || '(188)' || rec.dms_sls_perd_id
                      || '(189)' || rec.dms_offr_sku_line_id
                      || '(190)' || rec.dms_sls_typ_id
                      || '(191)' || rec.dms_unit_qty
                      || '(192)' || rec.trgt_dms_unit_qty
                      || '(193)' || rec.dms_sls_stus_cd
                      || '(194)' || rec.dms_sls_srce_id
                      || '(195)' || rec.dms_prev_unit_qty
                      || '(196)' || rec.dms_prev_sls_srce_id
                      || '(197)' || rec.dms_comsn_amt
                      || '(198)' || rec.dms_tax_amt
                      || '(199)' || rec.dms_net_to_avon_fct
                      || '(200)' || rec.dms_unit_ovrrd_ind
                      || '(201)' || rec.dms_currnt_est_ind
                      || '(202)' || rec.srce_reg_prc_amt
                      || '(203)' || rec.trgt_reg_prc_amt
                      );

                    ---offer_code
                    ls_offr_key :=      rec.srce_offr_id
                                     || '-'
                                     || rec.trgt_mrkt_id
                                     || '-'
                                     || rec.trgt_veh_id
                                     || '-'
                                     || rec.trgt_offr_perd_id;

                    -- Collect all the offer Info ; Starts Here
                    CASE l_process
                       WHEN 1 THEN
                            NULL;
                       WHEN 2 THEN

                            begin

                                select trgt_perd_id
                                  into rec.shpng_perd_id
                                  from
                                (
                                 select srce_mrkt_id,
                                        srce_perd_id,
                                        trgt_mrkt_id,
                                        trgt_perd_id
                                   from mrkt_perd mp,
                                        mrkt_perd_cmpgn_mapg mpcp
                                  where mp.mrkt_id = rec.srce_mrkt_id
                                    and mp.perd_typ = 'SC'
                                    and mp.perd_id >= rec.shpng_perd_id
                                    and mpcp.trgt_mrkt_id =  rec.trgt_mrkt_id
                                    and mp.mrkt_id = mpcp.srce_mrkt_id
                                    and mp.perd_id = mpcp.srce_perd_id
                                  order by mp.perd_id
                                ) where rownum = 1;
                            exception
                            when no_data_found then
                                rec.shpng_perd_id := rec.trgt_offr_perd_id;
                                rec.bilng_perd_id := rec.trgt_offr_perd_id;
                            end;

                            begin

                                select trgt_perd_id
                                  into rec.bilng_perd_id
                                  from
                                (
                                 select srce_mrkt_id,
                                        srce_perd_id,
                                        trgt_mrkt_id,
                                        trgt_perd_id
                                   from mrkt_perd mp,
                                        mrkt_perd_cmpgn_mapg mpcp
                                  where mp.mrkt_id = rec.srce_mrkt_id
                                    and mp.perd_typ = 'SC'
                                    and mp.perd_id >= rec.bilng_perd_id
                                    and mpcp.trgt_mrkt_id =  rec.trgt_mrkt_id
                                    and mp.mrkt_id = mpcp.srce_mrkt_id
                                    and mp.perd_id = mpcp.srce_perd_id
                                  order by mp.perd_id
                                ) where rownum = 1;
                            exception
                            when no_data_found then
                                rec.shpng_perd_id := rec.trgt_offr_perd_id;
                                rec.bilng_perd_id := rec.trgt_offr_perd_id;
                            end;

                       WHEN 3 THEN

                            rec.shpng_perd_id := rec.trgt_offr_perd_id;
                            rec.bilng_perd_id := rec.trgt_offr_perd_id;
                            rec.cust_pull_id  := NULL;
                            rec.offr_cls_id := 1;

                       WHEN 4 THEN
                            rec.shpng_perd_id := rec.trgt_offr_perd_id;
                            rec.bilng_perd_id := rec.trgt_offr_perd_id;
                            rec.cust_pull_id  := NULL;
                       ELSE
                          NULL;
                    END CASE;


                    IF t_tbl_offr_keys.EXISTS (ls_offr_key) THEN
                        ln_offr_id := t_tbl_offr_keys (ls_offr_key);
                    ELSE
                        IF rec.offr_rec_typ = 'OFFR_EXISTS' THEN
                           BEGIN
                            ln_offr_id := rec.trgt_offr_id;
                            UPDATE offr
                               SET offr_desc_txt               = rec.offr_desc_txt,
                                   offr_stus_cd                = 0,
                                   offr_stus_rsn_desc_txt      = NULL,
                                   brchr_plcmt_id              = rec.brchr_plcmt_id,
                                   pg_wght_pct                 = rec.pg_wght_pct,
                                   micr_ncpsltn_desc_txt       = decode(l_process, 3, micr_ncpsltn_desc_txt,
                                                                                  rec.micr_ncpsltn_desc_txt),
                                   micr_ncpsltn_ind            = rec.micr_ncpsltn_ind,
                                   mrkt_veh_perd_sctn_id       = rec.mrkt_veh_perd_sctn_id,
                                   sctn_page_ofs_nr            = rec.sctn_page_ofs_nr,
                                   featrd_side_cd              = rec.featrd_side_cd,
                                   flap_ind                    = null,
                                   enrgy_chrt_postn_id         = decode(l_process, 3, enrgy_chrt_postn_id,
                                                                                  rec.enrgy_chrt_postn_id),
                                   enrgy_chrt_offr_desc_txt    = null, --rec.enrgy_chrt_offr_desc_txt,
                                   shpng_perd_id               = rec.shpng_perd_id,
                                   bilng_perd_id               = rec.bilng_perd_id,
                                   offr_ntes_txt               = decode(l_process, 3, offr_ntes_txt, rec.offr_ntes_txt),
                                   rpt_sbtl_typ_id             = 2,
                                   unit_rptg_lvl_id            = rec.unit_rptg_lvl_id,
                                   offr_lyot_cmnts_txt         = decode(l_process, 3, offr_lyot_cmnts_txt, rec.offr_lyot_cmnts_txt),
                                   est_srce_id                 = NULL,
                                   est_stus_cd                 = NULL,
                                   offr_typ                    = 'CMP',
                                   std_offr_id                 = rec.std_offr_id,
                                   offr_link_ind               = 'Y',
                                   offr_link_id                = rec.srce_offr_id,
                                   prfl_offr_strgth_pct        = null,
                                   prfl_cnt                    = rec.prfl_cnt           ,
                                   sku_cnt                     = rec.sku_cnt            ,
                                   ssnl_evnt_id                = decode(l_process, 3, ssnl_evnt_id,
                                                                                  rec.ssnl_evnt_id),
                                   brchr_postn_id              = decode(l_process, 3, brchr_postn_id,
                                                                                  rec.brchr_postn_id),
                                   frnt_cvr_ind                = decode(l_process, 3, frnt_cvr_ind,
                                                                                  rec.frnt_cvr_ind),
                                   pg_typ_id                   = decode(l_process, 3, pg_typ_id,
                                                                                  rec.pg_typ_id),
                                   offr_prsntn_strnth_id       = rec.offr_prsntn_strnth_id,
                                   flap_pg_wght_pct            = decode(l_process, 3, flap_pg_wght_pct,
                                                                                  rec.flap_pg_wght_pct),
                                   offr_cls_id                 = rec.offr_cls_id,
                                   cust_pull_id                = rec.cust_pull_id,
                                   last_updt_user_id           = p_user_nm,
                                   last_updt_ts                = SYSDATE
                             WHERE offr_id = ln_offr_id;
                            t_tbl_offr_keys (ls_offr_key) := ln_offr_id;
                           EXCEPTION
                           WHEN OTHERS THEN
                               app_plsql_log.info (ls_stage || ': ' || 'ERROR While Updating Offr Table' || SQLERRM(SQLCODE));
                           END;
                        ELSIF rec.offr_rec_typ = 'OFFR_NEW' THEN
                             SELECT seq.NEXTVAL
                               INTO ln_offr_id
                               FROM DUAL;
                           BEGIN
                               INSERT INTO offr
                                           (offr_id,
                                            mrkt_id,
                                            offr_perd_id,
                                            veh_id,
                                            ver_id,
                                            offr_desc_txt,
                                            offr_stus_cd,
                                            offr_stus_rsn_desc_txt,
                                            brchr_plcmt_id,
                                            pg_wght_pct,
                                            micr_ncpsltn_desc_txt,
                                            micr_ncpsltn_ind,
                                            mrkt_veh_perd_sctn_id,
                                            sctn_page_ofs_nr,
                                            featrd_side_cd,
                                            flap_ind,
                                            enrgy_chrt_postn_id,
                                            enrgy_chrt_offr_desc_txt,
                                            shpng_perd_id,
                                            bilng_perd_id,
                                            offr_ntes_txt,
                                            rpt_sbtl_typ_id,
                                            unit_rptg_lvl_id,
                                            offr_lyot_cmnts_txt,
                                            est_srce_id,
                                            est_stus_cd,
                                            offr_typ,
                                            std_offr_id,
                                            offr_link_ind,
                                            offr_link_id,
                                            prfl_offr_strgth_pct,
                                            prfl_cnt,
                                            sku_cnt,
                                            ssnl_evnt_id,
                                            brchr_postn_id,
                                            frnt_cvr_ind,
                                            pg_typ_id,
                                            offr_prsntn_strnth_id,
                                            flap_pg_wght_pct,
                                            offr_cls_id,
                                            cust_pull_id,
                                            creat_user_id,
                                            creat_ts
                                           )
                                    VALUES (ln_offr_id,
                                            rec.trgt_mrkt_id,
                                            rec.trgt_offr_perd_id,
                                            rec.trgt_veh_id,
                                            rec.trgt_ver_id,
                                            rec.offr_desc_txt,
                                            0,
                                            NULL,
                                            rec.brchr_plcmt_id,
                                            rec.pg_wght_pct,
                                            DECODE(l_process, 3, '', rec.micr_ncpsltn_desc_txt),
                                            rec.micr_ncpsltn_ind,
                                            rec.mrkt_veh_perd_sctn_id,
                                            rec.sctn_page_ofs_nr,
                                            rec.featrd_side_cd,
                                            null,
                                            DECODE(l_process, 3, NULL, rec.enrgy_chrt_postn_id),
                                            NULL,
                                            rec.shpng_perd_id,
                                            rec.bilng_perd_id,
                                            rec.offr_ntes_txt,
                                            2,
                                            rec.unit_rptg_lvl_id,
                                            DECODE(l_process, 3, NULL, rec.offr_lyot_cmnts_txt),
                                            NULL, --rec.est_srce_id,
                                            NULL, --rec.est_stus_cd,
                                            'CMP', --rec.offr_typ,
                                            rec.std_offr_id,
                                            'Y',
                                            rec.srce_offr_id,
                                            NULL, --rec.prfl_offr_strgth_pct,
                                            rec.prfl_cnt,
                                            rec.sku_cnt ,
                                            rec.ssnl_evnt_id, --DECODE(l_process, 3, NULL, rec.ssnl_evnt_id),
                                            DECODE(l_process, 3, NULL, rec.brchr_postn_id),
                                            DECODE(l_process, 3, 'N', rec.frnt_cvr_ind),
                                            DECODE(l_process, 3, NULL, rec.pg_typ_id),
                                            rec.offr_prsntn_strnth_id,
                                            DECODE(l_process, 3, NULL, rec.flap_pg_wght_pct),
                                            rec.offr_cls_id,
                                            rec.cust_pull_id,
                                            p_user_nm,
                                            SYSDATE
                                           );
                               t_tbl_offr_keys (ls_offr_key) := ln_offr_id;

                           EXCEPTION
                           WHEN OTHERS THEN
                                p_status := 1;
                                app_plsql_log.info (  ls_stage || ': ' || 'ERROR While Inserting Offr Table' || SQLERRM(SQLCODE));
                           END;
                        END IF;
                        SRC_OFFR_CNT := 1;

                        BEGIN

                          delete from offr_trgt where offr_id = ln_offr_id;

                          INSERT INTO offr_trgt
                                     (offr_id,
                                      trgt_id,
                                      crncy_cd,
                                      catgry_id,
                                      brnd_fmly_id,
                                      brnd_id,
                                      form_id,
                                      sgmt_id,
                                      suplr_id,
                                      trgt_unit_qty,
                                      trgt_sls_amt,
                                      trgt_cost_amt,
                                      prft_amt,
                                      prft_pct,
                                      net_per_unit_amt,
                                      units_per_rep_qty,
                                      units_per_ord_qty,
                                      net_per_rep_amt,
                                      creat_user_id,
                                      creat_ts
                                     )
                              SELECT  ln_offr_id,
                                      seq.NEXTVAL,
                                      ls_crncy_cd, --rec.otrg_crncy_cd,
                                      rec.otrg_catgry_id,
                                      rec.otrg_brnd_fmly_id,
                                      rec.otrg_brnd_id,
                                      rec.otrg_form_id,
                                      rec.otrg_sgmt_id,
                                      rec.otrg_suplr_id,
                                      0, --rec.otrg_trgt_unit_qty,
                                      0, --rec.otrg_trgt_sls_amt,
                                      NULL, --rec.otrg_trgt_cost_amt,
                                      0, --rec.otrg_prft_amt,
                                      0, --rec.otrg_prft_pct,
                                      0, --rec.otrg_net_per_unit_amt,
                                      0, --rec.otrg_units_per_rep_qty,
                                      0, --rec.otrg_units_per_ord_qty,
                                      0, --rec.otrg_net_per_rep_amt,
                                      p_user_nm, SYSDATE
                                 FROM OFFR_TRGT
                                WHERE OFFR_ID =  rec.srce_offr_id;
                        EXCEPTION
                        WHEN OTHERS THEN
                           p_status := 1;
                           app_plsql_log.info (  ls_stage || ': ' || 'ERROR While Inserting Offr Table' || SQLERRM(SQLCODE));
                        END;

                    END IF;

                    --- offer code
                    ls_stage := 'Stage 19';
                    app_plsql_log.info (  ls_stage || ': ' || 'Offr Offr Target inserted/updated Successfully' || SQLERRM(SQLCODE));

                    if
                      rec.opscp_prfl_cd is not null and
                      rec.osl_sku_id is not null and
                      app_cpy_offr_trgt_prfls_info.get_sku_info(rec.opscp_prfl_cd,rec.osl_sku_id) is null then

                      app_plsql_log.info ('ignore! no valid sales class in target, or sku/prfl does not exist! '
                        || rec.opscp_prfl_cd || ',' || rec.osl_sku_id);

                    else



                    ---log

                    sku_info := app_cpy_offr_trgt_prfls_info.get_sku_data(rec.oppp_prfl_cd);

                    if sku_info is not null and sku_info.count > 0 then

                      for ip in sku_info.first .. sku_info.last loop

                        null;

                      end loop;

                    end if;


                  --price point exists in the source

                    CASE l_process
                    WHEN 2 THEN -- Copy to Local Market
                      null;
                    WHEN 3 THEN -- Copy to Local Vehicle
                      rec.opscp_prod_endrsmt_id := 1;
                    ELSE
                      null;
                    END CASE;


                   -- BEGIN at PROFILE LEVEL
                    if
                          rec.opscp_prfl_cd is not null and
                          rec.osl_sku_id is not null
                      then

                      BEGIN
                        ls_stage := 'Stage29: '|| p_parm_id;
                        INSERT INTO offr_prfl_sls_cls_plcmt
                          (
                            offr_id,
                            mrkt_id,
                            veh_id,
                            offr_perd_id,
                            prfl_cd,
                            sls_cls_cd,
                            pg_ofs_nr,
                            featrd_side_cd,
                            sku_cnt,
                            pg_wght_pct,
                            fxd_pg_wght_ind,
                            featrd_prfl_ind,
                            use_instrctns_ind,
                            prod_endrsmt_id
                        )
                       VALUES (
                                ln_offr_id,
                                rec.trgt_mrkt_id,
                                rec.trgt_veh_id,
                                rec.trgt_offr_perd_id,
                                rec.opscp_prfl_cd,
                                app_cpy_offr_trgt_prfls_info.get_sku_info(rec.opscp_prfl_cd,rec.osl_sku_id).sls_cls_cd,
                                rec.opscp_pg_ofs_nr,
                                rec.opscp_featrd_side_cd,
                                rec.opscp_sku_cnt,
                                rec.opscp_pg_wght_pct,
                                rec.opscp_fxd_pg_wght_ind,
                                rec.opscp_featrd_prfl_ind,
                                rec.opscp_use_instrctns_ind,
                                rec.opscp_prod_endrsmt_id
                        );
                      EXCEPTION
                        WHEN DUP_VAL_ON_INDEX
                          THEN

                          update
                            offr_prfl_sls_cls_plcmt
                          set
                            offr_prfl_sls_cls_plcmt.sku_offr_strgth_pct = null,
                            offr_prfl_sls_cls_plcmt.use_instrctns_ind = rec.opscp_use_instrctns_ind,
                            offr_prfl_sls_cls_plcmt.prod_endrsmt_id = rec.opscp_prod_endrsmt_id,
                            offr_prfl_sls_cls_plcmt.fxd_pg_wght_ind = rec.opscp_fxd_pg_wght_ind,
                            offr_prfl_sls_cls_plcmt.pg_wght_pct = rec.opscp_pg_wght_pct,
                            offr_prfl_sls_cls_plcmt.featrd_prfl_ind = rec.opscp_featrd_prfl_ind
                          where
                            offr_prfl_sls_cls_plcmt.offr_id = ln_offr_id
                            and offr_prfl_sls_cls_plcmt.sls_cls_cd = app_cpy_offr_trgt_prfls_info.get_sku_info(rec.opscp_prfl_cd,rec.osl_sku_id).sls_cls_cd
                            and offr_prfl_sls_cls_plcmt.prfl_cd = rec.opscp_prfl_cd
                            and offr_prfl_sls_cls_plcmt.pg_ofs_nr = rec.opscp_pg_ofs_nr
                            and offr_prfl_sls_cls_plcmt.featrd_side_cd = rec.opscp_featrd_side_cd;

                          null;
                        WHEN OTHERS
                          THEN
                            APP_PLSQL_LOG.info('ERROR!!! OPSCP not created: '||SQLERRM);
                      END;

                      -- price points start

                        CASE
                        -- Copy to Local Market
                        WHEN l_process = 2 THEN
                              rec.oppp_offr_id           := ln_offr_id;
                              rec.oppp_veh_id            := rec.trgt_veh_id;
                              rec.oppp_mrkt_id           := rec.trgt_mrkt_id;
                              rec.oppp_offr_perd_id      := rec.trgt_offr_perd_id;
                              rec.oppp_crncy_cd          := ls_crncy_cd;
                              rec.oppp_sls_prc_amt       := rec.calct_trgt_sls_prc_amt;
                              rec.oppp_comsn_typ         := rec.trgt_comsn_typ;
                              rec.oppp_sls_stus_cd       := 2;
                              rec.oppp_pymt_typ          := 1;
                              rec.oppp_sls_promtn_ind    := NULL;
                              rec.oppp_impct_prfl_cd     := NULL;
                              rec.oppp_awrd_sls_prc_amt  := 0;
                              rec.oppp_chrty_ovrrd_ind   := 'N';
                              rec.oppp_roylt_ovrrd_ind   := 'N';
                              rec.oppp_tax_type_id       := rec.trgt_tax_typ_id;
                              rec.oppp_offr_prfl_prcpt_link_id := rec.srce_oppp_id;

                        -- Copy to Local Vehicle
                        WHEN l_process = 3 THEN
                              rec.oppp_offr_id           := ln_offr_id;
                              rec.oppp_veh_id            := rec.trgt_veh_id;
                              rec.oppp_mrkt_id           := rec.trgt_mrkt_id;
                              rec.oppp_offr_perd_id      := rec.trgt_offr_perd_id;
                              rec.oppp_crncy_cd          := ls_crncy_cd;
                              rec.oppp_sls_prc_amt       := rec.calct_trgt_sls_prc_amt;
                              rec.oppp_sls_stus_cd       := 2;
                              rec.oppp_pymt_typ          := nvl(rec.oppp_pymt_typ,1);
                              rec.oppp_sls_promtn_ind    := NULL;
                              rec.oppp_impct_prfl_cd     := NULL;
                              rec.oppp_awrd_sls_prc_amt  := 0;
                              rec.oppp_chrty_ovrrd_ind   := 'N';
                              rec.oppp_roylt_ovrrd_ind   := 'N';
                              rec.oppp_tax_type_id       := rec.trgt_tax_typ_id;
                              rec.oppp_offr_prfl_prcpt_link_id := rec.srce_oppp_id;

                        -- Copy Vehicle to Campaign
                        WHEN l_process = 4 THEN
                              rec.oppp_offr_id           := ln_offr_id;
                              rec.oppp_veh_id            := rec.trgt_veh_id;
                              rec.oppp_mrkt_id           := rec.trgt_mrkt_id;
                              rec.oppp_offr_perd_id      := rec.trgt_offr_perd_id;
                              rec.oppp_crncy_cd          := ls_crncy_cd;
                              rec.oppp_sls_prc_amt       := rec.calct_trgt_sls_prc_amt;
                              rec.oppp_sls_stus_cd       := 2;
                              rec.oppp_pymt_typ          := nvl(rec.oppp_pymt_typ,1);
                              rec.oppp_sls_promtn_ind    := NULL;
                              rec.oppp_impct_prfl_cd     := NULL;
                              rec.oppp_tax_type_id       := rec.trgt_tax_typ_id;
                              rec.oppp_offr_prfl_prcpt_link_id := rec.srce_oppp_id;
                        ELSE
                              NULL;
                        END CASE;

                        -- Pricing Corridors
                        -- Allow existing logic to set default selling price based on existing copy rules
                        -- Then work out whether we need to recalculate based on Pricing Corridors configuration
                        app_plsql_log.info('Checking pricing corridors configuration market/promotion');
                        if trgt_pricing_mrkt_ind = 'Y' and rec.trgt_prc_promtn_ind = 'Y' then

                          select *
                          bulk collect into trgt_pricing_details
                          from table(pa_maps_pricing.get_pricing(rec.trgt_mrkt_id, rec.trgt_offr_perd_id, rec.trgt_veh_id,
                                                                 rec.oppp_prfl_cd, rec.trgt_reg_prc_amt, rec.oppp_promtn_id) );

                          app_plsql_log.info('Number of target price levels: '||trgt_pricing_details.count);

                          for i in nvl(trgt_pricing_details.FIRST, 1) .. nvl(trgt_pricing_details.LAST, 0) loop

                            -- check if this price level has been set up as a strategy for the target market
                            if strategy_exists_idx = 0 and
                               trgt_pricing_details(i).prc_strtgy_ind = 'Y'
                            then
                              strategy_exists_idx := i;
                            end if;

                            -- check if this price level is an applicable strategy for the target market
                            -- only true if source offer used a pricing strategy and target strategy is enabled
                            if strategy_used_idx = 0 and
                               trgt_strtgy_ind = 'Y' and
                               trgt_pricing_details(i).prc_strtgy_ind = 'Y' and
                               rec.plnd_prc_lvl_typ_cd is not null and
                               rec.plnd_prc_lvl_typ_cd = rec.prc_lvl_typ_cd
                            then
                              strategy_used_idx := i;
                            end if;

                            -- check if this price level is the same as the source offer
                            if price_level_idx = 0 and
                               rec.prc_lvl_typ_cd is not null and
                               trgt_pricing_details(i).prc_lvl_typ_cd = rec.prc_lvl_typ_cd
                            then
                              price_level_idx := i;
                            end if;
                          end loop;

                          app_plsql_log.info('Strategy Exists index : '||strategy_exists_idx);
                          app_plsql_log.info('Strategy To Use index : '||strategy_used_idx);
                          app_plsql_log.info('Price Level index: '||price_level_idx);

                          -- if a strategy was available, whether we use it or not, set PLND columns based on that
                          if strategy_exists_idx > 0 then
                            rec.trgt_plnd_prc_lvl_typ_id := trgt_pricing_details(strategy_exists_idx).prc_lvl_typ_id;
                            rec.trgt_plnd_prc_lvl_typ_cd := trgt_pricing_details(strategy_exists_idx).prc_lvl_typ_cd;
                            rec.trgt_plnd_min_prc_amt    := trgt_pricing_details(strategy_exists_idx).min_prc_amt;
                            rec.trgt_plnd_opt_prc_amt    := trgt_pricing_details(strategy_exists_idx).opt_prc_amt;
                            rec.trgt_plnd_max_prc_amt    := trgt_pricing_details(strategy_exists_idx).max_prc_amt;
                            rec.trgt_plnd_promtn_id      := trgt_pricing_details(strategy_exists_idx).strtgy_promtn_id;
                          end if;

                          -- if we've found an applicable strategy, use that for the price and price level columns
                          -- if there's no applicable strategy but a matching price level, use that instead
                          -- if there's neither, do nothing just leave the existing logic in place
                          if strategy_used_idx > 0 then
                            price_level_idx := strategy_used_idx;
                          end if;

                          if price_level_idx > 0 then
                            rec.oppp_sls_prc_amt    := trgt_pricing_details(price_level_idx).opt_prc_amt * rec.oppp_nr_for_qty;
                            rec.trgt_prc_lvl_typ_id := trgt_pricing_details(price_level_idx).prc_lvl_typ_id;
                            rec.trgt_prc_lvl_typ_cd := trgt_pricing_details(price_level_idx).prc_lvl_typ_cd;
                            rec.trgt_min_prc_amt    := trgt_pricing_details(price_level_idx).min_prc_amt;
                            rec.trgt_opt_prc_amt    := trgt_pricing_details(price_level_idx).opt_prc_amt;
                            rec.trgt_max_prc_amt    := trgt_pricing_details(price_level_idx).max_prc_amt;
                          end if;

                          app_plsql_log.info('Pricing Corridors - price = '||rec.oppp_sls_prc_amt);

                        else
                          app_plsql_log.info('Pricing corridors not applicable for market/promotion');
                        end if; -- target market uses Pricing Corridors

                      -- do we have a price point for this item?

                      new_price_point := true;

                      if rec.trgt_oppp_id is not null then

                        app_plsql_log.info (  'Offer profile price point sls cls cd: ' || rec.trgt_sls_cls_cd);


                        if rec.trgt_sls_cls_cd = app_cpy_offr_trgt_prfls_info.get_sku_info(rec.opscp_prfl_cd,rec.osl_sku_id).sls_cls_cd then

                          new_price_point := false;

                          app_plsql_log.info (  'Offer profile price point id for the item : ' || rec.dms_offr_sku_line_id || ' in the target is ' || rec.trgt_oppp_id);
                          ln_oppp_id := rec.trgt_oppp_id;

                        end if;

                        begin

                        update offr_prfl_prc_point
                        set
                          offr_prfl_prc_point.promtn_clm_id  = rec.oppp_promtn_clm_id,
                          offr_prfl_prc_point.promtn_id      = rec.oppp_promtn_id,
                          offr_prfl_prc_point.nr_for_qty     = decode(trim(parm_rec.retain_price),
                                                                           'Y', offr_prfl_prc_point.nr_for_qty,
                                                                                rec.oppp_nr_for_qty),
                          offr_prfl_prc_point.pymt_typ       = nvl(rec.oppp_pymt_typ,1),
                          offr_prfl_prc_point.comsn_typ      = rec.trgt_comsn_typ,
                          offr_prfl_prc_point.pg_ofs_nr      = rec.oppp_pg_ofs_nr,
                          offr_prfl_prc_point.featrd_side_cd = rec.oppp_featrd_side_cd,
                          offr_prfl_prc_point.tax_type_id    = rec.trgt_tax_typ_id,
                          offr_prfl_prc_point.tax_amt        = rec.trgt_tax_amt,
                          offr_prfl_prc_point.comsn_amt      = rec.trgt_comsn_amt
                        where
                          offr_prfl_prc_point.offr_prfl_prcpt_id = rec.trgt_oppp_id;

                        app_plsql_log.info (  'Unique columns of the Offer profile price point were updated : ' || rec.trgt_oppp_id);

                        exception when others then

                          app_plsql_log.info (  'EXCEPTION Unique columns of the Offer profile price point were not updated : ' || rec.trgt_oppp_id);

                        end;


                      end if;

                      if new_price_point and rec.trgt_osl_id is not null then

                        app_plsql_log.info (  'Offer profile price point id for the item : ' || rec.dms_offr_sku_line_id || ' in the target sql based on the trgt osl id');

                        begin

                          select
                            offr_sku_line.offr_prfl_prcpt_id into ln_oppp_id
                          from offr_sku_line where offr_sku_line.offr_sku_line_id = rec.trgt_osl_id;

                          new_price_point := false;

                          app_plsql_log.info (ln_oppp_id);

                        EXCEPTION
                          WHEN NO_DATA_FOUND THEN
                          app_plsql_log.info (  'No price point based on the trgt osl id!');
                        end;

                      end if;

                      if not new_price_point then

                        if parm_rec.retain_price <> 'Y' then

                          -- set the price level columns here too
                          -- they will either have values or have been left as null if no price level was found
                          update offr_prfl_prc_point opp
                          set    sls_prc_amt         = rec.oppp_sls_prc_amt,
                                 prc_lvl_typ_id      = rec.trgt_prc_lvl_typ_id,
                                 prc_lvl_typ_cd      = rec.trgt_prc_lvl_typ_cd,
                                 min_prc_amt         = rec.trgt_min_prc_amt,
                                 opt_prc_amt         = rec.trgt_opt_prc_amt,
                                 max_prc_amt         = rec.trgt_max_prc_amt,
                                 plnd_prc_lvl_typ_id = rec.trgt_plnd_prc_lvl_typ_id,
                                 plnd_prc_lvl_typ_cd = rec.trgt_plnd_prc_lvl_typ_cd,
                                 plnd_min_prc_amt    = rec.trgt_plnd_min_prc_amt,
                                 plnd_opt_prc_amt    = rec.trgt_plnd_opt_prc_amt,
                                 plnd_max_prc_amt    = rec.trgt_plnd_max_prc_amt,
                                 plnd_promtn_id      = rec.trgt_plnd_promtn_id
                          where  offr_prfl_prcpt_id = ln_oppp_id and
                                 exists (select * from offr_sku_line
                                         where  offr_prfl_prcpt_id  = opp.offr_prfl_Prcpt_link_id and
                                                offr_sku_line_id    = rec.srce_osl_id and
                                                nvl(dltd_ind, 'N') != 'Y');

                        app_plsql_log.info (  'Selling price of the price point ' || ln_oppp_id||'  was updated: '||rec.oppp_sls_prc_amt);

                        end if;

                      end if;

                      if new_price_point then

                        app_plsql_log.info (  'Offer profile price point id for the item : ' || rec.dms_offr_sku_line_id || ' in the target sql based on unique columns or new');


                        begin

                          select
                            offr_prfl_prc_point.offr_prfl_prcpt_id into ln_oppp_id
                          from
                            offr_prfl_prc_point
                          where
                                offr_prfl_prc_point.offr_id = rec.oppp_offr_id
                            and offr_prfl_prc_point.promtn_clm_id = rec.oppp_promtn_clm_id
                            and offr_prfl_prc_point.promtn_id = rec.oppp_promtn_id
                            and offr_prfl_prc_point.sls_cls_cd = app_cpy_offr_trgt_prfls_info.get_sku_info(rec.opscp_prfl_cd,rec.osl_sku_id).sls_cls_cd
                            and offr_prfl_prc_point.prfl_cd = rec.opscp_prfl_cd
                            and offr_prfl_prc_point.nr_for_qty = rec.oppp_nr_for_qty
                            and offr_prfl_prc_point.sls_prc_amt = rec.oppp_sls_prc_amt
                            and nvl(offr_prfl_prc_point.pymt_typ,1) = nvl(rec.oppp_pymt_typ,1)
                            and offr_prfl_prc_point.comsn_typ = rec.trgt_comsn_typ
                            and offr_prfl_prc_point.pg_ofs_nr = rec.oppp_pg_ofs_nr
                            and offr_prfl_prc_point.featrd_side_cd = rec.oppp_featrd_side_cd
                            and offr_prfl_prc_point.tax_type_id = rec.trgt_tax_typ_id
                            and rownum < 2;


                          new_price_point := false;

                          app_plsql_log.info (ln_oppp_id);

                        EXCEPTION
                          WHEN NO_DATA_FOUND THEN
                          app_plsql_log.info (  'No price point based on unique columns!');
                          app_plsql_log.info (  'SQL: '
                          || '  select '
                          || '    offr_prfl_prc_point.offr_prfl_prcpt_id '
                          || '  from '
                          || '    offr_prfl_prc_point '
                          || '  where '
                          || '    offr_prfl_prc_point.offr_id = ' || rec.oppp_offr_id
                          || '    and offr_prfl_prc_point.promtn_clm_id = ' || rec.oppp_promtn_clm_id
                          || '    and offr_prfl_prc_point.promtn_id = ' || rec.oppp_promtn_id
                          || '    and offr_prfl_prc_point.sls_cls_cd = ' || app_cpy_offr_trgt_prfls_info.get_sku_info(rec.opscp_prfl_cd,rec.osl_sku_id).sls_cls_cd
                          || '    and offr_prfl_prc_point.prfl_cd = ' || rec.opscp_prfl_cd
                          || '    and offr_prfl_prc_point.nr_for_qty = ' || rec.oppp_nr_for_qty
                          || '    and offr_prfl_prc_point.sls_prc_amt = ' || rec.calct_trgt_sls_prc_amt
                          || '    and nvl(offr_prfl_prc_point.pymt_typ,1) = ' || nvl(rec.oppp_pymt_typ,1)
                          || '    and offr_prfl_prc_point.comsn_typ = ' || rec.trgt_comsn_typ
                          || '    and offr_prfl_prc_point.pg_ofs_nr = ' || rec.oppp_pg_ofs_nr
                          || '    and offr_prfl_prc_point.featrd_side_cd = ' || rec.oppp_featrd_side_cd
                          || '    and offr_prfl_prc_point.tax_type_id = ' || rec.trgt_tax_typ_id
                          || '    and rownum < 2; '
                          );
                        end;
                      end if;

                      if new_price_point then

                          SELECT
                            seq.NEXTVAL
                            INTO ln_oppp_id
                          FROM DUAL;

                          begin
                            INSERT INTO offr_prfl_prc_point
                              (
                                offr_prfl_prcpt_id,
                                offr_id,
                                veh_id,
                                mrkt_id,
                                sls_cls_cd,
                                prfl_cd,
                                offr_perd_id,
                                pg_ofs_nr,
                                featrd_side_cd,
                                crncy_cd,
                                impct_catgry_id,
                                cnsmr_invstmt_bdgt_id,
                                promtn_id,
                                promtn_clm_id,
                                sls_prc_amt,
                                net_to_avon_fct,
                                ssnl_evnt_id,
                                sls_stus_cd,
                                sku_cnt,
                                sku_offr_strgth_pct,
                                est_unit_qty,
                                est_sls_amt,
                                est_cost_amt,
                                prfl_stus_rsn_desc_txt,
                                prfl_stus_cd,
                                tax_amt,
                                comsn_amt,
                                pymt_typ,
                                prc_point_desc_txt,
                                prmry_offr_ind,
                                sls_promtn_ind,
                                impct_prfl_cd,
                                awrd_sls_prc_amt,
                                nr_for_qty,
                                chrty_amt,
                                chrty_ovrrd_ind,
                                roylt_pct,
                                roylt_ovrrd_ind,
                                tax_type_id,
                                comsn_typ,
                                offr_prfl_prcpt_link_id,
                                sls_srce_id,
                                prc_lvl_typ_id,
                                prc_lvl_typ_cd,
                                min_prc_amt,
                                opt_prc_amt,
                                max_prc_amt,
                                plnd_prc_lvl_typ_id,
                                plnd_prc_lvl_typ_cd,
                                plnd_min_prc_amt,
                                plnd_opt_prc_amt,
                                plnd_max_prc_amt,
                                plnd_promtn_id
                              )
                              VALUES (
                                ln_oppp_id,
                                ln_offr_id,
                                rec.trgt_veh_id,
                                rec.trgt_mrkt_id,
                                app_cpy_offr_trgt_prfls_info.get_sku_info(rec.opscp_prfl_cd,rec.osl_sku_id).sls_cls_cd,
                                rec.opscp_prfl_cd,
                                rec.trgt_offr_perd_id,
                                rec.opscp_pg_ofs_nr,
                                rec.opscp_featrd_side_cd,
                                ls_crncy_cd,
                                rec.oppp_impct_catgry_id,
                                rec.oppp_cnsmr_invstmt_bdgt_id,
                                rec.oppp_promtn_id,
                                rec.oppp_promtn_clm_id,
                                rec.oppp_sls_prc_amt,
                                0,
                                rec.oppp_ssnl_evnt_id,
                                rec.oppp_sls_stus_cd,
                                rec.oppp_sku_cnt,
                                rec.oppp_sku_offr_strgth_pct,
                                rec.oppp_est_unit_qty,
                                rec.oppp_est_sls_amt,
                                rec.oppp_est_cost_amt,
                                rec.oppp_prfl_stus_rsn_desc_txt,
                                rec.oppp_prfl_stus_cd,
                                rec.trgt_tax_amt,
                                rec.trgt_comsn_amt,
                                rec.oppp_pymt_typ,
                                rec.oppp_prc_point_desc_txt,
                                null,
                                rec.oppp_sls_promtn_ind,
                                rec.oppp_impct_prfl_cd,
                                0,--rec.oppp_awrd_sls_prc_amt,
                                rec.oppp_nr_for_qty,
                                rec.charity,
                                'N',
                                rec.royalty,
                                'N',
                                rec.trgt_tax_typ_id,
                                rec.trgt_comsn_typ,
                                rec.oppp_offr_prfl_prcpt_link_id,
                                1,
                                rec.trgt_prc_lvl_typ_id,
                                rec.trgt_prc_lvl_typ_cd,
                                rec.trgt_min_prc_amt,
                                rec.trgt_opt_prc_amt,
                                rec.trgt_max_prc_amt,
                                rec.trgt_plnd_prc_lvl_typ_id,
                                rec.trgt_plnd_prc_lvl_typ_cd,
                                rec.trgt_plnd_min_prc_amt,
                                rec.trgt_plnd_opt_prc_amt,
                                rec.trgt_plnd_max_prc_amt,
                                rec.trgt_plnd_promtn_id
                                );

                        app_plsql_log.info (  'New price point was created! Price point id: ' || ln_oppp_id);


                        EXCEPTION
                        WHEN OTHERS   THEN

                          app_plsql_log.info (  'ERROR!!! New price point was not created! '||SQLERRM);

                        END;

                      else

                        UPDATE OFFR_PRFL_PRC_POINT
                           SET crncy_cd                 =  ls_crncy_cd,
                               impct_catgry_id          =  rec.oppp_impct_catgry_id,
                               cnsmr_invstmt_bdgt_id    =  rec.oppp_cnsmr_invstmt_bdgt_id,
                               net_to_avon_fct          =  0,
                               ssnl_evnt_id             =  rec.oppp_ssnl_evnt_id,
                               sls_stus_cd              =  decode(trim(parm_rec.retain_price), 'Y', offr_prfl_prc_point.sls_stus_cd, rec.oppp_sls_stus_cd) , --DRS 13Sep2011
                               sku_cnt                  =  rec.oppp_sku_cnt,
                               sku_offr_strgth_pct      =  rec.oppp_sku_offr_strgth_pct,
                               est_unit_qty             =  rec.oppp_est_unit_qty,
                               est_sls_amt              =  rec.oppp_est_sls_amt,
                               est_cost_amt             =  rec.oppp_est_cost_amt,
                               prfl_stus_rsn_desc_txt   =  rec.oppp_prfl_stus_rsn_desc_txt,
                               prfl_stus_cd             =  rec.oppp_prfl_stus_cd,
                               tax_amt                  =  rec.trgt_tax_amt,
                               comsn_amt                =  rec.trgt_comsn_amt,
                               prc_point_desc_txt       =  rec.oppp_prc_point_desc_txt,
                               prmry_offr_ind           =  null,
                               sls_promtn_ind           =  rec.oppp_sls_promtn_ind,
                               impct_prfl_cd            =  rec.oppp_impct_prfl_cd,
                               awrd_sls_prc_amt         =  decode(trim(parm_rec.retain_price), 'Y', offr_prfl_prc_point.awrd_sls_prc_amt, rec.oppp_awrd_sls_prc_amt) ,
                               chrty_amt                =  rec.charity,
                               chrty_ovrrd_ind          =  'N',
                               roylt_pct                =  rec.royalty,
                               roylt_ovrrd_ind          =  'N'
                         WHERE offr_prfl_prcpt_id = ln_oppp_id ;
                      end if;
                      -- price points end

                      CASE l_process
                        WHEN 2 THEN -- Copy to Local Market
                          NULL;

                        WHEN 3 THEN -- Copy to Local Vehicle
                          rec.oscs_micr_ncpsltn_ind := 'N';
                          rec.oscs_smplg_ind := 'N';
                          rec.oscs_mltpl_ind := 'N';
                          rec.oscs_cmltv_ind := 'N';
                          rec.oscs_incntv_id := 0;

                        WHEN 4 THEN -- Copy to Local Vehicle
                          NULL;

                        ELSE
                          NULL;

                      END CASE;

                      ls_stage := 'Stage35: '|| p_parm_id;

                      BEGIN

                                  INSERT INTO offr_sls_cls_sku
                                    (
                                      offr_id,
                                      sls_cls_cd,
                                      prfl_cd,
                                      pg_ofs_nr,
                                      featrd_side_cd,
                                      sku_id,
                                      mrkt_id,
                                      hero_ind,
                                      micr_ncpsltn_ind,
                                      smplg_ind,
                                      mltpl_ind,
                                      cmltv_ind,
                                      incntv_id,
                                      reg_prc_amt,
                                      cost_amt
                                    )
                                  VALUES (
                                    ln_offr_id,
                                    app_cpy_offr_trgt_prfls_info.get_sku_info(rec.opscp_prfl_cd,rec.osl_sku_id).sls_cls_cd,
                                    rec.oscs_prfl_cd,
                                    rec.oscs_pg_ofs_nr,
                                    rec.oscs_featrd_side_cd,
                                    rec.osl_sku_id,
                                    rec.trgt_mrkt_id,
                                    rec.oscs_hero_ind,
                                    rec.oscs_micr_ncpsltn_ind,
                                    rec.oscs_smplg_ind,
                                    rec.oscs_mltpl_ind,
                                    rec.oscs_cmltv_ind,
                                    rec.oscs_incntv_id,
                                    rec.trgt_reg_prc_amt,
                                    null --app_cpy_offr_trgt_prfls_info.get_sku_info(rec.opscp_prfl_cd,rec.osl_sku_id).cost_amt
                              );
                      EXCEPTION
                      WHEN DUP_VAL_ON_INDEX THEN

                          update
                            offr_sls_cls_sku
                          set
                            offr_sls_cls_sku.smplg_ind         = rec.oscs_smplg_ind,
                            offr_sls_cls_sku.mltpl_ind         = rec.oscs_mltpl_ind,
                            offr_sls_cls_sku.cmltv_ind         = rec.oscs_cmltv_ind,
                            offr_sls_cls_sku.hero_ind           = rec.oscs_hero_ind,
                            offr_sls_cls_sku.micr_ncpsltn_ind   = rec.oscs_micr_ncpsltn_ind,
                            offr_sls_cls_sku.incntv_id          = rec.oscs_incntv_id,
                            offr_sls_cls_sku.reg_prc_amt        = nvl (offr_sls_cls_sku.reg_prc_amt,rec.trgt_reg_prc_amt)
                          where
                            offr_sls_cls_sku.offr_id = ln_offr_id
                            and offr_sls_cls_sku.sls_cls_cd = app_cpy_offr_trgt_prfls_info.get_sku_info(rec.opscp_prfl_cd,rec.osl_sku_id).sls_cls_cd
                            and offr_sls_cls_sku.prfl_cd = rec.oscs_prfl_cd
                            and offr_sls_cls_sku.pg_ofs_nr = rec.oscs_pg_ofs_nr
                            and offr_sls_cls_sku.featrd_side_cd = rec.oscs_featrd_side_cd
                            and offr_sls_cls_sku.sku_id = rec.osl_sku_id;


                      WHEN OTHERS THEN
                          APP_PLSQL_LOG.info('ERROR!!! OSCS not created: '||SQLERRM);
                      END;

                        -- item osl starts

                        new_item := true;

                        if rec.trgt_osl_id is not null then

                          app_plsql_log.info (  'Offer sku line id for the item : ' || rec.dms_offr_sku_line_id || ' in the target is ' || rec.trgt_osl_id);
                          ln_osl_id := rec.trgt_osl_id;

                          new_item := false;

                        end if;


                        if new_item then

                          app_plsql_log.info (  'Offer sku line id for the item : ' || rec.dms_offr_sku_line_id || ' in the target sql based on unique columns or new');


                          begin

                            select
                              offr_sku_line.offr_sku_line_id into ln_osl_id
                            from
                              offr_sku_line
                            where
                              offr_sku_line.offr_prfl_prcpt_id = ln_oppp_id
                              and offr_sku_line.sku_id = rec.osl_sku_id
                              and rownum < 2;


                            new_item := false;

                            app_plsql_log.info (ln_osl_id);

                            EXCEPTION
                                      WHEN NO_DATA_FOUND THEN
                                      app_plsql_log.info (  'No item based on unique columns!');
                                      app_plsql_log.info (  'SQL: '
                                      || '  select '
                                      || '    offr_sku_line.offr_sku_line_id  '
                                      || '  from '
                                      || '    offr_sku_line '
                                      || '  where '
                                      || '    offr_sku_line.offr_prfl_prcpt_id = ' || ln_oppp_id
                                      || '    and offr_sku_line.sku_id = ' || rec.osl_sku_id
                                      || '    and rownum < 2; '
                                      );
                            end;
                        end if;

                        if new_item then

                            SELECT
                              seq.NEXTVAL
                              INTO ln_osl_id
                            FROM DUAL;

                            begin

                            INSERT INTO offr_sku_line (
                              offr_sku_line_id,
                              offr_id,
                              veh_id,
                              featrd_side_cd,
                              offr_perd_id,
                              mrkt_id,
                              sku_id,
                              pg_ofs_nr,
                              crncy_cd,
                              prfl_cd,
                              sls_cls_cd,
                              offr_prfl_prcpt_id,
                              promtn_desc_txt,
                              sls_prc_amt,
                              cost_typ,
                              prmry_sku_offr_ind,
                              dltd_ind,
                              unit_splt_pct,
                              demo_avlbl_ind,
                              offr_sku_line_link_id,
                              set_cmpnt_ind,
                              set_cmpnt_qty,
                              offr_sku_set_id,
                              unit_prc_amt,
                              line_nr,
                              line_nr_typ_id
                            )
                            VALUES
                            (
                              ln_osl_id,
                              ln_offr_id,
                              parm_rec.trgt_veh_id,
                              rec.osl_featrd_side_cd,
                              rec.trgt_offr_perd_id,
                              rec.trgt_mrkt_id,
                              rec.osl_sku_id,
                              rec.oscs_pg_ofs_nr,
                              ls_crncy_cd,
                              rec.oscs_prfl_cd,
                              app_cpy_offr_trgt_prfls_info.get_sku_info(rec.opscp_prfl_cd,rec.osl_sku_id).sls_cls_cd,
                              ln_oppp_id,
                              rec.osl_promtn_desc_txt,
                              rec.calct_trgt_sls_prc_amt,
                              'P', -- rec.osl_cost_typ,
                              rec.osl_prmry_sku_offr_ind,
                              rec.osl_dltd_ind,--ls_osl_dltd_ind,
                              0, -- rec.osl_unit_splt_pct,
                              rec.osl_demo_avlbl_ind,
                              rec.srce_osl_id,
                              rec.osl_set_cmpnt_ind,
                              rec.osl_set_cmpnt_qty,
                              rec.osl_offr_sku_set_id,
                              rec.osl_unit_prc_amt,
                              NULL,
                              NULL
                            );


                            app_plsql_log.info (  'New item was created! Offer Sku Line Id: ' || ln_osl_id);


                            EXCEPTION
                              WHEN OTHERS   THEN

                                app_plsql_log.info (  'ERROR!!! New item was not created! '||SQLERRM);

                            END;

                        else
                              update
                                offr_sku_line
                              set
                                offr_sku_line.sls_prc_amt = decode(trim(parm_rec.retain_price), 'Y', offr_sku_line.sls_prc_amt, rec.calct_trgt_sls_prc_amt),
                                 crncy_cd = ls_crncy_cd -- DRS 09Sep2011
                              where
                                offr_sku_line.offr_sku_line_id = ln_osl_id;

                              update
                                offr_sku_line
                              set
                                offr_sku_line.offr_sku_line_link_id = rec.srce_osl_id
                              where
                                offr_sku_line.offr_sku_line_id = ln_osl_id
                                and offr_sku_line.dltd_ind = 'Y';

                              app_plsql_log.info (  'Item update! Offer Sku Line Id: ' || ln_osl_id);


                              begin

                                update
                                  offr_sku_line
                                set
                                  featrd_side_cd = rec.osl_featrd_side_cd,
                                  pg_ofs_nr =rec.oscs_pg_ofs_nr
                                where
                                  offr_sku_line.offr_sku_line_id = ln_osl_id;


                              exception when others then

                                app_plsql_log.info (  'Item update! Pagination failed!  ' || ln_osl_id);

                              end;

                        end if;

                        if new_item then

                                        ---DMS INSERTS

                                        INSERT INTO dstrbtd_mrkt_sls
                                          (
                                            mrkt_id,
                                            offr_perd_id,
                                            veh_id,
                                            sls_perd_id,
                                            offr_sku_line_id,
                                            sls_typ_id,
                                            unit_qty,
                                            sls_stus_cd,
                                            sls_srce_id,
                                            prev_unit_qty,
                                            prev_sls_srce_id,
                                            comsn_amt,
                                            tax_amt,
                                            net_to_avon_fct ,
                                            unit_ovrrd_ind  ,
                                            currnt_est_ind
                                          )
                                        VALUES
                                          (
                                            rec.trgt_mrkt_id,
                                            rec.trgt_offr_perd_id,
                                            rec.trgt_veh_id,
                                            rec.trgt_offr_perd_id,
                                            ln_osl_id,
                                            1,
                                            decode(
                                              TRIM(parm_rec.mrkt_unit_calc_typ_id), 2, 0,  -- Set to Zero
                                                  3, 0,                                    -- Calculate
                                                  4, rec.dms_unit_qty,                     -- Copy Source Units
                                                  0
                                            ),
                                            2, --rec.dms_sls_stus_cd     ,
                                            1, --rec.dms_sls_srce_id     ,
                                            0, --rec.dms_prev_unit_qty   ,
                                            1, --rec.dms_prev_sls_srce_id,
                                            rec.trgt_comsn_amt, --rec.dms_comsn_amt,
                                            rec.trgt_tax_amt, --rec.dms_tax_amt,
                                            0,--ln_gta, --rec.dms_net_to_avon_fct,
                                            'N', --rec.dms_unit_ovrrd_ind,
                                            'Y' -- rec.dms_currnt_est_ind
                                        );

                                      -- item osl ends
                        else
                            UPDATE dstrbtd_mrkt_sls
                               SET UNIT_QTY         = decode(TRIM(parm_rec.mrkt_unit_calc_typ_id), 1, dstrbtd_mrkt_sls.unit_qty, -- Retain
                                                                                           2, 0,                         -- Set to Zero
                                                                                           3, 0,                         -- Calculate
                                                                                           4, rec.dms_unit_qty,          -- Copy Source Units
                                                                                           0
                                                            ),
                                   UNIT_OVRRD_IND   = decode(TRIM(parm_rec.mrkt_unit_calc_typ_id), 1, dstrbtd_mrkt_sls.unit_ovrrd_ind, 'N'),
                                   COMSN_AMT        = rec.trgt_comsn_amt,
                                   TAX_AMT          = rec.trgt_tax_amt,
                                   CURRNT_EST_IND   = decode(TRIM(parm_rec.mrkt_unit_calc_typ_id), 1, dstrbtd_mrkt_sls.currnt_est_ind, 'Y'),
                                   SLS_SRCE_ID      = decode(TRIM(parm_rec.mrkt_unit_calc_typ_id), 1, dstrbtd_mrkt_sls.sls_srce_id, 1),
                                   PREV_SLS_SRCE_ID = decode(TRIM(parm_rec.mrkt_unit_calc_typ_id), 1, dstrbtd_mrkt_sls.prev_sls_srce_id, 1),
                                   SLS_STUS_CD      = 2, -- 02-Oct-13 James said to do this! (FML)
                                   SLS_PERD_OFS_NR  = NULL,
                                   COST_AMT         = null
                             WHERE offr_sku_line_id = ln_osl_id
                               AND offr_perd_id     = sls_perd_id ;

                        end if;

                        -- item osl ends

                        processed_skus(rec.opscp_prfl_cd || '<-->' || rec.osl_sku_id) := app_cpy_offr_trgt_prfls_info.get_sku_info(rec.opscp_prfl_cd,rec.osl_sku_id);

                        if rec.SRCE_PRFL_RN=rec.TOTAL_PRFL_CD then

                          app_plsql_log.info ( '--->Last record for the profile code: '
                            || rec.opscp_prfl_cd || '<---'
                            || ' number of processed/total skus: '
                            || processed_skus.count || '/' || app_cpy_offr_trgt_prfls_info.get_sku_data(rec.opscp_prfl_cd).count);


                          if sku_info.count>0 then

                            app_plsql_log.info ( '--->Additional sales class code check');

                            for mi in sku_info.first .. sku_info.last loop

                              if not processed_skus.exists(rec.opscp_prfl_cd || '<-->' || sku_info(mi).sku_id) then

                                BEGIN
                                    INSERT INTO offr_prfl_sls_cls_plcmt
                                      (
                                        offr_id,
                                        mrkt_id,
                                        veh_id,
                                        offr_perd_id,
                                        prfl_cd,
                                        sls_cls_cd,
                                        pg_ofs_nr,
                                        featrd_side_cd,
                                        sku_cnt,
                                        pg_wght_pct,
                                        fxd_pg_wght_ind,
                                        featrd_prfl_ind,
                                        use_instrctns_ind,
                                        prod_endrsmt_id
                                    )
                                   VALUES (
                                            ln_offr_id,
                                            rec.trgt_mrkt_id,
                                            rec.trgt_veh_id,
                                            rec.trgt_offr_perd_id,
                                            rec.opscp_prfl_cd,
                                            app_cpy_offr_trgt_prfls_info.get_sku_info(rec.opscp_prfl_cd,sku_info(mi).sku_id).sls_cls_cd,
                                            rec.opscp_pg_ofs_nr,
                                            rec.opscp_featrd_side_cd,
                                            rec.opscp_sku_cnt,
                                            rec.opscp_pg_wght_pct,
                                            rec.opscp_fxd_pg_wght_ind,
                                            rec.opscp_featrd_prfl_ind,
                                            rec.opscp_use_instrctns_ind,
                                            1 --rec.opscp_prod_endrsmt_id
                                    );

                                    app_plsql_log.info ( '--->Additional sales class code: (new profile record)'
                                            || rec.oscs_prfl_cd || ','
                                            || sku_info(mi).sku_id || ','
                                            || app_cpy_offr_trgt_prfls_info.get_sku_info(rec.opscp_prfl_cd,sku_info(mi).sku_id).sls_cls_cd || ','
                                            || app_cpy_offr_trgt_prfls_info.get_sku_info(rec.opscp_prfl_cd,sku_info(mi).sku_id).reg_prc_amt || ','
                                            || app_cpy_offr_trgt_prfls_info.get_sku_info(rec.opscp_prfl_cd,sku_info(mi).sku_id).cost_amt || ','
                                            || ln_offr_id);

                                  EXCEPTION
                                    WHEN DUP_VAL_ON_INDEX
                                      THEN

                                      app_plsql_log.info ( '--->Additional sales class code (already exists) at profile level:'
                                            || rec.oscs_prfl_cd || ','
                                            || sku_info(mi).sku_id || ','
                                            || app_cpy_offr_trgt_prfls_info.get_sku_info(rec.opscp_prfl_cd,sku_info(mi).sku_id).sls_cls_cd || ','
                                            || app_cpy_offr_trgt_prfls_info.get_sku_info(rec.opscp_prfl_cd,sku_info(mi).sku_id).reg_prc_amt || ','
                                            || app_cpy_offr_trgt_prfls_info.get_sku_info(rec.opscp_prfl_cd,sku_info(mi).sku_id).cost_amt || ','
                                            || ln_offr_id);

                                      null;
                                    WHEN OTHERS
                                      THEN
                                        APP_PLSQL_LOG.info('EXTRA ERROR!!! OPSCP not created: '||SQLERRM);
                                  END;


                                  new_price_point := true;

                                  app_plsql_log.info (  'EXTRA: Offer profile price point id for the item : ' || rec.dms_offr_sku_line_id || ' in the target sql based on unique columns or new');


                                    begin

                                      select
                                        offr_prfl_prc_point.offr_prfl_prcpt_id into ln_oppp_id
                                      from
                                        offr_prfl_prc_point
                                      where
                                        offr_prfl_prc_point.offr_id = rec.oppp_offr_id
                                        and offr_prfl_prc_point.promtn_clm_id = rec.oppp_promtn_clm_id
                                        and offr_prfl_prc_point.promtn_id = rec.oppp_promtn_id
                                        and offr_prfl_prc_point.sls_cls_cd = app_cpy_offr_trgt_prfls_info.get_sku_info(rec.opscp_prfl_cd,sku_info(mi).sku_id).sls_cls_cd
                                        and offr_prfl_prc_point.prfl_cd = rec.opscp_prfl_cd
                                        and offr_prfl_prc_point.nr_for_qty = rec.oppp_nr_for_qty
                                        and offr_prfl_prc_point.sls_prc_amt = rec.calct_trgt_sls_prc_amt--todo change it for the extra sku
                                        and nvl(offr_prfl_prc_point.pymt_typ,1) = nvl(rec.oppp_pymt_typ,1)
                                        and offr_prfl_prc_point.comsn_typ = rec.trgt_comsn_typ
                                        and offr_prfl_prc_point.pg_ofs_nr = rec.oppp_pg_ofs_nr
                                        and offr_prfl_prc_point.featrd_side_cd = rec.oppp_featrd_side_cd
                                        and offr_prfl_prc_point.tax_type_id = rec.trgt_tax_typ_id
                                        and rownum < 2;


                                      new_price_point := false;

                                      app_plsql_log.info (ln_oppp_id);

                                    EXCEPTION
                                      WHEN NO_DATA_FOUND THEN
                                      app_plsql_log.info (  'EXTRA: No price point based on unique columns!');
                                      app_plsql_log.info (  'EXTRA: SQL: '
                                      || '  select '
                                      || '    offr_prfl_prc_point.offr_prfl_prcpt_id '
                                      || '  from '
                                      || '    offr_prfl_prc_point '
                                      || '  where '
                                      || '    offr_prfl_prc_point.offr_id = ' || rec.oppp_offr_id
                                      || '    and offr_prfl_prc_point.promtn_clm_id = ' || rec.oppp_promtn_clm_id
                                      || '    and offr_prfl_prc_point.promtn_id = ' || rec.oppp_promtn_id
                                      || '    and offr_prfl_prc_point.sls_cls_cd = ' || app_cpy_offr_trgt_prfls_info.get_sku_info(rec.opscp_prfl_cd,sku_info(mi).sku_id).sls_cls_cd
                                      || '    and offr_prfl_prc_point.prfl_cd = ' || rec.opscp_prfl_cd
                                      || '    and offr_prfl_prc_point.nr_for_qty = ' || rec.oppp_nr_for_qty
                                      || '    and offr_prfl_prc_point.sls_prc_amt = ' || rec.calct_trgt_sls_prc_amt -- todo change this for the new sku
                                      || '    and nvl(offr_prfl_prc_point.pymt_typ,1) = ' || nvl(rec.oppp_pymt_typ,1)
                                      || '    and offr_prfl_prc_point.comsn_typ = ' || rec.trgt_comsn_typ
                                      || '    and offr_prfl_prc_point.pg_ofs_nr = ' || rec.oppp_pg_ofs_nr
                                      || '    and offr_prfl_prc_point.featrd_side_cd = ' || rec.oppp_featrd_side_cd
                                      || '    and offr_prfl_prc_point.tax_type_id = ' || rec.trgt_tax_typ_id
                                      || '    and rownum < 2; '
                                      );
                                    end;

                                  if new_price_point then

                                      SELECT
                                        seq.NEXTVAL
                                        INTO ln_oppp_id
                                      FROM DUAL;

                                      begin
                                        INSERT INTO offr_prfl_prc_point
                                          (
                                            offr_prfl_prcpt_id,
                                            offr_id,
                                            veh_id,
                                            mrkt_id,
                                            sls_cls_cd,
                                            prfl_cd,
                                            offr_perd_id,
                                            pg_ofs_nr,
                                            featrd_side_cd,
                                            crncy_cd,
                                            impct_catgry_id,
                                            cnsmr_invstmt_bdgt_id,
                                            promtn_id,
                                            promtn_clm_id,
                                            sls_prc_amt,
                                            net_to_avon_fct,
                                            ssnl_evnt_id,
                                            sls_stus_cd,
                                            sku_cnt,
                                            sku_offr_strgth_pct,
                                            est_unit_qty,
                                            est_sls_amt,
                                            est_cost_amt,
                                            prfl_stus_rsn_desc_txt,
                                            prfl_stus_cd,
                                            tax_amt,
                                            comsn_amt,
                                            pymt_typ,
                                            prc_point_desc_txt,
                                            prmry_offr_ind,
                                            sls_promtn_ind,
                                            impct_prfl_cd,
                                            awrd_sls_prc_amt,
                                            nr_for_qty,
                                            chrty_amt,
                                            chrty_ovrrd_ind,
                                            roylt_pct,
                                            roylt_ovrrd_ind,
                                            tax_type_id,
                                            comsn_typ,
                                            offr_prfl_prcpt_link_id,
                                            sls_srce_id
                                          )
                                          VALUES (
                                            ln_oppp_id,
                                            ln_offr_id,
                                            rec.trgt_veh_id,
                                            rec.trgt_mrkt_id,
                                            app_cpy_offr_trgt_prfls_info.get_sku_info(rec.opscp_prfl_cd,sku_info(mi).sku_id).sls_cls_cd,
                                            rec.opscp_prfl_cd,
                                            rec.trgt_offr_perd_id,
                                            rec.opscp_pg_ofs_nr,
                                            rec.opscp_featrd_side_cd,
                                            ls_crncy_cd,
                                            rec.oppp_impct_catgry_id,
                                            rec.oppp_cnsmr_invstmt_bdgt_id,
                                            rec.oppp_promtn_id,
                                            rec.oppp_promtn_clm_id,
                                            rec.calct_trgt_sls_prc_amt,
                                            0,
                                            rec.oppp_ssnl_evnt_id,
                                            rec.oppp_sls_stus_cd,
                                            rec.oppp_sku_cnt,
                                            rec.oppp_sku_offr_strgth_pct,
                                            rec.oppp_est_unit_qty,
                                            rec.oppp_est_sls_amt,
                                            rec.oppp_est_cost_amt,
                                            rec.oppp_prfl_stus_rsn_desc_txt,
                                            rec.oppp_prfl_stus_cd,
                                            rec.trgt_tax_amt,
                                            rec.trgt_comsn_amt,
                                            rec.oppp_pymt_typ,
                                            rec.oppp_prc_point_desc_txt,
                                            null,
                                            rec.oppp_sls_promtn_ind,
                                            rec.oppp_impct_prfl_cd,
                                            0,--rec.oppp_awrd_sls_prc_amt,
                                            rec.oppp_nr_for_qty,
                                            rec.charity,
                                            'N',--rec.oppp_chrty_ovrrd_ind,
                                            rec.royalty,--rec.oppp_roylt_pct,
                                            'N',--rec.oppp_roylt_ovrrd_ind,
                                            rec.trgt_tax_typ_id,
                                            rec.trgt_comsn_typ,
                                            null,
                                            1
                                            );

                                    app_plsql_log.info (  'EXTRA: New price point was created! Price point id: ' || ln_oppp_id);


                                  EXCEPTION
                                    WHEN OTHERS   THEN

                                      app_plsql_log.info (  'EXTRA: ERROR!!! New price point was not created! '||SQLERRM);

                                    END;

                                  end if;


                                BEGIN

                                        INSERT INTO offr_sls_cls_sku
                                          (
                                            offr_id,
                                            sls_cls_cd,
                                            prfl_cd,
                                            pg_ofs_nr,
                                            featrd_side_cd,
                                            sku_id,
                                            mrkt_id,
                                            hero_ind,
                                            micr_ncpsltn_ind,
                                            smplg_ind,
                                            mltpl_ind,
                                            cmltv_ind,
                                            incntv_id,
                                            reg_prc_amt,
                                            cost_amt
                                          )
                                        VALUES (
                                          ln_offr_id,
                                          app_cpy_offr_trgt_prfls_info.get_sku_info(rec.opscp_prfl_cd,sku_info(mi).sku_id).sls_cls_cd,
                                          rec.oscs_prfl_cd,
                                          rec.oscs_pg_ofs_nr,
                                          rec.oscs_featrd_side_cd,
                                          sku_info(mi).sku_id,
                                          rec.trgt_mrkt_id,
                                          'N',
                                          'N',
                                          'N',
					  'N',
					  'N',
                                          rec.oscs_incntv_id,
                                          app_cpy_offr_trgt_prfls_info.get_sku_info(rec.opscp_prfl_cd,sku_info(mi).sku_id).reg_prc_amt,
                                          null
                                            );

                                          app_plsql_log.info ( '--->Additional sales class code:'
                                            || rec.oscs_prfl_cd || ','
                                            || sku_info(mi).sku_id || ','
                                            || app_cpy_offr_trgt_prfls_info.get_sku_info(rec.opscp_prfl_cd,sku_info(mi).sku_id).sls_cls_cd || ','
                                            || app_cpy_offr_trgt_prfls_info.get_sku_info(rec.opscp_prfl_cd,sku_info(mi).sku_id).reg_prc_amt || ','
                                            || app_cpy_offr_trgt_prfls_info.get_sku_info(rec.opscp_prfl_cd,sku_info(mi).sku_id).cost_amt || ','
                                            || ln_offr_id);

                                      EXCEPTION
                                      WHEN DUP_VAL_ON_INDEX
                                        THEN

                                        app_plsql_log.info ( '--->Additional sales class code (already exists):'
                                            || rec.oscs_prfl_cd || ','
                                            || sku_info(mi).sku_id || ','
                                            || app_cpy_offr_trgt_prfls_info.get_sku_info(rec.opscp_prfl_cd,sku_info(mi).sku_id).sls_cls_cd || ','
                                            || app_cpy_offr_trgt_prfls_info.get_sku_info(rec.opscp_prfl_cd,sku_info(mi).sku_id).reg_prc_amt || ','
                                            || app_cpy_offr_trgt_prfls_info.get_sku_info(rec.opscp_prfl_cd,sku_info(mi).sku_id).cost_amt || ','
                                            || ln_offr_id);

                                        null;
                                      END;


                                      -- item osl starts

                                      new_item := true;


                                      app_plsql_log.info (  'EXTRA: Offer sku line id for the item : ' || rec.dms_offr_sku_line_id || ' in the target sql based on unique columns or new');


                                      begin

                                        select
                                          offr_sku_line.offr_sku_line_id into ln_osl_id
                                        from
                                          offr_sku_line
                                        where
                                          offr_sku_line.offr_prfl_prcpt_id = ln_oppp_id
                                          and offr_sku_line.sku_id = sku_info(mi).sku_id
                                          and rownum < 2;


                                        new_item := false;

                                        app_plsql_log.info (ln_osl_id);

                                      EXCEPTION
                                        WHEN NO_DATA_FOUND THEN
                                        app_plsql_log.info (  'EXTRA: No item based on unique columns!');
                                        app_plsql_log.info (  'EXTRA: SQL: '
                                        || '  select '
                                        || '    offr_sku_line.offr_sku_line_id  '
                                        || '  from '
                                        || '    offr_sku_line '
                                        || '  where '
                                        || '    offr_sku_line.offr_prfl_prcpt_id = ' || ln_oppp_id
                                        || '    and offr_sku_line.sku_id = ' || sku_info(mi).sku_id
                                        || '    and rownum < 2; '
                                        );
                                      end;

                                      if new_item then

                                        SELECT
                                          seq.NEXTVAL
                                          INTO ln_osl_id
                                        FROM DUAL;

                                        begin

                                          INSERT INTO offr_sku_line (
                                            offr_sku_line_id,
                                            offr_id,
                                            veh_id,
                                            featrd_side_cd,
                                            offr_perd_id,
                                            mrkt_id,
                                            sku_id,
                                            pg_ofs_nr,
                                            crncy_cd,
                                            prfl_cd,
                                            sls_cls_cd,
                                            offr_prfl_prcpt_id,
                                            promtn_desc_txt,
                                            sls_prc_amt,
                                            cost_typ,
                                            prmry_sku_offr_ind,
                                            dltd_ind,
                                            unit_splt_pct,
                                            demo_avlbl_ind,
                                            offr_sku_line_link_id,
                                            set_cmpnt_ind,
                                            set_cmpnt_qty,
                                            offr_sku_set_id,
                                            unit_prc_amt
                                          )
                                          VALUES
                                          (
                                            ln_osl_id,
                                            ln_offr_id,
                                            parm_rec.trgt_veh_id,
                                            rec.osl_featrd_side_cd,
                                            rec.trgt_offr_perd_id,
                                            rec.trgt_mrkt_id,
                                            sku_info(mi).sku_id,
                                            rec.oscs_pg_ofs_nr,
                                            ls_crncy_cd,
                                            rec.oscs_prfl_cd,
                                            app_cpy_offr_trgt_prfls_info.get_sku_info(rec.opscp_prfl_cd,sku_info(mi).sku_id).sls_cls_cd,
                                            ln_oppp_id,
                                            rec.osl_promtn_desc_txt,
                                            rec.calct_trgt_sls_prc_amt,
                                            rec.osl_cost_typ,
                                            rec.osl_prmry_sku_offr_ind,
                                            'Y',--ls_osl_dltd_ind,
                                            rec.osl_unit_splt_pct,
                                            null,
                                            null,
                                            'N',
                                            0,
                                            null,
                                            rec.osl_unit_prc_amt
                                          );


                                          app_plsql_log.info (  'EXTRA: New item was created! Offer Sku Line Id: ' || ln_osl_id);



                                        EXCEPTION
                                          WHEN OTHERS   THEN

                                          app_plsql_log.info (  'EXTRA: ERROR!!! New item was not created! '||SQLERRM);

                                          app_plsql_log.info (  'EXTRA: SQL: '
                                          || ' INSERT INTO offr_sku_line ('
                                          || '   offr_sku_line_id, '
                                          || '  offr_id,'
                                          || '  veh_id, '
                                          || '  featrd_side_cd,'
                                          || '  offr_perd_id,'
                                          || '  mrkt_id,'
                                          || '  sku_id, '
                                          || '  pg_ofs_nr,'
                                          || '  crncy_cd,'
                                          || '  prfl_cd,'
                                          || '  sls_cls_cd,'
                                          || '  offr_prfl_prcpt_id,'
                                          || '  promtn_desc_txt,'
                                          || '  sls_prc_amt, '
                                          || '  cost_typ,'
                                          || '  prmry_sku_offr_ind,'
                                          || '  dltd_ind,'
                                          || '  unit_splt_pct,'
                                          || '  demo_avlbl_ind, '
                                          || '  offr_sku_line_link_id,'
                                          || '  set_cmpnt_ind,'
                                          || '  set_cmpnt_qty,'
                                          || '  offr_sku_set_id,'
                                          || '  unit_prc_amt'
                                          || ')'
                                          || 'VALUES'
                                          || '('
                                          ||  ln_osl_id|| ','
                                          ||    ln_offr_id|| ','
                                          ||    parm_rec.trgt_veh_id|| ','
                                          ||    rec.osl_featrd_side_cd|| ','
                                          ||    rec.trgt_offr_perd_id|| ','
                                          ||    rec.trgt_mrkt_id|| ','
                                          ||    sku_info(mi).sku_id|| ','
                                          ||    rec.oscs_pg_ofs_nr|| ','
                                          ||    ls_crncy_cd|| ','
                                          ||    rec.oscs_prfl_cd|| ','
                                          ||    app_cpy_offr_trgt_prfls_info.get_sku_info(rec.opscp_prfl_cd,sku_info(mi).sku_id).sls_cls_cd|| ','
                                          ||    ln_oppp_id|| ','
                                          ||    rec.osl_promtn_desc_txt|| ','
                                          ||    rec.calct_trgt_sls_prc_amt|| ','
                                          ||    rec.osl_cost_typ|| ','
                                          ||    rec.osl_prmry_sku_offr_ind|| ','
                                          ||    'Y,'--ls_osl_dltd_ind,
                                          ||    rec.osl_unit_splt_pct|| ','
                                          ||    'null'|| ','
                                          ||    'null'|| ','
                                          ||    'N'|| ','
                                          ||    '0'|| ','
                                          ||    'null' || ','
                                          ||    rec.osl_unit_prc_amt || ' '
                                          ||  ');');

                                        END;

                                      end if;

                                      if new_item then

                                        ---DMS INSERTS

                                        INSERT INTO dstrbtd_mrkt_sls
                                          (
                                            mrkt_id,
                                            offr_perd_id,
                                            veh_id,
                                            sls_perd_id,
                                            offr_sku_line_id,
                                            sls_typ_id,
                                            unit_qty,
                                            sls_stus_cd,
                                            sls_srce_id,
                                            prev_unit_qty,
                                            prev_sls_srce_id,
                                            comsn_amt,
                                            tax_amt,
                                            net_to_avon_fct ,
                                            unit_ovrrd_ind  ,
                                            currnt_est_ind
                                          )
                                        VALUES
                                          (
                                            rec.trgt_mrkt_id,
                                            rec.trgt_offr_perd_id,
                                            rec.trgt_veh_id,
                                            rec.trgt_offr_perd_id,
                                            ln_osl_id,
                                            1,
                                            0,
                                            2, --rec.dms_sls_stus_cd     ,
                                            1, --rec.dms_sls_srce_id     ,
                                            0, --rec.dms_prev_unit_qty   ,
                                            1, --rec.dms_prev_sls_srce_id,
                                            rec.trgt_comsn_amt, --rec.dms_comsn_amt       ,
                                            rec.trgt_tax_amt, --rec.dms_tax_amt         ,
                                            0,
                                            'N', --rec.dms_unit_ovrrd_ind  ,
                                            'Y'--rec.dms_currnt_est_ind
                                        );

                                      -- item osl ends
                                      end if;

                              end if;

                              null;

                            end loop;

                          end if;

                          processed_skus.delete;

                        end if;

                    end if;
        -- END AT PROFILE LEVEL



                 end if;

                 END LOOP; -- End of Main Query Loop

                 --regular price rule
                 P_ADD_ADD_REG_PRC_ITEMS (parm_rec.trgt_mrkt_id,parm_rec.trgt_offr_perd_id);


                 FOR key_rec IN (
                  select
                    offr.mrkt_id trgt_mrkt_id,
                    offr.offr_perd_id trgt_offr_perd_id,
                    offr.veh_id trgt_veh_id,
                    offr.offr_id trgt_offr_id,
                    offr.ver_id trgt_ver_id,
                    src_offr.mrkt_veh_perd_sctn_id srce_mrkt_veh_perd_sctn_id,
                    src_offr.offr_id srce_offr_id
                  from
                    offr,
                    offr src_offr
                  where
                    offr.mrkt_id = parm_rec.trgt_mrkt_id
                    and offr.veh_id = parm_rec.trgt_veh_id
                    and offr.offr_perd_id = parm_rec.trgt_offr_perd_id
                    and offr.offr_typ = 'CMP'
                    and offr.ver_id = 0
                    and offr.offr_link_id = src_offr.offr_id
                 )
                     LOOP
                        ls_stage := 'Stage46: '|| p_parm_id;
                        begin
                            SELECT BRCHR_PLCMT_ID, MRKT_VEH_PERD_SCTN_ID
                              INTO ln_brchr_plcmt_id, ln_mrkt_veh_perd_sctn_id
                              FROM MRKT_VEH_PERD_SCTN
                             WHERE MRKT_ID = key_rec.trgt_mrkt_id
                               and OFFR_PERD_ID = key_rec.trgt_offr_perd_id
                               and VER_ID = key_rec.trgt_ver_id
                             and VEH_ID = key_rec.trgt_veh_id
                               and MRKT_VEH_PERD_SCTN_ID = key_rec.srce_mrkt_veh_perd_sctn_id
                             ;
                        ls_stage := 'Stage47: '|| p_parm_id;
                        UPDATE OFFR O
                           SET BRCHR_PLCMT_ID = ln_brchr_plcmt_id,
                               MRKT_VEH_PERD_SCTN_ID = ln_mrkt_veh_perd_sctn_id,
                               UNIT_RPTG_LVL_ID = DECODE ((SELECT count(1)
                                                             FROM offr_prfl_sls_cls_plcmt
                                                            WHERE offr_id = key_rec.trgt_offr_id
                                                              AND ROWNUM = 1
                                                          ), 0 , 1, 2
                                                         ),
                               PRFL_CNT = (select count(distinct prfl_cd) from offr_prfl_prc_point where offr_id = o.offr_id),
                               SKU_CNT = (select count(distinct sku_id) from offr_sku_line where offr_id = o.offr_id),
                               MICR_NCPSLTN_IND = DECODE ((SELECT 'x'
                                                             FROM OFFR_SLS_CLS_SKU
                                                            WHERE offr_id = o.offr_id
                                                              AND MICR_NCPSLTN_IND = 'Y'
                                                              AND ROWNUM = 1
                                                          ), 'x' , 'Y', 'N'
                                                         )
                         WHERE offr_id = key_rec.trgt_offr_id;

                        exception
                        when no_data_found then
                            UPDATE OFFR O
                               SET SCTN_PAGE_OFS_NR = 1,
                                   MRKT_VEH_PERD_SCTN_ID = null,
                                   UNIT_RPTG_LVL_ID = DECODE ((SELECT 'x'
                                                                 FROM offr_prfl_sls_cls_plcmt
                                                                WHERE offr_id = key_rec.trgt_offr_id
                                                                  AND ROWNUM = 1
                                                              ), 'x' , 2, 1
                                                             ),
                                   PRFL_CNT = (select count(distinct prfl_cd) from offr_prfl_prc_point where offr_id = o.offr_id),
                                   SKU_CNT = (select count(distinct sku_id) from offr_sku_line where offr_id = o.offr_id),
                                   MICR_NCPSLTN_IND = DECODE ((SELECT 'x'
                                                                 FROM OFFR_SLS_CLS_SKU
                                                                WHERE offr_id = o.offr_id
                                                                  AND MICR_NCPSLTN_IND = 'Y'
                                                                  AND ROWNUM = 1
                                                              ), 'x' , 'Y', 'N'
                                                             )
                             WHERE offr_id = key_rec.trgt_offr_id;
                        when others then
                         null;
                        end;

                        begin
                            FOR opscp_rec in (select offr_id, sls_cls_cd, prfl_cd, pg_ofs_nr, featrd_side_cd
                                                from OFFR_PRFL_SLS_CLS_PLCMT
                                               where offr_id = key_rec.trgt_offr_id)
                            LOOP
                             UPDATE OFFR_PRFL_SLS_CLS_PLCMT
                                SET SKU_CNT = (SELECT COUNT(OSCS.SKU_ID)
                                                 from OFFR_SLS_CLS_SKU OSCS
                                                WHERE OSCS.OFFR_ID         = opscp_rec.OFFR_ID
                                                  AND OSCS.SLS_CLS_CD      = opscp_rec.SLS_CLS_CD
                                                  AND OSCS.PRFL_CD         = opscp_rec.PRFL_CD
                                                  AND OSCS.PG_OFS_NR       = opscp_rec.PG_OFS_NR
                                                  AND OSCS.FEATRD_SIDE_CD  = opscp_rec.FEATRD_SIDE_CD
                                              )
                              WHERE OFFR_ID         = opscp_rec.OFFR_ID
                                AND SLS_CLS_CD      = opscp_rec.SLS_CLS_CD
                                AND PRFL_CD         = opscp_rec.PRFL_CD
                                AND PG_OFS_NR       = opscp_rec.PG_OFS_NR
                                AND FEATRD_SIDE_CD  = opscp_rec.FEATRD_SIDE_CD
                             ;
                            END LOOP;
                        exception
                         when others then
                            null;
                        end;


                      pa_maps_copy.set_ms(key_rec.srce_offr_id, key_rec.trgt_offr_id);

                      begin
                        ln_est_links := pa_maps_copy.copy_estimate_links (key_rec.srce_offr_id, key_rec.trgt_offr_id, p_user_nm);
                      exception
                        when others then
                          null;
                      end;

                      begin

                        update
                          offr_sku_line
                        set
                          offr_sku_line.dltd_ind =
                            (select sosl.dltd_ind from offr_sku_line sosl where
                              sosl.offr_sku_line_id = offr_sku_line.offr_sku_line_link_id
                            )
                        where
                          offr_sku_line.offr_id = key_rec.trgt_offr_id
                          and offr_sku_line.offr_sku_line_link_id is not null;

                      exception
                        when others then
                        null;
                      end;

                      begin
                        PA_MAPS_COPY.update_unit_prc_amt(key_rec.trgt_offr_id);
                      exception
                        when others then
                          null;
                      end;




                      begin

                      for pp_gta in (
                      select
                        offr_prfl_prcpt_id,
                        PA_MAPS_GTA.GET_GTA_WITHOUT_PRICE_POINT (
                          mrkt_perd.gta_mthd_id,
                          offr_prfl_prc_point.sls_prc_amt,
                          offr_prfl_prc_point.chrty_amt,
                          offr_prfl_prc_point.awrd_sls_prc_amt,
                          offr_prfl_prc_point.comsn_amt,
                          offr_prfl_prc_point.tax_amt,
                          offr_prfl_prc_point.roylt_pct)
                          gta,
                        (
                          select count(*) from offr_sku_line where offr_sku_line.offr_prfl_prcpt_id = offr_prfl_prc_point.offr_prfl_prcpt_id
                        ) cc
                        from
                          offr_prfl_prc_point,mrkt_perd,offr
                        where
                          offr.mrkt_id = key_rec.trgt_mrkt_id
                          and offr.offr_perd_id = key_rec.trgt_offr_perd_id
                          and offr.offr_id = key_rec.trgt_offr_id
                          and offr.offr_id = offr_prfl_prc_point.offr_id
                          and mrkt_perd.mrkt_id = offr.mrkt_id
                          and mrkt_perd.perd_id = offr.offr_perd_id
                      ) loop


                      update offr_prfl_prc_point set
                        offr_prfl_prc_point.net_to_avon_fct = pp_gta.gta,
                        offr_prfl_prc_point.sku_cnt = pp_gta.cc
                        where offr_prfl_prc_point.offr_prfl_prcpt_id = pp_gta.offr_prfl_prcpt_id;

                      end loop;

                      exception
                        when others then
                          null;
                      end;



               end loop;


                 --deleteing unnecessary items, price points

                 begin

                 delete from dstrbtd_mrkt_sls where offr_sku_line_id in (
                  select offr_sku_line_id from (
                  select
                    offr_sku_line.offr_sku_line_id,
                    sum(decode(nvl(offr_sku_line.dltd_ind,'N'),'N',1,0)) over (partition by offr_prfl_prc_point.offr_prfl_prcpt_id) not_deleted_items
                  from
                    offr,offr_prfl_prc_point,offr_sku_line
                  where
                    offr.mrkt_id = parm_rec.trgt_mrkt_id
                    and offr.ver_id = 0
                    and offr.offr_perd_id = parm_rec.trgt_offr_perd_id
                    and offr.veh_id = parm_rec.trgt_veh_id
                    and offr.offr_id = offr_prfl_prc_point.offr_id
                    and offr_prfl_prc_point.offr_prfl_prcpt_id = offr_sku_line.offr_prfl_prcpt_id
                  )
                  where
                    not_deleted_items = 0
                  );

                 exception when others then
                  null;
                 end;

                 begin

                 delete from offr_sku_line where
                  mrkt_id = parm_rec.trgt_mrkt_id
                  and offr_perd_id = parm_rec.trgt_offr_perd_id
                  and veh_id = parm_rec.trgt_veh_id
                  and
                  not exists (
                    select
                      *
                    from
                      dstrbtd_mrkt_sls
                    where
                      dstrbtd_mrkt_sls.mrkt_id =parm_rec.trgt_mrkt_id and dstrbtd_mrkt_sls.offr_perd_id = parm_rec.trgt_offr_perd_id
                      and dstrbtd_mrkt_sls.offr_sku_line_id = offr_sku_line.offr_sku_line_id
                  );

                 exception when others then
                  null;
                 end;

                 begin

                 delete from offr_prfl_prc_point
                  where
                  mrkt_id = parm_rec.trgt_mrkt_id
                  and offr_perd_id = parm_rec.trgt_offr_perd_id
                  and veh_id = parm_rec.trgt_veh_id
                  and not exists (
                    select * from offr_sku_line
                    where
                    offr_sku_line.mrkt_id = parm_rec.trgt_mrkt_id
                    and offr_sku_line.offr_perd_id = parm_rec.trgt_offr_perd_id
                    and offr_sku_line.offr_prfl_prcpt_id = offr_prfl_prc_point.offr_prfl_prcpt_id
                  );

                 exception when others then
                  null;
                 end;

                 begin

                 delete from offr_sls_cls_sku where
                  offr_sls_cls_sku.mrkt_id = parm_rec.trgt_mrkt_id
                  and offr_sls_cls_sku.offr_id in (
                    select offr_id from offr
                    where
                    mrkt_id = parm_rec.trgt_mrkt_id
                    and offr_perd_id = parm_rec.trgt_offr_perd_id
                    and ver_id = 0
                    and veh_id = parm_rec.trgt_veh_id
                  )
                  and not exists (
                    select
                      *
                    from
                      offr_sku_line
                    where
                      offr_sku_line.mrkt_id = parm_rec.trgt_mrkt_id
                      and offr_sku_line.offr_perd_id = parm_rec.trgt_offr_perd_id
                      and offr_sku_line.offr_id = offr_sls_cls_sku.offr_id
                      and offr_sku_line.sls_cls_cd = offr_sls_cls_sku.sls_cls_cd
                      and offr_sku_line.prfl_Cd = offr_sls_cls_sku.prfl_cd
                      and offr_sku_line.pg_ofs_nr = offr_sls_cls_sku.pg_ofs_nr
                      and offr_sku_line.featrd_side_cd = offr_sls_cls_sku.featrd_side_cd
                      and offr_sku_line.sku_id = offr_sls_cls_sku.sku_id
                  );

                 exception when others then
                  null;
                 end;

                 begin

                 delete from offr_prfl_sls_cls_plcmt where
                  offr_prfl_sls_cls_plcmt.mrkt_id = parm_rec.trgt_mrkt_id
                  and offr_prfl_sls_cls_plcmt.offr_perd_id = parm_rec.trgt_offr_perd_id
                  and offr_prfl_sls_cls_plcmt.veh_id = parm_rec.trgt_veh_id
                  and not exists (
                    select
                      *
                    from
                      offr_prfl_prc_point
                    where
                      offr_prfl_prc_point.mrkt_id = parm_rec.trgt_mrkt_id
                      and offr_prfl_prc_point.offr_perd_id = parm_rec.trgt_offr_perd_id
                      and offr_prfl_prc_point.offr_id = offr_prfl_sls_cls_plcmt.offr_id
                      and offr_prfl_prc_point.sls_cls_cd = offr_prfl_sls_cls_plcmt.sls_cls_cd
                      and offr_prfl_prc_point.prfl_cd = offr_prfl_sls_cls_plcmt.prfl_cd
                      and offr_prfl_prc_point.pg_ofs_nr = offr_prfl_sls_cls_plcmt.pg_ofs_nr
                      and offr_prfl_prc_point.featrd_side_cd = offr_prfl_sls_cls_plcmt.featrd_side_cd
                  );

                 exception when others then
                  null;
                 end;


                    -- Enabled DRS 26Aug2011; Starts Here -- 3359
                    srce_mrkt_id    := parm_rec.srce_mrkt_id;
                    srce_offr_per_id:= parm_rec.srce_offr_perd_id;
                    srce_ver_id        := parm_rec.srce_ver_id;
                    trg_mrkt_id        := parm_rec.trgt_mrkt_id;
                    trg_offr_per_id    := parm_rec.trgt_offr_perd_id;
                    trg_ver_id        := parm_rec.trgt_ver_id;
                    trg_veh_id      := trg_veh_id||','||parm_rec.trgt_veh_id;

                    if l_process <> 1 then
                            BEGIN
                                IF l_job_typ NOT IN (1, 8)  THEN
                                    UPDATE_TRGT_UNITS(p_parm_id,
                                                      parm_rec.srce_mrkt_id,
                                                      parm_rec.srce_veh_id   ,
                                                      parm_rec.srce_offr_perd_id   ,
                                                      parm_rec.srce_ver_id     ,
                                                      parm_rec.trgt_mrkt_id    ,
                                                      parm_rec.trgt_veh_id     ,
                                                      parm_rec.trgt_offr_perd_id  ,
                                                      parm_rec.trgt_ver_id       ,
                                                      parm_rec.mrkt_unit_calc_typ_id,
                                                      'N',
                                                      p_status,
                                                      null,
                                                      null);


                                    SET_UNIT_SPLIT_AVG (
                                      parm_rec.srce_mrkt_id,
                                      parm_rec.srce_offr_perd_id,
                                      parm_rec.srce_veh_id,
                                      parm_rec.trgt_mrkt_id,
                                      parm_rec.trgt_offr_perd_id,
                                      parm_rec.trgt_veh_id
                                     );


                                    set_pso_ind (
                                      parm_rec.trgt_mrkt_id,
                                      parm_rec.trgt_offr_perd_id,
                                      parm_rec.trgt_veh_id
                                     );

                                    begin
                                      select
                                        min(ofs_dstrbtn_pct.perd_ofs_nr),
                                        max(ofs_dstrbtn_pct.perd_ofs_nr)
                                        into
                                        offset_min,offset_max
                                      from
                                        OFS_DSTRBTN_PCT
                                      where
                                      ofs_dstrbtn_pct.mrkt_id = trg_mrkt_id
                                      and ofs_dstrbtn_pct.offr_perd_id = trg_offr_per_id;
                                      exception when others then
                                        offset_min:=0;
                                        offset_max:=0;
                                      end;


                                    begin

                                    if offset_min<>0 or offset_max<>0 then

                                    PA_MAPS_COS.update_offsets(
                                            offset_min,
                                            offset_max,
                                            parm_rec.trgt_mrkt_id,
                                            parm_rec.trgt_veh_id,
                                            parm_rec.trgt_offr_perd_id);


                                    set_gta_dms(
                                      parm_rec.trgt_mrkt_id,
                                      parm_rec.trgt_offr_perd_id,
                                      parm_rec.trgt_veh_id
                                     );

                                    end if;

                                    exception
                                      when others then
                                      null;
                                    end;

                                END IF;

                                IF p_status <> 0 THEN
                                   null;
                                END IF;
                            EXCEPTION
                            WHEN OTHERS THEN
                                app_plsql_log.info('Error while updating Units values... (error code: ' ||
                               SQLCODE || ' error message: ' || SQLERRM || ')');
                            END;

                    end if;

                    begin

                      -- gta / sku count update in dms

                      update dstrbtd_mrkt_sls set
                        dstrbtd_mrkt_sls.net_to_avon_fct =
                        (
                          select max(offr_prfl_prc_point.net_to_avon_fct) from offr_sku_line,offr_prfl_prc_point
                          where
                          offr_sku_line.offr_prfl_prcpt_id = offr_prfl_prc_point.offr_prfl_prcpt_id
                          and offr_Sku_line.offr_sku_line_id = dstrbtd_mrkt_sls.offr_sku_line_id
                        )
                        where
                          dstrbtd_mrkt_sls.mrkt_id = parm_rec.trgt_mrkt_id
                          and dstrbtd_mrkt_sls.offr_perd_id = parm_rec.trgt_offr_perd_id
                          and dstrbtd_mrkt_sls.sls_typ_id = 1;

                      exception
                        when others then
                          null;
                      end;

               end if; -- if l_process = 1 then

             END IF; --IF l_continue_loop <> 'N' THEN
           END IF; -- for IF CONTINTION l_continue_loop <> 'N'
       l_continue_loop := 'Y';

      END LOOP; -- End of Parm rec Loop


      begin
       if l_process = 2 then

        app_plsql_log.info('l_partial_success before  log_review_job_rslts' || l_partial_success);
        log_review_job_rslts (
               p_task_id             ,
               p_parm_id             ,
               p_status              ,
               l_partial_success
            );
         app_plsql_log.info('l_partial_success after  log_review_job_rslts' || l_partial_success);
        select count(1) parm_cnt
          into ln_parm_cnt
          from (
               SELECT   distinct sd.trgt_mrkt_id, sd.trgt_offr_perd_id, sd.trgt_veh_id, sd.trgt_ver_id
                          FROM TABLE (cpy_offr.f_get_parms (p_parm_id)) sd
                         WHERE (   (l_job_typ IN (1, 8) AND srce_veh_id = trgt_veh_id)
                                OR (l_job_typ NOT IN (1, 8) AND 1 = 1)
                               )
               );

        if  ln_process_cnt = 0 then
            p_status := 1;
            l_partial_success := 0;
        elsif ln_process_cnt <> ln_parm_cnt then
            if p_task_id is null then
                p_status := 2;
                l_partial_success := 2;
            end if;
        end if;
       end if;

      exception
        when others then
        null;
      end;

       IF l_job_typ IN (1, 8) and l_process <> 1 THEN
          --
          srce_mrkt_id := -1;
          srce_offr_per_id := -1;
          srce_ver_id := -1;
          trg_mrkt_id := -1;
          trg_offr_per_id := -1;
          trg_ver_id := -1;
          --
          FOR parm_rec IN (SELECT DISTINCT sd.srce_mrkt_id, sd.srce_veh_id,
                                           sd.srce_offr_perd_id, sd.srce_ver_id,
                                           sd.trgt_mrkt_id, sd.trgt_veh_id,
                                           sd.trgt_offr_perd_id, sd.trgt_ver_id
                                      FROM TABLE (f_get_parms (p_parm_id)) sd
                                     WHERE ((l_job_typ IN (1, 8) AND srce_veh_id = trgt_veh_id)
                                            OR
                                            (l_job_typ NOT IN (1, 8) AND 1 = 1))
                                       AND srce_mrkt_id = srce_offr_perd_refmrkt
                                       AND trgt_mrkt_id = trgt_offr_perd_refmrkt
                                  ORDER BY sd.srce_mrkt_id, sd.srce_offr_perd_id, sd.srce_ver_id,
                                           sd.trgt_mrkt_id, sd.trgt_offr_perd_id, sd.trgt_ver_id)
            LOOP
            --
            if not (parm_rec.srce_mrkt_id = srce_mrkt_id
              and   parm_rec.srce_offr_perd_id = srce_offr_per_id
              and   parm_rec.srce_ver_id = srce_ver_id
              and   parm_rec.trgt_mrkt_id = trg_mrkt_id
              and   parm_rec.trgt_offr_perd_id = trg_offr_per_id
              and   parm_rec.trgt_ver_id = trg_ver_id) then
              -- copy export-on-demand details from source market to target
              if not (parm_rec.srce_mrkt_id = srce_mrkt_id
                and   parm_rec.srce_offr_perd_id = srce_offr_per_id
                and   parm_rec.trgt_mrkt_id = trg_mrkt_id) then
                pa_xport_on_dmnd.FB_COPY_XPORT_ON_DMND(parm_rec.srce_mrkt_id, 
                                                       parm_rec.trgt_mrkt_id, 
                                                       parm_rec.srce_offr_perd_id);
              end if;
              --
              UPDATE_TRGT_UNITS(p_parm_id,
                                parm_rec.srce_mrkt_id,
                                NULL  ,
                                parm_rec.srce_offr_perd_id,
                                parm_rec.srce_ver_id,
                                parm_rec.trgt_mrkt_id,
                                NULL,
                                parm_rec.trgt_offr_perd_id,
                                parm_rec.trgt_ver_id,
                                3,
                                'Y',
                                p_status,
                                trg_veh_id,
                                ls_fb_xcluded_sls_cls_cd
                               );
              --
              srce_mrkt_id := parm_rec.srce_mrkt_id;
              srce_offr_per_id := parm_rec.srce_offr_perd_id;
              srce_ver_id := parm_rec.srce_ver_id;
              trg_mrkt_id := parm_rec.trgt_mrkt_id;
              trg_offr_per_id := parm_rec.trgt_offr_perd_id;
              trg_ver_id := parm_rec.trgt_ver_id;
              -- 
            end if;

            SET_UNIT_SPLIT_AVG (parm_rec.srce_mrkt_id,
                                parm_rec.srce_offr_perd_id,
                                -1,
                                parm_rec.trgt_mrkt_id,
                                parm_rec.trgt_offr_perd_id,
                                parm_rec.trgt_veh_id
                               );

            SET_PSO_IND (parm_rec.trgt_mrkt_id,
                         parm_rec.trgt_offr_perd_id,
                         parm_rec.trgt_veh_id
                         );

            begin
              select
                min(ofs_dstrbtn_pct.perd_ofs_nr),
                max(ofs_dstrbtn_pct.perd_ofs_nr)
                into
                offset_min,offset_max
              from
                OFS_DSTRBTN_PCT
              where
                ofs_dstrbtn_pct.mrkt_id = parm_rec.trgt_mrkt_id
                and ofs_dstrbtn_pct.offr_perd_id = parm_rec.trgt_offr_perd_id;
            exception when others then
              offset_min:=0;
              offset_max:=0;
            end;
            --
            IF offset_min<>0 or offset_max<>0 THEN
               begin
                  PA_MAPS_COS.update_offsets( offset_min,
                                              offset_max,
                                              parm_rec.trgt_mrkt_id,
                                              parm_rec.trgt_veh_id,
                                              parm_rec.trgt_offr_perd_id);

                  SET_GTA_DMS(parm_rec.trgt_mrkt_id,
                              parm_rec.trgt_offr_perd_id,
                              parm_rec.trgt_veh_id
                              );

               exception
                  when others then
                  null;
               end;
            END IF;
          END LOOP;

       END IF;

      v_end := DBMS_UTILITY.get_time;

      ls_stage := 'End of CPY_OFFR';

      --- BATCH COPY -- KK END


      if p_status = 1 then
          app_plsql_log.info (   ls_stage || ': Parm_id Completed with Errors.. at: '
                              || SYSTIMESTAMP
                              || '; Elapsed Time: '
                              || (v_end - v_start) / 100
                             );
      else
          app_plsql_log.info (   ls_stage || ': Parm_id Completed Successfully.. at: '
                              || SYSTIMESTAMP
                              || '; Elapsed Time: '
                              || (v_end - v_start) / 100
                             );
      end if;

    app_plsql_log.info('Last status in CPY_OFFR while updating MPS_PLSQL_OUTPUT: ' || p_status);

        app_plsql_log.info ('l_process: ' || l_process
                            || '# ln_process_cnt:  '|| ln_process_cnt
                            || '# ln_parm_cnt '|| ln_parm_cnt
                            || '# p_status '|| p_status
                            || '# l_partial_success '|| l_partial_success );


        update mps_plsql_output
           set txt_value = decode(l_partial_success, 2,2, p_status)
         where key_value = p_parm_id
          ;

   EXCEPTION
      WHEN OTHERS
      THEN
         p_status := 1;
         app_plsql_log.info (ls_stage || ': ' || SQLERRM (SQLCODE));
   END copy_offr;

   PROCEDURE cpy_offr_scrn (
      cpy_offr_parms   IN       tbl_mps_parm,
      p_user_nm        IN       VARCHAR2,
      p_parm_id        OUT      mps_parm.mps_parm_id%TYPE
   )
   AS
      l_parm_id   mps_parm.mps_parm_id%TYPE;
      l_task_id   NUMBER                      := NULL;
      l_seq_nr    NUMBER;
      l_run_id    NUMBER;
      l_message   VARCHAR2 (100);
      l_status    NUMBER := 0;
   BEGIN
      parms (cpy_offr_parms, p_user_nm, l_parm_id);
      p_parm_id := l_parm_id;
      copy_offr(l_parm_id, p_user_nm, l_task_id, l_status);

   app_plsql_log.info('Last status in CPY_OFFR_SCREEN while updating MPS_PLSQL_OUTPUT: ' || l_status);

    update mps_plsql_output
       set txt_value = decode(txt_value, 2,2, l_status)
     where (run_id, seq_nr) = (select run_id, min(seq_nr)
                                 from mps_plsql_output
                                where key_value = l_parm_id
                                group by run_id);

   app_plsql_log.info('End of COPY_OFFER');

   EXCEPTION
   WHEN OTHERS THEN
    app_plsql_log.info('Last EXCEPTION status in CPY_OFFR_SCREEN while updating MPS_PLSQL_OUTPUT: 1');
    update mps_plsql_output
       set txt_value = '1'
     where (run_id, seq_nr) = (select run_id, min(seq_nr)
                                 from mps_plsql_output
                                where key_value = l_parm_id
                                group by run_id);
        app_plsql_log.info ('Unknown Error.. Contact System Administrator'  || ': ' || SQLERRM (SQLCODE));
   END;
END CPY_OFFR;
/
