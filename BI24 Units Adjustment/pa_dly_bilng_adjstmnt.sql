create or replace PACKAGE PA_DLY_BILNG_ADJSTMNT AS 
  /*********************************************************
  * History
  * Created by   : Schiff Gy
  * Date         : 25/10/2016
  * Description  : First created 
  ******************************************************/

  PROCEDURE SET_DLY_BILNG_ADJSTMNT(p_dly_bilng_id IN NUMBER,
                         p_new_bi24_units IN NUMBER,
                         p_user_id IN VARCHAR2,
                         p_stus OUT NUMBER);
                                  
  FUNCTION GET_DLY_BILNG_ADJSTMNT(p_mrkt_id IN NUMBER,
                        p_sls_perd_id IN NUMBER,
                        p_offr_perd_id IN NUMBER,
                        p_prcsng_dt IN DATE
                        ) RETURN OBJ_DLY_BILNG_ADJSTMNT_TABLE PIPELINED;                                

  FUNCTION GET_DLY_BILNG_ADJSTMNT2(p_dly_bilng_id_list IN NUMBER_ARRAY)
    RETURN OBJ_DLY_BILNG_ADJSTMNT_TABLE PIPELINED;                                

END PA_DLY_BILNG_ADJSTMNT;

create or replace PACKAGE BODY PA_DLY_BILNG_ADJSTMNT AS

  FUNCTION GET_DLY_BILNG_ADJSTMNT(p_mrkt_id IN NUMBER,
                        p_sls_perd_id IN NUMBER,
                        p_offr_perd_id IN NUMBER,
                        p_prcsng_dt IN DATE
                        ) RETURN OBJ_DLY_BILNG_ADJSTMNT_TABLE PIPELINED AS
  CURSOR cc IS
      select
        OBJ_DLY_BILNG_ADJSTMNT_LINE(
            dly_bilng.dly_bilng_id,
            dly_bilng.FSC_CD,
            dly_bilng.prod_desc_txt,
            dly_bilng.sku_id,
            mrkt_sku.LCL_SKU_NM,    
            dly_bilng.sls_prc_amt,
            dly_bilng.nr_for_qty,
            dly_bilng.sls_prc_amt/dly_bilng.nr_for_qty,
            DECODE((SELECT 1 FROM dual
              WHERE EXISTS( SELECT SKU_PRC_AMT FROM MRKT_PERD_SKU_PRC MPSP
                              WHERE MPSP.MRKT_ID=p_mrkt_id AND MPSP.OFFR_PERD_ID=p_sls_perd_id
                                AND MPSP.SKU_ID=mrkt_sku.SKU_ID AND MPSP.PRC_LVL_TYP_CD='RP' )
                AND EXISTS( SELECT HOLD_COSTS_IND FROM SKU_COST SC 
                              WHERE SC.MRKT_ID=p_mrkt_id AND SC.OFFR_PERD_ID=p_offr_perd_id
                                AND SC.SKU_ID=mrkt_sku.SKU_ID AND COST_TYP='P')
                AND (PA_MAPS_PUBLIC.get_sls_cls_cd(p_offr_perd_id, p_mrkt_id, mrkt_sku.avlbl_perd_id,
                         mrkt_sku.intrdctn_perd_id, mrkt_sku.demo_ofs_nr, mrkt_sku.demo_durtn_nr, mrkt_sku.new_durtn_nr,
                         mrkt_sku.stus_perd_id, mrkt_sku.dspostn_perd_id, mrkt_sku.on_stus_perd_id)!='-1')),'1','A','N'),
            NVL2(offr_sku_line.SKU_ID,'P','N'),
            dly_bilng.unit_qty,
            DLY_BILNG_ADJSTMNT.UNIT_QTY,
            DLY_BILNG_ADJSTMNT.LAST_UPDT_TS,
            DLY_BILNG_ADJSTMNT.USER_FRST_NM || ' ' || DLY_BILNG_ADJSTMNT.USER_LAST_NM) cline
      from (WITH ACT_FSC AS
             (SELECT FSC_CD, MAX(STRT_PERD_ID) MAX_PERD_ID 
                FROM MRKT_FSC 
                WHERE MRKT_ID=p_mrkt_id AND STRT_PERD_ID<=p_sls_perd_id
                  AND dltd_ind<>'Y' and dltd_ind<>'y'
                GROUP BY FSC_CD),
            ALL_FSC AS
            (SELECT *
             FROM MRKT_FSC 
             WHERE MRKT_ID=p_mrkt_id AND STRT_PERD_ID<=p_sls_perd_id
               AND dltd_ind<>'Y' and dltd_ind<>'y') 
            SELECT
              dly_bilng.dly_bilng_id,
              dly_bilng.unit_qty,
              dly_bilng.sls_prc_amt,
              dly_bilng.nr_for_qty,
              dly_bilng.prcsng_dt,
              ALL_FSC.FSC_CD,
              ALL_FSC.sku_id,
              ALL_FSC.prod_desc_txt
            FROM 
              DLY_BILNG,ACT_FSC,ALL_FSC 
            WHERE DLY_BILNG.MRKT_ID=p_mrkt_id 
              AND DLY_BILNG.OFFR_PERD_ID=p_offr_perd_id
              AND DLY_BILNG.SLS_PERD_ID = p_sls_perd_id
              AND DLY_BILNG.FSC_CD = ACT_FSC.FSC_CD
              AND ACT_FSC.FSC_CD = ALL_FSC.FSC_CD
              AND ACT_FSC.MAX_PERD_ID = ALL_FSC.STRT_PERD_ID
              AND trunc(PRCSNG_DT) = trunc(p_prcsng_dt)   
            order by FSC_CD,prcsng_dt
          ) dly_bilng,
          (select distinct sku_id
            from offr,offr_sku_line
            where offr_sku_line.mrkt_id =p_mrkt_id
              and offr.mrkt_id = p_mrkt_id
              and offr.ver_id = 0
              and offr.offr_perd_id = p_offr_perd_id
              and offr.offr_typ='CMP'
              and offr.offr_id = offr_sku_line.offr_id
              and offr_sku_line.offr_perd_id = p_offr_perd_id
              and offr_sku_line.dltd_ind<>'Y'
              and offr_sku_line.dltd_ind<>'y'
          ) offr_sku_line,
         (select MPS_USER.USER_FRST_NM, MPS_USER.USER_LAST_NM, DLY_BILNG_ADJSTMNT.*
           from MPS_USER,DLY_BILNG_ADJSTMNT
           where
             MPS_USER.USER_NM=DLY_BILNG_ADJSTMNT.LAST_UPDT_USER_ID(+)
         ) DLY_BILNG_ADJSTMNT,
         mrkt_sku
      where 
        mrkt_sku.mrkt_id = p_mrkt_id
        and dly_bilng.sku_id = mrkt_sku.sku_id
        and dly_bilng.sku_id = offr_sku_line.sku_id(+)
        and dly_bilng.dly_bilng_id = DLY_BILNG_ADJSTMNT.dly_bilng_id(+)
    ;        
  BEGIN
    FOR rec in cc LOOP
      pipe row(rec.cline);
    END LOOP; 
  END GET_DLY_BILNG_ADJSTMNT;
  
  FUNCTION GET_DLY_BILNG_ADJSTMNT2(p_dly_bilng_id_list IN NUMBER_ARRAY)
    RETURN OBJ_DLY_BILNG_ADJSTMNT_TABLE PIPELINED AS
  CURSOR cc IS
    WITH SDB AS
  (SELECT TT.COLUMN_VALUE DLY_BILNG_ID,
    DLY_BILNG.MRKT_ID,
    DLY_BILNG.SLS_PERD_ID,
    DLY_BILNG.OFFR_PERD_ID,
    DLY_BILNG.FSC_CD,
    DLY_BILNG.UNIT_QTY,
    DLY_BILNG.SLS_PRC_AMT,
    DLY_BILNG.NR_FOR_QTY,
    DLY_BILNG.SKU_ID,
    DLY_BILNG.OFFR_SKU_LINE_ID
  FROM TABLE(p_dly_bilng_id_list) TT,
    DLY_BILNG
  WHERE TT.COLUMN_VALUE = DLY_BILNG.DLY_BILNG_ID
  ),
  FSCD AS
  (SELECT T.MRKT_ID,
    T.FSC_CD,
    MFSC.PROD_DESC_TXT
  FROM
    (SELECT MAX(MF.FSC_CD) FSC_CD,
      MAX(MF.MRKT_ID) MRKT_ID,
      MAX(MF.STRT_PERD_ID) MAX_PERD_ID
    FROM sdb DB
    INNER JOIN MRKT_FSC MF
    ON DB.FSC_CD        = MF.FSC_CD
    WHERE DB.MRKT_ID    = MF.MRKT_ID
    AND DB.SLS_PERD_ID >= MF.STRT_PERD_ID
    AND MF.DLTD_IND    <> 'Y'
    AND MF.DLTD_IND    <> 'y'
    GROUP BY DB.DLY_BILNG_ID
    ) T
  INNER JOIN mrkt_fsc MFSC
    ON T.FSC_CD       = MFSC.FSC_CD
      AND T.MRKT_ID     = MFSC.MRKT_ID
      AND T.MAX_PERD_ID = MFSC.STRT_PERD_ID
  )
  SELECT OBJ_DLY_BILNG_ADJSTMNT_LINE(
    DB.DLY_BILNG_ID, DB.FSC_CD, FSCD.PROD_DESC_TXT,
    DB.SKU_ID, S.LCL_SKU_NM, DB.SLS_PRC_AMT, DB.NR_FOR_QTY,
    DB.SLS_PRC_AMT / DB.NR_FOR_QTY, 
    DECODE((SELECT 1 FROM dual 
      WHERE EXISTS(
          SELECT MPSP.SKU_PRC_AMT
          FROM MRKT_PERD_SKU_PRC MPSP
          WHERE MPSP.MRKT_ID      = DB.MRKT_ID
            AND MPSP.OFFR_PERD_ID   = DB.OFFR_PERD_ID
            AND MPSP.SKU_ID         = S.SKU_ID
            AND MPSP.PRC_LVL_TYP_CD = 'RP'
          )
        AND EXISTS(
          SELECT SC.HOLD_COSTS_IND
          FROM SKU_COST SC
          WHERE SC.MRKT_ID    = DB.MRKT_ID
            AND SC.OFFR_PERD_ID = DB.OFFR_PERD_ID
            AND SC.SKU_ID       = S.SKU_ID
            AND SC.COST_TYP     = 'P'
          )
        AND PA_MAPS_PUBLIC.get_sls_cls_cd(DB.OFFR_PERD_ID, DB.MRKT_ID,
              S.AVLBL_PERD_ID, S.INTRDCTN_PERD_ID, S.DEMO_OFS_NR, S.DEMO_DURTN_NR,
              S.NEW_DURTN_NR, S.STUS_PERD_ID, S.DSPOSTN_PERD_ID,
              S.ON_STUS_PERD_ID) != '-1'), '1', 'A', 'N'),
      DECODE((SELECT COUNT(*) FROM OFFR_SKU_LINE OSL
        WHERE OSL.OFFR_ID IN(
            SELECT O.OFFR_ID FROM OFFR O
            WHERE O.MRKT_ID    = DB.MRKT_ID
              AND O.VER_ID       = 0
              AND O.OFFR_TYP     = 'CMP'
              AND O.OFFR_PERD_ID = DB.OFFR_PERD_ID)
          AND OSL.DLTD_IND NOT IN ('Y', 'y')
          AND OSL.SKU_ID        = S.SKU_ID ), 0, 'N', 'P'),
      DB.UNIT_QTY, DBAT.UNIT_QTY, DBAT.LAST_UPDT_TS,
      U.USER_FRST_NM || ' ' || U.USER_LAST_NM
      ) cline
    FROM sdb DB, FSCD, MRKT_SKU S, DLY_BILNG_ADJSTMNT DBAT, MPS_USER U
    WHERE DB.MRKT_ID             = S.MRKT_ID(+)
      AND DB.SKU_ID              = S.SKU_ID(+)
      AND DBAT.LAST_UPDT_USER_ID = U.USER_NM(+)
      AND DB.MRKT_ID       = FSCD.MRKT_ID(+)
      AND DB.FSC_CD       = FSCD.FSC_CD(+)
      AND DB.DLY_BILNG_ID       = DBAT.DLY_BILNG_ID(+)
    ;
  BEGIN

    FOR rec in cc LOOP
      pipe row(rec.cline);
    END LOOP;
END GET_DLY_BILNG_ADJSTMNT2;

  PROCEDURE SET_DLY_BILNG_ADJSTMNT(p_dly_bilng_id IN NUMBER,
                         p_new_bi24_units IN NUMBER,
                         p_user_id IN VARCHAR2,
                         p_stus OUT NUMBER) AS
  /*********************************************************
  * INPUT p_new_bi24_units IS NULL handled as record has to be deleted
  *
  * Possible OUT Values
  * 0 - success
  * 1 - New BI24 Units value is negative (not allowed)
  * 2 - database error in DELETE, UPDATE or INSERT statements
  * 3 - Obligatory foreign keys (DLY_BILNG) not found
  ******************************************************/
    counter1 NUMBER;
  BEGIN
    p_STUS:=0;
    IF NVL(p_new_bi24_units,0)>=0 THEN
      SELECT count(*) INTO counter1 FROM DLY_BILNG WHERE DLY_BILNG_ID=p_dly_bilng_id;
      IF counter1>0 THEN
    -- p_new_bi24_units IS NULL, the record if exists
        IF p_new_bi24_units IS NULL THEN
          BEGIN
          SAVEPOINT before_delete;
            DELETE FROM DLY_BILNG_ADJSTMNT WHERE DLY_BILNG_ID=p_dly_bilng_id;
          EXCEPTION WHEN OTHERS THEN ROLLBACK TO before_delete; p_stus:=2;
          END;
    -- otherwise upsert using p_new_bi24_units
        ELSE
          BEGIN
          SAVEPOINT before_upsert; 		 
            MERGE INTO DLY_BILNG_ADJSTMNT trgt
              USING (SELECT p_dly_bilng_id t1 FROM dual) src
                ON (trgt.DLY_BILNG_ID=src.t1)
              WHEN MATCHED THEN
                UPDATE SET trgt.UNIT_QTY = p_new_bi24_units, trgt.LAST_UPDT_USER_ID=p_user_id
              WHEN NOT MATCHED THEN
                INSERT (DLY_BILNG_ID,UNIT_QTY,LAST_UPDT_USER_ID)
                  VALUES (p_dly_bilng_id,p_new_bi24_units,p_user_id);
  	       EXCEPTION WHEN OTHERS THEN ROLLBACK TO before_changes; p_stus:=2;
  		     END;	
        END IF;
      ELSE p_STUS:=3;  
      END IF;
    ELSE p_STUS:=1;
    END IF;
  END SET_DLY_BILNG_ADJSTMNT;
                                  
END PA_DLY_BILNG_ADJSTMNT;