/* TYPES */
@@Types/FSC_CD_LIST_ARRAY.sql
@@Types/OBJ_CASH_VAL_MANTNC_LINE.sql
@@Types/OBJ_CASH_VAL_MANTNC_TABLE.sql
@@Types/OBJ_DLY_BILNG_ADJSTMNT_LINE.sql
@@Types/OBJ_DLY_BILNG_ADJSTMNT_TABLE.sql
@@Types/PA_MANL_TREND_ADJSTMNT_LINE.sql
@@Types/PA_MANL_TREND_ADJSTMNT_TABLE.sql
@@Types/PA_SKU_BIAS_MANTNC_LINE.sql
@@Types/PA_SKU_BIAS_MANTNC_TABLE.sql
@@Types/SCT_TREND_CHECK_RPT_LINE.sql
@@Types/SCT_TREND_CHECK_RPT_TABLE.sql
@@Types/P94_RPT_HEAD_LINE.sql
@@Types/P94_RPT_HEAD_TABLE.sql
@@Types/OBJ_CUST_GRP_MANTNC_LINE.sql
@@Types/OBJ_CUST_GRP_MANTNC_TABLE.sql
@@Types/OBJ_CUST_GRP_MANTNC_GA_LINE.sql
@@Types/OBJ_CUST_GRP_MANTNC_GA_TABLE.sql
@@Types/OBJ_PA_TREND_ALLOC_CRRNT_LINE.sql
@@Types/OBJ_PA_TREND_ALLOC_CRRNT_TABLE.sql
@@Types/OBJ_PA_TREND_ALLOC_HIST_HD_LN.sql
@@Types/OBJ_PA_TREND_ALLOC_HIST_HD_TBL.sql
@@Types/OBJ_PA_TREND_ALLOC_HIST_DT_LN.sql
@@Types/OBJ_PA_TREND_ALLOC_HIST_DT_TBL.sql
@@Types/OBJ_PA_TREND_ALLOC_PRD_DTL_LN.sql
@@Types/OBJ_PA_TREND_ALLOC_PRD_DTL_TBL.sql
@@Types/OBJ_PA_TREND_ALLOC_RULES_LINE.sql
@@Types/OBJ_PA_TREND_ALLOC_RULES_TABLE.sql
@@Types/OBJ_PA_TREND_ALLOC_SGMNT_LINE.sql
@@Types/OBJ_PA_TREND_ALLOC_SGMNT_TABLE.sql
@@Types/OBJ_PA_TREND_ALLOC_VIEW_LINE.sql
@@Types/OBJ_PA_TREND_ALLOC_VIEW_TABLE.sql
@@Types/OBJ_TA_CONFIG_LINE.sql
@@Types/OBJ_TA_CONFIG_TABLE.sql
@@Types/OBJ_MAPS_MENU.sql
@@Types/T_MAPS_MENU.sql

/* TABLES */
@@Tables/CUSTM_RUL_MSTR.sql
@@Tables/CUSTM_RUL_PERD.sql
@@Tables/CUSTM_RUL_SKU_LIST.sql
@@Tables/CUSTM_SEG_MSTR.sql
@@Tables/CUSTM_SEG_PERD.sql
@@Tables/MRKT_PERD_PARTS.sql
@@Tables/MRKT_TRND_SLS_PERD_SLS_TYP.sql
@@Tables/MRKT_PERD_THROW_FORWRD_PRCT.sql
@@Tables/TEMP_CONFIG.sql
-----------------------------------------------
-- "OLD" version of tables has to be dropped --
-----------------------------------------------
drop table TREND_ALLOC_HIST_DTLS cascade constraints;
drop table TREND_ALLOC_HIST_DTLS_LOG cascade constraints;
-----------------------------------------------
-- "NEW" version of tables has to be created --
-----------------------------------------------
@@Tables/TREND_ALLOC_HIST_DTLS.sql
@@Tables/TREND_ALLOC_HIST_DTLS_LOG.sql
/*
@@Tables/TA_DICT.sql
@@Tables/TA_CONFIG.sql
*/
----------------------------------
-- alter table      TA_DICT     -- 
----------------------------------
@@Tables/alter_table_TA_DICT.sql

/* SEQUENCES */
@@Sequences/SEQ_RUL_ID.sql

/* TRIGGERS */
@@Triggers/TR_BUI_CUSTM_RUL_MSTR.sql
@@Triggers/TR_BUI_CUSTM_RUL_PERD.sql
@@Triggers/TR_BUI_CUSTM_RUL_SKULST.sql
@@Triggers/TR_BUI_CUSTMSEG_MSTR.sql
@@Triggers/TR_BUI_CUSTMSEG_PERD.sql
@@Triggers/TR_BUI_MRKTPERDPRTS.sql
@@Triggers/TR_BUI_MRKTTRNDSLSPERD_SLS_TYP.sql

/* PACKAGES */
@@Packages/PA_SIMS_MANTNC.sql
@@Packages/PA_SIMS_MANTNC_body.sql
@@Packages/PA_SIMS_CUST_GRP_MANTNC.sql
@@Packages/PA_SIMS_CUST_GRP_MANTNC_body.sql
@@Packages/PA_TREND_ALLOC.sql
@@Packages/PA_TREND_ALLOC_body.sql
@@Packages/PA_MAPS_MENU.sql
@@Packages/PA_MAPS_MENU_body.sql

/* initial LOAD */
@@Utils/initiate_MRKT_PERD_PARTS.sql
-- TA_DICT --
-- @@Utils/initiate_TA_DICT.sql
-- TA_CONFIG --
-- @@Utils/initiate_TA_CONFIG.sql
-- TREND_OFFST --
-- @@Utils/initiate_TREND_OFFST.sql
-- CONFIG_ITEM   and    MRKT_CONFIG_ITEM --
@@Utils/maintain_CONFIG_ITEM___MRKT_CONFIG_ITEM.sql

/* Jobs */
@@Utils/PA_TREND_ALLOC_JOB.sql

/* Recompile objects */
/*
BEGIN
    DBMS_UTILITY.COMPILE_SCHEMA('MAPS_DEV');
END;
/
*/

/* Grant rights */
/* @@Rights/grants.sql */

/* CREATE Synonyms */
/* @@Synonyms/Synonyms.sql */

/* Jobs */
/* @@Synonyms/synonyms.sql */

/* Show errors */
/*
WITH err
     AS (SELECT DISTINCT owner,
                         name,
                         TYPE,
                         line,
                         position,
                         sequence,
                         text
           FROM dba_errors
          WHERE sequence = 1 AND owner = 'MAPS_DEV')
SELECT DECODE (n, -1, '* ', ' ') || text text
  FROM (SELECT sequence n,
               owner,
               name,
               TYPE,
               line,
               LPAD (' ', position - 1, ' ') || '^' || text text
          FROM err
        UNION ALL
        SELECT DISTINCT
               -1 n,
               owner,
               name,
               TYPE,
               line,
               TYPE || ' ' || owner || '.' || name || ' line ' || line
          FROM err
        UNION ALL
        SELECT 0,
               owner,
               name,
               TYPE,
               line,
               text
          FROM dba_source
         WHERE (owner,
                name,
                TYPE,
                line) IN (SELECT owner,
                                 name,
                                 TYPE,
                                 line
                            FROM err)
        ORDER BY owner,
                 name,
                 TYPE,
                 line,
                 n);
*/

