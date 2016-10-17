  /*********************************************************
  * History
  * Created by   : Schiff Gy
  * Date         : 12/10/2016
  * Description  : queries needed to list cash_value and r_factor
  *                for selected periods
  ******************************************************/   
SELECT
  MAX(SLS_PERD_ID) as MAX_PERD_ID
FROM DLY_BILNG
WHERE MRKT_ID=68 -- Parameter to be replaced when used in JOOQ
GROUP BY MRKT_ID;

SELECT 
  T.MRKT_ID,
  T.PERD_ID,
  SP.SCT_CASH_VAL as cash_val,
  SP.SCT_R_FACTOR as rf_val,
  SP.LAST_UPDT_TS,
  U.USER_NM as MRKT_LAST_USER
FROM MRKT_PERD T
  LEFT JOIN MRKT_SLS_PERD SP
    ON SP.MRKT_ID=T.MRKT_ID AND SP.SLS_PERD_ID=T.PERD_ID
  LEFT JOIN MPS_USER U
    ON U.USER_NM=SP.LAST_UPDT_USER_ID
WHERE T.PERD_TYP='SC'
  AND T.MRKT_ID=68 -- Parameter to be replaced when used in JOOQ
  AND T.PERD_ID IN(20160301,20160302,20160303) -- Parameter to be replaced when used in JOOQ
ORDER BY T.MRKT_ID, T.PERD_ID desc
;