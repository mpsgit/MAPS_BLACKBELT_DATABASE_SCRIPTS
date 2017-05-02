INSERT INTO trend_offst
  (mrkt_id, eff_sls_perd_id, sls_typ_id, offst)
  SELECT mrkt_id,
         eff_sls_perd_id,
         sls_typ_id,
         CASE
           WHEN sls_typ_id IN (3, 103) THEN
            CASE
              WHEN m_cntry_cd = 'UK'
                   AND sls_typ_id = 3 THEN
               2
              ELSE
               1
            END
           ELSE
            0
         END offst
    FROM (SELECT sls_typ_id, sls_typ_nm
            FROM sls_typ_grp
            JOIN sls_typ
           USING (sls_typ_grp_id)
           WHERE sls_typ_grp_desc_txt = 'Trend') trt,
         (SELECT mrkt_id,
                 MIN(perd_id) eff_sls_perd_id,
                 MAX(cntry_cd) m_cntry_cd
            FROM mrkt_perd
            JOIN mrkt
           USING (mrkt_id)
           WHERE perd_id >= 20000301
           GROUP BY mrkt_id) mpl;
