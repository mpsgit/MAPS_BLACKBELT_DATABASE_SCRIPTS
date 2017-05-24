-- initial LOAD
INSERT INTO frcst_boost_xclusn_mrkt_perd
  (mrkt_id, trgt_offr_perd_id, catgry_id, sls_cls_cd, sgmt_id)
  (SELECT fx.mrkt_id,
          mp.perd_id,
          cscs.catgry_id,
          cscs.sls_cls_cd,
          cscs.sgmt_id
     FROM frcst_boost_xclusn fx,
          mrkt_perd mp,
          (SELECT catgry_id, sls_cls_cd, sgmt_id
             FROM catgry c, sls_cls sc, sgmt s
            WHERE catgry_nm IN ('COLOR',
                                'SKIN CARE',
                                'FRAGRANCE',
                                'PERSONAL CARE',
                                'HAIR CARE')
              AND sc.sls_cls_desc_txt = 'EXCESS'
              AND s.sgmt_nm <> 'IMPLEMENTS') cscs
    WHERE mp.mrkt_id = fx.mrkt_id
      AND mp.perd_id BETWEEN fx.strt_offr_perd_id AND
          nvl(fx.end_offr_perd_id, 99990399)
      AND mp.yr_nr BETWEEN 2016 AND 2020
      AND mp.perd_typ = 'SC');
