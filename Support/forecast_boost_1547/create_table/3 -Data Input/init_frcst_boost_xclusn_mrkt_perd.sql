-- initial LOAD
INSERT INTO frcst_boost_xclusn_mrkt_perd
  (mrkt_id,
   trgt_offr_perd_id,
   catgry_id_list,
   sls_cls_cd_list,
   sgmt_id_list)
  (SELECT fx.mrkt_id,
          mp.perd_id,
          cscs.catgry_id_list,
          cscs.sls_cls_cd_list,
          cscs.sgmt_id_list
     FROM frcst_boost_xclusn fx,
          mrkt_perd mp,
          (SELECT (SELECT listagg(catgry_id, ',') within GROUP(ORDER BY catgry_id)
                     FROM catgry
                    WHERE catgry_nm IN ('COLOR',
                                        'SKIN CARE',
                                        'FRAGRANCE',
                                        'PERSONAL CARE',
                                        'HAIR CARE')) AS catgry_id_list,
                  (SELECT listagg(sls_cls_cd, ',') within GROUP(ORDER BY sls_cls_cd)
                     FROM sls_cls
                    WHERE sls_cls_desc_txt = 'EXCESS') AS sls_cls_cd_list,
                  (SELECT listagg(sgmt_id, ',') within GROUP(ORDER BY sgmt_id)
                     FROM sgmt
                    WHERE sgmt_nm <> 'IMPLEMENTS') AS sgmt_id_list
             FROM dual) cscs
    WHERE mp.mrkt_id = fx.mrkt_id
      AND mp.perd_id BETWEEN fx.strt_offr_perd_id AND
          nvl(fx.end_offr_perd_id, 99990399)
      AND mp.yr_nr BETWEEN 2016 AND 2020
      AND mp.perd_typ = 'SC');
