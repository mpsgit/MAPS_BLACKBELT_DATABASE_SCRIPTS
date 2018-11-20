CREATE OR REPLACE PACKAGE BODY PA_MAPS_EDIT_OFFR
AS
  TYPE t_str_array IS TABLE OF VARCHAR2(32767);

  TYPE t_default_values IS RECORD (
    -- offr default values
    pg_wght_pct                 NUMBER,
    pg_typ_id                   NUMBER,
    ssnl_evnt_id                NUMBER,
    sctn_page_ofs_nr            NUMBER,
    featrd_side_cd              NUMBER,
    offr_stus_cd                NUMBER,
    offr_cls_id                 NUMBER,
    offr_typ                    VARCHAR2(3),
    flap_ind                    VARCHAR2(1),
    brchr_postn_id              NUMBER,
    unit_rptg_lvl_id            NUMBER,
    rpt_sbtl_typ_id             NUMBER,
    -- offr_prfl_prc_point default values
    promtn_clm_id               NUMBER,
    promtn_id                   NUMBER,
    nr_for_qty                  NUMBER,
    unit_qty                    NUMBER,
    sls_prc_amt                 NUMBER,
    wghtd_avg_cost_amt          NUMBER,
    sls_srce_id                 NUMBER,
    tax_type_id                 NUMBER,
    pymt_typ                    NUMBER,
    comsn_typ                   VARCHAR2(5),
    prmry_offr_ind              VARCHAR2(5),
    pg_ofs_nr                   VARCHAR2(1),
    concept_featrd_side_cd      NUMBER,
    chrty_amt                   VARCHAR2(5),
    awrd_sls_prc_amt            NUMBER,
    prod_endrsmt_id             NUMBER
  );

  co_config_item_id        CONSTANT config_item.config_item_id%TYPE := 3250;
  co_ci_defval_offr_id     CONSTANT config_item.config_item_id%TYPE := 9200;
  co_ci_defval_prcpt_id    CONSTANT config_item.config_item_id%TYPE := 9210;
  co_sls_typ_estimate      CONSTANT sls_typ.sls_typ_id%TYPE := 1;
  co_sls_typ_op_estimate   CONSTANT sls_typ.sls_typ_id%TYPE := 2;

  g_sls_typ_id             NUMBER := 1;

  g_run_id                 NUMBER := 0;
  g_user_id                mps_plsql_log.user_id%TYPE;

  e_oscs_dup_val           EXCEPTION;
  e_prcpnt_already_exists  EXCEPTION;

  -- WIF scenario procedures and functions

  FUNCTION get_scenario_list(p_mrkt_id      IN number_array,
                             p_offr_perd_id IN number_array,
                             p_veh_id       IN number_array) RETURN obj_scenario_table PIPELINED IS

    l_module_name          VARCHAR2(30) := 'GET_SCENARIO_LIST';

  BEGIN
    g_run_id  := app_plsql_output.generate_new_run_id;
    g_user_id := RTRIM(sys_context('USERENV', 'OS_USER'), 35);

    app_plsql_output.set_run_id(g_run_id);
    app_plsql_log.set_context(g_user_id, g_package_name, g_run_id);
    app_plsql_log.info(l_module_name || ' start');

    FOR rec IN (
      SELECT scnrio_id, scnrio_desc_txt
        FROM what_if_scnrio
       WHERE mrkt_id IN (SELECT * FROM TABLE(p_mrkt_id))
         AND (strt_perd_id IN (SELECT * FROM TABLE(p_offr_perd_id))
           OR end_perd_id IN (SELECT * FROM TABLE(p_offr_perd_id))
         )
         --AND veh_id IN (SELECT * FROM TABLE(p_veh_id))
         AND enbl_scnrio_ind = 'Y'
         AND shr_ind = 'Y'
    )
    LOOP
      PIPE ROW (obj_scenario_line(rec.scnrio_id, rec.scnrio_desc_txt));
    END LOOP;

    app_plsql_log.info(l_module_name || ' stop');

  EXCEPTION
    WHEN OTHERS THEN
      app_plsql_log.info(l_module_name || ' ' || SQLERRM(SQLCODE));
      RAISE;
  END get_scenario_list;

  PROCEDURE add_scenario(p_mrkt_id         IN NUMBER,
                         p_veh_id          IN NUMBER,
                         p_scnrio_desc_txt IN VARCHAR2,
                         p_strt_perd_id    IN NUMBER,
                         p_end_perd_id     IN NUMBER,
                         p_user_nm         IN VARCHAR2,
                         p_scnrio_id      OUT NUMBER) IS

    l_module_name          VARCHAR2(30) := 'ADD_SCENARIO';

  BEGIN
    app_plsql_log.info(l_module_name || ' start');

    SELECT seq.nextval INTO p_scnrio_id FROM dual;

    INSERT INTO what_if_scnrio (
      mrkt_id, veh_id, scnrio_id, scnrio_desc_txt, strt_perd_id, end_perd_id,
      creat_user_id, creat_ts, shr_ind, enbl_scnrio_ind, cpa_ind)
    VALUES (
      p_mrkt_id, p_veh_id, p_scnrio_id, p_scnrio_desc_txt, p_strt_perd_id, p_end_perd_id,
      p_user_nm, SYSDATE, 'Y', 'Y', 'Y');

    app_plsql_log.info(l_module_name || ' stop');

  EXCEPTION
    WHEN OTHERS THEN
      app_plsql_log.info(l_module_name || ' ' || SQLERRM(SQLCODE));
      RAISE;
  END add_scenario;

  PROCEDURE add_offr_to_scenario(p_mrkt_id   IN NUMBER,
                                 p_veh_id    IN NUMBER,
                                 p_scnrio_id IN NUMBER,
                                 p_offr_id   IN NUMBER) IS

    l_module_name          VARCHAR2(30) := 'ADD_OFFR_TO_SCENARIO';

    l_tran_id              NUMBER;

  BEGIN
    app_plsql_log.info(l_module_name || ' start');

    SELECT seq.nextval INTO l_tran_id FROM dual;

    INSERT INTO what_if_tran (
      mrkt_id, veh_id, scnrio_id, tran_id, tran_typ, offr_id)
    VALUES (
      p_mrkt_id, p_veh_id, p_scnrio_id, l_tran_id, 'WIF', p_offr_id);

    app_plsql_log.info(l_module_name || ' stop');

  EXCEPTION
    WHEN OTHERS THEN
      app_plsql_log.info(l_module_name || ' ' || SQLERRM(SQLCODE));
      RAISE;
  END add_offr_to_scenario;

  FUNCTION get_sprd_scenarios(p_mrkt_id      IN NUMBER,
                              p_offr_perd_id IN NUMBER,
                              p_veh_id       IN NUMBER,
                              p_mvps_id      IN NUMBER,
                              p_pg_nr        IN NUMBER,
                              p_offr_id      IN NUMBER) RETURN number_array IS

    l_module_name          VARCHAR2(30) := 'GET_SPRD_SCENARIOS';

    l_scnrio_id_arr        number_array;

  BEGIN
    app_plsql_log.info(l_module_name || ' start');

    SELECT DISTINCT scnrio_id
      BULK COLLECT INTO l_scnrio_id_arr
      FROM what_if_tran t
     WHERE t.offr_id IN (SELECT o.offr_id
                           FROM offr o
                          WHERE o.mrkt_veh_perd_sctn_id = p_mvps_id
                            AND o.sctn_page_ofs_nr = p_pg_nr
                            AND o.mrkt_id = p_mrkt_id
                            AND o.offr_perd_id = p_offr_perd_id
                            AND o.veh_id = p_veh_id
                            AND o.offr_id <> p_offr_id);

    app_plsql_log.info(l_module_name || ' stop');

    RETURN l_scnrio_id_arr;

  EXCEPTION
    WHEN OTHERS THEN
      app_plsql_log.info(l_module_name || ' ' || SQLERRM(SQLCODE));
      RAISE;
  END get_sprd_scenarios;

  PROCEDURE copy_offr_add_to_scnrio(p_offr_id       IN offr.offr_id%TYPE,
                                    p_offr_desc_txt IN offr.offr_desc_txt%TYPE,
                                    p_user_nm       IN VARCHAR2,
                                    p_mrkt_id       IN offr.mrkt_id%TYPE, 
                                    p_offr_perd_id  IN offr.offr_perd_id%TYPE, 
                                    p_veh_id        IN offr.veh_id%TYPE, 
                                    p_scnrio_id     IN what_if_scnrio.scnrio_id%TYPE,
                                    p_new_offr_id  OUT offr.offr_id%TYPE) IS

    l_module_name          VARCHAR2(30) := 'COPY_OFFR_ADD_TO_SCNRIO';
    l_log                  VARCHAR2(1000);

  BEGIN
    l_log := 'Copying the offer';
    p_new_offr_id := pa_maps_copy.copy_offer(par_offerid        => p_offr_id,
                                             par_newmarketid    => p_mrkt_id,
                                             par_newofferperiod => p_offr_perd_id,
                                             par_newvehid       => p_veh_id,
                                             par_newoffrdesc    => p_offr_desc_txt,
                                             par_zerounits      => TRUE,
                                             par_whatif         => TRUE,
                                             par_paginationcopy => TRUE,
                                             par_enrgychrt      => FALSE,
                                             par_user           => p_user_nm);

    l_log := 'Updating offr_desc_txt';
    UPDATE offr o
       SET o.offr_desc_txt = p_offr_desc_txt
     WHERE o.offr_id = p_new_offr_id;

    l_log := 'Adding offer to scenario';
    add_offr_to_scenario(p_mrkt_id   => p_mrkt_id,
                         p_veh_id    => p_veh_id,
                         p_scnrio_id => p_scnrio_id,
                         p_offr_id   => p_new_offr_id);

  EXCEPTION
    WHEN OTHERS THEN
      app_plsql_log.info(l_module_name || ' ' || l_log || ' ' || SQLERRM(SQLCODE));
      RAISE;
  END copy_offr_add_to_scnrio;

PROCEDURE create_wif_offrs_wif(p_user_nm        IN VARCHAR2,
                                 p_mvps_id        IN offr.mrkt_veh_perd_sctn_id%TYPE,
                                 p_pg_nr          IN offr.sctn_page_ofs_nr%TYPE,
                                 p_mrkt_id        IN offr.mrkt_id%TYPE,
                                 p_offr_perd_id   IN offr.offr_perd_id%TYPE,
                                 p_veh_id         IN offr.veh_id%TYPE,
                                 p_scnrio_id      IN what_if_scnrio.scnrio_id%TYPE,
                                 p_get_offr_table IN OUT NOCOPY obj_get_offr_table) IS

    l_module_name          VARCHAR2(30) := 'CREATE_WIF_OFFRS_WIF';
    l_log                  VARCHAR2(1000);

    l_new_offr_id          offr.offr_id%TYPE;
  BEGIN
    app_plsql_log.info(l_module_name || ' start');

     app_plsql_log.info('create_wif_offrs_wif start:'
    || '-> '|| p_user_nm
    || '-> '|| p_mvps_id
    || '-> '|| p_pg_nr
    || '-> '|| p_mrkt_id  
    || '-> '|| p_offr_perd_id  
    || '-> '|| p_veh_id        
    || '-> '|| p_scnrio_id   
    );

    l_log := 'Offer cursor';
    FOR offr_rec IN (
      SELECT o.offr_id,
             o.offr_desc_txt,
             o.offr_typ
        FROM offr o
       WHERE NOT EXISTS (
               SELECT 1
                 FROM offr o2,
                      what_if_tran t
                WHERE t.offr_id = o2.offr_id
                  AND t.tran_typ = 'WIF'
                  AND t.scnrio_id = p_scnrio_id
                  AND o2.mrkt_id = o.mrkt_id
                  AND o2.offr_perd_id = o.offr_perd_id
                  AND o2.veh_id = o.veh_id
                  AND o2.ver_id = o.ver_id
                  AND o2.mrkt_veh_perd_sctn_id = o.mrkt_veh_perd_sctn_id
                  AND o2.sctn_page_ofs_nr = o.sctn_page_ofs_nr
                  AND o2.offr_link_id = o.offr_id --KK
                  --AND REPLACE(o2.offr_desc_txt, ' (copy)') = REPLACE(o.offr_desc_txt, ' (copy)')
                  AND o2.offr_typ = 'WIF'
             )
         AND o.mrkt_veh_perd_sctn_id = p_mvps_id
         AND o.sctn_page_ofs_nr      = p_pg_nr
         AND o.mrkt_id               = p_mrkt_id
         AND o.offr_perd_id          = p_offr_perd_id
         AND o.veh_id                = p_veh_id
         AND o.ver_id                = 0
         AND o.offr_typ              = 'CMP'
    )
    LOOP
        
      app_plsql_log.info('create_wif_offrs_wif loop:'
    || '-> '|| offr_rec.offr_id
    || '-> '|| offr_rec.offr_desc_txt);  
    
      copy_offr_add_to_scnrio(offr_rec.offr_id, offr_rec.offr_desc_txt, p_user_nm, p_mrkt_id, p_offr_perd_id, p_veh_id, p_scnrio_id, l_new_offr_id);

      p_get_offr_table.EXTEND();
      p_get_offr_table(p_get_offr_table.LAST) := obj_get_offr_line(l_new_offr_id, 1);
    END LOOP;

    app_plsql_log.info(l_module_name || ' stop');

  EXCEPTION
    WHEN OTHERS THEN
      app_plsql_log.info(l_module_name || ' ' || l_log || ' ' || SQLERRM(SQLCODE));
      RAISE;
  END create_wif_offrs_wif;

  PROCEDURE create_wif_offrs_cmp(p_offr_id        IN NUMBER,
                                 p_user_nm        IN VARCHAR2,
                                 p_mrkt_id        IN offr.mrkt_id%TYPE,
                                 p_offr_perd_id   IN offr.offr_perd_id%TYPE,
                                 p_veh_id         IN offr.veh_id%TYPE,
                                 p_scnrio_id      IN what_if_scnrio.scnrio_id%TYPE,
                                 p_get_offr_table IN OUT NOCOPY obj_get_offr_table) IS

    l_module_name          VARCHAR2(30) := 'CREATE_WIF_OFFRS_CMP';
    l_log                  VARCHAR2(1000);

    l_new_offr_id          offr.offr_id%TYPE;

  BEGIN
    app_plsql_log.info(l_module_name || ' start');

    app_plsql_log.info('create_wif_offrs_cmp start:'
    || '-> '|| p_offr_id 
    || '-> '|| p_user_nm
    || '-> '|| p_mrkt_id  
    || '-> '|| p_offr_perd_id  
    || '-> '|| p_veh_id        
    || '-> '|| p_scnrio_id   
    );

    l_log := 'Offer cursor';
    FOR offr_rec IN (
      SELECT o.offr_id,
             o.offr_desc_txt,
             o.offr_typ
        FROM offr o
       WHERE NOT EXISTS (
               SELECT 1
                 FROM offr o2,
                      what_if_tran t
                WHERE t.offr_id(+) = o2.offr_id
                  AND t.tran_typ(+) = 'WIF'
                  AND t.scnrio_id = p_scnrio_id
                  AND o2.offr_link_id = o.offr_id --KK AND o2.offr_id = o.offr_link_id
                  AND o2.offr_typ = 'WIF'
                  AND o2.ver_id = o.ver_id
             )
         AND o.offr_id = p_offr_id
         AND o.ver_id  = 0
         AND o.offr_typ ='CMP'
    )
    LOOP
    
      app_plsql_log.info('create_wif_offrs_cmp loop:'
    || '-> '|| offr_rec.offr_id
    || '-> '|| offr_rec.offr_desc_txt);
    
      copy_offr_add_to_scnrio(offr_rec.offr_id, offr_rec.offr_desc_txt, p_user_nm, p_mrkt_id, p_offr_perd_id, p_veh_id, p_scnrio_id, l_new_offr_id);

      p_get_offr_table.EXTEND();
      p_get_offr_table(p_get_offr_table.LAST) := obj_get_offr_line(l_new_offr_id, 1);
    END LOOP;

    app_plsql_log.info(l_module_name || ' stop');

  EXCEPTION
    WHEN OTHERS THEN
      app_plsql_log.info(l_module_name || ' ' || l_log || ' ' || SQLERRM(SQLCODE));
      RAISE;
  END create_wif_offrs_cmp;

  PROCEDURE manage_scenario(p_offr_id        IN NUMBER,
                            p_user_nm        IN VARCHAR2,
                            p_get_offr_table IN OUT NOCOPY obj_get_offr_table) IS

    l_module_name          VARCHAR2(30) := 'MANAGE_SCENARIO';
    l_log                  VARCHAR2(1000);

    l_mvps_id              offr.mrkt_veh_perd_sctn_id%TYPE;
    l_pg_nr                offr.sctn_page_ofs_nr%TYPE;
    l_offr_typ             offr.offr_typ%TYPE;
    l_mrkt_id              offr.mrkt_id%TYPE;
    l_offr_perd_id         offr.offr_perd_id%TYPE;
    l_veh_id               offr.veh_id%TYPE;
    l_scnrio_id            what_if_tran.scnrio_id%TYPE;
    l_scnrio_id_arr        number_array;

  BEGIN
    app_plsql_log.info(l_module_name || ' start');

    l_log := 'Querying offr';
    SELECT o.mrkt_veh_perd_sctn_id,
           o.sctn_page_ofs_nr,
           o.offr_typ,
           o.mrkt_id,
           o.offr_perd_id,
           o.veh_id,
           t.scnrio_id
      INTO l_mvps_id,
           l_pg_nr,
           l_offr_typ,
           l_mrkt_id,
           l_offr_perd_id,
           l_veh_id,
           l_scnrio_id
      FROM offr o,
           what_if_tran t
     WHERE t.offr_id(+) = o.offr_id
       AND t.tran_typ(+) = 'WIF'
       AND o.offr_id = p_offr_id;

    l_log := 'Get spread scenarios';
    l_scnrio_id_arr := get_sprd_scenarios(l_mrkt_id, l_offr_perd_id, l_veh_id, l_mvps_id, l_pg_nr, p_offr_id);

    IF l_offr_typ = 'CMP' AND l_scnrio_id_arr.COUNT > 0 THEN

      l_log := 'Creating WIF offers for CMP offer';
      FOR scnr_ind IN l_scnrio_id_arr.FIRST .. l_scnrio_id_arr.LAST LOOP
        create_wif_offrs_cmp(p_offr_id, p_user_nm, l_mrkt_id, l_offr_perd_id, l_veh_id, l_scnrio_id_arr(scnr_ind), p_get_offr_table);
      END LOOP;

    ELSIF l_offr_typ = 'WIF' AND l_scnrio_id IS NOT NULL THEN

      l_log := 'Creating WIF offers for WIF offer';
      create_wif_offrs_wif(p_user_nm, l_mvps_id, l_pg_nr, l_mrkt_id, l_offr_perd_id, l_veh_id, l_scnrio_id, p_get_offr_table);
    ELSE
      app_plsql_log.info(l_module_name || ' no WIF offers created');
    END IF;

    app_plsql_log.info(l_module_name || ' stop');

  EXCEPTION
    WHEN OTHERS THEN
      app_plsql_log.info(l_module_name || ' ' || l_log || ' ' || SQLERRM(SQLCODE));
      RAISE;
  END manage_scenario;

  PROCEDURE get_offers_in_scnrio(p_offr_id               IN NUMBER,
                                 p_scnrio_id             IN NUMBER,
                                 p_mrkt_id               IN NUMBER,
                                 p_offr_perd_id          IN NUMBER,
                                 p_veh_id                IN NUMBER,
                                 p_ver_id                IN NUMBER,
                                 p_get_offr_table        IN OUT NOCOPY obj_get_offr_table) IS

    l_module_name          VARCHAR2(30) := 'GET_OFFERS_IN_SCNRIO';

  BEGIN
    app_plsql_log.info(l_module_name || ' start');

    FOR offr_rec IN (
      SELECT o.offr_id
        FROM offr o,
             what_if_tran t
       WHERE t.offr_id = o.offr_id
         AND t.tran_typ = 'WIF'
         AND t.scnrio_id = p_scnrio_id
         AND o.mrkt_id = p_mrkt_id
         AND o.offr_perd_id = p_offr_perd_id
         AND o.veh_id = p_veh_id
         AND o.ver_id = p_ver_id
         AND o.offr_id <> p_offr_id
    )
    LOOP
      p_get_offr_table.EXTEND;
      p_get_offr_table(p_get_offr_table.LAST) := obj_get_offr_line(offr_rec.offr_id, g_sls_typ_id);
    END LOOP;

    app_plsql_log.info(l_module_name || ' stop');

  EXCEPTION
    WHEN OTHERS THEN
      app_plsql_log.info(l_module_name || ' ' || SQLERRM(SQLCODE));
      RAISE;

  END get_offers_in_scnrio;

  -- lock procedures and functions

FUNCTION  lock_offr_chk(p_offr_id IN NUMBER, p_user_nm IN VARCHAR2) RETURN NUMBER
  AS
    ln_status NUMBER;
  BEGIN
    SELECT COUNT(*) INTO ln_status FROM offr_lock WHERE offr_lock.offr_id = p_offr_id AND offr_lock.user_nm = p_user_nm;
    --dbms_output.put_line('lock_offr_chk:' || ln_status);
    RETURN ln_status;
  END;

PROCEDURE lock_offr(p_offr_id      IN NUMBER,
                    p_user_nm      IN VARCHAR2,
                    p_clstr_id     IN NUMBER,
                    p_lock_user_nm OUT VARCHAR2,
                    p_status       OUT NUMBER) AS
  lv_lock_user VARCHAR2(35);
  ln_lock      NUMBER;
BEGIN
  BEGIN
    SELECT COUNT(*)
      INTO ln_lock
      FROM offr_lock
     WHERE offr_id = p_offr_id
       AND clstr_id = p_clstr_id;
  EXCEPTION
    WHEN no_data_found THEN
      ln_lock := 0;
  END;
  BEGIN
    SELECT user_nm
      INTO lv_lock_user
      FROM offr_lock
     WHERE offr_id = p_offr_id
       AND clstr_id = p_clstr_id;
  EXCEPTION
    WHEN no_data_found THEN
      lv_lock_user := NULL;
  END;
  IF ln_lock > 0
     AND p_user_nm <> lv_lock_user
     AND lv_lock_user IS NOT NULL THEN
    p_status       := 3; --locked by someone else
    p_lock_user_nm := lv_lock_user;
  ELSIF ln_lock > 0
        AND p_user_nm = lv_lock_user
        AND lv_lock_user IS NOT NULL THEN
    p_status       := 2; --already locked by user
    p_lock_user_nm := lv_lock_user;
  END IF;
  IF ln_lock = 0 THEN
    BEGIN
      INSERT INTO offr_lock
        (offr_id,lock_Ts, user_nm, sys_id, clstr_id)
      VALUES
        (p_offr_id, sysdate, p_user_nm, 1, p_clstr_id);
      p_status       := 1; --successful lock

      p_lock_user_nm := p_user_nm;
    EXCEPTION
      WHEN no_data_needed
      THEN RETURN;
      WHEN OTHERS THEN
        p_status       := 0; --error while locking
        p_lock_user_nm := NULL;
        app_plsql_log.info('Error while locking offer_id: ' ||
                           to_char(p_offr_id) || ' user name: ' ||
                           p_user_nm || ' error_msg:' || SQLERRM(SQLCODE));
    END;
  END IF;
END lock_offr;

PROCEDURE lock_offr(p_lock_offr IN obj_edit_offr_lock_table,
                    r_lock_offr OUT obj_edit_offr_lock_res_table) AS
BEGIN
    r_lock_offr := obj_edit_offr_lock_res_table();
FOR rec IN (SELECT * FROM TABLE(p_lock_offr))
  LOOP

    BEGIN
      r_lock_offr.extend();
      r_lock_offr(r_lock_offr.last) := obj_edit_offr_lock_res_line(NULL,rec.offr_id,NULL);
      lock_offr(rec.offr_id,rec.user_nm,rec.clstr_id,r_lock_offr(r_lock_offr.last).lock_user_nm,r_lock_offr(r_lock_offr.last).status);
    END;

  END LOOP;
  EXCEPTION WHEN OTHERS THEN
  app_plsql_log.info('Error while bulk locking, error_msg:' || SQLERRM(SQLCODE));

END lock_offr;

PROCEDURE unlock_offr(p_offr_id      IN NUMBER,
                      p_user_nm      IN VARCHAR2,
                      p_lock_user_nm OUT VARCHAR2,
                      p_status       OUT NUMBER) AS
  lv_lock_user VARCHAR2(35);
  ln_lock      NUMBER;
BEGIN
  BEGIN
    SELECT COUNT(*)
      INTO ln_lock
      FROM offr_lock
     WHERE offr_id = p_offr_id;
  EXCEPTION
    WHEN no_data_found THEN
      ln_lock := 0;
  END;
  BEGIN
    SELECT user_nm
      INTO lv_lock_user
      FROM offr_lock
     WHERE offr_id = p_offr_id;
  EXCEPTION
    WHEN no_data_found THEN
      lv_lock_user := NULL;
  END;
  IF ln_lock > 0
     AND p_user_nm <> lv_lock_user
     AND lv_lock_user IS NOT NULL THEN
    p_status       := 3; --locked by someone else
    p_lock_user_nm := lv_lock_user;
  ELSIF ln_lock > 0
        AND p_user_nm = lv_lock_user
        AND lv_lock_user IS NOT NULL THEN
        BEGIN
      DELETE FROM offr_lock
      WHERE offr_lock.offr_id = p_offr_id;
      p_status       := 1; --successful delete
      p_lock_user_nm := p_user_nm;
      EXCEPTION
      WHEN OTHERS THEN
        p_status       := 0; --error while unlocking
        p_lock_user_nm := NULL;
        app_plsql_log.info('Error while unlocking offer_id: ' ||
                           to_char(p_offr_id) || ' user name: ' ||
                           p_user_nm || ' error_msg:' || SQLERRM(SQLCODE));
      END;
  END IF;
  IF ln_lock = 0 THEN
    p_status       := 2; --offr is not locked
    p_lock_user_nm := NULL;
  END IF;
END unlock_offr;

PROCEDURE unlock_offr(p_unlock_offr IN obj_edit_offr_lock_table,
                      r_unlock_offr OUT obj_edit_offr_lock_res_table) AS
BEGIN
    r_unlock_offr := obj_edit_offr_lock_res_table();
FOR rec IN (SELECT * FROM TABLE(p_unlock_offr))
  LOOP

    BEGIN
      r_unlock_offr.extend();
      r_unlock_offr(r_unlock_offr.last) := obj_edit_offr_lock_res_line(NULL,rec.offr_id,NULL);
      unlock_offr(rec.offr_id,rec.user_nm,r_unlock_offr(r_unlock_offr.last).lock_user_nm,r_unlock_offr(r_unlock_offr.last).status);
    END;

  END LOOP;
  EXCEPTION WHEN OTHERS THEN
  app_plsql_log.info('Error while bulk unlocking, error_msg:' || SQLERRM(SQLCODE));

END unlock_offr;

PROCEDURE set_history(p_get_offr IN OBJ_GET_OFFR_TABLE, p_result OUT NUMBER)
AS
l_rowcount NUMBER;
BEGIN
   MERGE INTO EDIT_OFFR_HIST hist
   USING (SELECT * FROM table(pa_maps_edit_offr.get_offr(p_get_offr))) cur
   ON (
      nvl(hist.intrnl_offr_id         ,-1)  = nvl(cur.intrnl_offr_id             ,-1)
  AND nvl(hist.offr_sku_line_id       ,-1)  = nvl(cur.offr_sku_line_id           ,-1)
  AND nvl(hist.status                 ,-1)  = nvl(cur.status                     ,-1)
  AND nvl(hist.mrkt_id                ,-1)  = nvl(cur.mrkt_id                    ,-1)
  AND nvl(hist.offr_perd_id           ,-1)  = nvl(cur.offr_perd_id               ,-1)
  AND nvl(hist.offr_lock              ,-1)  = nvl(cur.offr_lock                  ,-1)
  AND nvl(hist.offr_lock_user         ,-1)  = nvl(cur.offr_lock_user             ,-1)
  AND nvl(hist.veh_id                 ,-1)  = nvl(cur.veh_id                     ,-1)
  AND nvl(hist.brchr_plcmnt_id        ,-1)  = nvl(cur.brchr_plcmnt_id            ,-1)
  AND nvl(hist.brchr_sctn_nm          ,-1)  = nvl(cur.brchr_sctn_nm              ,-1)
  AND nvl(hist.enrgy_chrt_postn_id    ,-1)  = nvl(cur.enrgy_chrt_postn_id        ,-1)
  AND nvl(hist.web_postn_id           ,-1)  = nvl(cur.web_postn_id               ,-1)
  AND nvl(hist.pg_nr                  ,-1)  = nvl(cur.pg_nr                      ,-1)
  AND nvl(hist.ctgry_id               ,-1)  = nvl(cur.ctgry_id                   ,-1)
  AND nvl(hist.brnd_id                ,-1)  = nvl(cur.brnd_id                    ,-1)
  AND nvl(hist.sgmt_id                ,-1)  = nvl(cur.sgmt_id                    ,-1)
  AND nvl(hist.form_id                ,-1)  = nvl(cur.form_id                    ,-1)
  AND nvl(hist.form_grp_id            ,-1)  = nvl(cur.form_grp_id                ,-1)
  AND nvl(hist.prfl_cd                ,-1)  = nvl(cur.prfl_cd                    ,-1)
  AND nvl(hist.sku_id                 ,-1)  = nvl(cur.sku_id                     ,-1)
  AND nvl(hist.fsc_cd                 ,-1)  = nvl(cur.fsc_cd                     ,-1)
  AND nvl(hist.prod_typ_id            ,-1)  = nvl(cur.prod_typ_id                ,-1)
  AND nvl(hist.gender_id              ,-1)  = nvl(cur.gender_id                  ,-1)
  AND nvl(hist.sls_cls_cd             ,-1)  = nvl(cur.sls_cls_cd                 ,-1)
  AND nvl(hist.pp_sls_cls_cd          ,-1)  = nvl(cur.pp_sls_cls_cd              ,-1)
  AND nvl(hist.offr_desc_txt          ,-1)  = nvl(cur.offr_desc_txt              ,-1)
  AND nvl(hist.offr_notes_txt         ,-1)  = nvl(cur.offr_notes_txt             ,-1)
  AND nvl(hist.offr_lyot_cmnts_txt    ,-1)  = nvl(cur.offr_lyot_cmnts_txt        ,-1)
  AND nvl(hist.featrd_side_cd         ,-1)  = nvl(cur.featrd_side_cd             ,-1)
  AND nvl(hist.concept_featrd_side_cd ,-1)  = nvl(cur.concept_featrd_side_cd     ,-1)
  AND nvl(hist.micr_ncpsltn_ind       ,-1)  = nvl(cur.micr_ncpsltn_ind           ,-1)
  AND nvl(hist.scntd_pg_typ_id        ,-1)  = nvl(cur.scntd_pg_typ_id            ,-1)
  AND nvl(hist.cnsmr_invstmt_bdgt_id  ,-1)  = nvl(cur.cnsmr_invstmt_bdgt_id      ,-1)
  AND nvl(hist.pymt_typ               ,-1)  = nvl(cur.pymt_typ                   ,-1)
  AND nvl(hist.promtn_id              ,-1)  = nvl(cur.promtn_id                  ,-1)
  AND nvl(hist.promtn_clm_id          ,-1)  = nvl(cur.promtn_clm_id              ,-1)
  AND nvl(hist.spndng_lvl             ,-1)  = nvl(cur.spndng_lvl                 ,-1)
  AND nvl(hist.comsn_typ              ,-1)  = nvl(cur.comsn_typ                  ,-1)
  AND nvl(hist.tax_type_id            ,-1)  = nvl(cur.tax_type_id                ,-1)
  AND nvl(hist.wsl_ind                ,-1)  = nvl(cur.wsl_ind                    ,-1)
  AND nvl(hist.offr_sku_set_id        ,-1)  = nvl(cur.offr_sku_set_id            ,-1)
  AND nvl(hist.cmpnt_qty              ,-1)  = nvl(cur.cmpnt_qty                  ,-1)
  AND nvl(hist.nr_for_qty             ,-1)  = nvl(cur.nr_for_qty                 ,-1)
  AND nvl(hist.nta_factor             ,-1)  = nvl(cur.nta_factor                 ,-1)
  AND nvl(hist.sku_cost               ,-1)  = nvl(cur.sku_cost                   ,-1)
  AND nvl(hist.lv_nta                 ,-1)  = nvl(cur.lv_nta                     ,-1)
  AND nvl(hist.lv_sp                  ,-1)  = nvl(cur.lv_sp                      ,-1)
  AND nvl(hist.lv_rp                  ,-1)  = nvl(cur.lv_rp                      ,-1)
  AND nvl(hist.lv_discount            ,-1)  = nvl(cur.lv_discount                ,-1)
  AND nvl(hist.lv_units               ,-1)  = nvl(cur.lv_units                   ,-1)
  AND nvl(hist.lv_total_cost          ,-1)  = nvl(cur.lv_total_cost              ,-1)
  AND nvl(hist.lv_gross_sales         ,-1)  = nvl(cur.lv_gross_sales             ,-1)
  AND nvl(hist.lv_dp_cash             ,-1)  = nvl(cur.lv_dp_cash                 ,-1)
  AND nvl(hist.lv_dp_percent          ,-1)  = nvl(cur.lv_dp_percent              ,-1)
  AND nvl(hist.ver_id                 ,-1)  = nvl(cur.ver_id                     ,-1)
  AND nvl(hist.sls_prc_amt            ,-1)  = nvl(cur.sls_prc_amt                ,-1)
  AND nvl(hist.reg_prc_amt            ,-1)  = nvl(cur.reg_prc_amt                ,-1)
  AND nvl(hist.line_nr                ,-1)  = nvl(cur.line_nr                    ,-1)
  AND nvl(hist.unit_qty               ,-1)  = nvl(cur.unit_qty                   ,-1)
  AND nvl(hist.dltd_ind               ,-1)  = nvl(cur.dltd_ind                   ,-1)
  AND nvl(hist.created_ts,to_date('1989.02.15','yyyy.mm.dd'))  = nvl(cur.created_ts,to_date('1989.02.15','yyyy.mm.dd'))
  AND nvl(hist.created_user_id        ,-1)  = nvl(cur.created_user_id            ,-1)
  AND nvl(hist.last_updt_ts,to_date('1989.02.15','yyyy.mm.dd'))  = nvl(cur.last_updt_ts,to_date('1989.02.15','yyyy.mm.dd'))
  AND nvl(hist.last_updt_user_id      ,-1)  = nvl(cur.last_updt_user_id          ,-1)
  AND nvl(hist.mrkt_veh_perd_sctn_id  ,-1)  = nvl(cur.mrkt_veh_perd_sctn_id      ,-1)
  AND nvl(hist.prfl_nm                ,-1)  = nvl(cur.prfl_nm                    ,-1)
  AND nvl(hist.sku_nm                 ,-1)  = nvl(cur.sku_nm                     ,-1)
  AND nvl(hist.comsn_typ_desc_txt     ,-1)  = nvl(cur.comsn_typ_desc_txt         ,-1)
  AND nvl(hist.tax_typ_desc_txt       ,-1)  = nvl(cur.tax_typ_desc_txt           ,-1)
  AND nvl(hist.offr_sku_set_nm        ,-1)  = nvl(cur.offr_sku_set_nm            ,-1)
  AND nvl(hist.sls_typ                ,-1)  = nvl(cur.sls_typ                    ,-1)
  AND nvl(hist.pc_sp_py               ,-1)  = nvl(cur.pc_sp_py                   ,-1)
  AND nvl(hist.pc_rp                  ,-1)  = nvl(cur.pc_rp                      ,-1)
  AND nvl(hist.pc_sp                  ,-1)  = nvl(cur.pc_sp                      ,-1)
  AND nvl(hist.pc_vsp                 ,-1)  = nvl(cur.pc_vsp                     ,-1)
  AND nvl(hist.pc_hit                 ,-1)  = nvl(cur.pc_hit                     ,-1)
  AND NVL(hist.pg_wght                ,-1)  = NVL(cur.pg_wght                    ,-1)
  AND NVL(hist.sprd_nr                ,-1)  = NVL(cur.sprd_nr                    ,-1)
  AND NVL(hist.offr_prfl_prcpt_id     ,-1)  = NVL(cur.offr_prfl_prcpt_id         ,-1)
  AND NVL(hist.offr_typ               ,-1)  = NVL(cur.offr_typ                   ,-1)
  AND NVL(hist.forcasted_units        ,-1)  = NVL(cur.forcasted_units            ,-1)
  AND NVL(hist.forcasted_date, to_date('1989.02.15','yyyy.mm.dd'))  = NVL(cur.forcasted_date, to_date('1989.02.15','yyyy.mm.dd'))
  AND NVL(hist.offr_cls_id            ,-1)  = NVL(cur.offr_cls_id                ,-1)
  AND NVL(hist.spcl_ordr_ind          ,-1)  = NVL(cur.spcl_ordr_ind              ,-1)
  AND NVL(hist.offr_ofs_nr            ,-1)  = NVL(cur.offr_ofs_nr                ,-1)
  AND NVL(hist.pp_ofs_nr              ,-1)  = NVL(cur.pp_ofs_nr                  ,-1)
  AND NVL(hist.impct_catgry_id        ,-1)  = NVL(cur.impct_catgry_id            ,-1)
  AND NVL(hist.hero_ind               ,-1)  = NVL(cur.hero_ind                   ,-1)
  AND NVL(hist.smplg_ind              ,-1)  = NVL(cur.smplg_ind                  ,-1)
  AND NVL(hist.mltpl_ind              ,-1)  = NVL(cur.mltpl_ind                  ,-1)
  AND NVL(hist.cmltv_ind              ,-1)  = NVL(cur.cmltv_ind                  ,-1)
  AND nvl(hist.micr_ncpsltn_desc_txt  ,-1)  = nvl(cur.micr_ncpsltn_desc_txt      ,-1)
   )
   WHEN NOT MATCHED THEN INSERT (hist_ts, intrnl_offr_id, offr_sku_line_id, status, mrkt_id, offr_perd_id, offr_lock, 
                                 offr_lock_user, veh_id, brchr_plcmnt_id, brchr_sctn_nm, enrgy_chrt_postn_id, web_postn_id, 
                                 pg_nr, ctgry_id, brnd_id, sgmt_id, form_id, form_grp_id, prfl_cd, sku_id, fsc_cd, 
                                 prod_typ_id, gender_id, sls_cls_cd, offr_desc_txt, offr_notes_txt, offr_lyot_cmnts_txt, 
                                 featrd_side_cd, concept_featrd_side_cd, micr_ncpsltn_ind, scntd_pg_typ_id, cnsmr_invstmt_bdgt_id, 
                                 pymt_typ, promtn_id, promtn_clm_id, spndng_lvl, comsn_typ, tax_type_id, wsl_ind, offr_sku_set_id, 
                                 cmpnt_qty, nr_for_qty, nta_factor, sku_cost, lv_nta, lv_sp, lv_rp, lv_discount, lv_units, 
                                 lv_total_cost, lv_gross_sales, lv_dp_cash, lv_dp_percent, ver_id, sls_prc_amt, reg_prc_amt, 
                                 line_nr, unit_qty, dltd_ind, created_ts, created_user_id, last_updt_ts, last_updt_user_id,
                                 mrkt_veh_perd_sctn_id, prfl_nm, sku_nm, comsn_typ_desc_txt, tax_typ_desc_txt, 
                                 offr_sku_set_nm, sls_typ, pc_sp_py, pc_rp, pc_sp, pc_vsp, pc_hit, pg_wght, sprd_nr, 
                                 offr_prfl_prcpt_id, offr_typ, forcasted_units, forcasted_date, offr_cls_id, spcl_ordr_ind, 
                                 offr_ofs_nr, pp_ofs_nr, impct_catgry_id, hero_ind, smplg_ind, mltpl_ind, cmltv_ind, pp_sls_cls_cd,micr_ncpsltn_desc_txt)
     VALUES (
             sysdate
            ,cur.intrnl_offr_id
            ,cur.offr_sku_line_id
            ,cur.status
            ,cur.mrkt_id
            ,cur.offr_perd_id
            ,cur.offr_lock
            ,cur.offr_lock_user
            ,cur.veh_id
            ,cur.brchr_plcmnt_id
            ,cur.brchr_sctn_nm
            ,cur.enrgy_chrt_postn_id
            ,cur.web_postn_id
            ,cur.pg_nr
            ,cur.ctgry_id
            ,cur.brnd_id
            ,cur.sgmt_id
            ,cur.form_id
            ,cur.form_grp_id
            ,cur.prfl_cd
            ,cur.sku_id
            ,cur.fsc_cd
            ,cur.prod_typ_id
            ,cur.gender_id
            ,cur.sls_cls_cd
            ,cur.offr_desc_txt
            ,cur.offr_notes_txt
            ,cur.offr_lyot_cmnts_txt
            ,cur.featrd_side_cd
            ,cur.concept_featrd_side_cd
            ,cur.micr_ncpsltn_ind
            ,cur.scntd_pg_typ_id
            ,cur.cnsmr_invstmt_bdgt_id
            ,cur.pymt_typ
            ,cur.promtn_id
            ,cur.promtn_clm_id
            ,cur.spndng_lvl
            ,cur.comsn_typ
            ,cur.tax_type_id
            ,cur.wsl_ind
            ,cur.offr_sku_set_id
            ,cur.cmpnt_qty
            ,cur.nr_for_qty
            ,cur.nta_factor
            ,cur.sku_cost
            ,cur.lv_nta
            ,cur.lv_sp
            ,cur.lv_rp
            ,cur.lv_discount
            ,cur.lv_units
            ,cur.lv_total_cost
            ,cur.lv_gross_sales
            ,cur.lv_dp_cash
            ,cur.lv_dp_percent
            ,cur.ver_id
            ,cur.sls_prc_amt
            ,cur.reg_prc_amt
            ,cur.line_nr
            ,cur.unit_qty
            ,cur.dltd_ind
            ,cur.created_ts
            ,cur.created_user_id
            ,cur.last_updt_ts
            ,cur.last_updt_user_id
            ,cur.mrkt_veh_perd_sctn_id
            ,cur.prfl_nm
            ,cur.sku_nm
            ,cur.comsn_typ_desc_txt
            ,cur.tax_typ_desc_txt
            ,cur.offr_sku_set_nm
            ,cur.sls_typ
            ,cur.pc_sp_py
            ,cur.pc_rp
            ,cur.pc_sp
            ,cur.pc_vsp
            ,cur.pc_hit
            ,cur.pg_wght
            ,cur.sprd_nr
            ,cur.offr_prfl_prcpt_id
            ,cur.offr_typ
            ,cur.forcasted_units
            ,cur.forcasted_date
            ,cur.offr_cls_id
            ,cur.spcl_ordr_ind
            ,cur.offr_ofs_nr
            ,cur.pp_ofs_nr
            ,cur.impct_catgry_id
            ,cur.hero_ind
            ,cur.smplg_ind
            ,cur.mltpl_ind
            ,cur.cmltv_ind
            ,cur.pp_sls_cls_cd
            ,cur.micr_ncpsltn_desc_txt
     );
     l_rowcount:=SQL%ROWCOUNT;
     app_plsql_log.info('Edit offr history merge rows inserted: ' || to_char(l_rowcount));
     IF l_rowcount > 0 THEN
        p_result := 2; --Changes were saved
     ELSE
        p_result := 1; --No changes found
     END IF;
     COMMIT;
     EXCEPTION WHEN OTHERS THEN
       p_result := 0;  --Error while saving
       ROLLBACK;
       app_plsql_log.info('Error in edit offr history merge ' || SQLERRM(SQLCODE));
END set_history;

PROCEDURE merge_history(p_get_offr IN OBJ_EDIT_OFFR_TABLE)
IS
PRAGMA AUTONOMOUS_TRANSACTION;
  l_rowcount NUMBER;
BEGIN

   MERGE INTO EDIT_OFFR_HIST hist
   USING (SELECT * FROM table(p_get_offr)) cur
   ON (
      nvl(hist.intrnl_offr_id         ,-1)  = nvl(cur.intrnl_offr_id             ,-1)
  AND nvl(hist.offr_sku_line_id       ,-1)  = nvl(cur.offr_sku_line_id           ,-1)
  AND nvl(hist.status                 ,-1)  = nvl(cur.status                     ,-1)
  AND nvl(hist.mrkt_id                ,-1)  = nvl(cur.mrkt_id                    ,-1)
  AND nvl(hist.offr_perd_id           ,-1)  = nvl(cur.offr_perd_id               ,-1)
  AND nvl(hist.offr_lock              ,-1)  = nvl(cur.offr_lock                  ,-1)
  AND nvl(hist.offr_lock_user         ,-1)  = nvl(cur.offr_lock_user             ,-1)
  AND nvl(hist.veh_id                 ,-1)  = nvl(cur.veh_id                     ,-1)
  AND nvl(hist.brchr_plcmnt_id        ,-1)  = nvl(cur.brchr_plcmnt_id            ,-1)
  AND nvl(hist.brchr_sctn_nm          ,-1)  = nvl(cur.brchr_sctn_nm              ,-1)
  AND nvl(hist.enrgy_chrt_postn_id    ,-1)  = nvl(cur.enrgy_chrt_postn_id        ,-1)
  AND nvl(hist.web_postn_id           ,-1)  = nvl(cur.web_postn_id               ,-1)
  AND nvl(hist.pg_nr                  ,-1)  = nvl(cur.pg_nr                      ,-1)
  AND nvl(hist.ctgry_id               ,-1)  = nvl(cur.ctgry_id                   ,-1)
  AND nvl(hist.brnd_id                ,-1)  = nvl(cur.brnd_id                    ,-1)
  AND nvl(hist.sgmt_id                ,-1)  = nvl(cur.sgmt_id                    ,-1)
  AND nvl(hist.form_id                ,-1)  = nvl(cur.form_id                    ,-1)
  AND nvl(hist.form_grp_id            ,-1)  = nvl(cur.form_grp_id                ,-1)
  AND nvl(hist.prfl_cd                ,-1)  = nvl(cur.prfl_cd                    ,-1)
  AND nvl(hist.sku_id                 ,-1)  = nvl(cur.sku_id                     ,-1)
  AND nvl(hist.fsc_cd                 ,-1)  = nvl(cur.fsc_cd                     ,-1)
  AND nvl(hist.prod_typ_id            ,-1)  = nvl(cur.prod_typ_id                ,-1)
  AND nvl(hist.gender_id              ,-1)  = nvl(cur.gender_id                  ,-1)
  AND nvl(hist.sls_cls_cd             ,-1)  = nvl(cur.sls_cls_cd                 ,-1)
  AND nvl(hist.pp_sls_cls_cd          ,-1)  = nvl(cur.pp_sls_cls_cd              ,-1)
  AND nvl(hist.offr_desc_txt          ,-1)  = nvl(cur.offr_desc_txt              ,-1)
  AND nvl(hist.offr_notes_txt         ,-1)  = nvl(cur.offr_notes_txt             ,-1)
  AND nvl(hist.offr_lyot_cmnts_txt    ,-1)  = nvl(cur.offr_lyot_cmnts_txt        ,-1)
  AND nvl(hist.featrd_side_cd         ,-1)  = nvl(cur.featrd_side_cd             ,-1)
  AND nvl(hist.concept_featrd_side_cd ,-1)  = nvl(cur.concept_featrd_side_cd     ,-1)
  AND nvl(hist.micr_ncpsltn_ind       ,-1)  = nvl(cur.micr_ncpsltn_ind           ,-1)
  AND nvl(hist.scntd_pg_typ_id        ,-1)  = nvl(cur.scntd_pg_typ_id            ,-1)
  AND nvl(hist.cnsmr_invstmt_bdgt_id  ,-1)  = nvl(cur.cnsmr_invstmt_bdgt_id      ,-1)
  AND nvl(hist.pymt_typ               ,-1)  = nvl(cur.pymt_typ                   ,-1)
  AND nvl(hist.promtn_id              ,-1)  = nvl(cur.promtn_id                  ,-1)
  AND nvl(hist.promtn_clm_id          ,-1)  = nvl(cur.promtn_clm_id              ,-1)
  AND nvl(hist.spndng_lvl             ,-1)  = nvl(cur.spndng_lvl                 ,-1)
  AND nvl(hist.comsn_typ              ,-1)  = nvl(cur.comsn_typ                  ,-1)
  AND nvl(hist.tax_type_id            ,-1)  = nvl(cur.tax_type_id                ,-1)
  AND nvl(hist.wsl_ind                ,-1)  = nvl(cur.wsl_ind                    ,-1)
  AND nvl(hist.offr_sku_set_id        ,-1)  = nvl(cur.offr_sku_set_id            ,-1)
  AND nvl(hist.cmpnt_qty              ,-1)  = nvl(cur.cmpnt_qty                  ,-1)
  AND nvl(hist.nr_for_qty             ,-1)  = nvl(cur.nr_for_qty                 ,-1)
  AND nvl(hist.nta_factor             ,-1)  = nvl(cur.nta_factor                 ,-1)
  AND nvl(hist.sku_cost               ,-1)  = nvl(cur.sku_cost                   ,-1)
  AND nvl(hist.lv_nta                 ,-1)  = nvl(cur.lv_nta                     ,-1)
  AND nvl(hist.lv_sp                  ,-1)  = nvl(cur.lv_sp                      ,-1)
  AND nvl(hist.lv_rp                  ,-1)  = nvl(cur.lv_rp                      ,-1)
  AND nvl(hist.lv_discount            ,-1)  = nvl(cur.lv_discount                ,-1)
  AND nvl(hist.lv_units               ,-1)  = nvl(cur.lv_units                   ,-1)
  AND nvl(hist.lv_total_cost          ,-1)  = nvl(cur.lv_total_cost              ,-1)
  AND nvl(hist.lv_gross_sales         ,-1)  = nvl(cur.lv_gross_sales             ,-1)
  AND nvl(hist.lv_dp_cash             ,-1)  = nvl(cur.lv_dp_cash                 ,-1)
  AND nvl(hist.lv_dp_percent          ,-1)  = nvl(cur.lv_dp_percent              ,-1)
  AND nvl(hist.ver_id                 ,-1)  = nvl(cur.ver_id                     ,-1)
  AND nvl(hist.sls_prc_amt            ,-1)  = nvl(cur.sls_prc_amt                ,-1)
  AND nvl(hist.reg_prc_amt            ,-1)  = nvl(cur.reg_prc_amt                ,-1)
  AND nvl(hist.line_nr                ,-1)  = nvl(cur.line_nr                    ,-1)
  AND nvl(hist.unit_qty               ,-1)  = nvl(cur.unit_qty                   ,-1)
  AND nvl(hist.dltd_ind               ,-1)  = nvl(cur.dltd_ind                   ,-1)
  AND nvl(hist.created_ts,to_date('1989.02.15','yyyy.mm.dd'))  = nvl(cur.created_ts,to_date('1989.02.15','yyyy.mm.dd'))
  AND nvl(hist.created_user_id        ,-1)  = nvl(cur.created_user_id            ,-1)
  AND nvl(hist.last_updt_ts,to_date('1989.02.15','yyyy.mm.dd'))  = nvl(cur.last_updt_ts,to_date('1989.02.15','yyyy.mm.dd'))
  AND nvl(hist.last_updt_user_id      ,-1)  = nvl(cur.last_updt_user_id          ,-1)
  AND nvl(hist.mrkt_veh_perd_sctn_id  ,-1)  = nvl(cur.mrkt_veh_perd_sctn_id      ,-1)
  AND nvl(hist.prfl_nm                ,-1)  = nvl(cur.prfl_nm                    ,-1)
  AND nvl(hist.sku_nm                 ,-1)  = nvl(cur.sku_nm                     ,-1)
  AND nvl(hist.comsn_typ_desc_txt     ,-1)  = nvl(cur.comsn_typ_desc_txt         ,-1)
  AND nvl(hist.tax_typ_desc_txt       ,-1)  = nvl(cur.tax_typ_desc_txt           ,-1)
  AND nvl(hist.offr_sku_set_nm        ,-1)  = nvl(cur.offr_sku_set_nm            ,-1)
  AND nvl(hist.sls_typ                ,-1)  = nvl(cur.sls_typ                    ,-1)
  AND nvl(hist.pc_sp_py               ,-1)  = nvl(cur.pc_sp_py                   ,-1)
  AND nvl(hist.pc_rp                  ,-1)  = nvl(cur.pc_rp                      ,-1)
  AND nvl(hist.pc_sp                  ,-1)  = nvl(cur.pc_sp                      ,-1)
  AND nvl(hist.pc_vsp                 ,-1)  = nvl(cur.pc_vsp                     ,-1)
  AND nvl(hist.pc_hit                 ,-1)  = nvl(cur.pc_hit                     ,-1)
  AND NVL(hist.pg_wght                ,-1)  = NVL(cur.pg_wght                    ,-1)
  AND NVL(hist.sprd_nr                ,-1)  = NVL(cur.sprd_nr                    ,-1)
  AND NVL(hist.offr_prfl_prcpt_id     ,-1)  = NVL(cur.offr_prfl_prcpt_id         ,-1)
  AND NVL(hist.offr_typ               ,-1)  = NVL(cur.offr_typ                   ,-1)
  AND NVL(hist.forcasted_units        ,-1)  = NVL(cur.forcasted_units            ,-1)
  AND NVL(hist.forcasted_date, to_date('1989.02.15','yyyy.mm.dd'))  = NVL(cur.forcasted_date, to_date('1989.02.15','yyyy.mm.dd'))
  AND NVL(hist.offr_cls_id            ,-1)  = NVL(cur.offr_cls_id                ,-1)
  AND NVL(hist.spcl_ordr_ind          ,-1)  = NVL(cur.spcl_ordr_ind              ,-1)
  AND NVL(hist.offr_ofs_nr            ,-1)  = NVL(cur.offr_ofs_nr                ,-1)
  AND NVL(hist.pp_ofs_nr              ,-1)  = NVL(cur.pp_ofs_nr                  ,-1)
  AND NVL(hist.impct_catgry_id        ,-1)  = NVL(cur.impct_catgry_id            ,-1)
  AND NVL(hist.hero_ind               ,-1)  = NVL(cur.hero_ind                   ,-1)
  AND NVL(hist.smplg_ind              ,-1)  = NVL(cur.smplg_ind                  ,-1)
  AND NVL(hist.mltpl_ind              ,-1)  = NVL(cur.mltpl_ind                  ,-1)
  AND NVL(hist.cmltv_ind              ,-1)  = NVL(cur.cmltv_ind                  ,-1)
  AND NVL(hist.micr_ncpsltn_desc_txt  ,-1)  = NVL(cur.micr_ncpsltn_desc_txt      ,-1)

   )
   WHEN NOT MATCHED THEN INSERT (hist_ts, intrnl_offr_id, offr_sku_line_id, status, mrkt_id, offr_perd_id, offr_lock, 
                                 offr_lock_user, veh_id, brchr_plcmnt_id, brchr_sctn_nm, enrgy_chrt_postn_id, web_postn_id, 
                                 pg_nr, ctgry_id, brnd_id, sgmt_id, form_id, form_grp_id, prfl_cd, sku_id, fsc_cd, 
                                 prod_typ_id, gender_id, sls_cls_cd, offr_desc_txt, offr_notes_txt, offr_lyot_cmnts_txt, 
                                 featrd_side_cd, concept_featrd_side_cd, micr_ncpsltn_ind, scntd_pg_typ_id, cnsmr_invstmt_bdgt_id,
                                 pymt_typ, promtn_id, promtn_clm_id, spndng_lvl, comsn_typ, tax_type_id, wsl_ind, offr_sku_set_id, 
                                 cmpnt_qty, nr_for_qty, nta_factor, sku_cost, lv_nta, lv_sp, lv_rp, lv_discount, lv_units, 
                                 lv_total_cost, lv_gross_sales, lv_dp_cash, lv_dp_percent, ver_id, sls_prc_amt, reg_prc_amt, 
                                 line_nr, unit_qty, dltd_ind, created_ts, created_user_id, last_updt_ts, last_updt_user_id,
                                 mrkt_veh_perd_sctn_id, prfl_nm, sku_nm, comsn_typ_desc_txt, tax_typ_desc_txt, 
                                 offr_sku_set_nm, sls_typ, pc_sp_py, pc_rp, pc_sp, pc_vsp, pc_hit, pg_wght, sprd_nr, 
                                 offr_prfl_prcpt_id, offr_typ, forcasted_units, forcasted_date, offr_cls_id, spcl_ordr_ind, 
                                 offr_ofs_nr, pp_ofs_nr, impct_catgry_id, hero_ind, smplg_ind, mltpl_ind, cmltv_ind, pp_sls_cls_cd, micr_ncpsltn_desc_txt)
     VALUES (
             sysdate
            ,cur.intrnl_offr_id
            ,cur.offr_sku_line_id
            ,cur.status
            ,cur.mrkt_id
            ,cur.offr_perd_id
            ,cur.offr_lock
            ,cur.offr_lock_user
            ,cur.veh_id
            ,cur.brchr_plcmnt_id
            ,cur.brchr_sctn_nm
            ,cur.enrgy_chrt_postn_id
            ,cur.web_postn_id
            ,cur.pg_nr
            ,cur.ctgry_id
            ,cur.brnd_id
            ,cur.sgmt_id
            ,cur.form_id
            ,cur.form_grp_id
            ,cur.prfl_cd
            ,cur.sku_id
            ,cur.fsc_cd
            ,cur.prod_typ_id
            ,cur.gender_id
            ,cur.sls_cls_cd
            ,cur.offr_desc_txt
            ,cur.offr_notes_txt
            ,cur.offr_lyot_cmnts_txt
            ,cur.featrd_side_cd
            ,cur.concept_featrd_side_cd
            ,cur.micr_ncpsltn_ind
            ,cur.scntd_pg_typ_id
            ,cur.cnsmr_invstmt_bdgt_id
            ,cur.pymt_typ
            ,cur.promtn_id
            ,cur.promtn_clm_id
            ,cur.spndng_lvl
            ,cur.comsn_typ
            ,cur.tax_type_id
            ,cur.wsl_ind
            ,cur.offr_sku_set_id
            ,cur.cmpnt_qty
            ,cur.nr_for_qty
            ,cur.nta_factor
            ,cur.sku_cost
            ,cur.lv_nta
            ,cur.lv_sp
            ,cur.lv_rp
            ,cur.lv_discount
            ,cur.lv_units
            ,cur.lv_total_cost
            ,cur.lv_gross_sales
            ,cur.lv_dp_cash
            ,cur.lv_dp_percent
            ,cur.ver_id
            ,cur.sls_prc_amt
            ,cur.reg_prc_amt
            ,cur.line_nr
            ,cur.unit_qty
            ,cur.dltd_ind
            ,cur.created_ts
            ,cur.created_user_id
            ,cur.last_updt_ts
            ,cur.last_updt_user_id
            ,cur.mrkt_veh_perd_sctn_id
            ,cur.prfl_nm
            ,cur.sku_nm
            ,cur.comsn_typ_desc_txt
            ,cur.tax_typ_desc_txt
            ,cur.offr_sku_set_nm
            ,cur.sls_typ
            ,cur.pc_sp_py
            ,cur.pc_rp
            ,cur.pc_sp
            ,cur.pc_vsp
            ,cur.pc_hit
            ,cur.pg_wght
            ,cur.sprd_nr
            ,cur.offr_prfl_prcpt_id
            ,cur.offr_typ
            ,cur.forcasted_units
            ,cur.forcasted_date
            ,cur.offr_cls_id
            ,cur.spcl_ordr_ind
            ,cur.offr_ofs_nr
            ,cur.pp_ofs_nr
            ,cur.impct_catgry_id
            ,cur.hero_ind
            ,cur.smplg_ind
            ,cur.mltpl_ind
            ,cur.cmltv_ind
            ,cur.pp_sls_cls_cd
            ,cur.micr_ncpsltn_desc_txt
            
     );
     l_rowcount:=SQL%ROWCOUNT;
     app_plsql_log.info('Edit offr history merge rows inserted: ' || to_char(l_rowcount));
         COMMIT;
     EXCEPTION WHEN OTHERS THEN
       ROLLBACK;
       app_plsql_log.info('Error in edit offr history merge ' || SQLERRM(SQLCODE));
END merge_history;

PROCEDURE log_params(p_module_name IN VARCHAR2,
                     p_offr_id     IN NUMBER,
                     p_sls_typ     IN NUMBER,
                     p_data_line   IN obj_edit_offr_table) IS

BEGIN
  FOR rec IN (
    SELECT * FROM TABLE(p_data_line)
     WHERE intrnl_offr_id = p_offr_id
       AND sls_typ        = p_sls_typ
  )
  LOOP
    app_plsql_log.info(SUBSTR(p_module_name || ', Params: ' ||
                  'status: ' ||                          rec.status ||
                  ', mrkt_id: ' ||                       rec.mrkt_id ||
                  ', offr_perd_id: ' ||                  rec.offr_perd_id ||
                  ', offr_lock: ' ||                     rec.offr_lock ||
                  ', offr_lock_user: ' ||                rec.offr_lock_user ||
                  ', offr_sku_line_id: ' ||              rec.offr_sku_line_id ||
                  ', veh_id: ' ||                        rec.veh_id ||
                  ', brchr_plcmnt_id: ' ||               rec.brchr_plcmnt_id ||
                  ', brchr_sctn_nm: ' ||                 rec.brchr_sctn_nm ||
                  ', enrgy_chrt_postn_id: ' ||           rec.enrgy_chrt_postn_id ||
                  ', web_postn_id: ' ||                  rec.web_postn_id ||
                  ', pg_nr: ' ||                         rec.pg_nr ||
                  ', ctgry_id: ' ||                      rec.ctgry_id ||
                  ', brnd_id: ' ||                       rec.brnd_id ||
                  ', sgmt_id: ' ||                       rec.sgmt_id ||
                  ', form_id: ' ||                       rec.form_id ||
                  ', form_grp_id: ' ||                   rec.form_grp_id ||
                  ', prfl_cd: ' ||                       rec.prfl_cd ||
                  ', sku_id: ' ||                        rec.sku_id ||
                  ', fsc_cd: ' ||                        rec.fsc_cd ||
                  ', prod_typ_id: ' ||                   rec.prod_typ_id ||
                  ', gender_id: ' ||                     rec.gender_id ||
                  ', sls_cls_cd: ' ||                    rec.sls_cls_cd ||
                  ', pp_sls_cls_cd: ' ||                 rec.pp_sls_cls_cd ||
                  ', item_sls_cls_cd: ' ||               rec.item_sls_cls_cd ||
                  ', offr_desc_txt: ' ||                 rec.offr_desc_txt ||
                  ', offr_notes_txt: ' ||                rec.offr_notes_txt ||
                  ', offr_lyot_cmnts_txt: ' ||           rec.offr_lyot_cmnts_txt ||
                  ', featrd_side_cd: ' ||                rec.featrd_side_cd ||
                  ', concept_featrd_side_cd: ' ||        rec.concept_featrd_side_cd ||
                  ', micr_ncpsltn_ind: ' ||              rec.micr_ncpsltn_ind ||
                  ', scntd_pg_typ_id: ' ||               rec.scntd_pg_typ_id ||
                  ', cnsmr_invstmt_bdgt_id: ' ||         rec.cnsmr_invstmt_bdgt_id ||
                  ', pymt_typ: ' ||                      rec.pymt_typ ||
                  ', promtn_id: ' ||                     rec.promtn_id ||
                  ', promtn_clm_id: ' ||                 rec.promtn_clm_id ||
                  ', spndng_lvl: ' ||                    rec.spndng_lvl ||
                  ', comsn_typ: ' ||                     rec.comsn_typ ||
                  ', tax_type_id: ' ||                   rec.tax_type_id ||
                  ', wsl_ind: ' ||                       rec.wsl_ind ||
                  ', offr_sku_set_id: ' ||               rec.offr_sku_set_id ||
                  ', cmpnt_qty: ' ||                     rec.cmpnt_qty ||
                  ', nr_for_qty: ' ||                    rec.nr_for_qty ||
                  ', nta_factor: ' ||                    rec.nta_factor ||
                  ', sku_cost: ' ||                      rec.sku_cost ||
                  ', lv_nta: ' ||                        rec.lv_nta ||
                  ', lv_sp: ' ||                         rec.lv_sp ||
                  ', lv_rp: ' ||                         rec.lv_rp ||
                  ', lv_discount: ' ||                   rec.lv_discount ||
                  ', lv_units: ' ||                      rec.lv_units ||
                  ', lv_total_cost: ' ||                 rec.lv_total_cost ||
                  ', lv_gross_sales: ' ||                rec.lv_gross_sales ||
                  ', lv_dp_cash: ' ||                    rec.lv_dp_cash ||
                  ', lv_dp_percent: ' ||                 rec.lv_dp_percent ||
                  ', ver_id: ' ||                        rec.ver_id ||
                  ', sls_prc_amt: ' ||                   rec.sls_prc_amt ||
                  ', reg_prc_amt: ' ||                   rec.reg_prc_amt ||
                  ', line_nr: ' ||                       rec.line_nr ||
                  ', unit_qty: ' ||                      rec.unit_qty ||
                  ', dltd_ind: ' ||                      rec.dltd_ind ||
                  ', created_ts: ' ||                    rec.created_ts ||
                  ', created_user_id: ' ||               rec.created_user_id ||
                  ', last_updt_ts: ' ||                  rec.last_updt_ts ||
                  ', last_updt_user_id: ' ||             rec.last_updt_user_id ||
                  ', intrnl_offr_id: ' ||                rec.intrnl_offr_id ||
                  ', mrkt_veh_perd_sctn_id: ' ||         rec.mrkt_veh_perd_sctn_id ||
                  ', prfl_nm: ' ||                       rec.prfl_nm ||
                  ', sku_nm: ' ||                        rec.sku_nm ||
                  ', comsn_typ_desc_txt: ' ||            rec.comsn_typ_desc_txt ||
                  ', tax_typ_desc_txt: ' ||              rec.tax_typ_desc_txt ||
                  ', offr_sku_Set_nm: ' ||               rec.offr_sku_Set_nm ||
                  ', sls_typ: ' ||                       rec.sls_typ ||
                  ', pc_sp_py: ' ||                      rec.pc_sp_py ||
                  ', pc_rp: ' ||                         rec.pc_rp ||
                  ', pc_sp: ' ||                         rec.pc_sp ||
                  ', pc_vsp: ' ||                        rec.pc_vsp ||
                  ', pc_hit: ' ||                        rec.pc_hit ||
                  ', pg_wght: ' ||                       rec.pg_wght ||
                  ', sprd_nr: ' ||                       rec.sprd_nr ||
                  ', offr_prfl_prcpt_id: ' ||            rec.offr_prfl_prcpt_id ||
                  ', has_unit_qty: ' ||                  rec.has_unit_qty ||
                  ', offr_typ: ' ||                      rec.offr_typ ||
                  ', forcasted_units: ' ||               rec.forcasted_units ||
                  ', cur.forcasted_date: ' ||            rec.forcasted_date ||
                  ', offr_cls_id: ' ||                   rec.offr_cls_id ||
                  ', spcl_ordr_ind: ' ||                 rec.spcl_ordr_ind ||
                  ', offr_ofs_nr: ' ||                   rec.offr_ofs_nr ||
                  ', pp_ofs_nr: ' ||                     rec.pp_ofs_nr ||
                  ', impct_catgry_id: ' ||               rec.impct_catgry_id ||
                  ', hero_ind: ' ||                      rec.hero_ind ||
                  ', smplg_ind: ' ||                     rec.smplg_ind ||
                  ', mltpl_ind: ' ||                     rec.mltpl_ind ||
                  ', cmltv_ind: ' ||                     rec.cmltv_ind
                  , 1, 4000));
  END LOOP;
END log_params;

PROCEDURE save_edit_offr_lines(p_offr_id   IN NUMBER,
                               p_sls_typ   IN NUMBER,
                               p_data_line IN obj_edit_offr_table) IS

  l_module_name           VARCHAR2(30) := 'SAVE_EDIT_OFFR_LINES';
  l_log                   VARCHAR2(1000);
  l_cnt                   NUMBER;
  l_sku_cnt               NUMBER;
  l_old_micr_ncpsltn_ind  CHAR(1);
  l_old_sls_prc_amt       NUMBER;
  l_old_cmpnt_qty         NUMBER;
  l_old_unit_qty          NUMBER;
  l_old_nr_for_qty        NUMBER;
  l_no_sku_line           BOOLEAN;
  l_oss_id_to_zero        NUMBER := NULL;
  l_rowcount              NUMBER;

  CURSOR c_p_data_line IS
    SELECT * FROM TABLE(p_data_line)
     WHERE intrnl_offr_id = p_offr_id
       AND sls_typ        = p_sls_typ;

BEGIN
  app_plsql_log.info(l_module_name || ' start');

  -- insert into offr_prfl_sls_cls_plcmt, offr_sls_cls_sku, pg_wght if necessary
  l_log := 'insert (offr_prfl_sls_cls_plcmt, offr_sls_cls_sku, pg_wght)';
  FOR rec IN (
    SELECT osl.pg_ofs_nr
          ,l.*
      FROM TABLE(p_data_line) l
          ,offr_sku_line osl
     WHERE osl.offr_sku_line_id = l.offr_sku_line_id
       AND l.intrnl_offr_id     = p_offr_id
       AND l.sls_typ            = p_sls_typ
  )
  LOOP
    SELECT COUNT(*)
      INTO l_sku_cnt
      FROM offr_sku_line osl
     WHERE osl.offr_id = p_offr_id
       AND osl.prfl_cd = rec.prfl_cd;

    SELECT COUNT(*)
      INTO l_cnt
      FROM offr_prfl_sls_cls_plcmt p
     WHERE p.offr_id        = rec.intrnl_offr_id
       AND p.sls_cls_cd     = rec.pp_sls_cls_cd
       AND p.prfl_cd        = rec.prfl_cd
       AND p.pg_ofs_nr      = rec.pp_ofs_nr
       AND p.featrd_side_cd = rec.concept_featrd_side_cd;

    IF l_cnt = 0 THEN
      INSERT INTO offr_prfl_sls_cls_plcmt
      (
        offr_id, sls_cls_cd, prfl_cd, pg_ofs_nr, featrd_side_cd, mrkt_id, veh_id, offr_perd_id, sku_cnt,
        pg_wght_pct, sku_offr_strgth_pct, pg_typ_id, creat_user_id, last_updt_user_id
      )
      VALUES
      (
        rec.intrnl_offr_id, rec.pp_sls_cls_cd, rec.prfl_cd, rec.pp_ofs_nr, rec.concept_featrd_side_cd, rec.mrkt_id,
        rec.veh_id, rec.offr_perd_id, l_sku_cnt, rec.pp_pg_wght, 0, 1, rec.offr_lock_user, rec.offr_lock_user
      );
    ELSE
      UPDATE offr_prfl_sls_cls_plcmt p
         SET p.pg_wght_pct    = rec.pp_pg_wght
       WHERE p.offr_id        = rec.intrnl_offr_id
         AND p.sls_cls_cd     = rec.pp_sls_cls_cd
         AND p.prfl_cd        = rec.prfl_cd
         AND p.pg_ofs_nr      = rec.pp_ofs_nr
         AND p.featrd_side_cd = rec.concept_featrd_side_cd;
    END IF;

    SELECT COUNT(*)
      INTO l_cnt
      FROM offr_sls_cls_sku s
     WHERE s.offr_id        = rec.intrnl_offr_id
       AND s.sls_cls_cd     = rec.sls_cls_cd
       AND s.prfl_cd        = rec.prfl_cd
       AND s.pg_ofs_nr      = rec.pp_ofs_nr
       AND s.featrd_side_cd = rec.concept_featrd_side_cd
       AND s.sku_id         = rec.sku_id;

    IF l_cnt = 0 THEN
      INSERT INTO offr_sls_cls_sku
      (
        offr_id, sls_cls_cd, prfl_cd, pg_ofs_nr, featrd_side_cd, sku_id, mrkt_id, smplg_ind, hero_ind,
        micr_ncpsltn_ind, scntd_pg_typ_id, reg_prc_amt, incntv_id, cost_amt, mltpl_ind, cmltv_ind, wsl_ind,
        creat_user_id, last_updt_user_id
      )
      VALUES
      (
        rec.intrnl_offr_id, rec.sls_cls_cd, rec.prfl_cd, rec.pp_ofs_nr, rec.concept_featrd_side_cd, rec.sku_id,
        rec.mrkt_id, 'N', 'N', decode(rec.scntd_pg_typ_id, null, 'N', 'Y'), rec.scntd_pg_typ_id, rec.reg_prc_amt, 
        NULL, NULL, 'N', 'N', rec.wsl_ind, rec.offr_lock_user, rec.offr_lock_user
      );
    END IF;

    SELECT COUNT(*)
      INTO l_cnt
      FROM pg_wght w
     WHERE w.pg_wght_pct = rec.pg_wght;

    IF l_cnt = 0 THEN
      INSERT INTO pg_wght
      (
        pg_wght_pct, mrkt_id, pg_wght_desc_txt, creat_user_id, last_updt_user_id
      )
      VALUES
      (
        rec.pg_wght, rec.mrkt_id, to_char(rec.pg_wght) || '%', rec.offr_lock_user, rec.offr_lock_user
      );
    END IF;
    
    SELECT COUNT(*)
      INTO l_cnt
      FROM pg_wght w
     WHERE w.pg_wght_pct = rec.pp_pg_wght;

    IF l_cnt = 0 THEN
      INSERT INTO pg_wght
      (
        pg_wght_pct, mrkt_id, pg_wght_desc_txt, creat_user_id, last_updt_user_id
      )
      VALUES
      (
        rec.pp_pg_wght, rec.mrkt_id, to_char(rec.pp_pg_wght) || '%', rec.offr_lock_user, rec.offr_lock_user
      );
    END IF;

  END LOOP;
  app_plsql_log.info(l_log || ' finished');

  l_log := 'offr_sls_cls_sku';
  FOR rec IN (
    SELECT l.pp_ofs_nr
          ,l.sku_id
          ,l.wsl_ind
          ,l.concept_featrd_side_cd
          ,l.intrnl_offr_id
          ,l.sls_cls_cd
          ,l.prfl_cd
          ,l.micr_ncpsltn_ind
          ,l.scntd_pg_typ_id
          ,l.offr_lock_user
      FROM TABLE(p_data_line) l
     WHERE l.intrnl_offr_id     = p_offr_id
       AND l.sls_typ            = p_sls_typ
  )
  LOOP
    BEGIN
      UPDATE offr_sls_cls_sku s
         SET s.wsl_ind           = rec.wsl_ind
            ,s.micr_ncpsltn_ind  = decode(rec.scntd_pg_typ_id, null, 'N', 'Y')
            ,s.scntd_pg_typ_id   = rec.scntd_pg_typ_id
            ,s.last_updt_user_id = rec.offr_lock_user
       WHERE s.offr_id        = rec.intrnl_offr_id
         AND s.sls_cls_cd     = rec.sls_cls_cd
         AND s.prfl_cd        = rec.prfl_cd
         AND s.pg_ofs_nr      = rec.pp_ofs_nr
         AND s.featrd_side_cd = rec.concept_featrd_side_cd
         AND s.sku_id         = rec.sku_id;

      l_rowcount := SQL%ROWCOUNT;
      app_plsql_log.info(l_log || ' OSCS rows updated: ' || l_rowcount);

    EXCEPTION
      WHEN dup_val_on_index THEN
        -- offr_sls_cls_sku couldn't have been updated (already exists with this concept feature side)
        RAISE e_oscs_dup_val;
    END;
  END LOOP;

  app_plsql_log.info(l_log || ' finished');

  -- update dstrbtd_mrkt_sls
  l_log := 'dstrbtd_mrkt_sls';
  FOR r_data_line IN c_p_data_line LOOP

    l_no_sku_line := FALSE;
    BEGIN
      SELECT micr_ncpsltn_ind
        INTO l_old_micr_ncpsltn_ind
        FROM offr_sls_cls_sku oscs
       WHERE oscs.offr_id        = r_data_line.intrnl_offr_id
         AND oscs.sls_cls_cd     = r_data_line.sls_cls_cd
         AND oscs.prfl_cd        = r_data_line.prfl_cd
         AND oscs.pg_ofs_nr      = (SELECT osl.pg_ofs_nr
                                      FROM offr_sku_line osl
                                     WHERE osl.offr_sku_line_id = r_data_line.offr_sku_line_id)
         AND oscs.featrd_side_cd = r_data_line.concept_featrd_side_cd
         AND oscs.sku_id         = r_data_line.sku_id;
    EXCEPTION
      WHEN no_data_found THEN
        -- there is no offr_sku_line from this offr_id
        l_no_sku_line := TRUE;
    END;

    IF l_no_sku_line = FALSE THEN
      SELECT oppp.sls_prc_amt
            ,oppp.nr_for_qty
            ,osl.set_cmpnt_qty
        INTO l_old_sls_prc_amt
            ,l_old_nr_for_qty
            ,l_old_cmpnt_qty
        FROM offr_prfl_prc_point oppp
            ,offr_sku_line osl
       WHERE osl.offr_prfl_prcpt_id = oppp.offr_prfl_prcpt_id
         AND osl.offr_sku_line_id = r_data_line.offr_sku_line_id;

      SELECT COUNT(*) -- check config_item
        INTO l_cnt
        FROM mrkt_config_item m
       WHERE m.config_item_id = co_config_item_id
         AND m.mrkt_config_item_val_txt = 'Y'
         AND m.mrkt_id = r_data_line.mrkt_id;

      SELECT SUM(dms.unit_qty) old_unit_qty
        INTO l_old_unit_qty
        FROM dstrbtd_mrkt_sls dms
       WHERE dms.mrkt_id          = r_data_line.mrkt_id
         AND dms.offr_perd_id     = r_data_line.offr_perd_id
         AND dms.sls_perd_id      = r_data_line.offr_perd_id
         AND dms.offr_sku_line_id = r_data_line.offr_sku_line_id
         AND dms.sls_typ_id       = r_data_line.sls_typ
         AND dms.ver_id           = r_data_line.ver_id;

      IF l_oss_id_to_zero = r_data_line.offr_sku_set_id OR
        (l_cnt > 0
        AND (l_old_sls_prc_amt <> r_data_line.sls_prc_amt
          OR l_old_nr_for_qty <> r_data_line.nr_for_qty
          OR l_old_cmpnt_qty <> r_data_line.cmpnt_qty
          OR NVL(l_old_micr_ncpsltn_ind, -1) <> NVL(r_data_line.micr_ncpsltn_ind, -1))
        AND l_old_unit_qty = r_data_line.unit_qty)
      THEN
        l_oss_id_to_zero := NVL(r_data_line.offr_sku_set_id, -1);
        UPDATE dstrbtd_mrkt_sls dms
           SET dms.unit_qty = 0
              ,dms.tax_amt   = pa_maps_gta.pri_get_tax_amount(
                                       dms.mrkt_id,
                                       r_data_line.tax_type_id,
                                       dms.offr_perd_id)
              ,dms.comsn_amt = pa_maps_gta.get_commission_percentage(
                                       dms.mrkt_id,
                                       dms.offr_perd_id,
                                       r_data_line.comsn_typ)
              ,dms.last_updt_user_id = r_data_line.offr_lock_user
         WHERE dms.mrkt_id           = r_data_line.mrkt_id
           AND dms.offr_perd_id      = r_data_line.offr_perd_id
           AND CASE
                 WHEN r_data_line.offr_sku_set_id IS NULL
                  AND dms.offr_sku_line_id = r_data_line.offr_sku_line_id THEN
                   1
                 WHEN r_data_line.offr_sku_set_id IS NOT NULL
                  AND dms.offr_sku_line_id IN (SELECT offr_sku_line_id
                                               FROM offr_sku_line
                                              WHERE offr_sku_set_id = r_data_line.offr_sku_set_id
                                            ) THEN
                   1
                 ELSE
                   0
               END = 1
           AND dms.sls_typ_id = r_data_line.sls_typ
           AND dms.ver_id     = r_data_line.ver_id;
      END IF;
    END IF;
  END LOOP;

  IF l_oss_id_to_zero IS NULL THEN
    MERGE INTO dstrbtd_mrkt_sls dms
    USING (SELECT *
             FROM TABLE(p_data_line)
            WHERE intrnl_offr_id = p_offr_id
              AND sls_typ        = p_sls_typ) dl
    ON (dms.offr_sku_line_id = dl.offr_sku_line_id
    AND dms.offr_perd_id     = dl.offr_perd_id
    AND dms.sls_perd_id      = dl.offr_perd_id
    AND dms.veh_id           = dl.veh_id
    AND dms.ver_id           = dl.ver_id
    AND dms.sls_typ_id       = dl.sls_typ)
    WHEN MATCHED THEN UPDATE
    SET dms.unit_qty = dl.unit_qty
       ,dms.tax_amt   = pa_maps_gta.pri_get_tax_amount(
                                       dms.mrkt_id,
                                       dl.tax_type_id,
                                       dms.offr_perd_id)
       ,dms.comsn_amt = pa_maps_gta.get_commission_percentage(
                                       dms.mrkt_id,
                                       dms.offr_perd_id,
                                       dl.comsn_typ)
       ,dms.last_updt_user_id = dl.last_updt_user_id;

    -- Calculate the offset units
    pa_maps_gta.set_offset_units(poffer_id => p_offr_id, psales_type_id => p_sls_typ);
  END IF;

  app_plsql_log.info(l_log || ' finished');

  l_log := 'offr_sku_line';
  MERGE INTO offr_sku_line osl
  USING (SELECT *
           FROM TABLE(p_data_line)
          WHERE intrnl_offr_id = p_offr_id
            AND sls_typ        = p_sls_typ) dl
  ON (osl.offr_sku_line_id = dl.offr_sku_line_id)
  WHEN MATCHED THEN UPDATE
  SET osl.featrd_side_cd = dl.concept_featrd_side_cd
     ,osl.pg_ofs_nr         = dl.pp_ofs_nr
     ,osl.set_cmpnt_qty     = DECODE(dl.dltd_ind, 'Y', 0, dl.cmpnt_qty)
     ,osl.set_cmpnt_ind     = DECODE(dl.dltd_ind, 'Y', 'N', osl.set_cmpnt_ind)
     ,osl.dltd_ind = dl.dltd_ind
     ,osl.sls_prc_amt = dl.sls_prc_amt
     ,osl.offr_sku_set_id   = DECODE(dl.dltd_ind, 'Y', NULL, dl.offr_sku_set_id)
     ,osl.last_updt_user_id = dl.offr_lock_user;

  l_rowcount := SQL%ROWCOUNT;
  app_plsql_log.info(l_log || ' finished, merge rowcount: ' || l_rowcount);

  l_log := 'offr_sku_set';
  MERGE INTO offr_sku_set s
  USING (SELECT DISTINCT
                CASE
                  WHEN MIN(oss.set_prc_amt) <> SUM(l.sls_prc_amt * l.cmpnt_qty) THEN
                    3
                  ELSE
                    MIN(oss.set_prc_typ_id)
                END AS set_prc_typ_id
               ,CASE
                  WHEN MIN(oss.set_prc_amt) <> SUM(l.sls_prc_amt * l.cmpnt_qty) THEN
                    SUM(l.sls_prc_amt * l.cmpnt_qty)
                  ELSE
                    MIN(oss.set_prc_amt)
                END AS sum_prc_amt
               ,l.offr_sku_set_nm
               ,l.offr_sku_set_id
               ,'2' AS concept_featrd_side_cd
               ,l.offr_lock_user
           FROM offr_sku_line osl
               ,offr_sku_set oss
               ,TABLE(p_data_line) l
          WHERE osl.offr_sku_line_id = l.offr_sku_line_id
            AND osl.offr_sku_set_id  = oss.offr_sku_set_id
            AND l.intrnl_offr_id     = p_offr_id
            AND l.sls_typ            = p_sls_typ
         GROUP BY l.offr_sku_set_id
                 ,l.offr_sku_set_id
                 ,l.offr_sku_set_nm
                 ,l.offr_lock_user) dl
  ON (s.offr_sku_set_id = dl.offr_sku_set_id)
  WHEN MATCHED THEN UPDATE
  SET s.set_prc_amt       = dl.sum_prc_amt
     ,s.set_prc_typ_id    = dl.set_prc_typ_id
     ,s.offr_sku_set_nm   = dl.offr_sku_set_nm
     ,s.featrd_side_cd    = dl.concept_featrd_side_cd
     ,s.last_updt_user_id = dl.offr_lock_user;

  l_rowcount := SQL%ROWCOUNT;
  app_plsql_log.info(l_log || ' finished, merge rowcount: ' || l_rowcount);

  l_log := 'call app_ms_public.populate_ms_key';
  app_ms_public.populate_ms_key(p_offr_id);
  app_plsql_log.info(l_log || ' finished');

  l_log := 'offr';
  MERGE INTO offr o
  USING (SELECT DISTINCT intrnl_offr_id
                        ,mrkt_id
                        ,offr_perd_id
                        ,veh_id
                        ,ver_id
                        ,enrgy_chrt_postn_id
                        ,web_postn_id
                        ,brchr_plcmnt_id
                        ,offr_desc_txt
                        ,offr_notes_txt
                        ,offr_lyot_cmnts_txt
                        ,featrd_side_cd
                        ,(SELECT MAX(NVL(oscs.micr_ncpsltn_ind, 'N'))
                            FROM offr_sls_cls_sku oscs,
                                 offr_sku_line osl
                           WHERE oscs.offr_id        = osl.offr_id
                             AND oscs.sls_cls_cd     = osl.sls_cls_cd
                             AND oscs.prfl_cd        = osl.prfl_cd
                             AND oscs.pg_ofs_nr      = osl.pg_ofs_nr
                             AND oscs.featrd_side_cd = osl.featrd_side_cd
                             AND oscs.sku_id         = osl.sku_id
                             AND oscs.offr_id        = intrnl_offr_id
                             AND nvl(osl.dltd_ind,'N') != 'Y') AS micr_ncpsltn_ind
                        ,pg_wght
                        ,offr_typ
                        ,offr_cls_id
                        ,mrkt_veh_perd_sctn_id
                        ,pg_nr
                        ,offr_lock_user
                        ,spcl_ordr_ind
                        ,offr_ofs_nr
                        ,micr_ncpsltn_desc_txt
           FROM TABLE(p_data_line)
          WHERE intrnl_offr_id = p_offr_id
            AND sls_typ        = p_sls_typ) dl
  ON (o.offr_id = dl.intrnl_offr_id)
  WHEN MATCHED THEN UPDATE
  SET o.enrgy_chrt_postn_id   = dl.enrgy_chrt_postn_id
     ,o.web_postn_id          = dl.web_postn_id
     ,o.brchr_plcmt_id        = dl.brchr_plcmnt_id
     ,o.offr_desc_txt         = dl.offr_desc_txt
     ,o.offr_ntes_txt         = dl.offr_notes_txt
     ,o.offr_lyot_cmnts_txt   = dl.offr_lyot_cmnts_txt
     ,o.featrd_side_cd        = dl.featrd_side_cd
     ,o.micr_ncpsltn_desc_txt = dl.micr_ncpsltn_desc_txt
     ,o.micr_ncpsltn_ind      = dl.micr_ncpsltn_ind
     ,o.pg_wght_pct           = dl.pg_wght
     ,o.offr_typ              = dl.offr_typ
     ,o.mrkt_veh_perd_sctn_id = dl.mrkt_veh_perd_sctn_id
     ,o.sctn_page_ofs_nr      = dl.offr_ofs_nr
     ,o.offr_cls_id           = dl.offr_cls_id
     ,o.last_updt_user_id     = dl.offr_lock_user
     ,o.spcl_ordr_ind         = dl.spcl_ordr_ind;

  l_rowcount := SQL%ROWCOUNT;
  app_plsql_log.info(l_log || ' finished, merge rowcount: ' || l_rowcount);

  l_log := 'offr_prfl_prc_point';
  MERGE INTO offr_prfl_prc_point p
  USING (SELECT DISTINCT osl.offr_prfl_prcpt_id
                        ,l.concept_featrd_side_cd
                        ,l.cnsmr_invstmt_bdgt_id
                        ,l.pymt_typ
                        ,l.promtn_id
                        ,l.promtn_clm_id
                        ,l.comsn_typ
                        ,l.nr_for_qty
                        ,l.sls_prc_amt
                        ,l.tax_type_id
                        ,pa_maps_gta.pri_get_tax_amount(l.mrkt_id, l.tax_type_id, l.offr_perd_id) AS tax_amt
                        ,pa_maps_gta.get_commission_percentage(l.mrkt_id, l.offr_perd_id, l.comsn_typ) AS comsn_amt
                        ,pa_maps_gta.get_gta_without_price_point(
                             pa_maps_gta.pri_get_gta_method_id(oppp.mrkt_id, oppp.offr_perd_id),
                             l.sls_prc_amt,
                             oppp.chrty_amt,
                             oppp.awrd_sls_prc_amt + l.sls_prc_amt,
                             pa_maps_gta.get_commission_percentage(l.mrkt_id, l.offr_perd_id, l.comsn_typ),
                             pa_maps_gta.pri_get_tax_amount(l.mrkt_id, l.tax_type_id, l.offr_perd_id),
                             oppp.roylt_pct) AS net_to_avon_fct
                        ,l.offr_lock_user
                        ,l.pp_ofs_nr
           FROM TABLE(p_data_line) l
               ,offr_sku_line osl
               ,offr_prfl_prc_point oppp
          WHERE osl.offr_sku_line_id    = l.offr_sku_line_id
            AND oppp.offr_prfl_prcpt_id = osl.offr_prfl_prcpt_id
            AND l.intrnl_offr_id        = p_offr_id
            AND l.sls_typ               = p_sls_typ) dl
  ON (p.offr_prfl_prcpt_id = dl.offr_prfl_prcpt_id)
  WHEN MATCHED THEN UPDATE
  SET p.featrd_side_cd        = dl.concept_featrd_side_cd
     ,p.cnsmr_invstmt_bdgt_id = dl.cnsmr_invstmt_bdgt_id
     ,p.pymt_typ              = dl.pymt_typ
     ,p.promtn_id             = dl.promtn_id
     ,p.promtn_clm_id         = dl.promtn_clm_id
     ,p.comsn_typ             = dl.comsn_typ
     ,p.nr_for_qty            = dl.nr_for_qty
     ,p.sls_prc_amt           = dl.sls_prc_amt
     ,p.tax_type_id           = dl.tax_type_id
     ,p.tax_amt               = dl.tax_amt
     ,p.comsn_amt             = dl.comsn_amt
     ,p.net_to_avon_fct       = dl.net_to_avon_fct
     ,p.last_updt_user_id     = dl.offr_lock_user
     ,p.pg_ofs_nr             = dl.pp_ofs_nr;

  l_rowcount := SQL%ROWCOUNT;
  app_plsql_log.info(l_log || ' finished, merge rowcount: ' || l_rowcount);

  l_log := 'dstrbtd_mrkt_sls:gta';
  FOR r_data_line IN (
    SELECT DISTINCT mrkt_id
                   ,veh_id
                   ,offr_perd_id
                   ,ver_id
                   ,sls_typ
                   ,intrnl_offr_id
                   ,offr_lock_user
      FROM TABLE(p_data_line)
     WHERE intrnl_offr_id = p_offr_id
       AND sls_typ        = p_sls_typ
  )
  LOOP
    UPDATE dstrbtd_mrkt_sls dms
       SET dms.net_to_avon_fct =
         pa_maps_gta.get_gta_without_price_point(
                     pa_maps_gta.pri_get_gta_method_id(dms.mrkt_id,
                                                       dms.offr_perd_id
                                                      ),
                              (SELECT oppp.sls_prc_amt
                                 FROM offr_prfl_prc_point oppp,
                                      offr_sku_line osl
                                WHERE dms.offr_sku_line_id = osl.offr_sku_line_id
                                  AND osl.offr_prfl_prcpt_id = oppp.offr_prfl_prcpt_id),
                              (SELECT oppp.chrty_amt
                                 FROM offr_prfl_prc_point oppp,
                                      offr_sku_line osl
                                WHERE dms.offr_sku_line_id = osl.offr_sku_line_id
                                  AND osl.offr_prfl_prcpt_id = oppp.offr_prfl_prcpt_id),
                              (SELECT oppp.sls_prc_amt + oppp.awrd_sls_prc_amt
                                 FROM offr_prfl_prc_point oppp,
                                      offr_sku_line osl
                                WHERE dms.offr_sku_line_id = osl.offr_sku_line_id
                                  AND osl.offr_prfl_prcpt_id = oppp.offr_prfl_prcpt_id),
                              dms.comsn_amt,
                              dms.tax_amt,
                              (SELECT oppp.roylt_pct
                                 FROM offr_prfl_prc_point oppp,
                                      offr_sku_line osl
                                WHERE dms.offr_sku_line_id = osl.offr_sku_line_id
                                  AND osl.offr_prfl_prcpt_id = oppp.offr_prfl_prcpt_id)
                      )
       ,last_updt_user_id  = r_data_line.offr_lock_user
    WHERE dms.mrkt_id      = r_data_line.mrkt_id
      AND dms.veh_id       = r_data_line.veh_id
      AND dms.offr_perd_id = r_data_line.offr_perd_id
      AND dms.ver_id       = r_data_line.ver_id
      AND dms.sls_typ_id   = r_data_line.sls_typ
      AND r_data_line.intrnl_offr_id =
                (SELECT oppp.offr_id
                   FROM offr_prfl_prc_point oppp, offr_sku_line osl
                  WHERE dms.offr_sku_line_id = osl.offr_sku_line_id
                    AND osl.offr_prfl_prcpt_id = oppp.offr_prfl_prcpt_id);
  END LOOP;

  app_plsql_log.info(l_log || ' finished');

  -- delete offr_prfl_sls_cls_plcmt when there is no child in offr_prfl_prc_point and offr_sls_cls_sku
  l_log := 'delete offr_sls_cls_sku and offr_prfl_sls_cls_plcmt';
  DELETE FROM offr_sls_cls_sku s
   WHERE NOT EXISTS (SELECT *
                     FROM offr_sku_line osl
                    WHERE osl.offr_id        = s.offr_id
                      AND osl.sls_cls_cd     = s.sls_cls_cd
                      AND osl.prfl_cd        = s.prfl_cd
                      AND osl.pg_ofs_nr      = s.pg_ofs_nr
                      AND osl.featrd_side_cd = s.featrd_side_cd
                      AND osl.sku_id         = s.sku_id)
     AND s.offr_id = p_offr_id;

  DELETE FROM offr_prfl_sls_cls_plcmt p
   WHERE NOT EXISTS (SELECT *
                     FROM offr_sls_cls_sku s
                    WHERE s.offr_id        = p.offr_id
                      AND s.sls_cls_cd     = p.sls_cls_cd
                      AND s.prfl_cd        = p.prfl_cd
                      AND s.pg_ofs_nr      = p.pg_ofs_nr
                      AND s.featrd_side_cd = p.featrd_side_cd)
     AND p.offr_id = p_offr_id;

  app_plsql_log.info(l_log || ' finished');

  l_log := 'delete due to item disabling';
  FOR rec IN (
    SELECT l.*
      FROM TABLE(p_data_line) l
     WHERE l.intrnl_offr_id = p_offr_id
       AND l.sls_typ        = p_sls_typ
    )
  LOOP
      BEGIN
        SELECT cnt
          INTO l_cnt
          FROM (
            SELECT osl.offr_prfl_prcpt_id, COUNT(*) cnt
              FROM offr_sku_line osl
             WHERE osl.offr_prfl_prcpt_id = rec.offr_prfl_prcpt_id
            GROUP BY osl.offr_prfl_prcpt_id
            HAVING MIN(NVL(osl.dltd_ind, 'N')) = 'Y'
        );
      EXCEPTION
        WHEN no_data_found THEN
          l_cnt := 0;
      END;

      IF l_cnt > 0 THEN
        FOR r_osl IN (
          SELECT offr_sku_line_id
            FROM offr_sku_line
           WHERE offr_prfl_prcpt_id = rec.offr_prfl_prcpt_id
        )
        LOOP
          DELETE FROM dly_bilng_offr_sku_line
           WHERE offr_sku_line_id = r_osl.offr_sku_line_id;

          DELETE FROM dly_bilng_trnd_offr_sku_line
           WHERE offr_sku_line_id = r_osl.offr_sku_line_id;

          DELETE FROM dstrbtd_mrkt_sls
           WHERE offr_sku_line_id = r_osl.offr_sku_line_id;

          DELETE FROM est_lnkg
           WHERE r_osl.offr_sku_line_id IN (offr_sku_line_id, offr_gift_sku_line_id);
        END LOOP;

        DELETE FROM offr_sku_line osl
        WHERE osl.offr_prfl_prcpt_id = rec.offr_prfl_prcpt_id;

        DELETE FROM offr_prfl_prc_point oppp
         WHERE oppp.offr_prfl_prcpt_id = rec.offr_prfl_prcpt_id;
      END IF;
  END LOOP;

  app_plsql_log.info(l_log || ' finished');
  app_plsql_log.info(l_module_name || ' stop');

EXCEPTION
  WHEN OTHERS THEN
    app_plsql_log.info(l_module_name || ', ' || l_log || ', offr_id: ' || p_offr_id ||
                       ', sls_typ: ' || p_sls_typ || ', Error: ' || SQLERRM);
    RAISE;
END save_edit_offr_lines;

-----------------------SAVE_EDIT_OFFR_TABLE----------------------------------
PROCEDURE save_edit_offr_table(p_data_line IN obj_edit_offr_table,
                               p_result    OUT obj_edit_offr_save_table,
                               p_pagination IN CHAR DEFAULT 'N') AS
--p_result 0 ora error,1 successful, 2 osl count diff fount, 3 lock problem

  l_run_id         NUMBER := app_plsql_output.generate_new_run_id;
  l_user_id        VARCHAR2(35) := USER();
  l_module_name    VARCHAR2(30) := 'SAVE_EDIT_OFFR_TABLE';
  l_rowcount       NUMBER;
  l_offr_table     obj_get_offr_table := obj_get_offr_table();
  l_scnrio_offrs   obj_get_offr_table := obj_get_offr_table();
  l_get_offr_table obj_edit_offr_table;
  l_offr_lock_user VARCHAR2(35);
  l_offr_lock      NUMBER;
  l_status         NUMBER;
  l_result         NUMBER;
BEGIN
  app_plsql_log.register(g_package_name || '.' || l_module_name);
  app_plsql_output.set_run_id(l_run_id);
  app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
  app_plsql_log.info(l_module_name || ' start');
  p_result := obj_edit_offr_save_table();

  FOR mrkt_prd_rec IN (
    SELECT DISTINCT mrkt_id, offr_perd_id
      FROM TABLE(p_data_line)
  )
  LOOP
    l_offr_table.delete;
    FOR rec IN (
      SELECT intrnl_offr_id, sls_typ
        FROM TABLE(p_data_line)
       WHERE mrkt_id = mrkt_prd_rec.mrkt_id
         AND offr_perd_id = mrkt_prd_rec.offr_perd_id
       GROUP BY intrnl_offr_id, sls_typ) LOOP
      l_offr_table.extend;
      l_offr_table(l_offr_table.last) := obj_get_offr_line(rec.intrnl_offr_id, nvl(rec.sls_typ,1));
    END LOOP;

    SELECT obj_edit_offr_line(status,
                              mrkt_id,
                              offr_perd_id,
                              offr_lock,
                              offr_lock_user,
                              offr_sku_line_id,
                              veh_id,
                              brchr_plcmnt_id,
                              brchr_sctn_nm,
                              enrgy_chrt_postn_id,
                              web_postn_id,
                              pg_nr,
                              ctgry_id,
                              brnd_id,
                              sgmt_id,
                              form_id,
                              form_grp_id,
                              prfl_cd,
                              sku_id,
                              fsc_cd,
                              prod_typ_id,
                              gender_id,
                              sls_cls_cd,
                              pp_sls_cls_cd,
                              item_sls_cls_cd,
                              offr_desc_txt,
                              offr_notes_txt,
                              offr_lyot_cmnts_txt,
                              featrd_side_cd,
                              concept_featrd_side_cd,
                              micr_ncpsltn_ind,
                              scntd_pg_typ_id,
                              cnsmr_invstmt_bdgt_id,
                              pymt_typ,
                              promtn_id,
                              promtn_clm_id,
                              spndng_lvl,
                              comsn_typ,
                              tax_type_id,
                              wsl_ind,
                              offr_sku_set_id,
                              cmpnt_qty,
                              nr_for_qty,
                              nta_factor,
                              sku_cost,
                              lv_nta,
                              lv_sp,
                              lv_rp,
                              lv_discount,
                              lv_units,
                              lv_total_cost,
                              lv_gross_sales,
                              lv_dp_cash,
                              lv_dp_percent,
                              ver_id,
                              sls_prc_amt,
                              reg_prc_amt,
                              line_nr,
                              unit_qty,
                              dltd_ind,
                              created_ts,
                              created_user_id,
                              last_updt_ts,
                              last_updt_user_id,
                              intrnl_offr_id,
                              mrkt_veh_perd_sctn_id,
                              prfl_nm,
                              sku_nm,
                              comsn_typ_desc_txt,
                              tax_typ_desc_txt,
                              offr_sku_set_nm,
                              sls_typ,
                              pc_sp_py,
                              pc_rp,
                              pc_sp,
                              pc_vsp,
                              pc_hit,
                              pg_wght,
                              pp_pg_wght,
                              sprd_nr,
                              offr_prfl_prcpt_id,
                              has_unit_qty,
                              offr_typ,
                              forcasted_units,
                              forcasted_date,
                              offr_cls_id,
                              spcl_ordr_ind,
                              offr_ofs_nr,
                              pp_ofs_nr,
                              impct_catgry_id,
                              hero_ind,
                              smplg_ind,
                              mltpl_ind,
                              cmltv_ind,
                              use_instrctns_ind,
                              pg_typ_id,
                              featrd_prfl_ind,
                              fxd_pg_wght_ind,
                              prod_endrsmt_id,
                              frc_mtch_mthd_id,
                              wghtd_avg_cost_amt,
                              incntv_id,
                              intrdctn_perd_id,
                              on_stus_perd_id,
                              dspostn_perd_id,
                              scnrio_id,
                              micr_ncpsltn_desc_txt,
                              offr_link_id,
                              profile_item_count
    )
    BULK COLLECT INTO l_get_offr_table
    FROM TABLE(pa_maps_edit_offr.get_offr(l_offr_table, p_pagination));

    merge_history(l_get_offr_table);

    -- check if the received osl -ids are the same as
    FOR offr_sls IN (
      SELECT offr_id, sls_typ, MAX(diff) AS diff
        FROM (
          SELECT offr_id, sls_typ, 1 AS diff -- different lines
            FROM (
              (SELECT intrnl_offr_id AS offr_id, sls_typ, offr_sku_line_id
                 FROM TABLE(l_get_offr_table)
               MINUS
                SELECT intrnl_offr_id AS offr_id, sls_typ, offr_sku_line_id
                  FROM TABLE(p_data_line)
                 WHERE mrkt_id = mrkt_prd_rec.mrkt_id
                   AND offr_perd_id = mrkt_prd_rec.offr_perd_id
              )
             UNION ALL
              (SELECT intrnl_offr_id AS offr_id, sls_typ, offr_sku_line_id
                 FROM TABLE(p_data_line)
                WHERE mrkt_id = mrkt_prd_rec.mrkt_id
                  AND offr_perd_id = mrkt_prd_rec.offr_perd_id
               MINUS
                SELECT intrnl_offr_id AS offr_id, sls_typ, offr_sku_line_id
                  FROM TABLE(l_get_offr_table)
              )
          )
          UNION
          SELECT offr_id, sls_typ, 0 AS diff -- identical lines
            FROM (
              SELECT intrnl_offr_id AS offr_id, sls_typ, offr_sku_line_id
                 FROM TABLE(l_get_offr_table)
              INTERSECT
               SELECT intrnl_offr_id AS offr_id, sls_typ, offr_sku_line_id
                 FROM TABLE(p_data_line)
                  WHERE mrkt_id = mrkt_prd_rec.mrkt_id
                    AND offr_perd_id = mrkt_prd_rec.offr_perd_id
          )
        )
        GROUP BY offr_id, sls_typ
    )
    LOOP
      IF offr_sls.diff = 0 THEN
        --app_plsql_log.info('save: offr_id: ' || offr_sls.offr_id || ', sls_typ: ' || offr_sls.sls_typ);
        SELECT DISTINCT offr_lock_user, offr_lock, status
          INTO l_offr_lock_user, l_offr_lock, l_status
          FROM TABLE(p_data_line)
         WHERE intrnl_offr_id = offr_sls.offr_id
           AND sls_typ = offr_sls.sls_typ;

        IF l_status = 1 THEN -- Check if user has right to modify without locking
          l_result := 1;
          BEGIN
            -- Without locking user can only modifiy units
            MERGE INTO dstrbtd_mrkt_sls dms
            USING (SELECT *
                     FROM TABLE(p_data_line)
                    WHERE intrnl_offr_id = offr_sls.offr_id
                      AND sls_typ        = offr_sls.sls_typ) dl
            ON (dms.offr_sku_line_id = dl.offr_sku_line_id
            AND dms.offr_perd_id     = dl.offr_perd_id
            AND dms.sls_perd_id      = dl.offr_perd_id
            AND dms.veh_id           = dl.veh_id
            AND dms.ver_id           = dl.ver_id
            AND dms.sls_typ_id       = dl.sls_typ)
            WHEN MATCHED THEN UPDATE
            SET dms.unit_qty = dl.unit_qty
               ,dms.last_updt_user_id = dl.last_updt_user_id;

            l_rowcount := SQL%ROWCOUNT;

            -- Calculate the offset units
            pa_maps_gta.set_offset_units(poffer_id => offr_sls.offr_id, psales_type_id => offr_sls.sls_typ);

            app_plsql_log.info('Setting units finished (without lock), merge rowcount: ' || l_rowcount);

          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK;
              app_plsql_log.info(l_module_name || ', ' || 'Setting units finished (without lock), offr_id: ' ||
                                 offr_sls.offr_id || ', sls_typ: ' || offr_sls.sls_typ || ', Error: ' || SQLERRM);
              l_result := 0;
          END;
        ELSE
          IF l_offr_lock = 0 OR lock_offr_chk(offr_sls.offr_id, l_offr_lock_user) <> 0 THEN
            l_result := 1;
            BEGIN
              save_edit_offr_lines(offr_sls.offr_id, offr_sls.sls_typ, p_data_line);

              --manage_scenario(offr_sls.offr_id, l_offr_lock_user, l_scnrio_offrs);

              IF l_scnrio_offrs.COUNT > 0 THEN
                FOR offr_idx IN l_scnrio_offrs.FIRST .. l_scnrio_offrs.LAST LOOP
                  p_result.EXTEND;
                  p_result(p_result.LAST) := obj_edit_offr_save_line(l_result, l_scnrio_offrs(offr_idx).p_offr_id, g_sls_typ_id);
                END LOOP;
              END IF;

              COMMIT;  --save changes for the offer

            EXCEPTION
              WHEN OTHERS THEN
                ROLLBACK; --no changes saved for the offer
                l_result := 0;
            END;
          ELSE
            l_result := 3; --lock problem
          END IF; -- lock_offr_chk
        END IF;

      ELSE
        l_result := 2;
      END IF;

      IF l_result <> 1 THEN
        app_plsql_log.info(l_module_name || ', Status: ' || l_result);
        log_params(l_module_name, offr_sls.offr_id, offr_sls.sls_typ, p_data_line);
      END IF;

      p_result.extend();
      p_result(p_result.last) := obj_edit_offr_save_line(l_result, offr_sls.offr_id, offr_sls.sls_typ);
    END LOOP; --offr and sls typ loop
  END LOOP; --mrkt_prd_rec

  app_plsql_log.info(l_module_name || ' stop');
EXCEPTION
  WHEN OTHERS THEN
    app_plsql_log.info(l_module_name || ' Error in save edit offr' || SQLERRM(SQLCODE));
END save_edit_offr_table;
-----------------------/SAVE_EDIT_OFFR_TABLE---------------------------------


FUNCTION get_history(p_get_offr IN OBJ_GET_OFFR_TABLE
                             ) RETURN OBJ_EDIT_OFFR_HIST_TABLE PIPELINED
  AS
   CURSOR c_offr IS
      SELECT o.*
       FROM TABLE(p_get_offr) poi JOIN edit_offr_hist o ON o.intrnl_offr_id = poi.p_offr_id AND o.sls_typ = poi.p_sls_typ ORDER BY o.hist_ts DESC, o.offr_sku_line_id;
  BEGIN

FOR offer IN c_offr LOOP
  pipe row (OBJ_EDIT_OFFR_HIST_LINE(
   offer.hist_ts
  ,offer.intrnl_offr_id
  ,offer.offr_sku_line_id
  ,offer.status
  ,offer.mrkt_id
  ,offer.offr_perd_id
  ,offer.offr_lock
  ,offer.offr_lock_user
  ,offer.veh_id
  ,offer.brchr_plcmnt_id
  ,offer.brchr_sctn_nm
  ,offer.enrgy_chrt_postn_id
  ,offer.web_postn_id
  ,offer.pg_nr
  ,offer.ctgry_id
  ,offer.brnd_id
  ,offer.sgmt_id
  ,offer.form_id
  ,offer.form_grp_id
  ,offer.prfl_cd
  ,offer.sku_id
  ,offer.fsc_cd
  ,offer.prod_typ_id
  ,offer.gender_id
  ,offer.sls_cls_cd
  ,offer.pp_sls_cls_cd
  ,offer.offr_desc_txt
  ,offer.offr_notes_txt
  ,offer.offr_lyot_cmnts_txt
  ,offer.featrd_side_cd
  ,offer.concept_featrd_side_cd
  ,offer.micr_ncpsltn_ind
  ,offer.scntd_pg_typ_id
  ,offer.cnsmr_invstmt_bdgt_id
  ,offer.pymt_typ
  ,offer.promtn_id
  ,offer.promtn_clm_id
 -- ,offer.cmbntn_offr_typ
  ,offer.spndng_lvl
  ,offer.comsn_typ
  ,offer.tax_type_id
  ,offer.wsl_ind
  ,offer.offr_sku_set_id
  ,offer.cmpnt_qty
  ,offer.nr_for_qty
  ,offer.nta_factor
  ,offer.sku_cost
  ,offer.lv_nta
  ,offer.lv_sp
  ,offer.lv_rp
  ,offer.lv_discount
  ,offer.lv_units
  ,offer.lv_total_cost
  ,offer.lv_gross_sales
  ,offer.lv_dp_cash
  ,offer.lv_dp_percent
  ,offer.ver_id
  ,offer.sls_prc_amt
  ,offer.reg_prc_amt
  ,offer.line_nr
  ,offer.unit_qty
  ,offer.dltd_ind
  ,offer.created_ts
  ,offer.created_user_id
  ,offer.last_updt_ts
  ,offer.last_updt_user_id
  ,offer.mrkt_veh_perd_sctn_id
  ,offer.prfl_nm
  ,offer.sku_nm
  ,offer.comsn_typ_desc_txt
  ,offer.tax_typ_desc_txt
  ,offer.offr_sku_set_nm
  ,offer.sls_typ
  ,offer.pc_sp_py
  ,offer.pc_rp
  ,offer.pc_sp
  ,offer.pc_vsp
  ,offer.pc_hit
  ,offer.pg_wght
  ,offer.sprd_nr
  ,offer.offr_prfl_prcpt_id
  ,offer.offr_typ
  ,offer.forcasted_units
  ,offer.forcasted_date
  ,offer.offr_cls_id
  ,offer.spcl_ordr_ind
  ,offer.offr_ofs_nr
  ,offer.pp_ofs_nr
  ,offer.impct_catgry_id
  ,offer.hero_ind
  ,offer.smplg_ind
  ,offer.mltpl_ind
  ,offer.cmltv_ind
  ,offer.micr_ncpsltn_desc_txt
  ));
END LOOP;

      EXCEPTION WHEN OTHERS THEN
       app_plsql_log.info('Error in getting offer history ' || SQLERRM(SQLCODE));
END get_history;

FUNCTION get_offr_pivot(p_get_offr   IN obj_get_offr_table)
    RETURN obj_edit_offr_table
    PIPELINED AS
    -- local variables

    l_offr_perd_id offr.offr_perd_id%TYPE;
    l_mrkt_id      offr.mrkt_id%TYPE;
    l_ver_id       offr.ver_id%TYPE;
    l_sls_typ      dstrbtd_mrkt_sls.sls_typ_id%TYPE;
    l_veh_ids      number_array;
    -- for LOG
    l_run_id  NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id VARCHAR(35) := USER();
    --
    l_module_name VARCHAR2(30) := 'GET_OFFR_PIVOT';

    l_min_mrkt_id      offr.mrkt_id%TYPE;
    l_min_offr_perd_id offr.offr_perd_id%TYPE;
    l_min_ver_id       offr.ver_id%TYPE;
    l_min_sls_typ      dstrbtd_mrkt_sls.sls_typ_id%TYPE;

  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start');

    SELECT MAX(NVL(p_sls_typ, -1)) INTO l_sls_typ FROM TABLE(p_get_offr);
    SELECT MAX(o.mrkt_id)
      INTO l_mrkt_id
      FROM TABLE(p_get_offr) poi
      JOIN offr o
        ON o.offr_id = poi.p_offr_id;
    SELECT MAX(o.ver_id)
      INTO l_ver_id
      FROM TABLE(p_get_offr) poi
      JOIN offr o
        ON o.offr_id = poi.p_offr_id;
    SELECT MAX(o.offr_perd_id)
      INTO l_offr_perd_id
      FROM TABLE(p_get_offr) poi
      JOIN offr o
        ON o.offr_id = poi.p_offr_id;
    SELECT DISTINCT (o.veh_id)
      BULK COLLECT
      INTO l_veh_ids
      FROM TABLE(p_get_offr) poi
      JOIN offr o
        ON o.offr_id = poi.p_offr_id;
    SELECT MIN(NVL(p_sls_typ, -1)) INTO l_min_sls_typ FROM TABLE(p_get_offr);
    SELECT MIN(o.mrkt_id)
      INTO l_min_mrkt_id
      FROM TABLE(p_get_offr) poi
      JOIN offr o
        ON o.offr_id = poi.p_offr_id;
    SELECT MIN(o.ver_id)
      INTO l_min_ver_id
      FROM TABLE(p_get_offr) poi
      JOIN offr o
        ON o.offr_id = poi.p_offr_id;
    SELECT MIN(o.offr_perd_id)
      INTO l_min_offr_perd_id
      FROM TABLE(p_get_offr) poi
      JOIN offr o
        ON o.offr_id = poi.p_offr_id;
    IF l_min_sls_typ <> l_sls_typ OR l_min_mrkt_id <> l_mrkt_id OR   l_min_ver_id <> l_ver_id OR  l_min_offr_perd_id <> l_offr_perd_id
    THEN
      RAISE_APPLICATION_ERROR(-20100, 'market, campaign, version and sales type must be the same to use this function');
    END IF;

    IF l_sls_typ = -1 THEN
      l_sls_typ := NULL;      
    END IF;

    dbms_output.put_line(l_sls_typ || l_mrkt_id || l_ver_id ||
                         l_offr_perd_id);

    FOR rec IN (

  SELECT NULL AS intrnl_offr_id
      --,l_sls_typ AS sls_typ
      ,NVL(osl_current.sales_type, (SELECT MAX(mps_sls_typ_id)
                                          FROM mrkt_veh_perd_ver mvpv
                                         WHERE mvpv.mrkt_id = l_mrkt_id
                                           AND mvpv.offr_perd_id = l_offr_perd_id
                                           AND mvpv.ver_id = l_ver_id
                                           AND mvpv.veh_id = o.veh_id)) AS sls_typ
       --latest version calculations
      ,NULL AS lv_nta
      ,NULL AS lv_sp
      ,NULL AS lv_rp
      ,NULL AS lv_discount
      ,NULL AS lv_units
      ,NULL AS lv_total_cost
      ,NULL AS lv_gross_sales
      ,NULL lv_dp_cash
      ,NULL lv_dp_percent
       --/latest version calculations
      ,0 AS status
      ,o.mrkt_id AS mrkt_id
      ,o.offr_perd_id AS offr_perd_id
      ,NULL AS offr_lock
      ,NULL AS offr_lock_user
      ,osl_current.offr_sku_line_id AS offr_sku_line_id
      ,o.veh_id AS veh_id
      ,NULL AS brchr_plcmnt_id
      ,NULL AS brchr_sctn_nm
      ,NULL AS enrgy_chrt_postn_id
      ,NULL AS web_postn_id
      ,NULL AS pg_nr
      ,prfl.catgry_id AS ctgry_id
      ,brnd_grp.brnd_fmly_id AS brnd_id
      ,prfl.sgmt_id AS sgmt_id
      ,NULL AS form_id
      ,NULL AS form_grp_id
      ,offr_prfl_prc_point.prfl_cd AS prfl_cd
      ,osl_current.sku_id AS sku_id
      ,NULL AS fsc_cd
      ,NULL AS prod_typ_id
      ,NULL AS gender_id
      ,NULL AS sls_cls_cd
      ,NULL AS pp_sls_cls_cd
      ,NULL AS item_sls_cls_cd
      ,NULL AS offr_desc_txt
      ,NULL AS offr_notes_txt
      ,NULL AS offr_lyot_cmnts_txt
      ,NULL AS featrd_side_cd
      ,NULL AS concept_featrd_side_cd
      ,NULL AS micr_ncpsltn_ind
      ,NULL AS scntd_pg_typ_id
      ,NULL AS cnsmr_invstmt_bdgt_id
      ,NULL AS pymt_typ
      ,NULL AS promtn_id
      ,NULL AS promtn_clm_id
       --,1 AS cmbntn_offr_typ
      ,'NEW_FEATURE' AS spndng_lvl
      ,NULL AS tax_type_id
      ,NULL AS wsl_ind
      ,NULL AS comsn_typ
      ,NULL AS offr_sku_set_id
      ,NULL AS cmpnt_qty
      ,offr_prfl_prc_point.nr_for_qty AS nr_for_qty
      ,offr_prfl_prc_point.net_to_avon_fct AS nta_factor
      ,osl_current.sku_cost_amt AS sku_cost
      ,o.ver_id AS ver_id
      ,offr_prfl_prc_point.sls_prc_amt AS sls_prc_amt
      ,NULL AS reg_prc_amt --sku reg prcb?l
      ,NULL AS line_nr
      ,osl_current.sum_unit_qty AS unit_qty
      ,osl_current.dltd_ind AS dltd_ind
      ,NULL AS created_ts
      ,NULL AS created_user_id
      ,NULL AS last_updt_user_id
      ,NULL AS last_updt_ts
      ,NULL AS mrkt_veh_perd_sctn_id
      ,prfl.prfl_nm AS prfl_nm
      ,NULL AS sku_nm
      ,NULL AS comsn_typ_desc_txt
      ,NULL AS tax_typ_desc_txt
      ,NULL AS offr_sku_set_nm
       --pricing corridors
      ,NULL AS pc_sp_py
      ,NULL AS pc_rp
      ,NULL AS pc_sp
      ,NULL as pc_vsp
      ,NULL AS pc_hit
      ,NULL AS pg_wght
      ,NULL AS pp_pg_wght
      ,NULL AS sprd_nr
      ,NULL AS offr_prfl_prcpt_id
      ,NULL AS has_unit_qty
      ,o.offr_typ
      ,NULL AS forcasted_units
      ,NULL AS forcasted_date
      ,NULL AS offr_cls_id
      ,NULL AS spcl_ordr_ind
      ,NULL AS offr_ofs_nr
      ,NULL AS pp_ofs_nr
      ,NULL AS impct_catgry_id
      ,NULL AS hero_ind
      ,NULL AS smplg_ind
      ,NULL AS mltpl_ind
      ,NULL AS cmltv_ind
      ,NULL AS use_instrctns_ind
      ,NULL AS pg_typ_id
      ,NULL AS featrd_prfl_ind
      ,NULL AS fxd_pg_wght_ind
      ,NULL AS prod_endrsmt_id
      ,NULL AS frc_mtch_mthd_id
      ,NULL AS wghtd_avg_cost_amt
      ,NULL AS incntv_id
      ,NULL AS intrdctn_perd_id
      ,NULL AS on_stus_perd_id
      ,NULL AS dspostn_perd_id
      ,NULL AS scnrio_id
      ,NULL AS micr_ncpsltn_desc_txt
      ,NULL AS offr_link_id
      ,NULL AS profile_item_count
--
  FROM (SELECT *
           FROM offr
          WHERE offr_id IN (SELECT p_offr_id FROM table(p_get_offr))
       AND offr.mrkt_id = l_mrkt_id
       AND offr.offr_perd_id = l_offr_perd_id
       AND offr.ver_id = l_ver_id) o
      ,(SELECT offr_sku_line.offr_sku_line_id
               ,offr_sku_line.sku_id
               ,offr_sku_line.featrd_side_cd
               ,offr_sku_line.pg_ofs_nr
               ,offr_sku_line.prfl_cd
               ,offr_sku_line.sls_cls_cd
               ,offr_sku_line.offr_id
               ,offr_sku_line.offr_sku_set_id
               ,offr_sku_line.offr_prfl_prcpt_id
               ,offr_sku_line.dltd_ind
               ,offr_sku_line.line_nr
               ,offr_sku_line.set_cmpnt_qty
               ,offr_sku_line.mrkt_id
               ,CASE
                  WHEN actual_sku.wghtd_avg_cost_amt IS NOT NULL THEN
                   actual_sku.wghtd_avg_cost_amt
                  ELSE
                   planned_sku.wghtd_avg_cost_amt
                END sku_cost_amt
               ,l_ver_id AS ver_id
               ,sku_reg_prc.reg_prc_amt
               ,sku.sku_nm
               --,l_sls_typ AS sales_type
               ,CASE WHEN l_sls_typ IS NULL THEN
                   (SELECT MAX(mps_sls_typ_id)
                      FROM mrkt_veh_perd_ver mvpv
                     WHERE mvpv.mrkt_id = l_mrkt_id
                       AND mvpv.offr_perd_id = l_offr_perd_id
                       AND mvpv.ver_id = l_ver_id
                       AND mvpv.veh_id = o.veh_id)
                  ELSE
                    l_sls_typ
                  END AS sales_type
               ,(SELECT MAX(sls_typ_id)
                   FROM dstrbtd_mrkt_sls
                  WHERE mrkt_id = l_mrkt_id
                    AND offr_perd_id = l_offr_perd_id
                    AND sls_perd_id = l_offr_perd_id
                    AND veh_id = offr_sku_line.veh_id
                    AND ver_id = l_ver_id) max_sales_type
               ,(SELECT nvl(SUM(unit_qty), 0) AS sum_unit_qty
                   FROM dstrbtd_mrkt_sls
                  WHERE sls_typ_id = --l_sls_typ
                                     (CASE WHEN l_sls_typ IS NULL THEN
                                       (SELECT MAX(mps_sls_typ_id)
                                          FROM mrkt_veh_perd_ver mvpv
                                         WHERE mvpv.mrkt_id = l_mrkt_id
                                           AND mvpv.offr_perd_id = l_offr_perd_id
                                           AND mvpv.ver_id = l_ver_id
                                           AND mvpv.veh_id = o.veh_id)
                                      ELSE
                                        l_sls_typ
                                      END)
                    AND mrkt_id = l_mrkt_id
                    AND offr_perd_id = l_offr_perd_id
                    AND sls_perd_id = l_offr_perd_id
                    AND offr_sku_line_id = offr_sku_line.offr_sku_line_id
                  GROUP BY offr_sku_line_id, sls_typ_id) sum_unit_qty
               ,(SELECT nvl(SUM(cost_amt), 0) AS sum_cost_amt
                   FROM dstrbtd_mrkt_sls
                  WHERE sls_typ_id = --l_sls_typ
                                     (CASE WHEN l_sls_typ IS NULL THEN
                                       (SELECT MAX(mps_sls_typ_id)
                                          FROM mrkt_veh_perd_ver mvpv
                                         WHERE mvpv.mrkt_id = l_mrkt_id
                                           AND mvpv.offr_perd_id = l_offr_perd_id
                                           AND mvpv.ver_id = l_ver_id
                                           AND mvpv.veh_id = o.veh_id)
                                      ELSE
                                        l_sls_typ
                                      END)
                    AND mrkt_id = l_mrkt_id
                    AND offr_perd_id = l_offr_perd_id
                    AND sls_perd_id = l_offr_perd_id
                    AND offr_sku_line_id = offr_sku_line.offr_sku_line_id
                  GROUP BY offr_sku_line_id, sls_typ_id) AS sum_cost_amt
           FROM offr_sku_line
               ,sku_cost    actual_sku
               ,sku_cost    planned_sku
               ,sku_reg_prc
               ,sku
               ,offr        o
          WHERE
           o.offr_id = offr_sku_line.offr_id
       AND o.mrkt_id = l_mrkt_id
       AND o.offr_perd_id = l_offr_perd_id
       AND o.ver_id = l_ver_id
       AND o.offr_id IN (SELECT p_offr_id FROM table(p_get_offr))

       AND offr_sku_line.sku_id = actual_sku.sku_id(+)
       AND actual_sku.cost_typ(+) = 'A'
       AND actual_sku.mrkt_id(+) = l_mrkt_id
       AND actual_sku.offr_perd_id(+) = l_offr_perd_id
       AND offr_sku_line.sku_id = planned_sku.sku_id(+)
       AND planned_sku.mrkt_id(+) = l_mrkt_id
       AND planned_sku.offr_perd_id(+) = l_offr_perd_id
       AND planned_sku.cost_typ(+) = 'P'
       AND sku_reg_prc.mrkt_id(+) = l_mrkt_id
       AND sku_reg_prc.offr_perd_id(+) = l_offr_perd_id
       AND sku_reg_prc.sku_id(+) = offr_sku_line.sku_id
       AND sku.sku_id(+) = offr_sku_line.sku_id) osl_current
      ,offr_prfl_prc_point
      ,prfl
      ,form
      ,brnd_grp
      ,brnd
      
 WHERE
--offr outer join on selected version
  o.offr_id = osl_current.offr_id(+)
 AND o.mrkt_id = l_mrkt_id
 AND o.offr_perd_id = l_offr_perd_id
 AND o.ver_id = l_ver_id
 
 AND osl_current.dltd_ind = 'N'
 AND osl_current.sum_unit_qty <> 0
 
--offr prfl prc point
 AND offr_prfl_prc_point.offr_prfl_prcpt_id(+) = osl_current.offr_prfl_prcpt_id
--brnd
 AND brnd.brnd_id(+) = prfl.brnd_id
 AND brnd_grp.brnd_grp_id(+) = brnd.brnd_grp_id
--new prfl and form table
 AND prfl.prfl_cd(+) = offr_prfl_prc_point.prfl_cd
 AND prfl.form_id = form.form_id(+)
   )
     LOOP
      PIPE ROW(obj_edit_offr_line(rec.status,
                                  rec.mrkt_id,
                                  rec.offr_perd_id,
                                  rec.offr_lock,
                                  rec.offr_lock_user,
                                  rec.offr_sku_line_id,
                                  rec.veh_id,
                                  rec.brchr_plcmnt_id,
                                  rec.brchr_sctn_nm,
                                  rec.enrgy_chrt_postn_id,
                                  rec.web_postn_id,
                                  rec.pg_nr,
                                  rec.ctgry_id,
                                  rec.brnd_id,
                                  rec.sgmt_id,
                                  rec.form_id,
                                  rec.form_grp_id,
                                  rec.prfl_cd,
                                  rec.sku_id,
                                  rec.fsc_cd,
                                  rec.prod_typ_id,
                                  rec.gender_id,
                                  rec.sls_cls_cd,
                                  rec.pp_sls_cls_cd,
                                  rec.item_sls_cls_cd,
                                  rec.offr_desc_txt,
                                  rec.offr_notes_txt,
                                  rec.offr_lyot_cmnts_txt,
                                  rec.featrd_side_cd,
                                  rec.concept_featrd_side_cd,
                                  rec.micr_ncpsltn_ind,
                                  rec.scntd_pg_typ_id,
                                  rec.cnsmr_invstmt_bdgt_id,
                                  rec.pymt_typ,
                                  rec.promtn_id,
                                  rec.promtn_clm_id,
                                  -- rec.cmbntn_offr_typ,
                                  rec.spndng_lvl,
                                  rec.comsn_typ,
                                  rec.tax_type_id,
                                  rec.wsl_ind,
                                  rec.offr_sku_set_id,
                                  rec.cmpnt_qty,
                                  rec.nr_for_qty,
                                  rec.nta_factor,
                                  rec.sku_cost,
                                  rec.lv_nta,
                                  rec.lv_sp,
                                  rec.lv_rp,
                                  rec.lv_discount,
                                  rec.lv_units,
                                  rec.lv_total_cost,
                                  rec.lv_gross_sales,
                                  rec.lv_dp_cash,
                                  rec.lv_dp_percent,
                                  rec.ver_id,
                                  rec.sls_prc_amt,
                                  rec.reg_prc_amt,
                                  rec.line_nr,
                                  rec.unit_qty,
                                  rec.dltd_ind,
                                  rec.created_ts,
                                  rec.created_user_id,
                                  rec.last_updt_ts,
                                  rec.last_updt_user_id,
                                  rec.intrnl_offr_id,
                                  rec.mrkt_veh_perd_sctn_id,
                                  rec.prfl_nm,
                                  rec.sku_nm,
                                  rec.comsn_typ_desc_txt,
                                  rec.tax_typ_desc_txt,
                                  rec.offr_sku_set_nm,
                                  rec.sls_typ,
                                  rec.pc_sp_py,
                                  rec.pc_rp,
                                  rec.pc_sp,
                                  rec.pc_vsp,
                                  rec.pc_hit,
                                  rec.pg_wght,
                                  rec.pp_pg_wght,
                                  rec.sprd_nr,
                                  rec.offr_prfl_prcpt_id,
                                  rec.has_unit_qty,
                                  rec.offr_typ,
                                  rec.forcasted_units,
                                  rec.forcasted_date,
                                  rec.offr_cls_id,
                                  rec.spcl_ordr_ind,
                                  rec.offr_ofs_nr,
                                  rec.pp_ofs_nr,
                                  rec.impct_catgry_id,
                                  rec.hero_ind,
                                  rec.smplg_ind,
                                  rec.mltpl_ind,
                                  rec.cmltv_ind,
                                  rec.use_instrctns_ind,
                                  rec.pg_typ_id,
                                  rec.featrd_prfl_ind,
                                  rec.fxd_pg_wght_ind,
                                  rec.prod_endrsmt_id,
                                  rec.frc_mtch_mthd_id,
                                  rec.wghtd_avg_cost_amt,
                                  rec.incntv_id,
                                  rec.intrdctn_perd_id,
                                  rec.on_stus_perd_id,
                                  rec.dspostn_perd_id,
                                  rec.scnrio_id,
                                  rec.micr_ncpsltn_desc_txt,
                                  rec.offr_link_id,
                                  rec.profile_item_count
                                  ));
    END LOOP;
    app_plsql_log.info(l_module_name || ' stop');
  EXCEPTION
    WHEN OTHERS THEN
      app_plsql_log.info('Error in get offr pivot' || SQLERRM(SQLCODE));

  END get_offr_pivot;


function get_edit_offr_table( p_filters    IN obj_edit_offr_filter_table,
                              p_pagination IN CHAR DEFAULT 'N'
                )
          return OBJ_EDIT_OFFR_TABLE pipelined
          as

    -- local variables
    CURSOR c_p_filter IS
      SELECT * FROM TABLE(p_filters) ORDER BY p_mrkt_id, p_offr_perd_id;
    -- for LOG
    l_run_id  NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id VARCHAR(35) := USER();
    --
    l_module_name    VARCHAR2(30) := 'GET_EDIT_OFFR_TABLE';
    l_get_offr_table obj_get_offr_table;
    l_obj_edit_offr_table obj_edit_offr_table;
    l_pagination          CHAR(1);
    l_scnrio_cnt          NUMBER;
    l_scnrio_id           NUMBER := 0;
begin
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start');
    l_get_offr_table := obj_get_offr_table();
    l_obj_edit_offr_table := obj_edit_offr_table();

  l_pagination := NVL(p_pagination, 'N');

  FOR p_filter IN c_p_filter LOOP --Filters from the screen loop

    IF p_filter.p_scnrio_id IS NOT NULL THEN
      l_scnrio_cnt := p_filter.p_scnrio_id.COUNT;
      l_scnrio_id := p_filter.p_scnrio_id(1);
    ELSE
      l_scnrio_cnt := 0;
    END IF;

    FOR offrs IN (
        --Return every offer id where not every item is disabled.
    WITH
    mrkt_tmp_fsc_master AS
     (SELECT
             mrkt_id, sku_id, fsc_cd
        FROM (SELECT mrkt_id
                    ,sku_id
                    ,mstr_fsc_cd fsc_cd
                    ,strt_perd_id from_strt_perd_id
                    ,nvl(lead(strt_perd_id, 1)
                         over(PARTITION BY mrkt_id, sku_id ORDER BY strt_perd_id),
                         99999999) to_strt_perd_id
                FROM mstr_fsc_asgnmt
               WHERE p_filter.p_mrkt_id = mrkt_id
                 AND p_filter.p_offr_perd_id >= strt_perd_id)
       WHERE p_filter.p_offr_perd_id >= from_strt_perd_id
         AND p_filter.p_offr_perd_id < to_strt_perd_id),
    mrkt_tmp_fsc AS
     (SELECT
             mrkt_id, sku_id, MAX(fsc_cd) fsc_cd
        FROM (SELECT mrkt_id
                    ,sku_id
                    ,fsc_cd fsc_cd
                    ,strt_perd_id from_strt_perd_id
                    ,nvl(lead(strt_perd_id, 1)
                         over(PARTITION BY mrkt_id, fsc_cd ORDER BY strt_perd_id),
                         99999999) to_strt_perd_id
                FROM mrkt_fsc
               WHERE p_filter.p_mrkt_id = mrkt_id
                 AND p_filter.p_offr_perd_id >= strt_perd_id
                 AND 'N' = dltd_ind)
       WHERE p_filter.p_offr_perd_id >= from_strt_perd_id
         AND p_filter.p_offr_perd_id < to_strt_perd_id
       GROUP BY mrkt_id, sku_id)
--
        SELECT o.offr_id AS p_offr_id,
               p_filter.p_sls_typ AS p_sls_typ,
               p_filter.p_offr_typ AS p_offr_typ
          FROM  offr o
               ,offr_sku_line osl
               ,offr_prfl_prc_point
               ,prfl
               ,brnd
               ,brnd_grp
               ,mrkt_tmp_fsc
               ,mrkt_tmp_fsc_master
               ,dstrbtd_mrkt_sls
               ,what_if_tran
         WHERE --offr_sku_line join
                   o.offr_id = osl.offr_id(+)
               AND CASE
                     WHEN p_filter.p_offr_typ IS NULL AND l_scnrio_cnt = 0 THEN
                       1
                     WHEN p_filter.p_offr_typ IS NULL AND l_scnrio_cnt > 0 AND o.offr_typ = 'CMP' THEN
                       1
                     WHEN p_filter.p_offr_typ IS NULL AND what_if_tran.scnrio_id IN (SELECT * FROM TABLE(p_filter.p_scnrio_id)) THEN
                       1
                     WHEN p_filter.p_offr_typ IN ('CMP', 'WIF') AND o.offr_typ = p_filter.p_offr_typ THEN
                       1
                     ELSE
                       0
                   END = 1
               AND o.mrkt_id = p_filter.p_mrkt_id
               AND o.offr_perd_id = p_filter.p_offr_perd_id
               AND o.ver_id = p_filter.p_ver_id
               --prfl, brnd, brnd_grp join
               AND brnd.brnd_id(+) = prfl.brnd_id
               AND brnd_grp.brnd_grp_id(+) = brnd.brnd_grp_id
               AND prfl.prfl_cd(+) = offr_prfl_prc_point.prfl_cd
               AND o.offr_perd_id = p_filter.p_offr_perd_id
               --mrkt_tmp_fsc and master
               AND osl.sku_id = mrkt_tmp_fsc_master.sku_id(+)
               AND osl.sku_id = mrkt_tmp_fsc.sku_id(+)
               AND mrkt_tmp_fsc_master.mrkt_id(+) = osl.mrkt_id
               AND mrkt_tmp_fsc.mrkt_id(+) = osl.mrkt_id
               --offr_prfl_prc_point
               AND offr_prfl_prc_point.offr_prfl_prcpt_id(+) = osl.offr_prfl_prcpt_id
               AND dstrbtd_mrkt_sls.offr_sku_line_id(+) = osl.offr_sku_line_id
               --what_if_tran
               AND what_if_tran.offr_id(+) = o.offr_id
               AND what_if_tran.tran_typ(+) = 'WIF'
               --FILTERS
           AND dstrbtd_mrkt_sls.sls_typ_id (+) = p_filter.p_sls_typ
           AND CASE
                 WHEN p_filter.p_offr_id IS NULL THEN
                  1
                 WHEN o.offr_id IN (SELECT * FROM TABLE(p_filter.p_offr_id)) THEN
                  1
                 ELSE
                  0
               END = 1
           AND CASE
                 WHEN p_filter.p_veh_id IS NULL THEN
                  1
                 WHEN o.veh_id IN (SELECT * FROM TABLE(p_filter.p_veh_id)) THEN
                  1
                 ELSE
                  0
               END = 1
           AND CASE
                 WHEN p_filter.p_brchr_plcmt_id IS NULL THEN
                  1
                 WHEN o.brchr_plcmt_id IN
                      (SELECT * FROM TABLE(p_filter.p_brchr_plcmt_id)) THEN
                  1
                 ELSE
                  0
               END = 1
           AND CASE
                 WHEN p_filter.p_catgry_id IS NULL THEN
                  1
                 WHEN prfl.catgry_id IN
                      (SELECT * FROM TABLE(p_filter.p_catgry_id)) THEN
                  1
                 ELSE
                  0
               END = 1
           AND CASE
                 WHEN p_filter.p_brnd_id IS NULL THEN
                  1
                 WHEN brnd_grp.brnd_fmly_id IN
                      (SELECT * FROM TABLE(p_filter.p_brnd_id)) THEN
                  1
                 ELSE
                  0
               END = 1
           AND CASE
                 WHEN p_filter.p_prfl_cd IS NULL THEN
                  1
                 WHEN offr_prfl_prc_point.prfl_cd IN
                      (SELECT * FROM TABLE(p_filter.p_prfl_cd)) THEN
                  1
                 ELSE
                  0
               END = 1
           AND CASE
                 WHEN p_filter.p_sku_id IS NULL THEN
                  1
                 WHEN osl.sku_id IN
                      (SELECT * FROM TABLE(p_filter.p_sku_id)) THEN
                  1
                 ELSE
                  0
               END = 1
           AND CASE
                 WHEN p_filter.p_fsc_cd IS NULL THEN
                  1
                 WHEN nvl(mrkt_tmp_fsc_master.fsc_cd, mrkt_tmp_fsc.fsc_cd) IN
                      (SELECT * FROM TABLE(p_filter.p_fsc_cd)) THEN
                  1
                 ELSE
                  0
               END = 1
           AND CASE
                 WHEN p_filter.p_enrgy_chrt_postn_id IS NULL THEN
                  1
                 WHEN o.enrgy_chrt_postn_id IN
                      (SELECT * FROM TABLE(p_filter.p_enrgy_chrt_postn_id)) THEN
                  1
                 ELSE
                  0
               END = 1
           AND CASE
                 WHEN p_filter.p_web_postn_id IS NULL THEN
                  1
                 WHEN o.web_postn_id IN
                      (SELECT * FROM TABLE(p_filter.p_web_postn_id)) THEN
                  1
                 ELSE
                  0
               END = 1
         GROUP BY o.offr_id
      ) LOOP --get valid offer id-s loop

              l_get_offr_table.extend();
              l_get_offr_table(l_get_offr_table.last) := obj_get_offr_line(offrs.p_offr_id, nvl(offrs.p_sls_typ,1));
   END LOOP; --get valid offer id-s loop

   IF l_pagination <> 'P' THEN

     FOR rec IN (SELECT o.* FROM TABLE(get_offr(l_get_offr_table,
                                                l_pagination,
                                                l_scnrio_id)) o,
                   mrkt_veh mv
             WHERE mv.mrkt_id(+) = o.mrkt_id
               AND mv.veh_id(+) = o.veh_id
             ORDER BY mv.seq_nr, o.pg_nr, o.intrnl_offr_id, o.prfl_nm, o.sku_nm)
     LOOP --Result loop
       l_obj_edit_offr_table.extend();
       l_obj_edit_offr_table(l_obj_edit_offr_table.last) :=   OBJ_EDIT_OFFR_LINE(rec.status,
                rec.mrkt_id,
                rec.offr_perd_id,
                rec.offr_lock,
                rec.offr_lock_user,
                rec.offr_sku_line_id,
                rec.veh_id,
                rec.brchr_plcmnt_id,
                rec.brchr_sctn_nm,
                rec.enrgy_chrt_postn_id,
                rec.web_postn_id,
                rec.pg_nr,
                rec.ctgry_id,
                rec.brnd_id,
                rec.sgmt_id,
                rec.form_id,
                rec.form_grp_id,
                rec.prfl_cd,
                rec.sku_id,
                rec.fsc_cd,
                rec.prod_typ_id,
                rec.gender_id,
                rec.sls_cls_cd,
                rec.pp_sls_cls_cd,
                rec.item_sls_cls_cd,
                rec.offr_desc_txt,
                rec.offr_notes_txt,
                rec.offr_lyot_cmnts_txt,
                rec.featrd_side_cd,
                rec.concept_featrd_side_cd,
                rec.micr_ncpsltn_ind,
                rec.scntd_pg_typ_id,
                rec.cnsmr_invstmt_bdgt_id,
                rec.pymt_typ,
                rec.promtn_id,
                rec.promtn_clm_id,
                --rec.cmbntn_offr_typ,
                rec.spndng_lvl,
                rec.comsn_typ,
                rec.tax_type_id,
                rec.wsl_ind,
                rec.offr_sku_set_id,
                rec.cmpnt_qty,
                rec.nr_for_qty,
                rec.nta_factor,
                rec.sku_cost,
                rec.lv_nta,
                rec.lv_sp,
                rec.lv_rp,
                rec.lv_discount,
                rec.lv_units,
                rec.lv_total_cost,
                rec.lv_gross_sales,
                rec.lv_dp_cash,
                rec.lv_dp_percent,
                rec.ver_id,
                rec.sls_prc_amt,
                rec.reg_prc_amt,
                rec.line_nr,
                rec.unit_qty,
                rec.dltd_ind,
                rec.created_ts,
                rec.created_user_id,
                rec.last_updt_ts,
                rec.last_updt_user_id,
                rec.intrnl_offr_id,
                rec.mrkt_veh_perd_sctn_id,
                rec.prfl_nm,
                rec.sku_nm,
                rec.comsn_typ_desc_txt,
                rec.tax_typ_desc_txt,
                rec.offr_sku_Set_nm,
                rec.sls_typ,
                rec.pc_sp_py,
                rec.pc_rp,
                rec.pc_sp,
                rec.pc_vsp,
                rec.pc_hit,
                rec.pg_wght,
                rec.pp_pg_wght,
                rec.sprd_nr,
                rec.offr_prfl_prcpt_id,
                rec.has_unit_qty,
                rec.offr_typ,
                rec.forcasted_units,
                rec.forcasted_date,
                rec.offr_cls_id,
                rec.spcl_ordr_ind,
                rec.offr_ofs_nr,
                rec.pp_ofs_nr,
                rec.impct_catgry_id,
                rec.hero_ind,
                rec.smplg_ind,
                rec.mltpl_ind,
                rec.cmltv_ind,
                rec.use_instrctns_ind,
                rec.pg_typ_id,
                rec.featrd_prfl_ind,
                rec.fxd_pg_wght_ind,
                rec.prod_endrsmt_id,
                rec.frc_mtch_mthd_id,
                rec.wghtd_avg_cost_amt,
                rec.incntv_id,
                rec.intrdctn_perd_id,
                rec.on_stus_perd_id,
                rec.dspostn_perd_id,
                rec.scnrio_id,
                rec.micr_ncpsltn_desc_txt,
                rec.offr_link_id,
                rec.profile_item_count
              );

    --app_plsql_log.info(l_module_name||' osl '||rec.offr_sku_line_id||', scented page='||rec.scntd_pg_typ_id);

    PIPE row(OBJ_EDIT_OFFR_LINE(rec.status,
                rec.mrkt_id,
                rec.offr_perd_id,
                rec.offr_lock,
                rec.offr_lock_user,
                rec.offr_sku_line_id,
                rec.veh_id,
                rec.brchr_plcmnt_id,
                rec.brchr_sctn_nm,
                rec.enrgy_chrt_postn_id,
                rec.web_postn_id,
                rec.pg_nr,
                rec.ctgry_id,
                rec.brnd_id,
                rec.sgmt_id,
                rec.form_id,
                rec.form_grp_id,
                rec.prfl_cd,
                rec.sku_id,
                rec.fsc_cd,
                rec.prod_typ_id,
                rec.gender_id,
                rec.sls_cls_cd,
                rec.pp_sls_cls_cd,
                rec.item_sls_cls_cd,
                rec.offr_desc_txt,
                rec.offr_notes_txt,
                rec.offr_lyot_cmnts_txt,
                rec.featrd_side_cd,
                rec.concept_featrd_side_cd,
                rec.micr_ncpsltn_ind,
                rec.scntd_pg_typ_id,
                rec.cnsmr_invstmt_bdgt_id,
                rec.pymt_typ,
                rec.promtn_id,
                rec.promtn_clm_id,
                rec.spndng_lvl,
                rec.comsn_typ,
                rec.tax_type_id,
                rec.wsl_ind,
                rec.offr_sku_set_id,
                rec.cmpnt_qty,
                rec.nr_for_qty,
                rec.nta_factor,
                rec.sku_cost,
                rec.lv_nta,
                rec.lv_sp,
                rec.lv_rp,
                rec.lv_discount,
                rec.lv_units,
                rec.lv_total_cost,
                rec.lv_gross_sales,
                rec.lv_dp_cash,
                rec.lv_dp_percent,
                rec.ver_id,
                rec.sls_prc_amt,
                rec.reg_prc_amt,
                rec.line_nr,
                rec.unit_qty,
                rec.dltd_ind,
                rec.created_ts,
                rec.created_user_id,
                rec.last_updt_ts,
                rec.last_updt_user_id,
                rec.intrnl_offr_id,
                rec.mrkt_veh_perd_sctn_id,
                rec.prfl_nm,
                rec.sku_nm,
                rec.comsn_typ_desc_txt,
                rec.tax_typ_desc_txt,
                rec.offr_sku_Set_nm,
                rec.sls_typ,
                rec.pc_sp_py,
                rec.pc_rp,
                rec.pc_sp,
                rec.pc_vsp,
                rec.pc_hit,
                rec.pg_wght,
                rec.pp_pg_wght,
                rec.sprd_nr,
                rec.offr_prfl_prcpt_id,
                rec.has_unit_qty,
                rec.offr_typ,
                rec.forcasted_units,
                rec.forcasted_date,
                rec.offr_cls_id,
                rec.spcl_ordr_ind,
                rec.offr_ofs_nr,
                rec.pp_ofs_nr,
                rec.impct_catgry_id,
                rec.hero_ind,
                rec.smplg_ind,
                rec.mltpl_ind,
                rec.cmltv_ind,
                rec.use_instrctns_ind,
                rec.pg_typ_id,
                rec.featrd_prfl_ind,
                rec.fxd_pg_wght_ind,
                rec.prod_endrsmt_id,
                rec.frc_mtch_mthd_id,
                rec.wghtd_avg_cost_amt,
                rec.incntv_id,
                rec.intrdctn_perd_id,
                rec.on_stus_perd_id,
                rec.dspostn_perd_id,
                rec.scnrio_id,
                rec.micr_ncpsltn_desc_txt,
                rec.offr_link_id,
                rec.profile_item_count
              ));

     END LOOP;--result loop
   ELSE
     FOR rec IN (SELECT o.* FROM TABLE(get_offr_pivot(l_get_offr_table)) o)
     LOOP --Result loop
       l_obj_edit_offr_table.extend();
       l_obj_edit_offr_table(l_obj_edit_offr_table.last) :=   OBJ_EDIT_OFFR_LINE(rec.status,
                  rec.mrkt_id,
                  rec.offr_perd_id,
                  rec.offr_lock,
                  rec.offr_lock_user,
                  rec.offr_sku_line_id,
                  rec.veh_id,
                  rec.brchr_plcmnt_id,
                  rec.brchr_sctn_nm,
                  rec.enrgy_chrt_postn_id,
                  rec.web_postn_id,
                  rec.pg_nr,
                  rec.ctgry_id,
                  rec.brnd_id,
                  rec.sgmt_id,
                  rec.form_id,
                  rec.form_grp_id,
                  rec.prfl_cd,
                  rec.sku_id,
                  rec.fsc_cd,
                  rec.prod_typ_id,
                  rec.gender_id,
                  rec.sls_cls_cd,
                  rec.pp_sls_cls_cd,
                  rec.item_sls_cls_cd,
                  rec.offr_desc_txt,
                  rec.offr_notes_txt,
                  rec.offr_lyot_cmnts_txt,
                  rec.featrd_side_cd,
                  rec.concept_featrd_side_cd,
                  rec.micr_ncpsltn_ind,
                  rec.scntd_pg_typ_id,
                  rec.cnsmr_invstmt_bdgt_id,
                  rec.pymt_typ,
                  rec.promtn_id,
                  rec.promtn_clm_id,
                  --rec.cmbntn_offr_typ,
                  rec.spndng_lvl,
                  rec.comsn_typ,
                  rec.tax_type_id,
                  rec.wsl_ind,
                  rec.offr_sku_set_id,
                  rec.cmpnt_qty,
                  rec.nr_for_qty,
                  rec.nta_factor,
                  rec.sku_cost,
                  rec.lv_nta,
                  rec.lv_sp,
                  rec.lv_rp,
                  rec.lv_discount,
                  rec.lv_units,
                  rec.lv_total_cost,
                  rec.lv_gross_sales,
                  rec.lv_dp_cash,
                  rec.lv_dp_percent,
                  rec.ver_id,
                  rec.sls_prc_amt,
                  rec.reg_prc_amt,
                  rec.line_nr,
                  rec.unit_qty,
                  rec.dltd_ind,
                  rec.created_ts,
                  rec.created_user_id,
                  rec.last_updt_ts,
                  rec.last_updt_user_id,
                  rec.intrnl_offr_id,
                  rec.mrkt_veh_perd_sctn_id,
                  rec.prfl_nm,
                  rec.sku_nm,
                  rec.comsn_typ_desc_txt,
                  rec.tax_typ_desc_txt,
                  rec.offr_sku_Set_nm,
                  rec.sls_typ,
                  rec.pc_sp_py,
                  rec.pc_rp,
                  rec.pc_sp,
                  rec.pc_vsp,
                  rec.pc_hit,
                  rec.pg_wght,
                  rec.pp_pg_wght,
                  rec.sprd_nr,
                  rec.offr_prfl_prcpt_id,
                  rec.has_unit_qty,
                  rec.offr_typ,
                  rec.forcasted_units,
                  rec.forcasted_date,
                  rec.offr_cls_id,
                  rec.spcl_ordr_ind,
                  rec.offr_ofs_nr,
                  rec.pp_ofs_nr,
                  rec.impct_catgry_id,
                  rec.hero_ind,
                  rec.smplg_ind,
                  rec.mltpl_ind,
                  rec.cmltv_ind,
                  rec.use_instrctns_ind,
                  rec.pg_typ_id,
                  rec.featrd_prfl_ind,
                  rec.fxd_pg_wght_ind,
                  rec.prod_endrsmt_id,
                  rec.frc_mtch_mthd_id,
                  rec.wghtd_avg_cost_amt,
                  rec.incntv_id,
                  rec.intrdctn_perd_id,
                  rec.on_stus_perd_id,
                  rec.dspostn_perd_id,
                  rec.scnrio_id,
                  rec.micr_ncpsltn_desc_txt,
                  rec.offr_link_id,
                  rec.profile_item_count
                      
                  );

      PIPE row(OBJ_EDIT_OFFR_LINE(rec.status,
                  rec.mrkt_id,
                  rec.offr_perd_id,
                  rec.offr_lock,
                  rec.offr_lock_user,
                  rec.offr_sku_line_id,
                  rec.veh_id,
                  rec.brchr_plcmnt_id,
                  rec.brchr_sctn_nm,
                  rec.enrgy_chrt_postn_id,
                  rec.web_postn_id,
                  rec.pg_nr,
                  rec.ctgry_id,
                  rec.brnd_id,
                  rec.sgmt_id,
                  rec.form_id,
                  rec.form_grp_id,
                  rec.prfl_cd,
                  rec.sku_id,
                  rec.fsc_cd,
                  rec.prod_typ_id,
                  rec.gender_id,
                  rec.sls_cls_cd,
                  rec.pp_sls_cls_cd,
                  rec.item_sls_cls_cd,
                  rec.offr_desc_txt,
                  rec.offr_notes_txt,
                  rec.offr_lyot_cmnts_txt,
                  rec.featrd_side_cd,
                  rec.concept_featrd_side_cd,
                  rec.micr_ncpsltn_ind,
                  rec.scntd_pg_typ_id,
                  rec.cnsmr_invstmt_bdgt_id,
                  rec.pymt_typ,
                  rec.promtn_id,
                  rec.promtn_clm_id,
                  --rec.cmbntn_offr_typ,
                  rec.spndng_lvl,
                  rec.comsn_typ,
                  rec.tax_type_id,
                  rec.wsl_ind,
                  rec.offr_sku_set_id,
                  rec.cmpnt_qty,
                  rec.nr_for_qty,
                  rec.nta_factor,
                  rec.sku_cost,
                  rec.lv_nta,
                  rec.lv_sp,
                  rec.lv_rp,
                  rec.lv_discount,
                  rec.lv_units,
                  rec.lv_total_cost,
                  rec.lv_gross_sales,
                  rec.lv_dp_cash,
                  rec.lv_dp_percent,
                  rec.ver_id,
                  rec.sls_prc_amt,
                  rec.reg_prc_amt,
                  rec.line_nr,
                  rec.unit_qty,
                  rec.dltd_ind,
                  rec.created_ts,
                  rec.created_user_id,
                  rec.last_updt_ts,
                  rec.last_updt_user_id,
                  rec.intrnl_offr_id,
                  rec.mrkt_veh_perd_sctn_id,
                  rec.prfl_nm,
                  rec.sku_nm,
                  rec.comsn_typ_desc_txt,
                  rec.tax_typ_desc_txt,
                  rec.offr_sku_Set_nm,
                  rec.sls_typ,
                  rec.pc_sp_py,
                  rec.pc_rp,
                  rec.pc_sp,
                  rec.pc_vsp,
                  rec.pc_hit,
                  rec.pg_wght,
                  rec.pp_pg_wght,
                  rec.sprd_nr,
                  rec.offr_prfl_prcpt_id,
                  rec.has_unit_qty,
                  rec.offr_typ,
                  rec.forcasted_units,
                  rec.forcasted_date,
                  rec.offr_cls_id,
                  rec.spcl_ordr_ind,
                  rec.offr_ofs_nr,
                  rec.pp_ofs_nr,
                  rec.impct_catgry_id,
                  rec.hero_ind,
                  rec.smplg_ind,
                  rec.mltpl_ind,
                  rec.cmltv_ind,
                  rec.use_instrctns_ind,
                  rec.pg_typ_id,
                  rec.featrd_prfl_ind,
                  rec.fxd_pg_wght_ind,
                  rec.prod_endrsmt_id,
                  rec.frc_mtch_mthd_id,
                  rec.wghtd_avg_cost_amt,
                  rec.incntv_id,
                  rec.intrdctn_perd_id,
                  rec.on_stus_perd_id,
                  rec.dspostn_perd_id,
                  rec.scnrio_id,
                  rec.micr_ncpsltn_desc_txt,
                  rec.offr_link_id,
                  rec.profile_item_count
                  ));

    END LOOP;
  END IF;

                l_get_offr_table.delete(); --empty the offer id-s for every filter line
                
                
  END LOOP;--Filters from the screen loop
        app_plsql_log.info(l_module_name || ' stop');
        BEGIN
        merge_history(l_obj_edit_offr_table);
        END;
     EXCEPTION
       WHEN NO_DATA_NEEDED THEN
         app_plsql_log.info('no mor data needed from edit offr');
                     when others then
                       app_plsql_log.info('Error in edit offr' || sqlerrm(sqlcode));

end get_edit_offr_table;

--Getting the data for edit offer based on offr_id
--mrkt_id, offr_perd_id, ver_id, p_sls_typ must be equal in each row in p_get_offr parameter
FUNCTION get_offr(p_get_offr   IN obj_get_offr_table,
                  p_pagination IN CHAR DEFAULT 'N',
                  p_scnrio_id  IN NUMBER DEFAULT 0)
    RETURN obj_edit_offr_table
    PIPELINED AS
    -- local variables

    l_offr_perd_id offr.offr_perd_id%TYPE;
    l_mrkt_id      offr.mrkt_id%TYPE;
    l_ver_id       offr.ver_id%TYPE;
    l_sls_typ      dstrbtd_mrkt_sls.sls_typ_id%TYPE;
    l_veh_ids      number_array;
    -- for LOG
    l_run_id  NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id VARCHAR(35) := USER();
    --
    l_module_name VARCHAR2(30) := 'GET_OFFR';

    l_min_mrkt_id      offr.mrkt_id%TYPE;
    l_min_offr_perd_id offr.offr_perd_id%TYPE;
    l_min_ver_id       offr.ver_id%TYPE;
    l_min_sls_typ      dstrbtd_mrkt_sls.sls_typ_id%TYPE;

    --pricing corridor variables
--    pricing_used     single_char := 'N';
    prc_enabled      single_char;
    prc_strt_perd_id mrkt_perd.perd_id%TYPE;

  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
    app_plsql_log.info(l_module_name || ' start');

    SELECT MAX(NVL(p_sls_typ, -1)) INTO l_sls_typ FROM TABLE(p_get_offr);
    SELECT MAX(o.mrkt_id)
      INTO l_mrkt_id
      FROM TABLE(p_get_offr) poi
      JOIN offr o
        ON o.offr_id = poi.p_offr_id;
    SELECT MAX(o.ver_id)
      INTO l_ver_id
      FROM TABLE(p_get_offr) poi
      JOIN offr o
        ON o.offr_id = poi.p_offr_id;
    SELECT MAX(o.offr_perd_id)
      INTO l_offr_perd_id
      FROM TABLE(p_get_offr) poi
      JOIN offr o
        ON o.offr_id = poi.p_offr_id;
    SELECT DISTINCT (o.veh_id)
      BULK COLLECT
      INTO l_veh_ids
      FROM TABLE(p_get_offr) poi
      JOIN offr o
        ON o.offr_id = poi.p_offr_id;
    SELECT MIN(NVL(p_sls_typ, -1)) INTO l_min_sls_typ FROM TABLE(p_get_offr);
    SELECT MIN(o.mrkt_id)
      INTO l_min_mrkt_id
      FROM TABLE(p_get_offr) poi
      JOIN offr o
        ON o.offr_id = poi.p_offr_id;
    SELECT MIN(o.ver_id)
      INTO l_min_ver_id
      FROM TABLE(p_get_offr) poi
      JOIN offr o
        ON o.offr_id = poi.p_offr_id;
    SELECT MIN(o.offr_perd_id)
      INTO l_min_offr_perd_id
      FROM TABLE(p_get_offr) poi
      JOIN offr o
        ON o.offr_id = poi.p_offr_id;
    IF l_min_sls_typ <> l_sls_typ OR l_min_mrkt_id <> l_mrkt_id OR   l_min_ver_id <> l_ver_id OR  l_min_offr_perd_id <> l_offr_perd_id
    THEN
      RAISE_APPLICATION_ERROR(-20100, 'market, campaign, version and sales type must be the same to use this function');
    END IF;

    IF l_sls_typ = -1 THEN
      l_sls_typ := NULL;      
    END IF;

    dbms_output.put_line(l_sls_typ || l_mrkt_id || l_ver_id ||
                         l_offr_perd_id);

    BEGIN
      --    dbms_output.put_line('prc enabled check'||' mrkt_id:'||l_mrkt_id);
      SELECT mrkt_config_item_val_txt
        INTO prc_enabled
        FROM mrkt_config_item
       WHERE mrkt_id = l_mrkt_id
         AND config_item_id = cfg_pricing_market;
      --    dbms_output.put_line('prc perd check');
      SELECT to_number(mrkt_config_item_val_txt)
        INTO prc_strt_perd_id
        FROM mrkt_config_item
       WHERE mrkt_id = l_mrkt_id
         AND config_item_id = cfg_pricing_start_perd;

    EXCEPTION
      WHEN OTHERS THEN
        null;
    END;

    FOR rec IN (

      WITH latest_version_and_sls_typ AS
                   (SELECT asl.ver_id
                          ,asl.sls_typ_id
                          ,asl.offr_perd_id
                          ,asl.veh_id
                      FROM (SELECT ver_id
                                  ,MAX(sls_typ_id) sls_typ_id
                                  ,offr_perd_id
                                  ,veh_id
                              FROM (SELECT ver_id
                                          ,sls_typ_id
                                          ,offr_perd_id
                                          ,veh_id
                                          ,seq_nr
                                          ,MAX(seq_nr) over(PARTITION BY offr_perd_id, veh_id) max_seq_nr
                                      FROM (SELECT dstrbtd_mrkt_sls.ver_id
                                                  ,dstrbtd_mrkt_sls.sls_typ_id
                                                  ,dstrbtd_mrkt_sls.offr_perd_id
                                                  ,dstrbtd_mrkt_sls.veh_id
                                                  ,MAX(ver.seq_nr) AS seq_nr
                                              FROM dstrbtd_mrkt_sls
                                              JOIN ver
                                                ON ver.ver_id =
                                                   dstrbtd_mrkt_sls.ver_id
                                             WHERE dstrbtd_mrkt_sls.mrkt_id =
                                                   l_mrkt_id
                                               AND dstrbtd_mrkt_sls.offr_perd_id =
                                                   l_offr_perd_id
                                               AND dstrbtd_mrkt_sls.sls_perd_id = l_offr_perd_id
                                               AND dstrbtd_mrkt_sls.ver_id <>
                                                   l_ver_id
                                               AND CASE
                                                     WHEN l_ver_id = 0
                                                          AND ver.seq_nr <= 1500 THEN
                                                      1
                                                     WHEN l_ver_id <> 0
                                                          AND
                                                          ver.seq_nr <
                                                          (SELECT seq_nr
                                                             FROM ver
                                                            WHERE ver_id = l_ver_id) THEN
                                                      1
                                                     ELSE
                                                      0
                                                   END = 1
                                               AND p_pagination = 'N'
                                             GROUP BY dstrbtd_mrkt_sls.ver_id
                                                     ,dstrbtd_mrkt_sls.sls_typ_id
                                                     ,dstrbtd_mrkt_sls.offr_perd_id
                                                     ,dstrbtd_mrkt_sls.veh_id))
                             WHERE max_seq_nr = seq_nr
                               AND seq_nr <> 100
                             GROUP BY ver_id, offr_perd_id, veh_id) asl),

latest_osl_stuff AS
 (SELECT osl.sku_id
        ,osl.offr_prfl_prcpt_id
        ,o.mrkt_id
        ,o.offr_perd_id
        ,osl.offr_sku_line_link_id
        ,SUM(dms.unit_qty) AS unit_qty
        ,dms.sls_typ_id
        ,SUM(dms.net_to_avon_fct * dms.unit_qty) / SUM(dms.unit_qty) AS net_to_avon_fct
    FROM offr o
    JOIN offr_sku_line osl
      ON osl.offr_id = o.offr_id
    JOIN dstrbtd_mrkt_sls dms
      ON dms.offr_sku_line_id = osl.offr_sku_line_id
    JOIN latest_version_and_sls_typ
      ON o.offr_perd_id = latest_version_and_sls_typ.offr_perd_id
     AND o.veh_id = latest_version_and_sls_typ.veh_id
     AND o.ver_id = latest_version_and_sls_typ.ver_id
     AND o.offr_perd_id = latest_version_and_sls_typ.offr_perd_id
     AND dms.sls_typ_id = latest_version_and_sls_typ.sls_typ_id
     AND dms.unit_qty <> 0
     AND p_pagination = 'N'
   GROUP BY osl.offr_sku_line_id
           ,osl.offr_sku_line_link_id
           ,osl.sku_id
           ,osl.offr_prfl_prcpt_id
           ,o.ver_id
           ,dms.sls_typ_id
           ,o.mrkt_id
           ,o.offr_perd_id),
mrkt_tmp_fsc_master AS
 (SELECT mrkt_id, sku_id, fsc_cd
    FROM (SELECT mrkt_id
                ,sku_id
                ,mstr_fsc_cd fsc_cd
                ,strt_perd_id from_strt_perd_id
                ,nvl(lead(strt_perd_id, 1)
                     over(PARTITION BY mrkt_id, sku_id ORDER BY strt_perd_id),
                     99999999) to_strt_perd_id
            FROM mstr_fsc_asgnmt
           WHERE l_mrkt_id = mrkt_id
             AND l_offr_perd_id >= strt_perd_id)
   WHERE l_offr_perd_id >= from_strt_perd_id
     AND l_offr_perd_id < to_strt_perd_id),
mrkt_tmp_fsc AS
 (SELECT mrkt_id, sku_id, MAX(fsc_cd) fsc_cd
    FROM (SELECT mrkt_id
                ,sku_id
                ,fsc_cd fsc_cd
                ,strt_perd_id from_strt_perd_id
                ,nvl(lead(strt_perd_id, 1)
                     over(PARTITION BY mrkt_id, fsc_cd ORDER BY strt_perd_id),
                     99999999) to_strt_perd_id
            FROM mrkt_fsc
           WHERE l_mrkt_id = mrkt_id
             AND l_offr_perd_id >= strt_perd_id
             AND 'N' = dltd_ind)
   WHERE l_offr_perd_id >= from_strt_perd_id
     AND l_offr_perd_id < to_strt_perd_id
   GROUP BY mrkt_id, sku_id),
frcst AS
  (SELECT ML_WHAT_IF_SCNRIO_OSL.offr_sku_line_id,
          offr_sku_line.offr_prfl_prcpt_id,
          ML_WHAT_IF_SCNRIO_OSL.ML_UNIT_QTY forcasted_units,
          ML_WHAT_IF_SCNRIO_OSL.last_updt_ts forcasted_date
     FROM offr_sku_line,
          ML_WHAT_IF_SCNRIO_OSL
    WHERE offr_sku_line.mrkt_id = ML_WHAT_IF_SCNRIO_OSL.mrkt_id
      AND offr_sku_line.offr_perd_id = ML_WHAT_IF_SCNRIO_OSL.offr_perd_id
      AND offr_sku_line.offr_sku_line_id = ML_WHAT_IF_SCNRIO_OSL.offr_sku_line_id
      AND ML_WHAT_IF_SCNRIO_OSL.scnrio_id = p_scnrio_id
  )
--
  SELECT o.offr_id  AS intrnl_offr_id
      --,l_sls_typ AS sls_typ
      ,NVL(osl_current.sales_type, (SELECT MAX(mps_sls_typ_id)
                                          FROM mrkt_veh_perd_ver mvpv
                                         WHERE mvpv.mrkt_id = l_mrkt_id
                                           AND mvpv.offr_perd_id = l_offr_perd_id
                                           AND mvpv.ver_id = l_ver_id
                                           AND mvpv.veh_id = o.veh_id)) AS sls_typ
       --latest version calculations
      ,osl_latest.net_to_avon_fct AS lv_nta
      ,osl_latest.offr_prfl_sls_prc_amt AS lv_sp
      ,osl_latest.reg_prc_amt AS lv_rp
      ,CASE
         WHEN osl_latest.reg_prc_amt = 0 THEN
          0
         WHEN osl_latest.offr_prfl_sls_prc_amt = 0 THEN
          100
         ELSE
          round(100 - (osl_latest.offr_prfl_sls_prc_amt /
                osl_latest.nr_for_qty / osl_latest.reg_prc_amt) * 100)
       END lv_discount

      ,osl_latest.unit_qty AS lv_units
      ,osl_latest.unit_qty * osl_latest.sku_cost_amt AS lv_total_cost
      ,CASE
         WHEN osl_latest.nr_for_qty = 0 THEN
          0
         ELSE
          round(osl_latest.offr_prfl_sls_prc_amt / osl_latest.nr_for_qty *
                osl_latest.unit_qty * osl_latest.net_to_avon_fct,
                4)
       END lv_gross_sales
      ,CASE
         WHEN osl_latest.nr_for_qty = 0 THEN
          0
         ELSE
          round(osl_latest.offr_prfl_sls_prc_amt / osl_latest.nr_for_qty *
                osl_latest.unit_qty * osl_latest.net_to_avon_fct -
                osl_latest.sku_cost_amt * osl_latest.unit_qty,
                4)
       END lv_dp_cash
      ,CASE
         WHEN osl_latest.nr_for_qty = 0
              OR osl_latest.net_to_avon_fct = 0
              OR osl_latest.unit_qty = 0
              OR osl_latest.offr_prfl_sls_prc_amt = 0 THEN
          0
         ELSE
          round((osl_latest.offr_prfl_sls_prc_amt / osl_latest.nr_for_qty *
                osl_latest.unit_qty * osl_latest.net_to_avon_fct -
                osl_latest.sku_cost_amt * osl_latest.unit_qty) /
                (osl_latest.offr_prfl_sls_prc_amt / osl_latest.nr_for_qty *
                osl_latest.unit_qty * osl_latest.net_to_avon_fct),
                4) * 100
       END lv_dp_percent
       --/latest version calculations
      ,0 AS status
      ,o.mrkt_id AS mrkt_id
      ,o.offr_perd_id AS offr_perd_id
      ,nvl2(offr_lock.sys_id, 1, 0) AS offr_lock
      ,offr_lock.user_nm AS offr_lock_user
      ,osl_current.offr_sku_line_id AS offr_sku_line_id
      ,o.veh_id AS veh_id
      ,o.brchr_plcmt_id AS brchr_plcmnt_id
      ,mrkt_veh_perd_sctn.sctn_nm AS brchr_sctn_nm
      ,o.enrgy_chrt_postn_id AS enrgy_chrt_postn_id
      ,o.web_postn_id AS web_postn_id
      ,MIN(decode(o.mrkt_veh_perd_sctn_id,
                  NULL,
                  o.mrkt_veh_perd_sctn_id,
                  mvps.strtg_page_nr + o.sctn_page_ofs_nr +
                  offr_prfl_prc_point.pg_ofs_nr +
                  decode(offr_prfl_prc_point.featrd_side_cd,
                         1,
                         1,
                         0,
                         0,
                         2,
                         0,
                         0))) over(PARTITION BY o.offr_id) AS pg_nr
      ,prfl.catgry_id AS ctgry_id
      ,brnd_grp.brnd_fmly_id AS brnd_id
      ,prfl.sgmt_id AS sgmt_id
      ,prfl.form_id AS form_id
      ,form.form_grp_id AS form_grp_id
      ,offr_prfl_prc_point.prfl_cd AS prfl_cd
      ,osl_current.sku_id AS sku_id
      ,nvl(mrkt_tmp_fsc_master.fsc_cd, mrkt_tmp_fsc.fsc_cd) AS fsc_cd
      ,prfl.prod_typ_id AS prod_typ_id
      ,prfl.gendr_id AS gender_id
      ,osl_current.sls_cls_cd AS sls_cls_cd
      ,offr_prfl_prc_point.sls_cls_cd AS pp_sls_cls_cd
      ,pa_maps_public.get_sls_cls_cd(o.offr_perd_id, o.mrkt_id, osl_current.sku_id) AS item_sls_cls_cd
      ,o.offr_desc_txt AS offr_desc_txt
      ,o.offr_ntes_txt AS offr_notes_txt
      ,substr(TRIM(o.offr_lyot_cmnts_txt), 0, 3000) AS offr_lyot_cmnts_txt
      ,o.featrd_side_cd AS featrd_side_cd
      ,offr_prfl_prc_point.featrd_side_cd AS concept_featrd_side_cd
      ,offr_sls_cls_sku.micr_ncpsltn_ind AS micr_ncpsltn_ind
      ,offr_sls_cls_sku.scntd_pg_typ_id AS scntd_pg_typ_id
      ,offr_prfl_prc_point.cnsmr_invstmt_bdgt_id AS cnsmr_invstmt_bdgt_id
      ,offr_prfl_prc_point.pymt_typ AS pymt_typ
      ,offr_prfl_prc_point.promtn_id AS promtn_id
      ,offr_prfl_prc_point.promtn_clm_id AS promtn_clm_id
       --,1 AS cmbntn_offr_typ
      ,'NEW_FEATURE' AS spndng_lvl
      ,offr_prfl_prc_point.tax_type_id AS tax_type_id
      ,offr_sls_cls_sku.wsl_ind AS wsl_ind
      ,offr_prfl_prc_point.comsn_typ AS comsn_typ
      ,offr_sku_set.offr_sku_set_id AS offr_sku_set_id
      ,osl_current.set_cmpnt_qty AS cmpnt_qty
      ,offr_prfl_prc_point.nr_for_qty AS nr_for_qty
      ,offr_prfl_prc_point.net_to_avon_fct AS nta_factor
      ,osl_current.sku_cost_amt AS sku_cost
      ,o.ver_id AS ver_id
      ,offr_prfl_prc_point.sls_prc_amt AS sls_prc_amt
      ,CASE
         WHEN osl_current.offr_sku_line_id IS NOT NULL THEN
          nvl(osl_current.reg_prc_amt, 0)
         ELSE
          NULL
       END AS reg_prc_amt --sku reg prcb?l
      ,osl_current.line_nr AS line_nr
      ,osl_current.sum_unit_qty AS unit_qty
      ,osl_current.dltd_ind AS dltd_ind
      ,o.creat_ts AS created_ts
      ,o.creat_user_id AS created_user_id
      ,MAX(greatest(os.log_user_id, o.last_updt_user_id))
        KEEP (DENSE_RANK FIRST ORDER BY greatest(NVL(os.log_ts, o.last_updt_ts), o.last_updt_ts)) 
        OVER (PARTITION BY greatest(NVL(os.log_ts, o.last_updt_ts), o.last_updt_ts)) AS last_updt_user_id
      ,greatest(NVL(os.log_ts, o.last_updt_ts), o.last_updt_ts) AS last_updt_ts
      ,mrkt_veh_perd_sctn.mrkt_veh_perd_sctn_id AS mrkt_veh_perd_sctn_id
      ,prfl.prfl_nm AS prfl_nm
      ,osl_current.sku_nm AS sku_nm
      ,comsn_typ.comsn_typ_desc_txt AS comsn_typ_desc_txt
      ,mrkt_tax_typ.tax_typ_desc_txt AS tax_typ_desc_txt
      ,offr_sku_set.offr_sku_set_nm AS offr_sku_set_nm
       --pricing corridors
      ,CASE
         WHEN p_pagination = 'N' THEN
           coalesce((SELECT MAX(spa) keep(dense_rank LAST ORDER BY skuid, sumu, vehid)
                  FROM (SELECT osl_sp.sku_id AS skuid
                              ,SUM(dms_sp.unit_qty) sumu
                              ,osl_sp.veh_id vehid
                              ,MAX(oppp.sls_prc_amt) keep(dense_rank LAST ORDER BY osl_sp.sku_id, osl_sp.veh_id) spa
                          FROM offr_prfl_prc_point oppp
                              ,offr_sku_line       osl_sp
                              ,dstrbtd_mrkt_sls    dms_sp
                              ,offr                o_sp
                         WHERE oppp.offr_prfl_prcpt_id =
                               osl_sp.offr_prfl_prcpt_id
                           AND osl_sp.mrkt_id = l_mrkt_id
                           AND osl_sp.offr_perd_id =
                               (SELECT MAX(tpymp.prev_yr_perd_id) keep(dense_rank LAST ORDER BY tpymp.last_updt_ts) AS prev_yr_perd_id
                                  FROM trnsfrm_prev_yr_mrkt_perd tpymp
                                 WHERE tpymp.mrkt_id = l_mrkt_id
                                   AND tpymp.perd_id = l_offr_perd_id)
                           AND dms_sp.offr_sku_line_id =
                               osl_sp.offr_sku_line_id
                           AND dms_sp.sls_typ_id IN (6, 7)
                           AND o_sp.ver_id = 0
                           AND o_sp.offr_id = osl_sp.offr_id
                         GROUP BY osl_sp.sku_id
                                 ,osl_sp.veh_id
                                 ,oppp.offr_prfl_prcpt_id)
                 WHERE vehid = o.veh_id
                   AND skuid = osl_current.sku_id),
                (SELECT MAX(spa) keep(dense_rank LAST ORDER BY skuid, sumu, vehid)
                   FROM (SELECT osl_sp.sku_id AS skuid
                               ,SUM(dms_sp.unit_qty) sumu
                               ,osl_sp.veh_id vehid
                               ,MAX(oppp.sls_prc_amt) keep(dense_rank LAST ORDER BY osl_sp.sku_id, osl_sp.veh_id) spa
                           FROM offr_prfl_prc_point oppp
                               ,offr_sku_line       osl_sp
                               ,dstrbtd_mrkt_sls    dms_sp
                               ,offr                o_sp
                          WHERE oppp.offr_prfl_prcpt_id =
                                osl_sp.offr_prfl_prcpt_id
                            AND osl_sp.mrkt_id = l_mrkt_id
                            AND osl_sp.offr_perd_id =
                                (SELECT MAX(tpymp.prev_yr_perd_id) keep(dense_rank LAST ORDER BY tpymp.last_updt_ts) AS prev_yr_perd_id
                                   FROM trnsfrm_prev_yr_mrkt_perd tpymp
                                  WHERE tpymp.mrkt_id = l_mrkt_id
                                    AND tpymp.perd_id = l_offr_perd_id)
                            AND dms_sp.offr_sku_line_id =
                                osl_sp.offr_sku_line_id
                            AND dms_sp.sls_typ_id IN (6, 7)
                            AND o_sp.ver_id = 0
                            AND o_sp.offr_id = osl_sp.offr_id
                          GROUP BY osl_sp.sku_id
                                  ,osl_sp.veh_id
                                  ,oppp.offr_prfl_prcpt_id)
                  WHERE vehid <> o.veh_id
                          AND skuid = osl_current.sku_id))
         ELSE
          NULL
         END AS pc_sp_py
      ,pricing.pc_rp
      ,pricing.pc_sp
      ,pricing.pc_vsp
      ,pricing.pc_hit
      ,o.pg_wght_pct AS pg_wght
      ,offr_prfl_sls_cls_plcmt.pg_wght_pct AS pp_pg_wght
      ,CASE
         WHEN offr_prfl_prc_point.featrd_side_cd = 1 THEN
          (SELECT MAX(decode(o.mrkt_veh_perd_sctn_id,
                             NULL,
                             o.mrkt_veh_perd_sctn_id,
                             mvps.strtg_page_nr + o.sctn_page_ofs_nr +
                             offr_prfl_prc_point.pg_ofs_nr +
                             decode(offr_prfl_prc_point.featrd_side_cd,
                                    1,
                                    1,
                                    0,
                                    0,
                                    2,
                                    1,
                                    0))) over(PARTITION BY o.offr_id)
             FROM dual) - 1
         ELSE
          (SELECT MIN(decode(o.mrkt_veh_perd_sctn_id,
                             NULL,
                             o.mrkt_veh_perd_sctn_id,
                             mvps.strtg_page_nr + o.sctn_page_ofs_nr +
                             offr_prfl_prc_point.pg_ofs_nr +
                             decode(offr_prfl_prc_point.featrd_side_cd,
                                    1,
                                    1,
                                    0,
                                    0,
                                    2,
                                    0,
                                    0))) over(PARTITION BY o.offr_id)
             FROM dual)
       END AS sprd_nr
      ,offr_prfl_prc_point.offr_prfl_prcpt_id AS offr_prfl_prcpt_id
      ,(nvl((SELECT MAX(1)
              FROM offr_sku_line osl, dstrbtd_mrkt_sls dms, sls_typ st
             WHERE dms.offr_sku_line_id = osl.offr_sku_line_id
               AND osl.offr_prfl_prcpt_id =
                   offr_prfl_prc_point.offr_prfl_prcpt_id
               AND st.sls_typ_id = dms.sls_typ_id
               AND osl.mrkt_id = l_mrkt_id
               AND osl.offr_perd_id = l_offr_perd_id
               AND st.sls_typ_grp_nm = 'Actual'
               AND dms.ver_id = 0
               AND dms.unit_qty > 0),
            0)) AS has_unit_qty
      ,o.offr_typ
      ,frcst.forcasted_units
      ,frcst.forcasted_date
      ,o.offr_cls_id
      ,NVL(o.spcl_ordr_ind, 'N') AS spcl_ordr_ind
      ,o.sctn_page_ofs_nr AS offr_ofs_nr
      ,offr_prfl_prc_point.pg_ofs_nr AS pp_ofs_nr
      ,offr_prfl_prc_point.impct_catgry_id
      ,offr_sls_cls_sku.hero_ind
      ,offr_sls_cls_sku.smplg_ind
      ,offr_sls_cls_sku.mltpl_ind
      ,offr_sls_cls_sku.cmltv_ind
      ,offr_prfl_sls_cls_plcmt.use_instrctns_ind
      ,offr_prfl_sls_cls_plcmt.pg_typ_id
      ,offr_prfl_sls_cls_plcmt.featrd_prfl_ind
      ,offr_prfl_sls_cls_plcmt.fxd_pg_wght_ind
      ,offr_prfl_sls_cls_plcmt.prod_endrsmt_id
      ,offr_prfl_prc_point.frc_mtch_mthd_id
      ,ROUND(DECODE(
         o.ver_id,
         0,
         CASE
            WHEN SUM(osl_current.sum_unit_qty) OVER (PARTITION BY offr_prfl_prc_point.offr_prfl_prcpt_id, osl_current.sales_type) = 0
               THEN AVG (osl_current.sku_cost_amt) OVER (PARTITION BY offr_prfl_prc_point.offr_prfl_prcpt_id, osl_current.sum_unit_qty)
            ELSE   SUM (osl_current.sku_cost_amt * osl_current.sum_unit_qty) OVER (PARTITION BY offr_prfl_prc_point.offr_prfl_prcpt_id, osl_current.sales_type)
                 / SUM (osl_current.sum_unit_qty) OVER (PARTITION BY offr_prfl_prc_point.offr_prfl_prcpt_id, osl_current.sales_type)
         END,
         CASE
            WHEN SUM (osl_current.sum_unit_qty) OVER (PARTITION BY offr_prfl_prc_point.offr_prfl_prcpt_id, osl_current.sales_type) = 0
               THEN AVG (osl_current.sum_cost_amt) OVER (PARTITION BY offr_prfl_prc_point.offr_prfl_prcpt_id, osl_current.sales_type)
            ELSE   SUM (  osl_current.sum_cost_amt
                        * osl_current.sum_unit_qty
                       ) OVER (PARTITION BY offr_prfl_prc_point.offr_prfl_prcpt_id, osl_current.sales_type)
                 / SUM (osl_current.sum_unit_qty) over (PARTITION BY offr_prfl_prc_point.offr_prfl_prcpt_id, osl_current.sales_type)
         END), 3) AS wghtd_avg_cost_amt
      ,offr_sls_cls_sku.incntv_id
      ,mrkt_sku.intrdctn_perd_id
      ,mrkt_sku.on_stus_perd_id
      ,mrkt_sku.dspostn_perd_id
      ,wit.scnrio_id
      ,o.micr_ncpsltn_desc_txt
      ,o.offr_link_id
      ,(SELECT COUNT(1)
          FROM sku s,
               mrkt_sku ms,
               mrkt_prfl mp
         WHERE mp.prfl_cd = s.prfl_cd
           AND ms.mrkt_id = mp.mrkt_id
           AND ms.sku_id = s.sku_id
           AND mp.dltd_ind = 'N'
           AND mp.mrkt_id = o.mrkt_id
           AND mp.prfl_cd = prfl.prfl_cd) AS profile_item_count
--           
  FROM (SELECT *
           FROM offr
          WHERE offr_id IN (SELECT p_offr_id FROM table(p_get_offr))
       AND offr.mrkt_id = l_mrkt_id
       AND offr.offr_perd_id = l_offr_perd_id
       AND offr.ver_id = l_ver_id) o
       ,offr_summry_log os
       ,(SELECT offr_sku_line.offr_sku_line_id
               ,offr_sku_line.sku_id
               ,offr_sku_line.featrd_side_cd
               ,offr_sku_line.pg_ofs_nr
               ,offr_sku_line.prfl_cd
               ,offr_sku_line.sls_cls_cd
               ,offr_sku_line.offr_id
               ,offr_sku_line.offr_sku_set_id
               ,offr_sku_line.offr_prfl_prcpt_id
               ,offr_sku_line.dltd_ind
               ,offr_sku_line.line_nr
               ,offr_sku_line.set_cmpnt_qty
               ,offr_sku_line.mrkt_id
               ,CASE
                  WHEN actual_sku.wghtd_avg_cost_amt IS NOT NULL THEN
                   actual_sku.wghtd_avg_cost_amt
                  ELSE
                   planned_sku.wghtd_avg_cost_amt
                END sku_cost_amt
               ,l_ver_id AS ver_id
               ,sku_reg_prc.reg_prc_amt
               ,sku.sku_nm
               --,l_sls_typ AS sales_type
               ,CASE WHEN l_sls_typ IS NULL THEN
                   (SELECT MAX(mps_sls_typ_id)
                      FROM mrkt_veh_perd_ver mvpv
                     WHERE mvpv.mrkt_id = l_mrkt_id
                       AND mvpv.offr_perd_id = l_offr_perd_id
                       AND mvpv.ver_id = l_ver_id
                       AND mvpv.veh_id = o.veh_id)
                  ELSE
                    l_sls_typ
                  END AS sales_type
               ,(SELECT MAX(sls_typ_id)
                   FROM dstrbtd_mrkt_sls
                  WHERE mrkt_id = l_mrkt_id
                    AND offr_perd_id = l_offr_perd_id
                    AND sls_perd_id = l_offr_perd_id
                    AND veh_id = offr_sku_line.veh_id
                    AND ver_id = l_ver_id) max_sales_type
               ,(SELECT nvl(SUM(unit_qty), 0) AS sum_unit_qty
                   FROM dstrbtd_mrkt_sls
                  WHERE sls_typ_id = --l_sls_typ
                                     (CASE WHEN l_sls_typ IS NULL THEN
                                       (SELECT MAX(mps_sls_typ_id)
                                          FROM mrkt_veh_perd_ver mvpv
                                         WHERE mvpv.mrkt_id = l_mrkt_id
                                           AND mvpv.offr_perd_id = l_offr_perd_id
                                           AND mvpv.ver_id = l_ver_id
                                           AND mvpv.veh_id = o.veh_id)
                                      ELSE
                                        l_sls_typ
                                      END)
                    AND mrkt_id = l_mrkt_id
                    AND offr_perd_id = l_offr_perd_id
                    AND sls_perd_id = l_offr_perd_id
                    AND offr_sku_line_id = offr_sku_line.offr_sku_line_id
                  GROUP BY offr_sku_line_id, sls_typ_id) sum_unit_qty
               ,(SELECT nvl(SUM(cost_amt), 0) AS sum_cost_amt
                   FROM dstrbtd_mrkt_sls
                  WHERE sls_typ_id = --l_sls_typ
                                     (CASE WHEN l_sls_typ IS NULL THEN
                                       (SELECT MAX(mps_sls_typ_id)
                                          FROM mrkt_veh_perd_ver mvpv
                                         WHERE mvpv.mrkt_id = l_mrkt_id
                                           AND mvpv.offr_perd_id = l_offr_perd_id
                                           AND mvpv.ver_id = l_ver_id
                                           AND mvpv.veh_id = o.veh_id)
                                      ELSE
                                        l_sls_typ
                                      END)
                    AND mrkt_id = l_mrkt_id
                    AND offr_perd_id = l_offr_perd_id
                    AND sls_perd_id = l_offr_perd_id
                    AND offr_sku_line_id = offr_sku_line.offr_sku_line_id
                  GROUP BY offr_sku_line_id, sls_typ_id) AS sum_cost_amt
           FROM offr_sku_line
               ,sku_cost    actual_sku
               ,sku_cost    planned_sku
               ,sku_reg_prc
               ,sku
               ,offr        o
          WHERE
           o.offr_id = offr_sku_line.offr_id
       AND o.mrkt_id = l_mrkt_id
       AND o.offr_perd_id = l_offr_perd_id
       AND o.ver_id = l_ver_id
       AND o.offr_id IN (SELECT p_offr_id FROM table(p_get_offr))

       AND offr_sku_line.sku_id = actual_sku.sku_id(+)
       AND actual_sku.cost_typ(+) = 'A'
       AND actual_sku.mrkt_id(+) = l_mrkt_id
       AND actual_sku.offr_perd_id(+) = l_offr_perd_id
       AND offr_sku_line.sku_id = planned_sku.sku_id(+)
       AND planned_sku.mrkt_id(+) = l_mrkt_id
       AND planned_sku.offr_perd_id(+) = l_offr_perd_id
       AND planned_sku.cost_typ(+) = 'P'
       AND sku_reg_prc.mrkt_id(+) = l_mrkt_id
       AND sku_reg_prc.offr_perd_id(+) = l_offr_perd_id
       AND sku_reg_prc.sku_id(+) = offr_sku_line.sku_id
       AND sku.sku_id(+) = offr_sku_line.sku_id) osl_current
      ,(SELECT osl_ltst.*
              ,CASE
                 WHEN actual_sku.wghtd_avg_cost_amt IS NOT NULL THEN
                  actual_sku.wghtd_avg_cost_amt
                 ELSE
                  planned_sku.wghtd_avg_cost_amt
               END sku_cost_amt
              ,sku_reg_prc.reg_prc_amt
              ,offr_prfl_prc_point.nr_for_qty
              ,offr_prfl_prc_point.sls_prc_amt AS offr_prfl_sls_prc_amt

          FROM latest_osl_stuff    osl_ltst
              ,offr_prfl_prc_point
              ,sku_reg_prc
              ,sku_cost            actual_sku
              ,sku_cost            planned_sku
         WHERE offr_prfl_prc_point.offr_prfl_prcpt_id =
               osl_ltst.offr_prfl_prcpt_id
           AND osl_ltst.sku_id = actual_sku.sku_id(+)
           AND actual_sku.cost_typ(+) = 'A'
           AND actual_sku.mrkt_id(+) = l_mrkt_id
           AND actual_sku.offr_perd_id(+) = l_offr_perd_id
           AND osl_ltst.sku_id = planned_sku.sku_id(+)
           AND planned_sku.mrkt_id(+) = l_mrkt_id
           AND planned_sku.offr_perd_id(+) = l_offr_perd_id
           AND planned_sku.cost_typ(+) = 'P'
           AND sku_reg_prc.sku_id(+) = osl_ltst.sku_id
           AND sku_reg_prc.offr_perd_id(+) = l_offr_perd_id
           AND sku_reg_prc.mrkt_id(+) = l_mrkt_id
           AND osl_ltst.mrkt_id = l_mrkt_id
           AND osl_ltst.offr_perd_id = l_offr_perd_id
           AND p_pagination = 'N') osl_latest
      ,offr_prfl_prc_point
      ,prfl
      ,form
      ,offr_sku_set
      ,offr_lock
      ,offr_sls_cls_sku
      ,offr_prfl_sls_cls_plcmt
      ,mrkt_veh_perd_sctn
      ,(SELECT *
          FROM mrkt_veh_perd_sctn t2
         WHERE t2.mrkt_id = l_mrkt_id
           AND t2.ver_id = l_ver_id
           AND t2.offr_perd_id = l_offr_perd_id
        UNION
        SELECT DISTINCT t2.mrkt_id
                       ,t2.offr_perd_id
                       ,t2.ver_id
                       ,1
                       ,0
                       ,t2.veh_id
                       ,0
                       ,0
                       ,NULL
                       ,0
                       ,0
                       ,USER
                       ,SYSDATE
                       ,USER
                       ,SYSDATE
          FROM mrkt_veh_perd_ver t2
         WHERE t2.mrkt_id = l_mrkt_id
           AND t2.ver_id = l_ver_id
           AND t2.offr_perd_id = l_offr_perd_id) mvps
      ,comsn_typ
      ,mrkt_tax_typ
      ,brnd_grp
      ,brnd
      ,mrkt_tmp_fsc
      ,mrkt_tmp_fsc_master
      ,frcst
      ,(SELECT 
           mpsp.rp AS pc_rp
          ,mpsp.sp AS pc_sp
          ,mpsp.vsp AS pc_vsp
          ,mpsp.hit AS pc_hit
          ,mpsp.sku_id AS sku_id
        FROM (SELECT mps.*
              FROM (SELECT mrkt_perd_sku_prc.mrkt_id,mrkt_perd_sku_prc.offr_perd_id,mrkt_perd_sku_prc.sku_id,mrkt_perd_sku_prc.prc_lvl_typ_cd,mrkt_perd_sku_prc.crncy_cd,mrkt_perd_sku_prc.sku_prc_amt,mrkt_perd_sku_prc.cmpnt_discnt_pct
                      FROM mrkt_perd_sku_prc, mrkt_prc_lvl mpl
                     WHERE mrkt_perd_sku_prc.mrkt_id = l_mrkt_id
                       AND mrkt_perd_sku_prc.offr_perd_id = l_offr_perd_id
                       AND mrkt_perd_sku_prc.sku_id IN
                           (SELECT s.sku_id
                              FROM sku s, sku_reg_prc srp, mrkt_sku ms
                             WHERE ms.mrkt_id = l_mrkt_id
                               AND ms.sku_id = s.sku_id
                               AND nvl(ms.dltd_ind, 'N') != 'Y'
                               AND srp.mrkt_id = l_mrkt_id
                               AND srp.offr_perd_id = l_offr_perd_id
                               AND srp.sku_id = s.sku_id)
                          
                       AND mpl.mrkt_id(+) = mrkt_perd_sku_prc.mrkt_id
                       AND mpl.prc_lvl_typ_cd(+) =
                           mrkt_perd_sku_prc.prc_lvl_typ_cd
                       AND (mpl.strt_perd_id IS NULL OR
                           mpl.strt_perd_id <= l_offr_perd_id)
                       AND (mpl.end_perd_id IS NULL OR
                           mpl.end_perd_id >= l_offr_perd_id)
                       AND p_pagination = 'N'
                    )
            pivot(MAX(sku_prc_amt)
               FOR prc_lvl_typ_cd IN('RP' AS rp,
                                     'HIT' AS hit,
                                     'VSP' AS vsp,
                                     'LPY' AS lpy,
                                     'SP' AS sp)) mps) mpsp
     ) pricing
      ,mrkt_sku
      ,what_if_tran wit
 WHERE
--mrkt_tmp_fsc and master
     osl_current.sku_id = mrkt_tmp_fsc_master.sku_id(+)
 AND osl_current.sku_id = mrkt_tmp_fsc.sku_id(+)
 AND mrkt_tmp_fsc_master.mrkt_id(+) = osl_current.mrkt_id
 AND mrkt_tmp_fsc.mrkt_id(+) = osl_current.mrkt_id
--offr outer join on selected version
 AND o.offr_id = osl_current.offr_id(+)
 AND o.mrkt_id = l_mrkt_id
 AND o.offr_perd_id = l_offr_perd_id
 AND o.ver_id = l_ver_id
 AND osl_current.offr_sku_line_id = osl_latest.offr_sku_line_link_id(+)
--mrkt_veh_perd_sctn
 AND mrkt_veh_perd_sctn.mrkt_veh_perd_sctn_id(+) = o.mrkt_veh_perd_sctn_id
 AND mrkt_veh_perd_sctn.mrkt_id(+) = o.mrkt_id
 AND mrkt_veh_perd_sctn.offr_perd_id(+) = o.offr_perd_id
 AND mrkt_veh_perd_sctn.brchr_plcmt_id(+) = o.brchr_plcmt_id
 AND mrkt_veh_perd_sctn.ver_id(+) = o.ver_id
 AND mrkt_veh_perd_sctn.veh_id(+) = o.veh_id
--offr prfl prc point
 AND offr_prfl_prc_point.offr_prfl_prcpt_id(+) = osl_current.offr_prfl_prcpt_id
--offr_summary_log
 AND o.offr_id = os.offr_id (+)
--brnd
 AND brnd.brnd_id(+) = prfl.brnd_id
 AND brnd_grp.brnd_grp_id(+) = brnd.brnd_grp_id
--new prfl and form table
 AND prfl.prfl_cd(+) = offr_prfl_prc_point.prfl_cd
 AND prfl.form_id = form.form_id(+)

--offr sku set
 AND offr_sku_set.offr_sku_set_id(+) = osl_current.offr_sku_set_id

--offr lock
 AND offr_lock.offr_id(+) = o.offr_id

--offr sls cls sku
 AND offr_sls_cls_sku.offr_id(+) = osl_current.offr_id
 AND offr_sls_cls_sku.sls_cls_cd(+) = osl_current.sls_cls_cd
 AND offr_sls_cls_sku.prfl_cd(+) = osl_current.prfl_cd
 AND offr_sls_cls_sku.pg_ofs_nr(+) = osl_current.pg_ofs_nr
 AND offr_sls_cls_sku.featrd_side_cd(+) = osl_current.featrd_side_cd
 AND offr_sls_cls_sku.sku_id(+) = osl_current.sku_id

--offr_prfl_sls_cls_plcmt
 AND offr_prfl_sls_cls_plcmt.offr_id(+) = offr_prfl_prc_point.offr_id
 AND offr_prfl_sls_cls_plcmt.sls_cls_cd(+) = offr_prfl_prc_point.sls_cls_cd
 AND offr_prfl_sls_cls_plcmt.pg_ofs_nr(+) = offr_prfl_prc_point.pg_ofs_nr
 AND offr_prfl_sls_cls_plcmt.featrd_side_cd(+) =
 offr_prfl_prc_point.featrd_side_cd
 AND offr_prfl_sls_cls_plcmt.prfl_cd(+) = offr_prfl_prc_point.prfl_cd
--comsn_typ
 AND comsn_typ.comsn_typ(+) = offr_prfl_prc_point.comsn_typ
--tax_typ
 AND mrkt_tax_typ.mrkt_id(+) = offr_prfl_prc_point.mrkt_id
 AND mrkt_tax_typ.tax_type_id(+) = offr_prfl_prc_point.tax_type_id
--mvps
 AND mvps.mrkt_veh_perd_sctn_id(+) =
 decode(o.mrkt_veh_perd_sctn_id, NULL, 1, o.mrkt_veh_perd_sctn_id)
 AND mvps.veh_id = o.veh_id
--frcst
 AND frcst.offr_sku_line_id (+) = osl_current.offr_sku_line_id
 AND frcst.offr_prfl_prcpt_id (+) = osl_current.offr_prfl_prcpt_id
 --pricing
 AND pricing.sku_id(+) = osl_current.sku_id
 --mrkt_sku
 AND mrkt_sku.mrkt_id(+) = l_mrkt_id
 AND mrkt_sku.sku_id(+) = osl_current.sku_id
 --what_if_tran
 AND wit.offr_id(+) = o.offr_id
 AND wit.tran_typ(+) = 'WIF'
   )
     LOOP
--app_plsql_log.info(l_module_name||' osl '||rec.offr_sku_line_id||', scented page='||rec.scntd_pg_typ_id);
      PIPE ROW(obj_edit_offr_line(rec.status,
                                  rec.mrkt_id,
                                  rec.offr_perd_id,
                                  rec.offr_lock,
                                  rec.offr_lock_user,
                                  rec.offr_sku_line_id,
                                  rec.veh_id,
                                  rec.brchr_plcmnt_id,
                                  rec.brchr_sctn_nm,
                                  rec.enrgy_chrt_postn_id,
                                  rec.web_postn_id,
                                  rec.pg_nr,
                                  rec.ctgry_id,
                                  rec.brnd_id,
                                  rec.sgmt_id,
                                  rec.form_id,
                                  rec.form_grp_id,
                                  rec.prfl_cd,
                                  rec.sku_id,
                                  rec.fsc_cd,
                                  rec.prod_typ_id,
                                  rec.gender_id,
                                  rec.sls_cls_cd,
                                  rec.pp_sls_cls_cd,
                                  rec.item_sls_cls_cd,
                                  rec.offr_desc_txt,
                                  rec.offr_notes_txt,
                                  rec.offr_lyot_cmnts_txt,
                                  rec.featrd_side_cd,
                                  rec.concept_featrd_side_cd,
                                  rec.micr_ncpsltn_ind,
                                  rec.scntd_pg_typ_id,
                                  rec.cnsmr_invstmt_bdgt_id,
                                  rec.pymt_typ,
                                  rec.promtn_id,
                                  rec.promtn_clm_id,
                                  -- rec.cmbntn_offr_typ,
                                  rec.spndng_lvl,
                                  rec.comsn_typ,
                                  rec.tax_type_id,
                                  rec.wsl_ind,
                                  rec.offr_sku_set_id,
                                  rec.cmpnt_qty,
                                  rec.nr_for_qty,
                                  rec.nta_factor,
                                  rec.sku_cost,
                                  rec.lv_nta,
                                  rec.lv_sp,
                                  rec.lv_rp,
                                  rec.lv_discount,
                                  rec.lv_units,
                                  rec.lv_total_cost,
                                  rec.lv_gross_sales,
                                  rec.lv_dp_cash,
                                  rec.lv_dp_percent,
                                  rec.ver_id,
                                  rec.sls_prc_amt,
                                  rec.reg_prc_amt,
                                  rec.line_nr,
                                  rec.unit_qty,
                                  rec.dltd_ind,
                                  rec.created_ts,
                                  rec.created_user_id,
                                  rec.last_updt_ts,
                                  rec.last_updt_user_id,
                                  rec.intrnl_offr_id,
                                  rec.mrkt_veh_perd_sctn_id,
                                  rec.prfl_nm,
                                  rec.sku_nm,
                                  rec.comsn_typ_desc_txt,
                                  rec.tax_typ_desc_txt,
                                  rec.offr_sku_set_nm,
                                  rec.sls_typ,
                                  rec.pc_sp_py,
                                  rec.pc_rp,
                                  rec.pc_sp,
                                  rec.pc_vsp,
                                  rec.pc_hit,
                                  rec.pg_wght,
                                  rec.pp_pg_wght,
                                  rec.sprd_nr,
                                  rec.offr_prfl_prcpt_id,
                                  rec.has_unit_qty,
                                  rec.offr_typ,
                                  rec.forcasted_units,
                                  rec.forcasted_date,
                                  rec.offr_cls_id,
                                  rec.spcl_ordr_ind,
                                  rec.offr_ofs_nr,
                                  rec.pp_ofs_nr,
                                  rec.impct_catgry_id,
                                  rec.hero_ind,
                                  rec.smplg_ind,
                                  rec.mltpl_ind,
                                  rec.cmltv_ind,
                                  rec.use_instrctns_ind,
                                  rec.pg_typ_id,
                                  rec.featrd_prfl_ind,
                                  rec.fxd_pg_wght_ind,
                                  rec.prod_endrsmt_id,
                                  rec.frc_mtch_mthd_id,
                                  rec.wghtd_avg_cost_amt,
                                  rec.incntv_id,
                                  rec.intrdctn_perd_id,
                                  rec.on_stus_perd_id,
                                  rec.dspostn_perd_id,
                                  rec.scnrio_id,
                                  rec.micr_ncpsltn_desc_txt,
                                  rec.offr_link_id,
                                  rec.profile_item_count
                                  ));
    END LOOP;
    app_plsql_log.info(l_module_name || ' stop');
  EXCEPTION
    WHEN OTHERS THEN
      app_plsql_log.info('Error in get offr' || SQLERRM(SQLCODE));

  END get_offr;

  FUNCTION get_sls_typ_and_grp(p_mrkt_perd IN obj_edit_offr_mrkt_perd_table)
    RETURN obj_edit_offr_sls_typ_table
    PIPELINED AS
    ln_sls_typ_grp_id NUMBER;
    ln_max_sls_typ_id NUMBER;
  BEGIN
    FOR mp IN (SELECT * FROM TABLE(p_mrkt_perd)) LOOP
      --market period loop
      FOR veh IN (SELECT veh_id, mps_sls_typ_id
                    FROM mrkt_veh_perd
                   WHERE mrkt_id = mp.mrkt_id
                     AND offr_perd_id = mp.offr_perd_id) LOOP
        --veh_id loop
        BEGIN
          SELECT nvl(MAX(dms.sls_typ_id), veh.mps_sls_typ_id)
            INTO ln_max_sls_typ_id
            FROM dstrbtd_mrkt_sls dms
           WHERE dms.mrkt_id = mp.mrkt_id
             AND dms.veh_id = veh.veh_id
             AND dms.ver_id = mp.ver_id
             AND dms.offr_perd_id = mp.offr_perd_id
             AND dms.sls_perd_id = mp.offr_perd_id;

          SELECT sls_typ_grp_id
            INTO ln_sls_typ_grp_id
            FROM sls_typ
           WHERE sls_typ_id = ln_max_sls_typ_id;
        EXCEPTION
          WHEN no_data_found THEN
            ln_max_sls_typ_id := 1;
            ln_sls_typ_grp_id := 1;
          WHEN OTHERS THEN
            app_plsql_log.info('Error in get_sls_typ_grp function: ' ||
                               SQLERRM(SQLCODE));
        END;

        PIPE ROW(obj_edit_offr_sls_typ_line(ln_max_sls_typ_id,
                                            ln_sls_typ_grp_id,
                                            veh.veh_id,
                                            mp.mrkt_id,
                                            mp.offr_perd_id,
                                            mp.ver_id));

      END LOOP; --veh_id loop
    END LOOP; --market period loop

  END get_sls_typ_and_grp;

  FUNCTION parse_config_items(p_mrkt_id IN NUMBER,
                              p_config_item_id IN NUMBER) RETURN t_str_array IS

    co_pattern                CONSTANT VARCHAR2(5) := '[^,]+';

    l_str_array               t_str_array;
    l_conf_item_val_txt       mrkt_config_item.mrkt_config_item_val_txt%TYPE;
  BEGIN
    SELECT mci.mrkt_config_item_val_txt
      INTO l_conf_item_val_txt
      FROM mrkt_config_item mci
     WHERE mci.config_item_id = p_config_item_id
       AND mci.mrkt_id = p_mrkt_id;

    SELECT TRIM(regexp_substr(l_conf_item_val_txt, co_pattern, 1, LEVEL))
    BULK COLLECT INTO l_str_array
      FROM dual
    CONNECT BY regexp_substr(l_conf_item_val_txt, co_pattern, 1, LEVEL) IS NOT NULL;

    RETURN l_str_array;
  END parse_config_items;

  FUNCTION query_default_values(p_mrkt_id IN NUMBER) RETURN t_default_values IS
    l_default_arr            t_str_array;
    l_default_values         t_default_values;
  BEGIN
    l_default_arr := parse_config_items(p_mrkt_id, co_ci_defval_offr_id);

    l_default_values.pg_wght_pct      := to_number(l_default_arr(1));
    l_default_values.pg_typ_id        := to_number(l_default_arr(2));
    l_default_values.ssnl_evnt_id     := to_number(l_default_arr(3));
    l_default_values.sctn_page_ofs_nr := to_number(l_default_arr(4));
    l_default_values.featrd_side_cd   := to_number(l_default_arr(5));
    l_default_values.offr_stus_cd     := to_number(l_default_arr(6));
    l_default_values.offr_cls_id      := to_number(l_default_arr(7));
    l_default_values.offr_typ         := l_default_arr(8);
    l_default_values.flap_ind         := l_default_arr(9);
    l_default_values.brchr_postn_id   := to_number(l_default_arr(10));
    l_default_values.unit_rptg_lvl_id := to_number(l_default_arr(11));
    l_default_values.rpt_sbtl_typ_id  := to_number(l_default_arr(12));

    l_default_arr.DELETE;
    l_default_arr := parse_config_items(p_mrkt_id, co_ci_defval_prcpt_id);

    l_default_values.promtn_clm_id          := to_number(l_default_arr(1));
    l_default_values.promtn_id              := to_number(l_default_arr(2));
    l_default_values.nr_for_qty             := to_number(l_default_arr(3));
    l_default_values.unit_qty               := to_number(l_default_arr(4));
    l_default_values.sls_prc_amt            := to_number(l_default_arr(5));
    l_default_values.wghtd_avg_cost_amt     := to_number(l_default_arr(6));
    l_default_values.sls_srce_id            := to_number(l_default_arr(7));
    l_default_values.tax_type_id            := to_number(l_default_arr(8));
    l_default_values.pymt_typ               := l_default_arr(9);
    l_default_values.comsn_typ              := l_default_arr(10);
    l_default_values.prmry_offr_ind         := l_default_arr(11);
    l_default_values.pg_ofs_nr              := to_number(l_default_arr(12));
    l_default_values.concept_featrd_side_cd := l_default_arr(13);
    l_default_values.chrty_amt              := to_number(l_default_arr(14));
    l_default_values.awrd_sls_prc_amt       := to_number(l_default_arr(15));
    l_default_values.prod_endrsmt_id        := to_number(l_default_arr(16));

    RETURN l_default_values;

  END query_default_values;

    FUNCTION get_offr_table(p_offr_id    IN NUMBER,
                          p_sls_typ_id IN NUMBER DEFAULT 1,
                          p_pagination IN CHAR DEFAULT 'N') RETURN obj_edit_offr_table IS

    l_offr_table             obj_get_offr_table := obj_get_offr_table();
    l_edit_offr_table        obj_edit_offr_table;
  BEGIN
    l_offr_table.extend;
    l_offr_table(l_offr_table.last) := obj_get_offr_line(p_offr_id, nvl(p_sls_typ_id,1));

    SELECT obj_edit_offr_line(
                status, mrkt_id, offr_perd_id, offr_lock, offr_lock_user, offr_sku_line_id, veh_id, brchr_plcmnt_id, brchr_sctn_nm,
                enrgy_chrt_postn_id, web_postn_id, pg_nr, ctgry_id, brnd_id, sgmt_id, form_id, form_grp_id, prfl_cd, sku_id, fsc_cd,
                prod_typ_id, gender_id, sls_cls_cd, pp_sls_cls_cd, item_sls_cls_cd, offr_desc_txt, offr_notes_txt, offr_lyot_cmnts_txt, featrd_side_cd,
                concept_featrd_side_cd, micr_ncpsltn_ind, scntd_pg_typ_id, cnsmr_invstmt_bdgt_id, pymt_typ, promtn_id, promtn_clm_id, spndng_lvl,
                comsn_typ, tax_type_id, wsl_ind, offr_sku_set_id, cmpnt_qty, nr_for_qty, nta_factor, sku_cost, lv_nta, lv_sp, lv_rp,
                lv_discount, lv_units, lv_total_cost, lv_gross_sales, lv_dp_cash, lv_dp_percent, ver_id, sls_prc_amt, reg_prc_amt, line_nr,
                unit_qty, dltd_ind, created_ts, created_user_id, last_updt_ts, last_updt_user_id, intrnl_offr_id, mrkt_veh_perd_sctn_id,
                prfl_nm, sku_nm, comsn_typ_desc_txt, tax_typ_desc_txt, offr_sku_set_nm, sls_typ, pc_sp_py, pc_rp, pc_sp, pc_vsp, pc_hit,
                pg_wght, pp_pg_wght, sprd_nr, offr_prfl_prcpt_id, has_unit_qty, offr_typ, forcasted_units, forcasted_date, offr_cls_id, spcl_ordr_ind,
                offr_ofs_nr, pp_ofs_nr, impct_catgry_id, hero_ind, smplg_ind, mltpl_ind, cmltv_ind, use_instrctns_ind, pg_typ_id, featrd_prfl_ind,
                fxd_pg_wght_ind, prod_endrsmt_id, frc_mtch_mthd_id, wghtd_avg_cost_amt, incntv_id, intrdctn_perd_id, on_stus_perd_id, dspostn_perd_id,
                scnrio_id,micr_ncpsltn_desc_txt,offr_link_id, profile_item_count)
      BULK COLLECT
      INTO l_edit_offr_table
      FROM TABLE(get_offr(l_offr_table, p_pagination));

      RETURN l_edit_offr_table;
  END get_offr_table;

  FUNCTION get_offr_table(p_get_offr_table IN obj_get_offr_table,
                          p_pagination IN CHAR DEFAULT 'N')
    RETURN obj_edit_offr_table IS

    l_edit_offr_table        obj_edit_offr_table;
  BEGIN

    SELECT obj_edit_offr_line(
                status, mrkt_id, offr_perd_id, offr_lock, offr_lock_user, offr_sku_line_id, veh_id, brchr_plcmnt_id, brchr_sctn_nm,
                enrgy_chrt_postn_id, web_postn_id, pg_nr, ctgry_id, brnd_id, sgmt_id, form_id, form_grp_id, prfl_cd, sku_id, fsc_cd,
                prod_typ_id, gender_id, sls_cls_cd, pp_sls_cls_cd, item_sls_cls_cd, offr_desc_txt, offr_notes_txt, offr_lyot_cmnts_txt, featrd_side_cd,
                concept_featrd_side_cd, micr_ncpsltn_ind, scntd_pg_typ_id, cnsmr_invstmt_bdgt_id, pymt_typ, promtn_id, promtn_clm_id, spndng_lvl,
                comsn_typ, tax_type_id, wsl_ind, offr_sku_set_id, cmpnt_qty, nr_for_qty, nta_factor, sku_cost, lv_nta, lv_sp, lv_rp,
                lv_discount, lv_units, lv_total_cost, lv_gross_sales, lv_dp_cash, lv_dp_percent, ver_id, sls_prc_amt, reg_prc_amt, line_nr,
                unit_qty, dltd_ind, created_ts, created_user_id, last_updt_ts, last_updt_user_id, intrnl_offr_id, mrkt_veh_perd_sctn_id,
                prfl_nm, sku_nm, comsn_typ_desc_txt, tax_typ_desc_txt, offr_sku_set_nm, sls_typ, pc_sp_py, pc_rp, pc_sp, pc_vsp, pc_hit,
                pg_wght, pp_pg_wght, sprd_nr, offr_prfl_prcpt_id, has_unit_qty, offr_typ, forcasted_units, forcasted_date, offr_cls_id, spcl_ordr_ind,
                offr_ofs_nr, pp_ofs_nr, impct_catgry_id, hero_ind, smplg_ind, mltpl_ind, cmltv_ind, use_instrctns_ind, pg_typ_id, featrd_prfl_ind,
                fxd_pg_wght_ind, prod_endrsmt_id, frc_mtch_mthd_id, wghtd_avg_cost_amt, incntv_id, intrdctn_perd_id, on_stus_perd_id, dspostn_perd_id,
                scnrio_id, micr_ncpsltn_desc_txt, offr_link_id, profile_item_count )
      BULK COLLECT
      INTO l_edit_offr_table
      FROM TABLE(get_offr(p_get_offr_table, p_pagination));

      RETURN l_edit_offr_table;
  END get_offr_table;






  PROCEDURE add_concept(p_offr_id               IN NUMBER,
                           p_mrkt_id          IN     NUMBER,
                           p_offr_perd_id     IN     NUMBER,
                           p_veh_id           IN     NUMBER,
                           p_featrd_side_cd   IN     VARCHAR2,
                           p_prfl_cd          IN     NUMBER,
                           p_default_values   IN     t_default_values,
                           p_user_nm          IN     VARCHAR2,
                        p_status               OUT NUMBER) IS

    l_procedure_name         VARCHAR2(50) := 'ADD_CONCEPT';
    l_location               VARCHAR2(1000);

        l_default_values          t_default_values;

        l_found                   NUMBER := 0;
        l_offr_prfl_prcpt_id      NUMBER;
        l_offr_sku_line_id        NUMBER;
    l_crncy_cd               VARCHAR2(5);
        l_tax_pct                 NUMBER;
        l_comsn_pct               NUMBER;
        l_gta_mthd_id             NUMBER;
        l_net_to_avon_fct         NUMBER;
        l_sls_prc_amt             offr_prfl_prc_point.sls_prc_amt%TYPE;
        l_promtn_desc_txt         promtn.promtn_desc_txt%TYPE;
        l_promtn_clm_desc_txt     promtn_clm.promtn_clm_desc_txt%TYPE;
        l_cnsmr_invstmt_bdgt_id   offr_prfl_prc_point.cnsmr_invstmt_bdgt_id%TYPE;
        l_pymt_typ                offr_prfl_prc_point.pymt_typ%TYPE;
        l_comsn_typ               offr_prfl_prc_point.comsn_typ%TYPE;
        l_tax_type_id             offr_prfl_prc_point.tax_type_id%TYPE;
        l_micr_ncpsltn_ind        offr_sls_cls_sku.micr_ncpsltn_ind%TYPE := 'N';
        l_wsl_ind                 offr_sls_cls_sku.wsl_ind%TYPE := 'N';

    BEGIN
        l_default_values := p_default_values;

        l_cnsmr_invstmt_bdgt_id := NULL;
        l_pymt_typ := p_default_values.pymt_typ;
        l_comsn_typ := p_default_values.comsn_typ;
        l_tax_type_id := p_default_values.tax_type_id;

    FOR sku_rec IN (
      SELECT rownum,
             pa_maps_public.get_sls_cls_cd(p_offr_perd_id, p_mrkt_id, s.sku_id) sls_cls_cd,
                       s.sku_id,
                       srp.reg_prc_amt
        FROM sku s,
             sku_reg_prc srp
                 WHERE     srp.sku_id = s.sku_id
                       AND srp.mrkt_id = p_mrkt_id
                       AND srp.offr_perd_id = p_offr_perd_id
                       AND s.prfl_cd = p_prfl_cd
                       AND s.dltd_ind = '0'
         AND pa_maps_public.get_sls_cls_cd(p_offr_perd_id, p_mrkt_id, s.sku_id) <> '-1'
    )
        LOOP
            BEGIN
                l_location := 'sales class placement check';
        SELECT 1 INTO l_found
                  FROM offr_prfl_sls_cls_plcmt
                 WHERE     offr_id = p_offr_id
                       AND sls_cls_cd = sku_rec.sls_cls_cd
                       AND prfl_cd = p_prfl_cd
                       AND pg_ofs_nr = l_default_values.pg_ofs_nr
                       AND featrd_side_cd = l_default_values.featrd_side_cd;
            EXCEPTION
        WHEN no_data_found THEN

                    l_location := 'create sales class placement';
        INSERT INTO offr_prfl_sls_cls_plcmt
          ( offr_id, sls_cls_cd, prfl_cd, pg_ofs_nr, featrd_side_cd, mrkt_id, veh_id,
            offr_perd_id, sku_cnt, pg_wght_pct, prod_endrsmt_id, pg_typ_id, creat_user_id)
        VALUES
          ( p_offr_id, sku_rec.sls_cls_cd, p_prfl_cd, l_default_values.pg_ofs_nr, l_default_values.featrd_side_cd, p_mrkt_id, p_veh_id,
            p_offr_perd_id, 0, l_default_values.pg_wght_pct, l_default_values.prod_endrsmt_id,
            l_default_values.pg_typ_id, p_user_nm);

            END;

            BEGIN
                -- calculate commission and tax
                l_location := 'calculate tax';
                BEGIN
          l_tax_type_id := pa_maps_gta.get_default_tax_type_id(p_mrkt_id, p_prfl_cd, sku_rec.sls_cls_cd, p_offr_perd_id, p_veh_id);
          l_tax_pct := get_tax_rate(p_mrkt_id, l_tax_type_id, p_offr_perd_id);
                EXCEPTION
          WHEN OTHERS THEN
                        l_tax_pct := 0;
                END;

                l_location := 'calculate commission';
                BEGIN
          l_comsn_typ := pa_maps_gta.get_commission_type(p_mrkt_id, p_veh_id, p_offr_perd_id, p_prfl_cd, 'N',
                             NULL, NULL, NULL, NULL, NULL);
          l_comsn_pct := get_comsn_pct(p_mrkt_id, p_offr_perd_id, l_comsn_typ);
                EXCEPTION
          WHEN OTHERS THEN
                        l_comsn_pct := 0;
                END;

                l_location := 'promotion check';
                BEGIN
                    SELECT p.promtn_desc_txt
                      INTO l_promtn_desc_txt
                      FROM promtn p
                     WHERE p.promtn_id = l_default_values.promtn_id;
                EXCEPTION
          WHEN no_data_found THEN
                        NULL;
                END;

                BEGIN
                    SELECT p.promtn_clm_desc_txt
                      INTO l_promtn_clm_desc_txt
                      FROM promtn_clm p
                     WHERE p.promtn_clm_id = l_default_values.promtn_clm_id;
                EXCEPTION
          WHEN no_data_found THEN
                        NULL;
                END;

                    l_sls_prc_amt := sku_rec.reg_prc_amt;

                l_location := 'price point check';
                SELECT offr_prfl_prcpt_id, crncy_cd, net_to_avon_fct
                  INTO l_offr_prfl_prcpt_id, l_crncy_cd, l_net_to_avon_fct
                  FROM offr_prfl_prc_point opp
                 WHERE     opp.offr_id = p_offr_id
                       AND opp.prfl_cd = p_prfl_cd
                       AND opp.sls_cls_cd = sku_rec.sls_cls_cd
                       AND opp.sls_prc_amt = l_sls_prc_amt
                       AND opp.nr_for_qty = l_default_values.nr_for_qty
                       AND opp.pg_ofs_nr = l_default_values.pg_ofs_nr
                       AND opp.pymt_typ = l_pymt_typ
                       AND opp.comsn_typ = l_comsn_typ
                       AND opp.tax_type_id = l_tax_type_id
                       AND opp.promtn_id = l_default_values.promtn_id
                       AND opp.promtn_clm_id = l_default_values.promtn_clm_id
           AND opp.featrd_side_cd = l_default_values.featrd_side_cd;

        IF sku_rec.rownum = 1 THEN
                    RAISE e_prcpnt_already_exists;
                END IF;

            EXCEPTION
        WHEN no_data_found THEN

                    -- read market-specific data for price point
                    l_location := 'get currency';
          SELECT crncy_cd INTO l_crncy_cd
                      FROM mrkt_perd
          WHERE  mrkt_id = p_mrkt_id AND
                 perd_id = p_offr_perd_id;

                    -- calculate GTA
                    l_location := 'calculate GTA';
                    BEGIN
            SELECT nvl(gta_mthd_id, 1) INTO l_gta_mthd_id
                          FROM mrkt_perd
            WHERE  mrkt_id = p_mrkt_id AND
                   perd_id = p_offr_perd_id;

            l_net_to_avon_fct := pa_maps_gta.get_gta_without_price_point(l_gta_mthd_id, l_sls_prc_amt, l_default_values.chrty_amt,
                                                                         l_default_values.awrd_sls_prc_amt, l_comsn_pct, l_tax_pct, 0);
                    EXCEPTION
            WHEN OTHERS THEN
                            l_net_to_avon_fct := 0;
                    END;

          SELECT seq.NEXTVAL INTO l_offr_prfl_prcpt_id FROM dual;

                    l_location := 'create price point';
          INSERT INTO offr_prfl_prc_point
            ( offr_prfl_prcpt_id, offr_id, promtn_clm_id, veh_id, promtn_id, mrkt_id, cnsmr_invstmt_bdgt_id,
              sls_cls_cd, prfl_cd, ssnl_evnt_id, offr_perd_id, crncy_cd, sku_cnt, nr_for_qty,
              est_unit_qty, est_sls_amt, est_cost_amt, sls_srce_id, sls_prc_amt,
              tax_amt, pymt_typ, comsn_amt, comsn_typ, net_to_avon_fct, prmry_offr_ind,
              pg_ofs_nr, featrd_side_cd, chrty_amt, awrd_sls_prc_amt, tax_type_id, creat_user_id)
          VALUES
            ( l_offr_prfl_prcpt_id, p_offr_id, l_default_values.promtn_clm_id, p_veh_id, l_default_values.promtn_id, p_mrkt_id, l_cnsmr_invstmt_bdgt_id,
              sku_rec.sls_cls_cd, p_prfl_cd, l_default_values.ssnl_evnt_id, p_offr_perd_id, l_crncy_cd, 0, l_default_values.nr_for_qty,
              l_default_values.unit_qty, l_sls_prc_amt, l_default_values.wghtd_avg_cost_amt, l_default_values.sls_srce_id, l_sls_prc_amt,
              l_tax_pct, l_pymt_typ, l_comsn_pct, l_comsn_typ, l_net_to_avon_fct, l_default_values.prmry_offr_ind,
              l_default_values.pg_ofs_nr, l_default_values.featrd_side_cd, l_default_values.chrty_amt, l_default_values.awrd_sls_prc_amt, l_tax_type_id, p_user_nm);

                    -- new profile so increment profile counter for Offer
                    UPDATE offr
          SET    prfl_cnt = nvl(prfl_cnt, 0) + 1,
                           last_updt_user_id = p_user_nm
                     WHERE offr_id = p_offr_id;

        WHEN e_prcpnt_already_exists THEN
                    RAISE;
            END;

            BEGIN
                l_location := 'sales class sku check';
        SELECT 1 INTO l_found
                  FROM offr_sls_cls_sku
                 WHERE     offr_id = p_offr_id
                       AND sls_cls_cd = sku_rec.sls_cls_cd
                       AND prfl_cd = p_prfl_cd
                       AND pg_ofs_nr = l_default_values.pg_ofs_nr
                       AND featrd_side_cd = l_default_values.featrd_side_cd
                       AND sku_id = sku_rec.sku_id;
            EXCEPTION
        WHEN no_data_found THEN
                    l_location := 'create sales class sku';
          INSERT INTO offr_sls_cls_sku
            ( offr_id, sls_cls_cd, prfl_cd, pg_ofs_nr, featrd_side_cd, sku_id, mrkt_id,
              smplg_ind, hero_ind, micr_ncpsltn_ind, wsl_ind, reg_prc_amt, cost_amt, creat_user_id)
          VALUES
            ( p_offr_id, sku_rec.sls_cls_cd, p_prfl_cd, l_default_values.pg_ofs_nr, l_default_values.featrd_side_cd,
              sku_rec.sku_id, p_mrkt_id, 'N', 'N', l_micr_ncpsltn_ind, l_wsl_ind, sku_rec.reg_prc_amt, l_default_values.wghtd_avg_cost_amt, p_user_nm);

            END;

      SELECT seq.NEXTVAL INTO l_offr_sku_line_id FROM dual;

            l_location := 'create OSL';
      INSERT INTO offr_sku_line
        (offr_sku_line_id, offr_id, veh_id, featrd_side_cd, offr_perd_id, mrkt_id, sku_id,
         pg_ofs_nr, prfl_cd, crncy_cd, prmry_sku_offr_ind, sls_cls_cd, offr_prfl_prcpt_id,
         demo_avlbl_ind, dltd_ind, unit_splt_pct, sls_prc_amt, cost_typ, creat_user_id)
      VALUES
        (l_offr_sku_line_id, p_offr_id, p_veh_id, l_default_values.featrd_side_cd, p_offr_perd_id, p_mrkt_id,
         sku_rec.sku_id, l_default_values.pg_ofs_nr, p_prfl_cd, l_crncy_cd, 'N', sku_rec.sls_cls_cd,
         l_offr_prfl_prcpt_id, 'N', 'N', 0, l_sls_prc_amt, 'P', p_user_nm);

            -- also need to create DMS record(s) based on MVPV Sales Type derived during campaign validation
      IF g_sls_typ_id in (co_sls_typ_estimate, co_sls_typ_op_estimate) THEN
                -- create estimate DMS record
                l_location := 'create DMS for estimate';
        INSERT INTO dstrbtd_mrkt_sls
          (mrkt_id, sls_perd_id, offr_sku_line_id, sls_typ_id, sls_srce_id, offr_perd_id,
           veh_id, unit_qty, comsn_amt, tax_amt, net_to_avon_fct, cost_amt, creat_user_id)
        VALUES
          (p_mrkt_id, p_offr_perd_id, l_offr_sku_line_id, co_sls_typ_estimate, l_default_values.sls_srce_id, p_offr_perd_id,
           p_veh_id, l_default_values.unit_qty, l_comsn_pct, l_tax_pct, l_net_to_avon_fct, l_default_values.wghtd_avg_cost_amt, p_user_nm);
            END IF;

      IF g_sls_typ_id in (co_sls_typ_op_estimate) THEN
                -- create operational estimate DMS record
                l_location := 'create DMS for operational estimate';
        INSERT INTO dstrbtd_mrkt_sls
          (mrkt_id, sls_perd_id, offr_sku_line_id, sls_typ_id, sls_srce_id, offr_perd_id,
           veh_id, unit_qty, comsn_amt, tax_amt, net_to_avon_fct, cost_amt, creat_user_id)
        VALUES
          (p_mrkt_id, p_offr_perd_id, l_offr_sku_line_id, co_sls_typ_op_estimate, l_default_values.sls_srce_id, p_offr_perd_id,
           p_veh_id, l_default_values.unit_qty, l_comsn_pct, l_tax_pct, l_net_to_avon_fct, l_default_values.wghtd_avg_cost_amt, p_user_nm);
            END IF;

            -- new sku added so increment sku counters for OFFR, OPSCP and OPP
            UPDATE offr
      SET    sku_cnt = nvl(sku_cnt, 0) + 1,
                   last_updt_user_id = p_user_nm
             WHERE offr_id = p_offr_id;

            UPDATE offr_prfl_sls_cls_plcmt
      SET    sku_cnt = nvl(sku_cnt, 0) + 1,
                   last_updt_user_id = p_user_nm
      WHERE  offr_id        = p_offr_id and
             sls_cls_cd     = sku_rec.sls_cls_cd and
             prfl_cd        = p_prfl_cd and
             pg_ofs_nr      = 0 and
             featrd_side_cd = p_featrd_side_cd;

            UPDATE offr_prfl_prc_point
      SET    sku_cnt = nvl(sku_cnt, 0) + 1,
                   last_updt_user_id = p_user_nm
             WHERE offr_prfl_prcpt_id = l_offr_prfl_prcpt_id;

        END LOOP;                                                   -- sku_rec

        p_status := co_exec_status_success;

    EXCEPTION
    WHEN e_prcpnt_already_exists THEN
            p_status := co_exec_status_prcpnt_ex;

    WHEN OTHERS THEN
      app_plsql_log.info(l_procedure_name || ': Error adding concept at ' || l_location);
      app_plsql_log.info(l_procedure_name || ': ' || SQLERRM(SQLCODE));
            p_status := co_exec_status_failed;
            RAISE;

    END add_concept;
  
  PROCEDURE add_pricepoint(p_offr_id               IN NUMBER,
        p_mrkt_id          IN     NUMBER,
        p_offr_perd_id     IN     NUMBER,
        p_veh_id           IN     NUMBER,
        p_pp_rec           IN     offr_prfl_prc_point%ROWTYPE,
        p_default_values   IN     t_default_values,
        p_user_nm          IN     VARCHAR2,
                           p_status               OUT NUMBER) IS

    l_procedure_name         VARCHAR2(50) := 'ADD_PRICEPOINT';
    l_location               VARCHAR2(1000);

        l_default_values       t_default_values;

        l_found                NUMBER := 0;
        l_offr_prfl_prcpt_id   offr_prfl_prc_point.offr_prfl_prcpt_id%TYPE;
        l_tax_amt              offr_prfl_prc_point.tax_amt%TYPE;
        l_comsn_amt            offr_prfl_prc_point.comsn_amt%TYPE;
        l_net_to_avon_fct      offr_prfl_prc_point.net_to_avon_fct%TYPE;
        l_offr_sku_line_id     offr_sku_line.offr_sku_line_id%TYPE;
        l_wsl_ind              offr_sls_cls_sku.wsl_ind%TYPE := 'N';
    BEGIN
        l_default_values := p_default_values;

        l_location := 'price point check';
    SELECT COUNT(*) INTO l_found
          FROM offr_prfl_prc_point opp
         WHERE     opp.offr_id = p_offr_id
               AND opp.prfl_cd = p_pp_rec.prfl_cd
               AND opp.sls_cls_cd = p_pp_rec.sls_cls_cd
               AND opp.sls_prc_amt = p_pp_rec.sls_prc_amt
               AND opp.nr_for_qty = l_default_values.nr_for_qty
               AND opp.pg_ofs_nr = l_default_values.pg_ofs_nr
               AND opp.pymt_typ = l_default_values.pymt_typ
               AND opp.comsn_typ = l_default_values.comsn_typ
               AND opp.tax_type_id = l_default_values.tax_type_id
               AND opp.promtn_id = l_default_values.promtn_id
               AND opp.promtn_clm_id = l_default_values.promtn_clm_id
               AND opp.featrd_side_cd = l_default_values.featrd_side_cd;

        IF l_found > 0 THEN
            RAISE e_prcpnt_already_exists;
        END IF;

        l_location := 'sales class placement check';
      BEGIN
        SELECT 1 INTO l_found
              FROM offr_prfl_sls_cls_plcmt
             WHERE     offr_id = p_offr_id
                   AND sls_cls_cd = p_pp_rec.sls_cls_cd
                   AND prfl_cd = p_pp_rec.prfl_cd
                   AND pg_ofs_nr = l_default_values.pg_ofs_nr
                   AND featrd_side_cd = l_default_values.featrd_side_cd;
        EXCEPTION
      WHEN no_data_found THEN

                l_location := 'create sales class placement';
      INSERT INTO offr_prfl_sls_cls_plcmt
        ( offr_id, sls_cls_cd, prfl_cd, pg_ofs_nr, featrd_side_cd, mrkt_id, veh_id,
          offr_perd_id, sku_cnt, pg_wght_pct, prod_endrsmt_id, pg_typ_id, creat_user_id)
      VALUES
        ( p_offr_id, p_pp_rec.sls_cls_cd, p_pp_rec.prfl_cd, l_default_values.pg_ofs_nr, l_default_values.featrd_side_cd,
          p_mrkt_id, p_veh_id, p_offr_perd_id, 0, l_default_values.pg_wght_pct, l_default_values.prod_endrsmt_id,
          l_default_values.pg_typ_id, p_user_nm);
        END;

        BEGIN
      l_tax_amt := get_tax_rate(p_mrkt_id, l_default_values.tax_type_id, p_offr_perd_id);
        EXCEPTION
      WHEN OTHERS THEN
                l_tax_amt := 0;
        END;

        BEGIN
      l_comsn_amt := get_comsn_pct(p_mrkt_id, p_offr_perd_id, l_default_values.comsn_typ);
        EXCEPTION
      WHEN OTHERS THEN
                l_comsn_amt := 0;
        END;

    l_net_to_avon_fct := pa_maps_gta.get_gta_without_price_point(pa_maps_gta.pri_get_gta_method_id(p_mrkt_id,
                                                                                                   p_offr_perd_id),
                                                                 p_pp_rec.sls_prc_amt, l_default_values.chrty_amt,
                                                                 l_default_values.awrd_sls_prc_amt, l_comsn_amt, l_tax_amt, 0);

    SELECT seq.NEXTVAL INTO l_offr_prfl_prcpt_id FROM dual;

        l_location := 'create price point';
    INSERT INTO offr_prfl_prc_point
      ( offr_prfl_prcpt_id, offr_id, promtn_clm_id, veh_id, promtn_id, mrkt_id, cnsmr_invstmt_bdgt_id,
        sls_cls_cd, prfl_cd, ssnl_evnt_id, offr_perd_id, crncy_cd, sku_cnt, nr_for_qty,
        est_unit_qty, est_sls_amt, est_cost_amt, sls_srce_id, sls_prc_amt,
        tax_amt, pymt_typ, comsn_amt, comsn_typ, net_to_avon_fct, prmry_offr_ind,
        pg_ofs_nr, featrd_side_cd, chrty_amt, awrd_sls_prc_amt, tax_type_id, creat_user_id)
    VALUES
      ( l_offr_prfl_prcpt_id, p_offr_id, l_default_values.promtn_clm_id, p_veh_id, l_default_values.promtn_id, p_mrkt_id, p_pp_rec.cnsmr_invstmt_bdgt_id,
        p_pp_rec.sls_cls_cd, p_pp_rec.prfl_cd, l_default_values.ssnl_evnt_id, p_offr_perd_id, p_pp_rec.crncy_cd, 0, l_default_values.nr_for_qty,
        l_default_values.unit_qty, p_pp_rec.est_sls_amt, l_default_values.wghtd_avg_cost_amt, l_default_values.sls_srce_id, 0,
        l_tax_amt, l_default_values.pymt_typ, l_comsn_amt, l_default_values.comsn_typ, l_net_to_avon_fct, l_default_values.prmry_offr_ind, l_default_values.pg_ofs_nr,
        l_default_values.featrd_side_cd, l_default_values.chrty_amt, l_default_values.awrd_sls_prc_amt, l_default_values.tax_type_id, p_user_nm);

        -- new profile so increment profile counter for Offer
        l_location := 'update offr prfl_cnt';
        UPDATE offr
    SET    prfl_cnt = nvl(prfl_cnt, 0) + 1,
               last_updt_user_id = p_user_nm
         WHERE offr_id = p_offr_id;

    FOR sku_rec IN (
      SELECT osl.*,
             srp.reg_prc_amt
        FROM offr_sku_line osl,
             sku_reg_prc srp
       WHERE srp.sku_id = osl.sku_id
         AND srp.mrkt_id = p_mrkt_id
         AND srp.offr_perd_id = p_offr_perd_id
         AND osl.offr_prfl_prcpt_id = p_pp_rec.offr_prfl_prcpt_id
    )
        LOOP
            BEGIN
                l_location := 'sales class sku check';
        SELECT 1 INTO l_found
                  FROM offr_sls_cls_sku
                 WHERE     offr_id = p_offr_id
                       AND sls_cls_cd = sku_rec.sls_cls_cd
                       AND prfl_cd = p_pp_rec.prfl_cd
                       AND pg_ofs_nr = l_default_values.featrd_side_cd
                       AND featrd_side_cd = l_default_values.featrd_side_cd
                       AND sku_id = sku_rec.sku_id;
            EXCEPTION
        WHEN no_data_found THEN
                    l_location := 'create sales class sku';
          INSERT INTO offr_sls_cls_sku
            ( offr_id, sls_cls_cd, prfl_cd, pg_ofs_nr, featrd_side_cd, sku_id, mrkt_id,
              smplg_ind, hero_ind, micr_ncpsltn_ind, scntd_pg_typ_id, wsl_ind, reg_prc_amt, cost_amt, creat_user_id)
          VALUES
            ( p_offr_id, sku_rec.sls_cls_cd, p_pp_rec.prfl_cd, 0, l_default_values.featrd_side_cd,
              sku_rec.sku_id, p_mrkt_id, 'N', 'N', 'N', NULL, l_wsl_ind, sku_rec.reg_prc_amt,
              l_default_values.wghtd_avg_cost_amt, p_user_nm);
            END;

      SELECT seq.NEXTVAL INTO l_offr_sku_line_id FROM dual;

            l_location := 'create OSL';
      INSERT INTO offr_sku_line
        (offr_sku_line_id, offr_id, veh_id, featrd_side_cd, offr_perd_id, mrkt_id, sku_id,
         pg_ofs_nr, prfl_cd, crncy_cd, prmry_sku_offr_ind, sls_cls_cd, offr_prfl_prcpt_id,
         demo_avlbl_ind, dltd_ind, unit_splt_pct, sls_prc_amt, cost_typ, creat_user_id)
      VALUES
        (l_offr_sku_line_id, p_offr_id, p_veh_id, l_default_values.featrd_side_cd, p_offr_perd_id, p_mrkt_id,
         sku_rec.sku_id, l_default_values.pg_ofs_nr, p_pp_rec.prfl_cd, sku_rec.crncy_cd, 'N', sku_rec.sls_cls_cd,
         l_offr_prfl_prcpt_id, 'N', 'N', 0, sku_rec.reg_prc_amt, 'P', p_user_nm);

      -- also need to create DMS record(s) based on MVPV Sales Type derived during campaign validation
      IF g_sls_typ_id in (co_sls_typ_estimate, co_sls_typ_op_estimate) THEN

                -- create estimate DMS record
                l_location := 'create DMS for estimate';
        INSERT INTO dstrbtd_mrkt_sls
          (mrkt_id, sls_perd_id, offr_sku_line_id, sls_typ_id, sls_srce_id, offr_perd_id,
           veh_id, unit_qty, comsn_amt, tax_amt, net_to_avon_fct, cost_amt, creat_user_id)
        VALUES
          (p_mrkt_id, p_offr_perd_id, l_offr_sku_line_id, co_sls_typ_estimate, l_default_values.sls_srce_id, p_offr_perd_id,
           p_veh_id, l_default_values.unit_qty, l_comsn_amt, l_tax_amt, l_net_to_avon_fct, l_default_values.wghtd_avg_cost_amt, p_user_nm);
      END IF;

      IF g_sls_typ_id in (co_sls_typ_op_estimate) THEN

                -- create operational estimate DMS record
                l_location := 'create DMS for operational estimate';
        INSERT INTO dstrbtd_mrkt_sls
          (mrkt_id, sls_perd_id, offr_sku_line_id, sls_typ_id, sls_srce_id, offr_perd_id,
           veh_id, unit_qty, comsn_amt, tax_amt, net_to_avon_fct, cost_amt, creat_user_id)
        VALUES
          (p_mrkt_id, p_offr_perd_id, l_offr_sku_line_id, co_sls_typ_op_estimate, l_default_values.sls_srce_id, p_offr_perd_id,
           p_veh_id, l_default_values.unit_qty, l_comsn_amt, l_tax_amt, l_net_to_avon_fct, l_default_values.wghtd_avg_cost_amt, p_user_nm);
            END IF;

            -- new sku added so increment sku counters for OFFR, OPSCP and OPP
            UPDATE offr
         SET sku_cnt = nvl(sku_cnt, 0) + 1,
                   last_updt_user_id = p_user_nm
             WHERE offr_id = p_offr_id;

            UPDATE offr_prfl_sls_cls_plcmt
         SET sku_cnt = nvl(sku_cnt, 0) + 1,
                   last_updt_user_id = p_user_nm
             WHERE     offr_id = p_offr_id
                   AND sls_cls_cd = sku_rec.sls_cls_cd
                   AND prfl_cd = p_pp_rec.prfl_cd
                   AND pg_ofs_nr = 0
                   AND featrd_side_cd = p_pp_rec.featrd_side_cd;

            UPDATE offr_prfl_prc_point
         SET sku_cnt            = nvl(sku_cnt, 0) + 1,
             sls_prc_amt        = sku_rec.reg_prc_amt,
                   last_updt_user_id = p_user_nm
             WHERE offr_prfl_prcpt_id = l_offr_prfl_prcpt_id;

        END LOOP;

        p_status := co_exec_status_success;

    EXCEPTION
    WHEN e_prcpnt_already_exists THEN
            p_status := co_exec_status_prcpnt_ex;

    WHEN OTHERS THEN
      app_plsql_log.info(l_procedure_name || ': Error adding pricepoint at ' || l_location);
      app_plsql_log.info(l_procedure_name || ': ' || SQLERRM(SQLCODE));
            p_status := co_exec_status_failed;
            RAISE;

    END add_pricepoint;
    
  PROCEDURE add_offer(p_mrkt_id                IN NUMBER,
                      p_offr_perd_id           IN NUMBER,
                      p_veh_id                 IN NUMBER,
                      p_offr_desc_txt          IN VARCHAR2,
                      p_mrkt_veh_perd_sctn_id  IN NUMBER,
                      p_sctn_page_ofs_nr       IN NUMBER,
                      p_featrd_side_cd         IN VARCHAR2,
                      p_pg_wght                IN NUMBER,
                      p_offr_typ               IN VARCHAR2,
                      p_scnrio_id              IN NUMBER,
                      p_scnrio_nm              IN VARCHAR2,
                      p_prfl_cd_list           IN number_array,
                      p_user_nm                IN VARCHAR2,
                      p_clstr_id               IN NUMBER,
                      p_status                OUT NUMBER,
                      p_edit_offr_table       OUT obj_edit_offr_table,
                      p_pagination             IN CHAR DEFAULT 'N') IS

    l_procedure_name         VARCHAR2(50) := 'ADD_OFFER';
    l_location               VARCHAR2(1000);

    l_default_values         t_default_values;

    l_get_offr_table         obj_get_offr_table := obj_get_offr_table();
    l_lock_user_nm           VARCHAR2(35);
    l_offr_id                NUMBER;
    l_lock_status            NUMBER;
    l_offr_cls_id            NUMBER;
    l_brchr_plcmt_id         offr.brchr_plcmt_id%TYPE;
    l_scnrio_id              what_if_scnrio.scnrio_id%TYPE := p_scnrio_id;

  BEGIN
    g_run_id  := app_plsql_output.generate_new_run_id;
    g_user_id := RTRIM(sys_context('USERENV', 'OS_USER'), 35);

    app_plsql_output.set_run_id(g_run_id);
    app_plsql_log.set_context(g_user_id, g_package_name, g_run_id);
    app_plsql_log.info(l_procedure_name || ' start');

    p_status := co_exec_status_success;

    l_location := 'initializing default values';
    l_default_values := query_default_values(p_mrkt_id);

    l_location := 'Query mrkt_veh_perd_sctn';
    -- Get page offset number
    BEGIN
      SELECT mvps.brchr_plcmt_id
        INTO l_brchr_plcmt_id
        FROM mrkt_veh_perd_sctn mvps
       WHERE mvps.mrkt_veh_perd_sctn_id = p_mrkt_veh_perd_sctn_id
         AND mrkt_id                    = p_mrkt_id
         AND offr_perd_id               = p_offr_perd_id
         AND veh_id                     = p_veh_id
         AND ver_id                     = 0;
    EXCEPTION
      WHEN no_data_found THEN
        l_brchr_plcmt_id := NULL;
    END;

    l_location := 'Getting default offer class id';
    BEGIN
      SELECT mv.dfalt_offr_cls_id
        INTO l_offr_cls_id
        FROM mrkt_veh mv
       WHERE mv.mrkt_id = p_mrkt_id
         AND mv.veh_id = p_veh_id;

      IF l_offr_cls_id IS NULL THEN
        l_offr_cls_id := l_default_values.offr_cls_id;
      END IF;

    EXCEPTION
      WHEN no_data_found THEN
        l_offr_cls_id := l_default_values.offr_cls_id;
    END;

    SELECT seq.NEXTVAL INTO l_offr_id FROM dual;

    l_location := 'insert offr';
    INSERT INTO offr
          ( offr_id, mrkt_id, offr_perd_id, veh_id, ver_id, pg_wght_pct, ssnl_evnt_id,
            offr_desc_txt, mrkt_veh_perd_sctn_id, offr_typ, brchr_plcmt_id, sctn_page_ofs_nr,
            prfl_cnt, sku_cnt, featrd_side_cd, flap_ind, offr_stus_cd, bilng_perd_id, shpng_perd_id,
            brchr_postn_id, unit_rptg_lvl_id, rpt_sbtl_typ_id, pg_typ_id, offr_cls_id, creat_user_id, spcl_ordr_ind)
        VALUES
          ( l_offr_id, p_mrkt_id, p_offr_perd_id, p_veh_id, 0, p_pg_wght, l_default_values.ssnl_evnt_id,
            p_offr_desc_txt, p_mrkt_veh_perd_sctn_id, NVL(p_offr_typ, l_default_values.offr_typ), l_brchr_plcmt_id,
            p_sctn_page_ofs_nr, 0, 0, p_featrd_side_cd, l_default_values.flap_ind, l_default_values.offr_stus_cd,
            p_offr_perd_id, p_offr_perd_id, l_default_values.brchr_postn_id, l_default_values.unit_rptg_lvl_id,
            l_default_values.rpt_sbtl_typ_id, l_default_values.pg_typ_id, l_offr_cls_id, p_user_nm, 'N');

    IF p_prfl_cd_list IS NOT NULL AND p_prfl_cd_list.COUNT > 0 THEN
      l_location := 'Calling add_concept';
      FOR i IN p_prfl_cd_list.FIRST .. p_prfl_cd_list.LAST LOOP
        add_concept(l_offr_id,
                    p_mrkt_id,
                    p_offr_perd_id,
                    p_veh_id,
                    p_featrd_side_cd,
                    p_prfl_cd_list(i),
                    l_default_values,
                    p_user_nm,
                    p_status);
      END LOOP;
    END IF; -- p_prfl_cd_list IS NOT NULL AND p_prfl_cd_list.COUNT > 0

    IF p_offr_typ = 'WIF' THEN
      IF l_scnrio_id IS NULL THEN
        add_scenario(p_mrkt_id         => p_mrkt_id,
                     p_veh_id          => p_veh_id,
                     p_scnrio_desc_txt => p_scnrio_nm,
                     p_strt_perd_id    => p_offr_perd_id,
                     p_end_perd_id     => p_offr_perd_id,
                     p_user_nm         => p_user_nm,
                     p_scnrio_id       => l_scnrio_id);
      END IF;
      add_offr_to_scenario(p_mrkt_id   => p_mrkt_id,
                           p_veh_id    => p_veh_id,
                           p_scnrio_id => l_scnrio_id,
                           p_offr_id   => l_offr_id);

      get_offers_in_scnrio(p_offr_id               => l_offr_id,
                           p_scnrio_id             => p_scnrio_id,
                           p_mrkt_id               => p_mrkt_id,
                           p_offr_perd_id          => p_offr_perd_id,
                           p_veh_id                => p_veh_id,
                           p_ver_id                => 0,
                           p_get_offr_table        => l_get_offr_table);
    END IF;

    --manage_scenario(l_offr_id, p_user_nm, l_get_offr_table);

    lock_offr(l_offr_id, p_user_nm, p_clstr_id, l_lock_user_nm, l_lock_status);

    l_get_offr_table.EXTEND();
    l_get_offr_table(l_get_offr_table.LAST) := obj_get_offr_line(l_offr_id, 1); --KK null

    COMMIT;

    l_location := 'getting offer table';
    p_edit_offr_table := get_offr_table(l_get_offr_table, p_pagination);

    app_plsql_log.info(l_procedure_name || ' end');

  EXCEPTION
    WHEN OTHERS THEN
      p_status := co_exec_status_failed;
      app_plsql_log.info(l_procedure_name || ': Error adding offer at ' || l_location);
      app_plsql_log.info(l_procedure_name || ': ' || SQLERRM(SQLCODE));

      ROLLBACK;
  END add_offer;

  PROCEDURE add_concepts_to_offr(p_offr_id          IN NUMBER,
                                 p_prfl_cd_list     IN number_array,
                                 p_user_nm          IN VARCHAR2,
                                 p_clstr_id         IN NUMBER,
                                 p_status          OUT NUMBER,
                                 p_edit_offr_table OUT obj_edit_offr_table,
                                 p_pagination       IN CHAR DEFAULT 'N') IS

    l_procedure_name         VARCHAR2(50) := 'ADD_CONCEPTS_TO_OFFR';
    l_location               VARCHAR2(1000);

    l_default_values         t_default_values;

    l_lock_user_nm           VARCHAR2(35);
    l_lock_status            NUMBER;

    l_mrkt_id                NUMBER;
    l_offr_perd_id           NUMBER;
    l_veh_id                 NUMBER;
    l_featrd_side_cd         VARCHAR2(5);

    e_lock_failed            EXCEPTION;
  BEGIN
    g_run_id  := app_plsql_output.generate_new_run_id;
    g_user_id := RTRIM(sys_context('USERENV', 'OS_USER'), 35);

    app_plsql_output.set_run_id(g_run_id);
    app_plsql_log.set_context(g_user_id, g_package_name, g_run_id);
    app_plsql_log.info(l_procedure_name || ' start');

    p_status := co_exec_status_success;

    l_location := 'lock check';
    lock_offr(p_offr_id, p_user_nm, p_clstr_id, l_lock_user_nm, l_lock_status);
    IF l_lock_status NOT IN (1, 2) THEN
      RAISE e_lock_failed;
    END IF;

    l_location := 'Querying the existing offer';
    SELECT o.mrkt_id,
           o.offr_perd_id,
           o.veh_id,
           o.featrd_side_cd
      INTO l_mrkt_id,
           l_offr_perd_id,
           l_veh_id,
           l_featrd_side_cd
      FROM offr o
     WHERE o.offr_id = p_offr_id;

    l_location := 'initializing default values';
    l_default_values := query_default_values(l_mrkt_id);

    l_location := 'Adding concepts';
    FOR i IN p_prfl_cd_list.FIRST .. p_prfl_cd_list.LAST LOOP
      add_concept(p_offr_id,
                         l_mrkt_id,
                         l_offr_perd_id,
                         l_veh_id,
                         l_featrd_side_cd,
                  p_prfl_cd_list(i),
                         l_default_values,
                         p_user_nm,
                         p_status);

      IF p_status = co_exec_status_prcpnt_ex THEN
                ROLLBACK;
        app_plsql_log.info(l_procedure_name || ': Pricepoint with default values already exists. offr_prfl_prcpt_id: ' || p_prfl_cd_list(i));

                RETURN;
            END IF;
        END LOOP;

    COMMIT;

    l_location := 'getting offer table';
    p_edit_offr_table := get_offr_table(p_offr_id => p_offr_id, p_pagination => p_pagination);

    app_plsql_log.info(l_procedure_name || ' stop');

  EXCEPTION
    WHEN e_lock_failed THEN
      p_status := co_exec_status_failed;
      app_plsql_log.info(l_procedure_name || ': Lock failed, user: ' || p_user_nm || ', Status: ' || l_lock_status);

      ROLLBACK;

    WHEN OTHERS THEN
      p_status := co_exec_status_failed;
      app_plsql_log.info(l_procedure_name || ': Error adding concepts to offer at ' || l_location);
      app_plsql_log.info(l_procedure_name || ': ' || SQLERRM(SQLCODE));

      ROLLBACK;
  END add_concepts_to_offr;

  PROCEDURE add_prcpoints_to_offr(p_offr_id                  IN NUMBER,
                                  p_offr_prfl_prcpt_id_list  IN number_array,
                                  p_user_nm                  IN VARCHAR2,
                                  p_clstr_id                 IN NUMBER,
                                  p_status                  OUT NUMBER,
                                  p_edit_offr_table         OUT obj_edit_offr_table,
                                  p_pagination               IN CHAR DEFAULT 'N') IS

    l_procedure_name         VARCHAR2(50) := 'ADD_PRCPOINTS_TO_OFFR';
    l_location               VARCHAR2(1000);

    l_default_values         t_default_values;
    
    l_lock_user_nm           VARCHAR2(35);
    l_lock_status            NUMBER;

    l_mrkt_id                NUMBER;
    l_offr_perd_id           NUMBER;
    l_veh_id                 NUMBER;

    e_lock_failed            EXCEPTION;
  BEGIN
    g_run_id  := app_plsql_output.generate_new_run_id;
    g_user_id := RTRIM(sys_context('USERENV', 'OS_USER'), 35);

    app_plsql_output.set_run_id(g_run_id);
    app_plsql_log.set_context(g_user_id, g_package_name, g_run_id);
    app_plsql_log.info(l_procedure_name || ' start');

    p_status := co_exec_status_success;

    lock_offr(p_offr_id, p_user_nm, p_clstr_id, l_lock_user_nm, l_lock_status);
    IF l_lock_status NOT IN (1, 2) THEN
      RAISE e_lock_failed;
    END IF;

    l_location := 'Querying the existing offer';
    SELECT o.mrkt_id,
           o.offr_perd_id,
           o.veh_id
      INTO l_mrkt_id,
           l_offr_perd_id,
           l_veh_id
      FROM offr o
     WHERE o.offr_id = p_offr_id;

    l_location := 'initializing default values';
    l_default_values := query_default_values(l_mrkt_id);

    l_location := 'Adding pricepoints';
    FOR prpct_rec IN (
      SELECT p.*
                  FROM offr_prfl_prc_point p
       WHERE p.offr_prfl_prcpt_id IN (SELECT column_value FROM TABLE(p_offr_prfl_prcpt_id_list))
    )
        LOOP
      add_pricepoint(p_offr_id,
                            l_mrkt_id,
                            l_offr_perd_id,
                            l_veh_id, 
                            prpct_rec,
                            l_default_values,
                            p_user_nm,
                            p_status);

      IF p_status = co_exec_status_prcpnt_ex THEN
                ROLLBACK;
        app_plsql_log.info(l_procedure_name || ': Pricepoint with default values already exists. offr_prfl_prcpt_id: ' || prpct_rec.offr_prfl_prcpt_id);

                RETURN;
            END IF;
        END LOOP;

    COMMIT;

    l_location := 'getting offer table';
    p_edit_offr_table := get_offr_table(p_offr_id => p_offr_id, p_pagination => p_pagination);

    app_plsql_log.info(l_procedure_name || ' stop');

  EXCEPTION
    WHEN e_lock_failed THEN
      p_status := co_exec_status_failed;
      app_plsql_log.info(l_procedure_name || ': Lock failed, user: ' || p_user_nm || ', Status: ' || l_lock_status);

      ROLLBACK;

    WHEN OTHERS THEN
      p_status := co_exec_status_failed;
      app_plsql_log.info(l_procedure_name || ': Error adding pricepoints to offer at ' || l_location);
      app_plsql_log.info(l_procedure_name || ': ' || SQLERRM(SQLCODE));

      ROLLBACK;
  END add_prcpoints_to_offr;

  PROCEDURE copy_offer(p_copy_offr_table   IN obj_copy_offr_table,
                       p_user_nm           IN VARCHAR2,
                       p_status           OUT NUMBER,
                       p_edit_offr_table  OUT obj_edit_offr_table,
                       p_pagination        IN CHAR DEFAULT 'N') IS

    l_procedure_name         VARCHAR2(50) := 'COPY_OFFER';
    l_location               VARCHAR2(1000);

    l_obj_copy_offr          obj_copy_offr_line;
    l_offr_table             obj_get_offr_table := obj_get_offr_table();

    l_new_offr_id            offr.offr_id%TYPE;
    l_old_offr_id            offr.offr_id%TYPE;
    l_offr_desc_txt          offr.offr_desc_txt%TYPE;
    l_scnrio_id              what_if_scnrio.scnrio_id%TYPE;
    l_whatif                 BOOLEAN;

    e_copy_offer_failed      EXCEPTION;
  BEGIN
    g_run_id  := app_plsql_output.generate_new_run_id;
    g_user_id := RTRIM(sys_context('USERENV', 'OS_USER'), 35);

    app_plsql_output.set_run_id(g_run_id);
    app_plsql_log.set_context(g_user_id, g_package_name, g_run_id);
    app_plsql_log.info(l_procedure_name || ' start');

    p_status := co_exec_status_success;

    FOR i IN p_copy_offr_table.FIRST .. p_copy_offr_table.LAST LOOP

      l_obj_copy_offr := p_copy_offr_table(i);

      FOR j IN l_obj_copy_offr.offr_id.FIRST .. l_obj_copy_offr.offr_id.LAST LOOP

        l_old_offr_id := l_obj_copy_offr.offr_id(j);

        l_location := 'Querying original offer';
        SELECT SUBSTR(o.offr_desc_txt || ' (copy)', 1, 254)
          INTO l_offr_desc_txt
          FROM offr o
         WHERE o.offr_id = l_old_offr_id;

        l_whatif := FALSE;
        IF l_obj_copy_offr.trg_offr_typ = 'WIF' THEN
          l_whatif := TRUE;
        END IF;

        l_location := 'Calling pa_maps_copy.copy_offer';
        l_new_offr_id := pa_maps_copy.copy_offer(par_offerid        => l_old_offr_id,
                                                 par_newmarketid    => l_obj_copy_offr.trg_mrkt_id,
                                                 par_newofferperiod => l_obj_copy_offr.trg_offr_perd_id,
                                                 par_newvehid       => l_obj_copy_offr.trg_veh_id,
                                                 par_newoffrdesc    => l_offr_desc_txt,
                                                 par_zerounits      => CASE WHEN l_obj_copy_offr.trg_zerounits = 1 THEN TRUE
                                                                            WHEN l_obj_copy_offr.trg_zerounits = 0 THEN FALSE
                                                                            ELSE FALSE
                                                                       END,
                                                 par_whatif         => l_whatif,
                                                 par_enrgychrt      => CASE WHEN l_obj_copy_offr.trg_enrgychrt = 1 THEN TRUE
                                                                            WHEN l_obj_copy_offr.trg_enrgychrt = 0 THEN FALSE
                                                                            ELSE FALSE
                                                                       END,
                                                 par_paginationcopy => TRUE,
                                                 par_user           => p_user_nm);
        IF l_new_offr_id = -1 THEN
          RAISE e_copy_offer_failed;
        END IF;
        
        l_location := 'Scenario management';
        IF l_whatif THEN
          l_scnrio_id := l_obj_copy_offr.trg_scnrio_id;
          IF l_scnrio_id IS NULL THEN
            add_scenario(p_mrkt_id         => l_obj_copy_offr.trg_mrkt_id,
                         p_veh_id          => l_obj_copy_offr.trg_veh_id,
                         p_scnrio_desc_txt => l_obj_copy_offr.trg_scnrio_nm,
                         p_strt_perd_id    => l_obj_copy_offr.trg_offr_perd_id,
                         p_end_perd_id     => l_obj_copy_offr.trg_offr_perd_id,
                         p_user_nm         => p_user_nm,
                         p_scnrio_id       => l_scnrio_id);
          END IF;
          add_offr_to_scenario(p_mrkt_id   => l_obj_copy_offr.trg_mrkt_id,
                               p_veh_id    => l_obj_copy_offr.trg_veh_id,
                               p_scnrio_id => l_scnrio_id,
                               p_offr_id   => l_new_offr_id);
        END IF;

        --manage_scenario(l_new_offr_id, p_user_nm, l_offr_table);

        l_offr_table.EXTEND;
        l_offr_table(l_offr_table.LAST) := obj_get_offr_line(l_new_offr_id, 1);

      END LOOP;
    END LOOP;

    l_location := 'Get edit offer table';
    SELECT obj_edit_offr_line(
                status, mrkt_id, offr_perd_id, offr_lock, offr_lock_user, offr_sku_line_id, veh_id, brchr_plcmnt_id, brchr_sctn_nm,
                enrgy_chrt_postn_id, web_postn_id, pg_nr, ctgry_id, brnd_id, sgmt_id, form_id, form_grp_id, prfl_cd, sku_id, fsc_cd,
                prod_typ_id, gender_id, sls_cls_cd, pp_sls_cls_cd, item_sls_cls_cd, offr_desc_txt, offr_notes_txt, offr_lyot_cmnts_txt, featrd_side_cd,
                concept_featrd_side_cd, micr_ncpsltn_ind, scntd_pg_typ_id, cnsmr_invstmt_bdgt_id, pymt_typ, promtn_id, promtn_clm_id, spndng_lvl,
                comsn_typ, tax_type_id, wsl_ind, offr_sku_set_id, cmpnt_qty, nr_for_qty, nta_factor, sku_cost, lv_nta, lv_sp, lv_rp,
                lv_discount, lv_units, lv_total_cost, lv_gross_sales, lv_dp_cash, lv_dp_percent, ver_id, sls_prc_amt, reg_prc_amt, line_nr,
                unit_qty, dltd_ind, created_ts, created_user_id, last_updt_ts, last_updt_user_id, intrnl_offr_id, mrkt_veh_perd_sctn_id,
                prfl_nm, sku_nm, comsn_typ_desc_txt, tax_typ_desc_txt, offr_sku_set_nm, sls_typ, pc_sp_py, pc_rp, pc_sp, pc_vsp, pc_hit,
                pg_wght, pp_pg_wght, sprd_nr, offr_prfl_prcpt_id, has_unit_qty, offr_typ, forcasted_units, forcasted_date, offr_cls_id, spcl_ordr_ind,
                offr_ofs_nr, pp_ofs_nr, impct_catgry_id, hero_ind, smplg_ind, mltpl_ind, cmltv_ind, use_instrctns_ind, pg_typ_id, featrd_prfl_ind,
                fxd_pg_wght_ind, prod_endrsmt_id, frc_mtch_mthd_id, wghtd_avg_cost_amt, incntv_id, intrdctn_perd_id, on_stus_perd_id, dspostn_perd_id,
                scnrio_id,micr_ncpsltn_desc_txt,offr_link_id, profile_item_count)
      BULK COLLECT INTO p_edit_offr_table
      FROM TABLE(get_offr(l_offr_table, p_pagination));

    COMMIT;

    app_plsql_log.info(l_procedure_name || ' stop');

  EXCEPTION
    WHEN e_copy_offer_failed THEN
      p_status := co_exec_status_failed;
      app_plsql_log.info(l_procedure_name || ': Copying offer failed. offr_id: ' || l_old_offr_id);

      ROLLBACK;

    WHEN OTHERS THEN
      p_status := co_exec_status_failed;
      app_plsql_log.info(l_procedure_name || ': Error while copying offers at ' || l_location || ', offr_id: ' || l_old_offr_id);
      app_plsql_log.info(l_procedure_name || ': ' || SQLERRM(SQLCODE));

      ROLLBACK;

  END copy_offer;

  FUNCTION chk_sku_lines(p_offr_id     IN offr.offr_id%TYPE,
                         p_osl_records IN obj_edit_offr_table) RETURN BOOLEAN IS
    l_cnt  INTEGER;
  BEGIN
    SELECT COUNT(*)
      INTO l_cnt
      FROM (
        SELECT t.offr_sku_line_id
          FROM offr_sku_line t
         WHERE t.offr_id = p_offr_id
        MINUS
        SELECT r.offr_sku_line_id
          FROM TABLE(p_osl_records) r
         WHERE r.intrnl_offr_id = p_offr_id
      );

    RETURN l_cnt = 0;

  END chk_sku_lines;

  PROCEDURE del_offer_with_deps(p_offr_id IN offr.offr_id%TYPE) IS

    l_procedure_name         VARCHAR2(50) := 'DEL_OFFER_WITH_DEPS';
    l_location               VARCHAR2(1000);

  BEGIN
    SAVEPOINT del_offr;

    l_location := 'deleting dstrbtd_mrkt_sls';
    DELETE FROM dstrbtd_mrkt_sls dms
     WHERE dms.offr_sku_line_id IN (
       SELECT offr_sku_line_id
         FROM offr_sku_line
        WHERE offr_prfl_prcpt_id IN (
          SELECT offr_prfl_prcpt_id
            FROM offr_prfl_prc_point
           WHERE offr_id = p_offr_id
          )
       );

    l_location := 'deleting offr_sku_line';
    DELETE FROM offr_sku_line osl
     WHERE osl.offr_prfl_prcpt_id IN (
       SELECT offr_prfl_prcpt_id
         FROM offr_prfl_prc_point
        WHERE offr_id = p_offr_id
       );

    l_location := 'deleting offr_prfl_prc_point';
    DELETE FROM offr_prfl_prc_point p
     WHERE p.offr_id = p_offr_id;

    l_location := 'deleting offr_sls_cls_sku';
    DELETE FROM offr_sls_cls_sku s
     WHERE s.offr_id        = p_offr_id;

    l_location := 'deleting offr_prfl_sls_cls_plcmt';
    DELETE FROM offr_prfl_sls_cls_plcmt p
     WHERE p.offr_id        = p_offr_id;

    l_location := 'deleting offr';
    DELETE FROM offr o
     WHERE o.offr_id = p_offr_id;

  EXCEPTION
    WHEN OTHERS THEN
      app_plsql_log.info(l_procedure_name || ': Error deleting offers at ' || l_location || ', offr_id: ' || p_offr_id);
      app_plsql_log.info(l_procedure_name || ': ' || SQLERRM(SQLCODE));

      ROLLBACK TO del_offr;

      RAISE;

  END del_offer_with_deps;

  PROCEDURE add_to_edit_offr_table(p_offr_id         IN offr.offr_id%TYPE,
                                   p_status          IN NUMBER,
                                   p_osl_records     IN obj_edit_offr_table,
                                   p_edit_offr_table IN OUT NOCOPY obj_edit_offr_table) IS
  BEGIN
    FOR osl_rec IN (
      SELECT *
        FROM TABLE(p_osl_records)
       WHERE intrnl_offr_id = p_offr_id
    )
    LOOP
      p_edit_offr_table.EXTEND;
      p_edit_offr_table(p_edit_offr_table.LAST)
        := obj_edit_offr_line(p_status, osl_rec.mrkt_id, osl_rec.offr_perd_id, osl_rec.offr_lock, osl_rec.offr_lock_user,
                              osl_rec.offr_sku_line_id, osl_rec.veh_id, osl_rec.brchr_plcmnt_id, osl_rec.brchr_sctn_nm,
                              osl_rec.enrgy_chrt_postn_id, osl_rec.web_postn_id, osl_rec.pg_nr, osl_rec.ctgry_id, osl_rec.brnd_id, osl_rec.sgmt_id,
                              osl_rec.form_id, osl_rec.form_grp_id, osl_rec.prfl_cd, osl_rec.sku_id, osl_rec.fsc_cd,
                              osl_rec.prod_typ_id, osl_rec.gender_id, osl_rec.sls_cls_cd, osl_rec.pp_sls_cls_cd,  osl_rec.item_sls_cls_cd, osl_rec.offr_desc_txt,
                              osl_rec.offr_notes_txt, osl_rec.offr_lyot_cmnts_txt, osl_rec.featrd_side_cd,
                              osl_rec.concept_featrd_side_cd, osl_rec.micr_ncpsltn_ind, osl_rec.scntd_pg_typ_id, osl_rec.cnsmr_invstmt_bdgt_id,
                              osl_rec.pymt_typ, osl_rec.promtn_id, osl_rec.promtn_clm_id, osl_rec.spndng_lvl, osl_rec.comsn_typ,
                              osl_rec.tax_type_id, osl_rec.wsl_ind, osl_rec.offr_sku_set_id, osl_rec.cmpnt_qty,
                              osl_rec.nr_for_qty, osl_rec.nta_factor, osl_rec.sku_cost, osl_rec.lv_nta, osl_rec.lv_sp,
                              osl_rec.lv_rp, osl_rec.lv_discount, osl_rec.lv_units, osl_rec.lv_total_cost, osl_rec.lv_gross_sales,
                              osl_rec.lv_dp_cash, osl_rec.lv_dp_percent, osl_rec.ver_id, osl_rec.sls_prc_amt, osl_rec.reg_prc_amt,
                              osl_rec.line_nr, osl_rec.unit_qty, osl_rec.dltd_ind, osl_rec.created_ts, osl_rec.created_user_id,
                              osl_rec.last_updt_ts, osl_rec.last_updt_user_id, osl_rec.intrnl_offr_id, osl_rec.mrkt_veh_perd_sctn_id,
                              osl_rec.prfl_nm, osl_rec.sku_nm, osl_rec.comsn_typ_desc_txt, osl_rec.tax_typ_desc_txt, osl_rec.offr_sku_set_nm,
                              osl_rec.sls_typ, osl_rec.pc_sp_py, osl_rec.pc_rp, osl_rec.pc_sp, osl_rec.pc_vsp, osl_rec.pc_hit,
                              osl_rec.pg_wght, osl_rec.pp_pg_wght, osl_rec.sprd_nr, osl_rec.offr_prfl_prcpt_id, osl_rec.has_unit_qty, osl_rec.offr_typ,
                              osl_rec.forcasted_units, osl_rec.forcasted_date, osl_rec.offr_cls_id, osl_rec.spcl_ordr_ind,
                              osl_rec.offr_ofs_nr, osl_rec.pp_ofs_nr, osl_rec.impct_catgry_id, osl_rec.hero_ind, osl_rec.smplg_ind, osl_rec.mltpl_ind,
                              osl_rec.cmltv_ind,  osl_rec.use_instrctns_ind,  osl_rec.pg_typ_id,  osl_rec.featrd_prfl_ind,  osl_rec.fxd_pg_wght_ind,
                              osl_rec.prod_endrsmt_id, osl_rec.frc_mtch_mthd_id, osl_rec.wghtd_avg_cost_amt, osl_rec.incntv_id,
                              osl_rec.intrdctn_perd_id, osl_rec.on_stus_perd_id, osl_rec.dspostn_perd_id, osl_rec.scnrio_id,
                              osl_rec.micr_ncpsltn_desc_txt,osl_rec.offr_link_id, osl_rec.profile_item_count);
    END LOOP;
  END add_to_edit_offr_table;

  PROCEDURE delete_offers(p_osl_records      IN obj_edit_offr_table,
                          p_edit_offr_table OUT obj_edit_offr_table) IS

    l_procedure_name         VARCHAR2(50) := 'DELETE_OFFERS';
    l_location               VARCHAR2(1000);

    l_offr_id                offr.offr_id%TYPE;
    l_status                 NUMBER;

    e_not_all_sku_lines      EXCEPTION;
    e_lock_failed            EXCEPTION;
  BEGIN
    g_run_id  := app_plsql_output.generate_new_run_id;
    g_user_id := RTRIM(sys_context('USERENV', 'OS_USER'), 35);

    app_plsql_output.set_run_id(g_run_id);
    app_plsql_log.set_context(g_user_id, g_package_name, g_run_id);
    app_plsql_log.info(l_procedure_name || ' start');

    p_edit_offr_table := obj_edit_offr_table();

    l_location := 'offr_rec loop';
    FOR offr_rec IN (
      SELECT DISTINCT intrnl_offr_id offr_id,
                      offr_lock_user
        FROM TABLE(p_osl_records)
    )
    LOOP
      BEGIN
        l_status := co_eo_stat_success;
        l_offr_id := offr_rec.offr_id;

        IF NOT chk_sku_lines(l_offr_id, p_osl_records) THEN
          RAISE e_not_all_sku_lines;
        END IF;

        IF offr_rec.offr_lock_user IS NOT NULL AND lock_offr_chk(l_offr_id, offr_rec.offr_lock_user) = 0 THEN
          RAISE e_lock_failed;
        END IF;

        l_location := 'calling del_offer_with_deps';
        del_offer_with_deps(l_offr_id);

      EXCEPTION
        WHEN e_not_all_sku_lines THEN
          l_status := co_eo_stat_part_lines;
        WHEN e_lock_failed THEN
          l_status := co_eo_stat_lock_failure;
        WHEN OTHERS THEN
          l_status := co_eo_stat_error;
          app_plsql_log.info(l_procedure_name || ': Error deleting offers at ' || l_location || ', offr_id: ' || l_offr_id);
          app_plsql_log.info(l_procedure_name || ': ' || SQLERRM(SQLCODE));
      END;

      l_location := 'adding data to edit_offr_table';
      add_to_edit_offr_table(l_offr_id, l_status, p_osl_records, p_edit_offr_table);

    END LOOP;

    COMMIT;

    app_plsql_log.info(l_procedure_name || ' stop');

  EXCEPTION
    WHEN OTHERS THEN
      app_plsql_log.info(l_procedure_name || ': Error deleting offers at ' || l_location || ', offr_id: ' || l_offr_id);
      app_plsql_log.info(l_procedure_name || ': ' || SQLERRM(SQLCODE));

      ROLLBACK;

  END delete_offers;

  PROCEDURE del_prcpt_with_deps(p_prcpt_id IN offr_prfl_prc_point.offr_prfl_prcpt_id%TYPE) IS

    l_procedure_name         VARCHAR2(50) := 'DEL_PRCPT_WITH_DEPS';
    l_location               VARCHAR2(1000);

    l_cnt                    INTEGER;

  BEGIN
    SAVEPOINT del_prcpt;

    l_location := 'deleting dstrbtd_mrkt_sls';
    DELETE FROM dstrbtd_mrkt_sls dms
     WHERE dms.offr_sku_line_id IN (
       SELECT offr_sku_line_id
         FROM offr_sku_line
        WHERE offr_prfl_prcpt_id = p_prcpt_id
       );

    l_location := 'deleting offr_sku_line';
    DELETE FROM offr_sku_line osl
     WHERE osl.offr_prfl_prcpt_id = p_prcpt_id;

    l_location := 'deleting offr_prfl_prc_point';
    DELETE FROM offr_prfl_prc_point p
     WHERE p.offr_prfl_prcpt_id = p_prcpt_id;

    l_location := 'delete offr_sls_cls_sku and offr_prfl_sls_cls_plcmt';
    FOR rec IN (
      SELECT *
        FROM offr_sku_line osl
       WHERE osl.offr_prfl_prcpt_id = p_prcpt_id
    )
    LOOP
      SELECT COUNT(*)
        INTO l_cnt
        FROM offr_sls_cls_sku s
       WHERE NOT EXISTS (SELECT *
                           FROM offr_sku_line osl
                          WHERE osl.offr_id        = s.offr_id
                            AND osl.sls_cls_cd     = s.sls_cls_cd
                            AND osl.prfl_cd        = s.prfl_cd
                            AND osl.pg_ofs_nr      = s.pg_ofs_nr
                            AND osl.featrd_side_cd = s.featrd_side_cd
                            AND osl.sku_id         = s.sku_id)
         AND s.offr_id        = rec.offr_id
         AND s.sls_cls_cd     = rec.sls_cls_cd
         AND s.prfl_cd        = rec.prfl_cd
         AND s.pg_ofs_nr      = rec.pg_ofs_nr
         AND s.featrd_side_cd = rec.featrd_side_cd
         AND s.sku_id         = rec.sku_id;

      IF l_cnt > 0 THEN
        DELETE FROM offr_sls_cls_sku s
         WHERE s.offr_id        = rec.offr_id
           AND s.sls_cls_cd     = rec.sls_cls_cd
           AND s.prfl_cd        = rec.prfl_cd
           AND s.pg_ofs_nr      = rec.pg_ofs_nr
           AND s.featrd_side_cd = rec.featrd_side_cd
           AND s.sku_id         = rec.sku_id;
      END IF;

      SELECT COUNT(*)
        INTO l_cnt
        FROM offr_prfl_sls_cls_plcmt p
       WHERE NOT EXISTS (SELECT *
                           FROM offr_prfl_prc_point s
                          WHERE s.offr_id        = p.offr_id
                            AND s.sls_cls_cd     = p.sls_cls_cd
                            AND s.prfl_cd        = p.prfl_cd
                            AND s.pg_ofs_nr      = p.pg_ofs_nr
                            AND s.featrd_side_cd = p.featrd_side_cd)
         AND NOT EXISTS (SELECT *
                           FROM offr_sls_cls_sku s
                          WHERE s.offr_id        = p.offr_id
                            AND s.sls_cls_cd     = p.sls_cls_cd
                            AND s.prfl_cd        = p.prfl_cd
                            AND s.pg_ofs_nr      = p.pg_ofs_nr
                            AND s.featrd_side_cd = p.featrd_side_cd)
         AND p.offr_id        = rec.offr_id
         AND p.sls_cls_cd     = rec.sls_cls_cd
         AND p.prfl_cd        = rec.prfl_cd
         AND p.pg_ofs_nr      = rec.pg_ofs_nr
         AND p.featrd_side_cd = rec.featrd_side_cd;

      IF l_cnt > 0 THEN
        DELETE FROM offr_prfl_sls_cls_plcmt p
         WHERE p.offr_id        = rec.offr_id
           AND p.sls_cls_cd     = rec.sls_cls_cd
           AND p.prfl_cd        = rec.prfl_cd
           AND p.pg_ofs_nr      = rec.pg_ofs_nr
           AND p.featrd_side_cd = rec.featrd_side_cd;
      END IF;
    END LOOP;

  EXCEPTION
    WHEN OTHERS THEN
      app_plsql_log.info(l_procedure_name || ': Error deleting pricepoints at ' || l_location || ', offr_prfl_prcpt_id: ' || p_prcpt_id);
      app_plsql_log.info(l_procedure_name || ': ' || SQLERRM(SQLCODE));

      ROLLBACK TO del_prcpt;

      RAISE;

  END del_prcpt_with_deps;

  PROCEDURE delete_prcpoints(p_osl_records      IN obj_edit_offr_table,
                             p_edit_offr_table OUT obj_edit_offr_table,
                             p_pagination       IN CHAR DEFAULT 'N') IS

    l_procedure_name         VARCHAR2(50) := 'DELETE_PRCPOINTS';
    l_location               VARCHAR2(1000);

    l_get_offr_table         obj_get_offr_table := obj_get_offr_table();
    l_failed_prcpnts         number_array := number_array();
    l_lock_failed_prcpnts    number_array := number_array();
    l_prcpt_id               offr_prfl_prc_point.offr_prfl_prcpt_id%TYPE;
    l_cnt                    INTEGER;

    e_lock_failed            EXCEPTION;

  BEGIN
    g_run_id  := app_plsql_output.generate_new_run_id;
    g_user_id := RTRIM(sys_context('USERENV', 'OS_USER'), 35);

    app_plsql_output.set_run_id(g_run_id);
    app_plsql_log.set_context(g_user_id, g_package_name, g_run_id);
    app_plsql_log.info(l_procedure_name || ' start');

    p_edit_offr_table := obj_edit_offr_table();

    l_location := 'offr_rec loop';
    FOR offr_rec IN (
      SELECT DISTINCT intrnl_offr_id offr_id,
                      offr_lock_user,last_updt_user_id
        FROM TABLE(p_osl_records)
    )
    LOOP
      BEGIN
        IF offr_rec.offr_lock_user IS NOT NULL AND lock_offr_chk(offr_rec.offr_id, offr_rec.offr_lock_user) = 0 THEN
          RAISE e_lock_failed;
        END IF;

        l_location := 'prcpt_rec loop';
        FOR prcpt_rec IN (
          SELECT DISTINCT offr_prfl_prcpt_id
            FROM TABLE(p_osl_records)
        )
        LOOP
          BEGIN
            l_prcpt_id := prcpt_rec.offr_prfl_prcpt_id;

            l_location := 'calling del_prcpt_with_deps';
            del_prcpt_with_deps(l_prcpt_id);

          EXCEPTION
            WHEN OTHERS THEN
              RAISE;
          END;
        END LOOP; -- prcpt_rec loop

        UPDATE offr o
           SET o.sku_cnt = (SELECT COUNT(1)
                              FROM offr_sku_line osl,
                                   offr_prfl_prc_point oppp
                             WHERE osl.offr_prfl_prcpt_id = oppp.offr_prfl_prcpt_id
                               AND oppp.offr_id = offr_rec.offr_id),
               o.prfl_cnt = (SELECT COUNT(1)
                               FROM offr_prfl_prc_point oppp
                              WHERE oppp.offr_id = offr_rec.offr_id),
               o.last_updt_user_id = offr_rec.last_updt_user_id
         WHERE o.offr_id = offr_rec.offr_id;

      EXCEPTION
        WHEN e_lock_failed THEN
          app_plsql_log.info(l_procedure_name || ': Error deleting pricepoints (lock failure) at ' || l_location || ', offr_prfl_prcpt_id: ' || l_prcpt_id);
          app_plsql_log.info(l_procedure_name || ': ' || SQLERRM(SQLCODE));

          l_lock_failed_prcpnts.EXTEND;
          l_lock_failed_prcpnts(l_lock_failed_prcpnts.LAST) := l_prcpt_id;

        WHEN OTHERS THEN
          app_plsql_log.info(l_procedure_name || ': Error deleting pricepoints at ' || l_location || ', offr_prfl_prcpt_id: ' || l_prcpt_id);
          app_plsql_log.info(l_procedure_name || ': ' || SQLERRM(SQLCODE));

          l_failed_prcpnts.EXTEND;
          l_failed_prcpnts(l_failed_prcpnts.LAST) := l_prcpt_id;

      END;

      l_get_offr_table.EXTEND;
      l_get_offr_table(l_get_offr_table.LAST) := obj_get_offr_line(offr_rec.offr_id, 1);

    END LOOP; -- offr_rec loop

    COMMIT;

    l_location := 'getting offer table';
    p_edit_offr_table := get_offr_table(l_get_offr_table, p_pagination);

    l_location := 'update status column in result';
    FOR i IN p_edit_offr_table.FIRST .. p_edit_offr_table.LAST LOOP
      SELECT COUNT(1)
        INTO l_cnt
        FROM dual
       WHERE p_edit_offr_table(i).offr_prfl_prcpt_id IN (SELECT column_value FROM TABLE(l_lock_failed_prcpnts));

      IF l_cnt > 0 THEN
        p_edit_offr_table(i).status := co_eo_stat_lock_failure;
      ELSE
        SELECT COUNT(1)
          INTO l_cnt
          FROM dual
         WHERE p_edit_offr_table(i).offr_prfl_prcpt_id IN (SELECT column_value FROM TABLE(l_failed_prcpnts));

        IF l_cnt > 0 THEN
          p_edit_offr_table(i).status := co_eo_stat_error;
        ELSE
          p_edit_offr_table(i).status := co_eo_stat_success;
        END IF;
      END IF;
    END LOOP;

    app_plsql_log.info(l_procedure_name || ' stop');

  EXCEPTION
    WHEN OTHERS THEN
      app_plsql_log.info(l_procedure_name || ': Error deleting pricepoints at ' || l_location || ', offr_prfl_prcpt_id: ' || l_prcpt_id);
      app_plsql_log.info(l_procedure_name || ': ' || SQLERRM(SQLCODE));

      p_edit_offr_table := NULL;

      ROLLBACK;

  END delete_prcpoints;
  
  PROCEDURE copy_pricepoint(p_pp_rec            IN offr_prfl_prc_point%ROWTYPE,
                            p_trgt_offr_id      IN NUMBER,
                            p_trgt_pg_ofs_nr    IN NUMBER,
                            p_trgt_ftrd_side_cd IN VARCHAR2,
                            p_user_nm           IN VARCHAR2,
                            p_status           OUT VARCHAR2) IS

    l_procedure_name         VARCHAR2(50) := 'COPY_PRICEPOINT';
    l_location               VARCHAR2(1000);

    l_offr_prfl_prcpt_id     NUMBER;
    l_offr_sku_line_id       NUMBER;
    l_mrkt_id                NUMBER;
    l_veh_id                 NUMBER;
    l_offr_perd_id           NUMBER;
    l_pg_wght_pct            NUMBER;
    l_prod_endrsmt_id        NUMBER;
    l_pg_typ_id              NUMBER;
    l_smplg_ind              CHAR(1);
    l_hero_ind               CHAR(1);
    l_scntd_pg_typ_id        offr_sls_cls_sku.scntd_pg_typ_id%TYPE;
    l_wsl_ind                CHAR(1);
    l_cost_amt               NUMBER;
    l_reg_prc_amt            NUMBER;
    l_found                  NUMBER := 0;

  BEGIN
    SELECT o.mrkt_id, o.veh_id, o.offr_perd_id
      INTO l_mrkt_id, l_veh_id, l_offr_perd_id
      FROM offr o
     WHERE o.offr_id = p_trgt_offr_id;
  
    BEGIN
      l_location := 'sales class placement check';
      SELECT 1 INTO l_found
        FROM offr_prfl_sls_cls_plcmt
       WHERE offr_id        = p_trgt_offr_id
         AND sls_cls_cd     = p_pp_rec.sls_cls_cd
         AND prfl_cd        = p_pp_rec.prfl_cd
         AND pg_ofs_nr      = p_trgt_pg_ofs_nr
         AND featrd_side_cd = p_trgt_ftrd_side_cd;
    EXCEPTION
      WHEN no_data_found THEN

        l_location := 'query source offr_prfl_sls_cls_plcmt';
        SELECT p.pg_wght_pct, p.prod_endrsmt_id, p.pg_typ_id
          INTO l_pg_wght_pct, l_prod_endrsmt_id, l_pg_typ_id
          FROM offr_prfl_sls_cls_plcmt p
         WHERE p.offr_id        = p_pp_rec.offr_id
           AND p.sls_cls_cd     = p_pp_rec.sls_cls_cd
           AND p.prfl_cd        = p_pp_rec.prfl_cd
           AND p.pg_ofs_nr      = p_pp_rec.pg_ofs_nr
           AND p.featrd_side_cd = p_pp_rec.featrd_side_cd;
          
      l_location := 'create sales class placement';
      INSERT INTO offr_prfl_sls_cls_plcmt
        ( offr_id, sls_cls_cd, prfl_cd, pg_ofs_nr, featrd_side_cd, mrkt_id, veh_id,
          offr_perd_id, sku_cnt, pg_wght_pct, prod_endrsmt_id, pg_typ_id, creat_user_id)
      VALUES
        ( p_trgt_offr_id, p_pp_rec.sls_cls_cd, p_pp_rec.prfl_cd, p_trgt_pg_ofs_nr,
          p_trgt_ftrd_side_cd, l_mrkt_id, l_veh_id, l_offr_perd_id, 0, l_pg_wght_pct,
          l_prod_endrsmt_id, l_pg_typ_id, p_user_nm);

    END;

    l_location := 'Pricepoint check';
    BEGIN
      SELECT 1 INTO l_found
        FROM offr_prfl_prc_point opp
       WHERE opp.offr_id        = p_trgt_offr_id
         AND opp.prfl_cd        = p_pp_rec.prfl_cd
         AND opp.sls_cls_cd     = p_pp_rec.sls_cls_cd
         AND opp.sls_prc_amt    = p_pp_rec.sls_prc_amt
         AND opp.nr_for_qty     = p_pp_rec.nr_for_qty
         AND opp.pg_ofs_nr      = p_trgt_pg_ofs_nr
         AND opp.pymt_typ       = p_pp_rec.pymt_typ
         AND opp.comsn_typ      = p_pp_rec.comsn_typ
         AND opp.tax_type_id    = p_pp_rec.tax_type_id
         AND opp.promtn_id      = p_pp_rec.promtn_id
         AND opp.promtn_clm_id  = p_pp_rec.promtn_clm_id
         AND opp.featrd_side_cd = p_trgt_ftrd_side_cd;

         RAISE e_prcpnt_already_exists;

    EXCEPTION
      WHEN no_data_found THEN
        NULL;
    END;

    SELECT seq.NEXTVAL INTO l_offr_prfl_prcpt_id FROM dual;

    l_location := 'create price point';
    INSERT INTO offr_prfl_prc_point
      ( offr_prfl_prcpt_id, offr_id, promtn_clm_id, veh_id, promtn_id, mrkt_id, cnsmr_invstmt_bdgt_id,
        sls_cls_cd, prfl_cd, ssnl_evnt_id, offr_perd_id, crncy_cd, sku_cnt, nr_for_qty,
        est_unit_qty, est_sls_amt, est_cost_amt, sls_srce_id, sls_prc_amt,
        tax_amt, pymt_typ, comsn_amt, comsn_typ, net_to_avon_fct, prmry_offr_ind,
        pg_ofs_nr, featrd_side_cd, chrty_amt, awrd_sls_prc_amt, tax_type_id, creat_user_id)
    VALUES
      ( l_offr_prfl_prcpt_id, p_trgt_offr_id, p_pp_rec.promtn_clm_id, l_veh_id, p_pp_rec.promtn_id, 
        l_mrkt_id, p_pp_rec.cnsmr_invstmt_bdgt_id, p_pp_rec.sls_cls_cd, p_pp_rec.prfl_cd,
        p_pp_rec.ssnl_evnt_id, l_offr_perd_id, p_pp_rec.crncy_cd, 0, p_pp_rec.nr_for_qty,
        p_pp_rec.est_unit_qty, p_pp_rec.est_sls_amt, p_pp_rec.est_cost_amt, p_pp_rec.sls_srce_id, p_pp_rec.sls_prc_amt,
        p_pp_rec.tax_amt, p_pp_rec.pymt_typ, p_pp_rec.comsn_amt, p_pp_rec.comsn_typ, p_pp_rec.net_to_avon_fct,
        p_pp_rec.prmry_offr_ind, p_trgt_pg_ofs_nr, p_trgt_ftrd_side_cd, p_pp_rec.chrty_amt, p_pp_rec.awrd_sls_prc_amt,
        p_pp_rec.tax_type_id, p_user_nm);

    -- new profile so increment profile counter for Offer
    UPDATE offr o
    SET    prfl_cnt = nvl(prfl_cnt, 0) + 1,
           last_updt_user_id = p_user_nm
    WHERE  offr_id  = p_trgt_offr_id;

    FOR sku_rec IN (
      SELECT s.*
        FROM offr_sku_line s
       WHERE s.offr_prfl_prcpt_id = p_pp_rec.offr_prfl_prcpt_id
    )
    LOOP
      BEGIN
        l_location := 'sales class sku check';
        SELECT 1 INTO l_found
          FROM offr_sls_cls_sku
         WHERE offr_id        = p_trgt_offr_id
           AND sls_cls_cd     = p_pp_rec.sls_cls_cd
           AND prfl_cd        = p_pp_rec.prfl_cd
           AND pg_ofs_nr      = p_trgt_pg_ofs_nr
           AND featrd_side_cd = p_trgt_ftrd_side_cd
           AND sku_id         = sku_rec.sku_id;
      EXCEPTION
        WHEN no_data_found THEN
          
          l_location := 'query source offr_sls_cls_sku';
          SELECT s.smplg_ind, s.hero_ind, s.scntd_pg_typ_id, s.wsl_ind, s.cost_amt, s.reg_prc_amt
            INTO l_smplg_ind, l_hero_ind, l_scntd_pg_typ_id, l_wsl_ind, l_cost_amt, l_reg_prc_amt
            FROM offr_sls_cls_sku s
           WHERE s.offr_id        = p_pp_rec.offr_id
             AND s.sls_cls_cd     = p_pp_rec.sls_cls_cd
             AND s.prfl_cd        = p_pp_rec.prfl_cd
             AND s.pg_ofs_nr      = p_pp_rec.pg_ofs_nr
             AND s.featrd_side_cd = p_pp_rec.featrd_side_cd
             AND s.sku_id         = sku_rec.sku_id;
        
          l_location := 'create sales class sku';
          INSERT INTO offr_sls_cls_sku
            ( offr_id, sls_cls_cd, prfl_cd, pg_ofs_nr, featrd_side_cd, sku_id, mrkt_id,
              smplg_ind, hero_ind, micr_ncpsltn_ind, scntd_pg_typ_id, wsl_ind, reg_prc_amt, cost_amt, creat_user_id)
          VALUES
            ( p_trgt_offr_id, p_pp_rec.sls_cls_cd, p_pp_rec.prfl_cd, p_trgt_pg_ofs_nr, p_trgt_ftrd_side_cd,
              sku_rec.sku_id, l_mrkt_id, l_smplg_ind, l_hero_ind, decode(l_scntd_pg_typ_id, null, 'N', 'Y'), l_scntd_pg_typ_id,
              l_wsl_ind, l_reg_prc_amt, l_cost_amt, p_user_nm);
      END;

      SELECT seq.NEXTVAL INTO l_offr_sku_line_id FROM dual;

      l_location := 'create OSL';
      INSERT INTO offr_sku_line
        (offr_sku_line_id, offr_id, veh_id, featrd_side_cd, offr_perd_id, mrkt_id, sku_id,
         pg_ofs_nr, prfl_cd, crncy_cd, prmry_sku_offr_ind, sls_cls_cd, offr_prfl_prcpt_id,
         demo_avlbl_ind, dltd_ind, unit_splt_pct, sls_prc_amt, cost_typ, creat_user_id)
      VALUES
        (l_offr_sku_line_id, p_trgt_offr_id, l_veh_id, p_trgt_ftrd_side_cd, l_offr_perd_id, l_mrkt_id,
         sku_rec.sku_id, p_trgt_pg_ofs_nr, p_pp_rec.prfl_cd, sku_rec.crncy_cd, sku_rec.prmry_sku_offr_ind,
         sku_rec.sls_cls_cd, l_offr_prfl_prcpt_id, sku_rec.demo_avlbl_ind, sku_rec.dltd_ind, sku_rec.unit_splt_pct,
         sku_rec.sls_prc_amt, sku_rec.cost_typ, p_user_nm);

      INSERT INTO dstrbtd_mrkt_sls
        (mrkt_id, sls_perd_id, offr_sku_line_id, sls_typ_id, sls_srce_id, offr_perd_id,
         veh_id, unit_qty, comsn_amt, tax_amt, net_to_avon_fct, cost_amt, creat_user_id)
      SELECT l_mrkt_id, sls_perd_id, l_offr_sku_line_id, sls_typ_id, sls_srce_id, l_offr_perd_id,
             l_veh_id, unit_qty, comsn_amt, tax_amt, net_to_avon_fct, cost_amt, p_user_nm
        FROM dstrbtd_mrkt_sls dms
       WHERE dms.offr_sku_line_id = sku_rec.offr_sku_line_id;

    -- new sku added so increment sku counters for OFFR, OPSCP and OPP
      UPDATE offr o
      SET    sku_cnt = nvl(sku_cnt, 0) + 1,
             last_updt_user_id = p_user_nm
      WHERE  offr_id = p_trgt_offr_id;

      UPDATE offr_prfl_sls_cls_plcmt
      SET    sku_cnt = nvl(sku_cnt, 0) + 1,
             last_updt_user_id = p_user_nm
      WHERE  offr_id        = p_trgt_offr_id
         AND sls_cls_cd     = sku_rec.sls_cls_cd
         AND prfl_cd        = sku_rec.prfl_cd
         AND pg_ofs_nr      = p_trgt_pg_ofs_nr
         AND featrd_side_cd = p_trgt_ftrd_side_cd;

      UPDATE offr_prfl_prc_point
      SET    sku_cnt = nvl(sku_cnt, 0) + 1,
             last_updt_user_id = p_user_nm
      WHERE  offr_prfl_prcpt_id = l_offr_prfl_prcpt_id;

    END LOOP;

    p_status := co_exec_status_success;

  EXCEPTION
    WHEN e_prcpnt_already_exists THEN
      app_plsql_log.info(l_procedure_name || ': Pricepoint already exists, offr_prfl_prcpt_id: ' || p_pp_rec.offr_prfl_prcpt_id);
      RAISE;
      
    WHEN OTHERS THEN
      app_plsql_log.info(l_procedure_name || ': Error adding offer at ' || l_location);
      app_plsql_log.info(l_procedure_name || ': ' || SQLERRM(SQLCODE));
      p_status := co_exec_status_failed;
      RAISE;

  END copy_pricepoint;

  PROCEDURE copy_prcpts_to_offr(p_offr_prfl_prcpt_ids IN number_array,
                                p_trgt_offr_id        IN NUMBER,
                                p_trgt_pg_ofs_nr      IN NUMBER,
                                p_trgt_ftrd_side_cd   IN VARCHAR2,
                                p_user_nm             IN VARCHAR2,
                                p_clstr_id            IN NUMBER,
                                p_move_ind            IN VARCHAR2,
                                p_status             OUT NUMBER,
                                p_edit_offr_table    OUT obj_edit_offr_table) IS

    l_procedure_name         VARCHAR2(50) := 'COPY_PRCPTS_TO_OFFR';
    l_location               VARCHAR2(1000);
    
    l_lock_user_nm           VARCHAR2(35);
    l_lock_status            NUMBER;
    l_src_offr_id            offr.offr_id%TYPE;

    e_src_lock_failed        EXCEPTION;
    e_trgt_lock_failed       EXCEPTION;
  BEGIN
    g_run_id  := app_plsql_output.generate_new_run_id;
    g_user_id := RTRIM(sys_context('USERENV', 'OS_USER'), 35);

    app_plsql_output.set_run_id(g_run_id);
    app_plsql_log.set_context(g_user_id, g_package_name, g_run_id);
    app_plsql_log.info(l_procedure_name || ' start');

    l_location := 'Getting source offr_id';
    SELECT offr_id
      INTO l_src_offr_id
      FROM offr_prfl_prc_point p
     WHERE p.offr_prfl_prcpt_id = p_offr_prfl_prcpt_ids(1);

    l_location := 'Source offr lock check';
    IF p_move_ind = 'Y' THEN
      lock_offr(l_src_offr_id, p_user_nm, p_clstr_id, l_lock_user_nm, l_lock_status);
      IF l_lock_status NOT IN (1, 2) THEN
        RAISE e_src_lock_failed;
      END IF;
    END IF;

    l_location := 'Target offr lock check';
    lock_offr(p_trgt_offr_id, p_user_nm, p_clstr_id, l_lock_user_nm, l_lock_status);
    IF l_lock_status NOT IN (1, 2) THEN
      RAISE e_trgt_lock_failed;
    END IF;

    l_location := 'Copying pricepoints';
    FOR r_pp_rec IN (
      SELECT p.*
        FROM offr_prfl_prc_point p
       WHERE p.offr_prfl_prcpt_id IN (SELECT column_value FROM TABLE(p_offr_prfl_prcpt_ids))
    )
    LOOP
      BEGIN
        copy_pricepoint(r_pp_rec,
                        p_trgt_offr_id,
                        p_trgt_pg_ofs_nr,
                        p_trgt_ftrd_side_cd,
                        p_user_nm,
                        p_status);

        IF p_move_ind = 'Y' THEN
          del_prcpt_with_deps(r_pp_rec.offr_prfl_prcpt_id);
        END IF;

      EXCEPTION
        WHEN e_prcpnt_already_exists THEN
          RAISE;
        WHEN OTHERS THEN
          app_plsql_log.info(l_procedure_name || ': Error copying pricepoints at ' || l_location || ', offr_prfl_prcpt_id: ' || r_pp_rec.offr_prfl_prcpt_id);
          app_plsql_log.info(l_procedure_name || ': ' || SQLERRM(SQLCODE));

          RAISE;
      END;
    END LOOP;

    COMMIT;

    l_location := 'getting offer table';
    p_edit_offr_table := get_offr_table(p_offr_id => p_trgt_offr_id, p_pagination => 'Y');

    app_plsql_log.info(l_procedure_name || ' stop');

  EXCEPTION
    WHEN e_src_lock_failed THEN
      p_status := co_exec_status_failed;
      app_plsql_log.info(l_procedure_name || ': Source offer lock failed, user: ' || p_user_nm || ', Status: ' || l_lock_status);

      ROLLBACK;

    WHEN e_trgt_lock_failed THEN
      p_status := co_exec_status_failed;
      app_plsql_log.info(l_procedure_name || ': Target offer lock failed, user: ' || p_user_nm || ', Status: ' || l_lock_status);

      ROLLBACK;

    WHEN e_prcpnt_already_exists THEN
      p_status := co_exec_status_prcpnt_ex;
      ROLLBACK;

    WHEN OTHERS THEN
      p_status := co_exec_status_failed;
      app_plsql_log.info(l_procedure_name || ': Error copying pricepoints at ' || l_location);
      app_plsql_log.info(l_procedure_name || ': ' || SQLERRM(SQLCODE));

      ROLLBACK;
  END copy_prcpts_to_offr;

PROCEDURE get_sprd_data(p_mrkt_id IN NUMBER, 
    p_offr_perd_id IN NUMBER, 
    p_veh_id IN NUMBER, 
    p_ver_id IN NUMBER, 
    p_sprd_nr IN NUMBER,  
    p_page_data OUT CLOB,  
    p_img_url OUT VARCHAR2)
 AS
      l_clob CLOB;
      l_img_url VARCHAR2(4000);
--      l_return_obj obj_edit_offr_pg_edit_line;
    BEGIN

    SELECT page_data, img_url
      INTO l_clob,l_img_url
      FROM mrkt_veh_perd_sprd
     WHERE mrkt_id = p_mrkt_id
       AND offr_perd_id = p_offr_perd_id
       AND veh_id = p_veh_id
       AND sprd_nr = p_sprd_nr
       AND ver_id = p_ver_id;
       
       p_page_data := l_clob;
       p_img_url := l_img_url;

    END get_sprd_data;

  PROCEDURE set_sprd_data(p_mrkt_id      IN NUMBER,
                          p_offr_perd_id IN NUMBER,
                          p_veh_id       IN NUMBER,
                          p_ver_id       IN NUMBER,
                          p_sprd_nr      IN NUMBER,
                          p_user_id      IN VARCHAR2,
                          p_page_data    IN CLOB,
                          p_img_url      IN VARCHAR2,
                          p_status         OUT NUMBER,
                          p_error_txt      OUT VARCHAR2) AS
    l_exist NUMBER;  
  BEGIN
    dbms_output.put_line('setting page data for spread');
    p_status := 1;
    SELECT COUNT(*)
      INTO l_exist
      FROM mrkt_veh_perd_sprd
     WHERE mrkt_id = p_mrkt_id
       AND offr_perd_id = p_offr_perd_id
       AND veh_id = p_veh_id
       AND sprd_nr = p_sprd_nr
       AND ver_id = p_ver_id;
    IF l_exist = 0 THEN
      INSERT INTO mrkt_veh_perd_sprd (mrkt_id,
                                      offr_perd_id,
                                      veh_id,
                                      ver_id,
                                      sprd_nr,
                                      page_data,
                                      img_url,
                                      creat_user_id)
                                      VALUES (
                                      p_mrkt_id,
                                      p_offr_perd_id,
                                      p_veh_id,
                                      p_ver_id,
                                      p_sprd_nr,
                                      p_page_data,
                                      p_img_url,
                                      p_user_id
                                      );
    ELSE
      UPDATE mrkt_veh_perd_sprd
         SET page_data = p_page_data,
             img_url = p_img_url,
             last_updt_user_id = p_user_id
       WHERE mrkt_id = p_mrkt_id
         AND offr_perd_id = p_offr_perd_id
         AND veh_id = p_veh_id
         AND sprd_nr = p_sprd_nr;
         
    END IF;

    COMMIT;
    EXCEPTION WHEN OTHERS THEN 
      p_status := 0;
      p_error_txt := SQLERRM(SQLCODE);
      ROLLBACK;
  END set_sprd_data;

END PA_MAPS_EDIT_OFFR;
/
