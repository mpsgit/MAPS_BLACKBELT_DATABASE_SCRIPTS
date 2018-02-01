create or replace PACKAGE BODY PA_SIMS_CUST_GRP_MANTNC AS

  FUNCTION get_cust_grps(p_mrkt_id      IN mrkt_perd.mrkt_id%TYPE,
                         p_trgt_perd_id IN mrkt_perd.perd_id%TYPE)
    RETURN obj_cust_grp_mantnc_table
    PIPELINED AS
    CURSOR cc IS
      SELECT obj_cust_grp_mantnc_line(rul_id,
                                      rul_nm,
                                      rul_desc,
                                      sku_id,
                                      sku_nm,
                                      catgry_id,
                                      prfl.brnd_id,
                                      pa_maps_public.get_mstr_fsc_cd(mrkt_id,
                                                                     sku_id,
                                                                     p_trgt_perd_id),
                                      pa_maps_public.get_fsc_desc(mrkt_id,
                                                                  p_trgt_perd_id,
                                                                  pa_maps_public.get_mstr_fsc_cd(mrkt_id,
                                                                                                 sku_id,
                                                                                                 p_trgt_perd_id))) cline
        FROM custm_rul_mstr crm
        JOIN custm_rul_sku_list crsl
       USING (rul_id)
        LEFT JOIN sku
       USING (sku_id)
        LEFT JOIN prfl
       USING (prfl_cd)
       WHERE mrkt_id = p_mrkt_id;
  BEGIN
    FOR rec IN cc LOOP
      PIPE ROW(rec.cline);
    END LOOP;
  END get_cust_grps;

  FUNCTION get_cust_grp_actv(p_mrkt_id IN mrkt_perd.mrkt_id%TYPE)
    RETURN obj_cust_grp_mantnc_ga_table
    PIPELINED AS
    CURSOR cc IS
      SELECT obj_cust_grp_mantnc_ga_line(rul_id, campgn_perd_id) cline
        FROM custm_rul_mstr crm
        JOIN custm_rul_perd crp
       USING (rul_id)
       WHERE mrkt_id = p_mrkt_id;
  BEGIN
    FOR rec IN cc LOOP
      PIPE ROW(rec.cline);
    END LOOP;
  END get_cust_grp_actv;

  PROCEDURE set_cust_grp_actv(p_mrkt_id           IN NUMBER,
                              p_rul_id            IN NUMBER,
                              p_trgt_perd_id_list IN number_array,
                              p_user_id           IN VARCHAR2,
                              p_stus              OUT NUMBER) AS
    l_run_id         NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'set_cust_grp_actv';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       to_char(p_mrkt_id) || ', ' ||
                                       'p_rul_id: ' || to_char(p_rul_id) || ', ' ||
                                       'p_user_id: ' || l_user_id || ')';
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
  
    DELETE FROM custm_rul_perd
     WHERE rul_id = p_rul_id
       AND campgn_perd_id NOT IN (SELECT * FROM TABLE(p_trgt_perd_id_list));
  
    MERGE INTO custm_rul_perd d
    USING (SELECT column_value col FROM TABLE(p_trgt_perd_id_list)) s
    ON (d.rul_id = p_rul_id AND d.campgn_perd_id = s.col)
    WHEN NOT MATCHED THEN
      INSERT (d.rul_id, d.campgn_perd_id) VALUES (p_rul_id, s.col);
    COMMIT;
    p_stus := 0;
    app_plsql_log.info(l_module_name || ' COMMIT, status_code: ' ||
                       to_char(p_stus) || l_parameter_list);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      p_stus := 1;
      app_plsql_log.info(l_module_name || ' FAILED, error code: ' ||
                         SQLCODE || ' error message: ' || SQLERRM ||
                         l_parameter_list);
    
  END set_cust_grp_actv;

  PROCEDURE set_cust_grps(p_mrkt_id     IN NUMBER,
                          p_rul_id      IN NUMBER DEFAULT NULL,
                          p_rul_nm      IN VARCHAR2,
                          p_rul_desc    IN VARCHAR2,
                          p_sku_id_list IN number_array,
                          p_user_id     IN VARCHAR2,
                          p_stus        OUT NUMBER) AS
    l_rul_id NUMBER;
    -- for log                         
    l_run_id         NUMBER := app_plsql_output.generate_new_run_id;
    l_user_id        VARCHAR2(35) := nvl(p_user_id, USER());
    l_module_name    VARCHAR2(30) := 'set_cust_grps';
    l_parameter_list VARCHAR2(2048) := ' (p_mrkt_id: ' ||
                                       to_char(p_mrkt_id) || ', ' ||
                                       'p_rul_id: ' || to_char(p_rul_id) || ', ' ||
                                       'p_rul_nm: ' || to_char(p_rul_nm) || ', ' ||
                                       'p_rul_desc: ' ||
                                       to_char(p_rul_desc) || ', ' ||
                                       'p_user_id: ' || l_user_id || ')';
  BEGIN
    app_plsql_log.register(g_package_name || '.' || l_module_name);
    app_plsql_output.set_run_id(l_run_id);
    app_plsql_log.set_context(l_user_id, g_package_name, l_run_id);
  
    MERGE INTO custm_rul_mstr d
    USING (SELECT p_rul_id col FROM dual) s
    ON (d.rul_id = s.col)
    WHEN MATCHED THEN
      UPDATE SET rul_nm = p_rul_nm, rul_desc = p_rul_desc
    WHEN NOT MATCHED THEN
      INSERT
        (d.mrkt_id, d.rul_nm, d.rul_desc)
      VALUES
        (p_mrkt_id, p_rul_nm, p_rul_desc);
  
    IF p_rul_id IS NULL THEN
      SELECT MAX(rul_id) INTO l_rul_id FROM custm_rul_mstr;
    ELSE
      l_rul_id := p_rul_id;
    END IF;
  
    DELETE FROM custm_rul_sku_list WHERE rul_id = l_rul_id;
  
    INSERT INTO custm_rul_sku_list
      (rul_id, sku_id)
      SELECT l_rul_id, column_value FROM TABLE(p_sku_id_list);
  
    COMMIT;
    p_stus := 0;
    app_plsql_log.info(l_module_name || ' COMMIT, status_code: ' ||
                       to_char(p_stus) || l_parameter_list);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      p_stus := 1;
      app_plsql_log.info(l_module_name || ' FAILED, error code: ' ||
                         SQLCODE || ' error message: ' || SQLERRM ||
                         l_parameter_list);
  END set_cust_grps;

END PA_SIMS_CUST_GRP_MANTNC;
/
show error
