CREATE OR REPLACE PACKAGE BODY pa_maps_menu AS

  -- local constants
  C_MAPS_SYSTEM      CONSTANT number := 1;
  C_MAPS_ADMIN_ROLE  CONSTANT number := 1;

  -- global logging info for package
  g_run_id           NUMBER := 0;
  g_user_id          mps_plsql_log.user_id%TYPE := 'PA_MAPS_MENU';

  -- ----------------------------------------------------------------------- --
  -- Procedure: GET_DYNAMIC_MENU (external)                                  --
  --                                                                         --
  -- Returns an array of MAPS menu items in hierarchical order. This will    --
  -- exclude:                                                                --
  --                                                                         --
  -- (a) menu items which are soft-deleted (DLTD_IND='Y')                    --
  -- (b) menu items which are in the sub-tree of any other soft-deleted item --
  -- (c) clickable menu items with a MAPS object id which is not available   --
  --     to the specified user via their user access rights                  --
  -- (d) menu items where all clickable menu items in their sub-tree have    --
  --     been excluded for one of the reasons above.                         --
  --                                                                         --
  -- Note: some menu items have a URL but no object id (eg Help) and these   --
  --       will not be affected by user rights ie hey will always be sent to --
  --       MAPS as long as they satisfy all other rules                      --
  --                                                                         --
  -- ----------------------------------------------------------------------- --
  PROCEDURE get_dynamic_menu
  ( p_user_nm        IN     mps_user.user_nm%TYPE,
    p_maps_menu         OUT T_MAPS_MENU,
    p_return_status     OUT number
  ) IS

    l_procedure_name  varchar2(50) := 'GET_DYNAMIC_MENU: ';
    user_ok           number := 0;

  BEGIN

    g_run_id  := APP_PLSQL_OUTPUT.generate_new_run_id;
    g_user_id := rtrim(sys_context('USERENV','OS_USER'),35);

    APP_PLSQL_OUTPUT.set_run_id(g_run_id);
    APP_PLSQL_LOG.set_context(g_user_id, 'PA_MAPS_MENU', g_run_id);
    --APP_PLSQL_LOG.info(l_procedure_name||'start');
    --APP_PLSQL_LOG.info(l_procedure_name||'user='||p_user_nm);

    p_maps_menu := T_MAPS_MENU();

    SELECT count(*) into user_ok
    FROM   mps_user
    WHERE  user_nm    = p_user_nm and
           enabld_ind = 'Y';

    IF user_ok = 0 THEN

      p_return_status := PA_MAPS_ERRORS.INVALID_USER;
      --APP_PLSQL_LOG.info(l_procedure_name||'user '||p_user_nm||' is not valid');

    ELSE

      p_return_status := PA_MAPS_ERRORS.SUCCESS;

      SELECT            OBJ_MAPS_MENU(level, enbld_ind, menu_item_desc_txt, maps_scrn_url, maps_app_ver, TREND_FSC_FOOTER)
      BULK COLLECT INTO p_maps_menu
      FROM              maps_menu_item m1
      WHERE             is_deleted(menu_item_id) = 'N' and
                        ( ( obj_id is not null and
                            has_access(p_user_nm, obj_id) = 'Y') or
                          ( obj_id is null and
                            ( maps_scrn_url is not null or
                              0 < ( SELECT count(*)
                                    FROM   maps_menu_item
                                    WHERE  level > 1 and dltd_ind = 'N' and
                                           ( (obj_id is not null and has_access(p_user_nm, obj_id) = 'Y') or
                                             (obj_id is null and maps_scrn_url is not null)
                                           )
                                    START WITH menu_item_id = m1.menu_item_id
                                    CONNECT BY PRIOR menu_item_id = parnt_menu_item_id)) )
                        )
      START WITH        parnt_menu_item_id IS NULL
      CONNECT BY PRIOR  menu_item_id = parnt_menu_item_id
      ORDER SIBLINGS BY seq_nr;

    END IF; -- check user

    --APP_PLSQL_LOG.info(l_procedure_name||'end');

  END get_dynamic_menu;

  -- ------------------------------------------------------------------------- --
  -- Function: GET_DYNAMIC_MENU (external)                                     --
  --                                                                           --
  -- Returns result of call to GET_DYNAMIC_MENU procedure                      --
  -- in form of a pipliened table                                              --
  --                                                                           --
  -- ------------------------------------------------------------------------- --


  function get_dynamic_menu
  (  p_user_nm  IN     mps_user.user_nm%TYPE )
  return T_MAPS_MENU pipelined is
    l_menu_item T_MAPS_MENU;
    l_return_status number;
   begin

    PA_MAPS_MENU.GET_DYNAMIC_MENU(
    P_USER_NM => p_user_nm,
    P_MAPS_MENU => l_menu_item,
    P_RETURN_STATUS => l_return_status
    );

    if l_return_status = PA_MAPS_ERRORS.SUCCESS then
       FOR I IN 1..L_MENU_ITEM.COUNT LOOP
          pipe row (l_menu_item(i));
       end loop;
    end if;

    end get_dynamic_menu;

  -- ------------------------------------------------------------------------- --
  -- Function: IS_DELETED (external)                                           --
  --                                                                           --
  -- Recursive function to determine if a menu item is soft-deleted or if it   --
  -- inherits soft-deleted status from any earlier menu item in its hierarchy. --
  --                                                                           --
  -- ------------------------------------------------------------------------- --
  FUNCTION is_deleted
  (
    p_menu_item_id  IN maps_menu_item.menu_item_id%TYPE
  )
  RETURN char IS

    l_dltd_ind char(1);

  BEGIN

    SELECT decode(dltd_ind, 'Y', dltd_ind,
                            decode(parnt_menu_item_id, null, dltd_ind,
                                                       is_deleted(parnt_menu_item_id)))
    INTO   l_dltd_ind
    FROM   maps_menu_item
    WHERE  menu_item_id = p_menu_item_id;

    RETURN l_dltd_ind;

  END is_deleted;


  -- ------------------------------------------------------------------------- --
  -- Function: HAS_ACCESS (external)                                           --
  --                                                                           --
  -- Determines whether or not the specified user has access to a given MAPS   --
  -- screen. A user has access if:                                             --
  --                                                                           --
  -- (a) they have been granted the MAPS Administrator role                    --
  --     or                                                                    --
  -- (b) they belong to at least one MAPS user group for the cluster which has --
  --     access to the screen                                                  --
  --                                                                           --
  -- Note: assumes the user has already been authenticated by MAPS for access  --
  --       to the system and the cluster.                                      --
  -- ------------------------------------------------------------------------- --
  FUNCTION has_access
  (
    p_user_nm  IN     mps_user.user_nm%TYPE,
    p_obj_id   IN     mps_obj.obj_id%TYPE
  )
  RETURN char IS

    access_ind char(1);

  BEGIN

    SELECT decode(count(*), 0, 'N', 'Y') INTO access_ind
    FROM   mps_user_rl r, mps_user u
    WHERE  u.user_nm  = p_user_nm and
           r.user_nm  = u.user_nm and
           r.clstr_id = u.clstr_id and
           r.sys_id   = u.sys_id and
           r.sys_id   = C_MAPS_SYSTEM and
           r.rl_id    = C_MAPS_ADMIN_ROLE;

    IF access_ind = 'N' THEN

      SELECT decode(sign(count(*)), 0, 'N', 'Y') INTO access_ind
      FROM   mps_user u, mps_user_grp_user g, mps_user_grp_obj o
      WHERE  u.user_nm     = p_user_nm and
             g.user_nm     = u.user_nm and
             g.clstr_id    = u.clstr_id and
             g.sys_id      = C_MAPS_SYSTEM and
             o.user_grp_id = g.user_grp_id and
             o.clstr_id    = g.clstr_id and
             o.sys_id      = g.sys_id and
             o.obj_id      = p_obj_id;

    END IF;

    RETURN access_ind;

  END has_access;

END pa_maps_menu;
/
show error
