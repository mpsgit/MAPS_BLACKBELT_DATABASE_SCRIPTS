CREATE OR REPLACE PACKAGE pa_maps_menu AS

  /********************************************************************************
  * Modification History
  *
  * Date         : 22/01/2013
  * Developer    : Fiona Lindsay
  * Description  : Initial Creation
  *                Generation of dynamic menu for MAPS
  *
  * Date         : 30/01/2013
  * Developer    : Fiona Lindsay
  * Description  : Menu Items returned to MAPS now restricted based on user access rights
  *
  * Date         : 15/03/2013
  * Developer    : Fiona Lindsay
  * Description  : Commented out PL/SQL logging as it's not really necessary
  *                Code left in so it can be re-activated easily if needed
  *
  * Date         : 15/03/2013
  * Developer    : Fiona Lindsay
  * Description  : Fixed issue in has_access() for users in more than one system
  *
  * Date         : 15/03/2013
  * Developer    : Fiona Lindsay
  * Description  : Added maps_app_ver to get_dynamic_menu to handle menu items which need
  *                a full url to be generated from the local application config
  *
  *********************************************************************************/

  -- external functions/procedures
  PROCEDURE get_dynamic_menu
  ( p_user_nm        IN     mps_user.user_nm%TYPE,
    p_maps_menu         OUT T_MAPS_MENU,
    p_return_status     OUT number
  );

  function get_dynamic_menu
  (  p_user_nm  IN     mps_user.user_nm%TYPE )
  return T_MAPS_MENU pipelined
  ;

  FUNCTION is_deleted
  (
    p_menu_item_id  IN maps_menu_item.menu_item_id%TYPE
  )
  RETURN char;
  PRAGMA RESTRICT_REFERENCES (is_deleted, WNDS);

  FUNCTION has_access
  (
    p_user_nm  IN     mps_user.user_nm%TYPE,
    p_obj_id   IN     mps_obj.obj_id%TYPE
  )
  RETURN char;
  PRAGMA RESTRICT_REFERENCES (has_access, WNDS);

END pa_maps_menu;
/
show error
