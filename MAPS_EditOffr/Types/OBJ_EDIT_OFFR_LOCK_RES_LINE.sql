CREATE OR REPLACE TYPE obj_edit_offr_lock_res_line FORCE AS OBJECT
(
                    lock_user_nm  VARCHAR2(35),
                    offr_id       NUMBER,
                    status        NUMBER
);
/
