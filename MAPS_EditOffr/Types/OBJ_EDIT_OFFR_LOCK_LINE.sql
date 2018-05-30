CREATE OR REPLACE TYPE obj_edit_offr_lock_line FORCE AS OBJECT
(
                    offr_id      NUMBER,
                    user_nm      VARCHAR2(35),
                    clstr_id     NUMBER
);
/
