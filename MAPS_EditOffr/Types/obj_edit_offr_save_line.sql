CREATE OR REPLACE TYPE obj_edit_offr_save_line FORCE AS OBJECT
(
                             p_status        NUMBER,
                             p_offr_id       NUMBER,
                             p_sls_typ       NUMBER
);
/

CREATE OR REPLACE TYPE obj_edit_offr_save_table FORCE 
AS TABLE OF obj_edit_offr_save_line;
/
