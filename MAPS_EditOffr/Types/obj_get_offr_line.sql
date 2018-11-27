CREATE OR REPLACE TYPE obj_get_offr_line FORCE AS OBJECT
(
                             p_offr_id        NUMBER,
                             p_sls_typ        NUMBER
);
/

CREATE OR REPLACE TYPE obj_get_offr_table FORCE 
AS TABLE OF obj_get_offr_line;
/
