CREATE OR REPLACE TYPE obj_get_scnrio_offr_line force AS OBJECT
(
  offr_id              NUMBER,
  sls_typ_id           NUMBER
);
/

CREATE OR REPLACE TYPE obj_get_scnrio_offr_table FORCE 
as table of obj_get_scnrio_offr_line;
/
