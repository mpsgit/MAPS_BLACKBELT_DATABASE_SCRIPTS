CREATE OR REPLACE TYPE obj_get_scnrio_line force AS OBJECT
(
  scnrio_id            NUMBER,
  sls_typ_id           NUMBER,
  ver_id               NUMBER
);
/

CREATE OR REPLACE TYPE obj_get_scnrio_table FORCE 
as table of obj_get_scnrio_line;
/
