CREATE OR REPLACE TYPE obj_scenario_line FORCE IS OBJECT (
  scnrio_id          NUMBER,
  scnrio_desc_txt    VARCHAR2(100)
);
/

CREATE OR REPLACE TYPE obj_scenario_table FORCE IS TABLE OF obj_scenario_line;
/
