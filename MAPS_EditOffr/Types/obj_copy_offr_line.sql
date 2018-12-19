CREATE OR REPLACE TYPE obj_copy_offr_line FORCE AS OBJECT (
  offr_id                     number_array,
  trg_mrkt_id                 NUMBER,
  trg_offr_perd_id            NUMBER,
  trg_veh_id                  NUMBER,
  trg_offr_typ                VARCHAR2(5),
  trg_scnrio_id               NUMBER,
  trg_scnrio_nm               VARCHAR2(100),
  trg_zerounits               NUMBER,
  trg_enrgychrt               NUMBER,
  trg_mrkt_veh_perd_sctn_id   NUMBER,
  trg_offr_ofs_nr             NUMBER,
  trg_featrd_side_cd          VARCHAR2(5)
);
/

CREATE OR REPLACE TYPE obj_copy_offr_table FORCE
AS TABLE OF obj_copy_offr_line;
/
