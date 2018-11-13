CREATE OR REPLACE TYPE obj_copy_offr_line FORCE AS OBJECT (
  offr_id           number_array,
  trg_mrkt_id       NUMBER,
  trg_offr_perd_id  NUMBER,
  trg_veh_id        NUMBER,
  trg_offr_typ      VARCHAR2(5),
  trg_scnrio_id     NUMBER,
  trg_scnrio_nm     VARCHAR2(100),
  trg_zerounits     NUMBER,
  trg_enrgychrt     NUMBER
);
/
