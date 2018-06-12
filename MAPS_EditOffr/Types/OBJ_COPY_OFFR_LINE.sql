CREATE OR REPLACE TYPE obj_copy_offr_line FORCE AS OBJECT (
  offr_id           number_array,
  trg_mrkt_id       NUMBER,
  trg_offr_perd_id  NUMBER,
  trg_veh_id        NUMBER,
  trg_offr_typ      VARCHAR2(5)
);
/
