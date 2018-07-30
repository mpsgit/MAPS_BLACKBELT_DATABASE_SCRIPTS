ALTER TABLE edit_offr_hist ADD
(
  offr_typ               VARCHAR2(5),
  forcasted_units        NUMBER,
  forcasted_date         DATE,
  offr_cls_id            NUMBER,
  spcl_ordr_ind          CHAR(1),
  offr_ofs_nr            NUMBER,
  pp_ofs_nr              NUMBER
);
