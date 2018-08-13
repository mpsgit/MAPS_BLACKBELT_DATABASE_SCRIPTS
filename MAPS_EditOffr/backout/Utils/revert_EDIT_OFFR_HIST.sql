ALTER TABLE edit_offr_hist SET UNUSED COLUMN offr_typ;
ALTER TABLE edit_offr_hist SET UNUSED COLUMN forcasted_units;
ALTER TABLE edit_offr_hist SET UNUSED COLUMN forcasted_date;
ALTER TABLE edit_offr_hist SET UNUSED COLUMN offr_cls_id;
ALTER TABLE edit_offr_hist SET UNUSED COLUMN spcl_ordr_ind;
ALTER TABLE edit_offr_hist SET UNUSED COLUMN offr_ofs_nr;
ALTER TABLE edit_offr_hist SET UNUSED COLUMN pp_ofs_nr;

ALTER TABLE edit_offr_hist DROP UNUSED COLUMNS;
