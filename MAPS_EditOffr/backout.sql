@backout\Utils\revert_EDIT_OFFR_HIST.sql
@backout\Utils\revert_CONFIG_ITEM.sql

DROP TYPE obj_copy_offr_table;
DROP TYPE obj_copy_offr_line;

@backout\Types\OBJ_EDIT_OFFR_FILTER_LINE.sql
@backout\Types\OBJ_EDIT_OFFR_FILTER_TABLE.sql
@backout\Types\OBJ_EDIT_OFFR_LINE.sql
@backout\Types\OBJ_EDIT_OFFR_TABLE.sql
@backout\Types\OBJ_EDIT_OFFR_HIST_LINE.sql
@backout\Types\OBJ_EDIT_OFFR_HIST_TABLE.sql

@backout\Packages\PA_MAPS_EDIT_OFFR.sql
show errors

@backout\Packages\PA_MAPS_EDIT_OFFR_body.sql
show errors
