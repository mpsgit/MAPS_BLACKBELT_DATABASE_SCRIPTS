CREATE OR REPLACE TYPE obj_edit_offr_sls_typ_line FORCE AS OBJECT
(
                    max_sls_typ_id       NUMBER,
                    sls_typ_grp_id       NUMBER,
                    veh_id               NUMBER,
                    mrkt_id              NUMBER,
                    offr_perd_id         NUMBER,
                    ver_id               NUMBER
);
/

CREATE OR REPLACE TYPE obj_edit_offr_sls_typ_table FORCE 
AS TABLE OF obj_edit_offr_sls_typ_line;
/
