CREATE OR REPLACE TYPE obj_edit_offr_mrkt_perd_line FORCE AS OBJECT
(
                    mrkt_id              NUMBER,
                    offr_perd_id         NUMBER,
                    ver_id               NUMBER
);
/

CREATE OR REPLACE TYPE obj_edit_offr_mrkt_perd_table FORCE 
AS TABLE OF obj_edit_offr_mrkt_perd_line;
/
