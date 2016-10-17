ALTER TABLE MRKT_PERD 
NOCOMPRESS;

ALTER TABLE MRKT_SLS_PERD 
ADD (EST_BILNG_DT DATE);

ALTER TABLE MRKT_SLS_PERD 
ADD (BST_BILNG_DT DATE);

COMMENT ON COLUMN MRKT_SLS_PERD.EST_BILNG_DT IS 'Billing day for EST for given campaign – this will be date only, no time portion';

COMMENT ON COLUMN MRKT_SLS_PERD.BST_BILNG_DT IS 'Billing day for BST for given campaign – this will be date only, no time portion';

ALTER TABLE MRKT_PERD 
COMPRESS;