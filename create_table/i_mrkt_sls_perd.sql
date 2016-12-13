ALTER TABLE MRKT_SLS_PERD
ADD CONSTRAINT FK_MRKTPERD_MRKTSLSPERD FOREIGN KEY
(
  MRKT_ID 
, SLS_PERD_ID 
)
REFERENCES MRKT_PERD
(
  MRKT_ID 
, PERD_ID 
)
ENABLE;

ALTER TABLE MRKT_SLS_PERD
ADD CONSTRAINT C_MSP_SLSPERDID CHECK 
(SLS_PERD_ID >= 20050301)
ENABLE;

ALTER TABLE MRKT_SLS_PERD
ADD CONSTRAINT C_MSP_SCTAUTCLCBSTIND CHECK 
(SCT_AUTCLC_BST_IND IN('Y','N'))
ENABLE;

ALTER TABLE MRKT_SLS_PERD
ADD CONSTRAINT C_MSP_SCTAUTCLCESTIND CHECK 
(SCT_AUTCLC_EST_IND IN('Y','N'))
ENABLE;

