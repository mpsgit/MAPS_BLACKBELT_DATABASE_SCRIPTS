create or replace type OBJ_CUST_GRP_MANTNC_LINE FORCE as object 
(
  RULID number,
  RULNM varchar2(20),
  RULDESC varchar2(200),
  SKUID number,
  SKUNM varchar2(100),
  CATGRYID number,
  BRANDID number,
  FSCCD number,
  FSCDESC varchar2(100)
);
/
show error
