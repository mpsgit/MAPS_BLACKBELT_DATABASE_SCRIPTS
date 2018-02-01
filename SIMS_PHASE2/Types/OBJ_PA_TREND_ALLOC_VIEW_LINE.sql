create or replace type OBJ_PA_TREND_ALLOC_VIEW_LINE FORCE as object
(
  NOT_PLANND_UNTS      NUMBER,
  HAS_SAVE             CHAR(1),
  LAST_RUN             DATE,
  IS_STARTED           CHAR(1),
  IS_COMPLETE          CHAR(1),
  IS_SAVED             CHAR(1),
  USE_OFFERS_ON_SCHED  CHAR(1),
  USE_OFFERS_OFF_SCHED CHAR(1)
);
/
show error
