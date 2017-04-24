begin
  --drop
  DBMS_SCHEDULER.drop_job(job_name => 'PA_TREND_ALLOC_JOB', force => true);
end;

