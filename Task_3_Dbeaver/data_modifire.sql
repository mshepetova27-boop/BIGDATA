
select * from user_logs order by random () limit 5;

update user_logs 
set 
    s_course_viewed_avg = replace(s_course_viewed_avg::text, ',', '.'),
    s_q_attempt_viewed_avg = replace(s_q_attempt_viewed_avg::text, ',', '.'),
    s_a_course_module_viewed_avg = replace(s_a_course_module_viewed_avg::text, ',', '.'),
    s_a_submission_status_viewed_avg = replace(s_a_submission_status_viewed_avg::text, ',', '.')
where 
    s_course_viewed_avg::text like '%,%' or 
    s_q_attempt_viewed_avg::text like '%,%' or 
    s_a_course_module_viewed_avg::text like '%,%' or 
    s_a_submission_status_viewed_avg::text like '%,%';

alter table user_logs 
alter column s_all_avg type real using s_all_avg::real;

alter TABLE user_logs 
    alter column s_course_viewed_avg type real using nullif(s_course_viewed_avg::text, '')::real,
    alter column s_q_attempt_viewed_avg type real using nullif(s_q_attempt_viewed_avg::text, '')::real,
    alter column s_a_course_module_viewed_avg type real using nullif(s_a_course_module_viewed_avg::text, '')::real,
    alter column s_a_submission_status_viewed_avg type real using nullif(s_a_submission_status_viewed_avg::text, '')::real;


select AVG(s_all_avg) AS average_activity from user_logs;    

