SELECT userid, courseid, departments.name
FROM user_logs
         INNER JOIN departments ON user_logs.Depart = departments.id
LIMIT 10