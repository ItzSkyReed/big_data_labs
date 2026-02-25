/*
 Найдите среднее значение по колонке s_all_avg. Эта колонка должна содержать среднюю активность.
 У вас появится ошибка. Да, придется исправить проблемы с данными.
 Их две: не тот тип, и проблема с запятыми.
 */
BEGIN;

ALTER TABLE user_logs
    ALTER COLUMN s_all_avg TYPE float -- меняем тип данных с varchar на float
        USING REPLACE(s_all_avg, ',', '.')::float; -- приводим с запятой на точку числа в колонке и преобразовываем в float

SELECT avg(s_all_avg)
FROM user_logs;

ROLLBACK; -- не сохраняем изменения