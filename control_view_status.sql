-- tech.tech_control_view_status исходный текст

CREATE OR REPLACE VIEW tech.tech_control_view_status
AS WITH cfg_table AS (
         SELECT cfg.table_name,
            cfg.schema_name,
            cfg.incident_id,
            cfg.dataflow_id,
            cfg.dataflow_desc,
            cfg.periodicl,
            cfg.shuptime,
            cfg.data_engineer,
            cfg.nifi_url
           FROM tech.tech_cfg_control_table cfg
          WHERE cfg.deprecated IS NOT TRUE
        ), uniq_rec_with_max_ts AS (
         SELECT ctl.schema_name,
            ctl.table_name,
            ctl.rowscount,
            ctl.maxrecdate,
            ctl.time_up,
            date_trunc('day'::text, ctl.time_up) AS time_up_without_time,
            ctl.update_timestamp
           FROM ( SELECT sub_ctl.table_name,
                    sub_ctl.rowscount,
                    sub_ctl.time_up,
                    sub_ctl.maxrecdate,
                    sub_ctl.update_timestamp,
                    sub_ctl.schema_name,
                    max(sub_ctl.update_timestamp::text) OVER (PARTITION BY sub_ctl.table_name, sub_ctl.schema_name) AS maxdatetime
                   FROM tech.tech_control_table_log sub_ctl) ctl
          WHERE ctl.update_timestamp::text = ctl.maxdatetime
          ORDER BY ctl.schema_name, ctl.table_name
        ), aggr_info AS (
         SELECT cfg_table.schema_name,
            cfg_table.table_name,
            cfg_table.incident_id,
            cfg_table.dataflow_id,
            cfg_table.dataflow_desc,
            cfg_table.periodicl,
            cfg_table.shuptime,
            cfg_table.data_engineer,
            uniq_rec_with_max_ts.rowscount,
            uniq_rec_with_max_ts.maxrecdate,
            uniq_rec_with_max_ts.time_up,
            uniq_rec_with_max_ts.update_timestamp,
            cfg_table.nifi_url
           FROM cfg_table
             LEFT JOIN uniq_rec_with_max_ts ON cfg_table.table_name::text = uniq_rec_with_max_ts.table_name::text AND cfg_table.schema_name::text = uniq_rec_with_max_ts.schema_name::text
        ), days_number_from_last_working_day AS (
         SELECT CURRENT_DATE - max(dict_calendar.full_date) AS ok_interval
           FROM nsi.dict_calendar
          WHERE NOT dict_calendar.dayoff AND dict_calendar.full_date < CURRENT_DATE
        ), next_or_current_working_day AS (
         SELECT min(dict_calendar.full_date) AS next_or_current_working_day
           FROM nsi.dict_calendar
          WHERE NOT dict_calendar.dayoff AND dict_calendar.full_date >= CURRENT_DATE
        ), week_day AS (
         SELECT week_day.full_date,
            week_day.dayoff,
                CASE
                    WHEN week_day.dow_name::text = 'Понедельник'::text THEN 'ПН'::text
                    WHEN week_day.dow_name::text = 'Вторник'::text THEN 'ВТ'::text
                    WHEN week_day.dow_name::text = 'Среда'::text THEN 'СР'::text
                    WHEN week_day.dow_name::text = 'Четверг'::text THEN 'ЧТ'::text
                    WHEN week_day.dow_name::text = 'Пятница'::text THEN 'ПТ'::text
                    WHEN week_day.dow_name::text = 'Суббота'::text THEN 'СБ'::text
                    WHEN week_day.dow_name::text = 'Воскресенье'::text THEN 'ВС'::text
                    ELSE NULL::text
                END AS week_day
           FROM nsi.dict_calendar week_day
        )
 SELECT aggr_info.schema_name,
    aggr_info.table_name,
    aggr_info.incident_id,
    aggr_info.dataflow_id,
    aggr_info.dataflow_desc,
    aggr_info.periodicl,
    aggr_info.shuptime,
    aggr_info.data_engineer,
    aggr_info.rowscount,
    aggr_info.maxrecdate,
    aggr_info.time_up,
    aggr_info.update_timestamp,
        CASE
            WHEN aggr_info.periodicl::text = 'ЧС'::text AND (CURRENT_TIMESTAMP - aggr_info.time_up) < '01:00:00'::interval THEN 'OK'::text
            WHEN aggr_info.periodicl::text = '20МИН'::text AND (CURRENT_TIMESTAMP - aggr_info.time_up) < '00:20:00'::interval THEN 'OK'::text
            WHEN aggr_info.periodicl::text = '10МИН'::text AND (CURRENT_TIMESTAMP - aggr_info.time_up) < '00:10:00'::interval THEN 'OK'::text
            WHEN aggr_info.periodicl::text = '5МИН'::text AND (CURRENT_TIMESTAMP - aggr_info.time_up) < '00:05:00'::interval THEN 'OK'::text
            WHEN aggr_info.periodicl::text = 'ГД'::text AND (CURRENT_TIMESTAMP - aggr_info.time_up) < '365 days'::interval THEN 'OK'::text
            WHEN aggr_info.periodicl::text = 'ПГ'::text AND (CURRENT_TIMESTAMP - aggr_info.time_up) < '180 days'::interval THEN 'OK'::text
            WHEN aggr_info.periodicl::text = 'ЕЖ'::text AND (CURRENT_TIMESTAMP - aggr_info.time_up) < '1 day'::interval THEN 'OK'::text
            WHEN aggr_info.periodicl::text = 'МС'::text AND (CURRENT_TIMESTAMP - aggr_info.time_up) < '30 days'::interval THEN 'OK'::text
            WHEN aggr_info.periodicl::text = 'МС'::text AND (CURRENT_TIMESTAMP - aggr_info.time_up) >= '30 days'::interval AND (aggr_info.dataflow_id::text = ANY (ARRAY['RST.022.A'::character varying::text, 'MSH.008.A'::character varying::text])) AND CURRENT_DATE < (date_trunc('month'::text, aggr_info.time_up::date + '32 days'::interval)::date + 25) THEN 'OK'::text
            WHEN aggr_info.periodicl::text = 'КВ'::text AND (CURRENT_TIMESTAMP - aggr_info.time_up) < '90 days'::interval THEN 'OK'::text
            WHEN aggr_info.periodicl::text = 'ДН'::text AND (CURRENT_TIMESTAMP - aggr_info.time_up) < '14 days'::interval THEN 'OK'::text
            WHEN aggr_info.periodicl::text = 'ПН'::text AND (CURRENT_TIMESTAMP - aggr_info.time_up) < '7 days'::interval THEN 'OK'::text
            WHEN aggr_info.periodicl::text = 'ПН'::text AND (CURRENT_TIMESTAMP - aggr_info.time_up) >= '7 days'::interval AND CURRENT_DATE < (( SELECT min(week_day.full_date) AS min
               FROM aggr_info aggr_info_1
                 LEFT JOIN week_day ON aggr_info_1.periodicl::text = week_day.week_day
              WHERE NOT week_day.dayoff AND week_day.full_date >= aggr_info_1.time_up::timestamp without time zone AND week_day.week_day = 'ПН'::text)) THEN 'OK'::text
            WHEN aggr_info.periodicl::text = 'ВТ'::text AND (CURRENT_TIMESTAMP - aggr_info.time_up) < '7 days'::interval THEN 'OK'::text
            WHEN aggr_info.periodicl::text = 'ВТ'::text AND (CURRENT_TIMESTAMP - aggr_info.time_up) >= '7 days'::interval AND CURRENT_DATE < (( SELECT min(week_day.full_date) AS min
               FROM aggr_info aggr_info_1
                 LEFT JOIN week_day ON aggr_info_1.periodicl::text = week_day.week_day
              WHERE NOT week_day.dayoff AND week_day.full_date >= aggr_info_1.time_up::timestamp without time zone AND week_day.week_day = 'ВТ'::text)) THEN 'OK'::text
            WHEN aggr_info.periodicl::text = 'СР'::text AND (CURRENT_TIMESTAMP - aggr_info.time_up) < '7 days'::interval THEN 'OK'::text
            WHEN aggr_info.periodicl::text = 'СР'::text AND (CURRENT_TIMESTAMP - aggr_info.time_up) >= '7 days'::interval AND CURRENT_DATE < (( SELECT min(week_day.full_date) AS min
               FROM aggr_info aggr_info_1
                 LEFT JOIN week_day ON aggr_info_1.periodicl::text = week_day.week_day
              WHERE NOT week_day.dayoff AND week_day.full_date >= aggr_info_1.time_up::timestamp without time zone AND week_day.week_day = 'СР'::text)) THEN 'OK'::text
            WHEN aggr_info.periodicl::text = 'ЧТ'::text AND (CURRENT_TIMESTAMP - aggr_info.time_up) < '7 days'::interval THEN 'OK'::text
            WHEN aggr_info.periodicl::text = 'ЧТ'::text AND (CURRENT_TIMESTAMP - aggr_info.time_up) >= '7 days'::interval AND CURRENT_DATE < (( SELECT min(week_day.full_date) AS min
               FROM aggr_info aggr_info_1
                 LEFT JOIN week_day ON aggr_info_1.periodicl::text = week_day.week_day
              WHERE NOT week_day.dayoff AND week_day.full_date >= aggr_info_1.time_up::timestamp without time zone AND week_day.week_day = 'ЧТ'::text)) THEN 'OK'::text
            WHEN aggr_info.periodicl::text = 'ПТ'::text AND (CURRENT_TIMESTAMP - aggr_info.time_up) < '7 days'::interval THEN 'OK'::text
            WHEN aggr_info.periodicl::text = 'ПТ'::text AND (CURRENT_TIMESTAMP - aggr_info.time_up) >= '7 days'::interval AND CURRENT_DATE < (( SELECT min(week_day.full_date) AS min
               FROM aggr_info aggr_info_1
                 LEFT JOIN week_day ON aggr_info_1.periodicl::text = week_day.week_day
              WHERE NOT week_day.dayoff AND week_day.full_date >= aggr_info_1.time_up::timestamp without time zone AND week_day.week_day = 'ПТ'::text)) THEN 'OK'::text
            WHEN aggr_info.periodicl::text = 'СБ'::text AND (CURRENT_TIMESTAMP - aggr_info.time_up) < '7 days'::interval THEN 'OK'::text
            WHEN aggr_info.periodicl::text = 'СБ'::text AND (CURRENT_TIMESTAMP - aggr_info.time_up) >= '7 days'::interval AND CURRENT_DATE < (( SELECT min(week_day.full_date) AS min
               FROM aggr_info aggr_info_1
                 LEFT JOIN week_day ON aggr_info_1.periodicl::text = week_day.week_day
              WHERE NOT week_day.dayoff AND week_day.full_date >= aggr_info_1.time_up::timestamp without time zone AND week_day.week_day = 'СБ'::text)) THEN 'OK'::text
            WHEN aggr_info.periodicl::text = 'ВС'::text AND (CURRENT_TIMESTAMP - aggr_info.time_up) < '7 days'::interval THEN 'OK'::text
            WHEN aggr_info.periodicl::text = 'ВС'::text AND (CURRENT_TIMESTAMP - aggr_info.time_up) >= '7 days'::interval AND CURRENT_DATE < (( SELECT min(week_day.full_date) AS min
               FROM aggr_info aggr_info_1
                 LEFT JOIN week_day ON aggr_info_1.periodicl::text = week_day.week_day
              WHERE NOT week_day.dayoff AND week_day.full_date >= aggr_info_1.time_up::timestamp without time zone AND week_day.week_day = 'ВС'::text)) THEN 'OK'::text
            WHEN aggr_info.periodicl::text = 'РД'::text AND (CURRENT_TIMESTAMP - aggr_info.time_up) < '1 day'::interval THEN 'OK'::text
            WHEN aggr_info.periodicl::text = 'РД'::text AND CURRENT_DATE <> (( SELECT next_or_current_working_day.next_or_current_working_day
               FROM next_or_current_working_day)) AND (CURRENT_DATE - aggr_info.time_up::date) <= (( SELECT days_number_from_last_working_day.ok_interval
               FROM days_number_from_last_working_day)) THEN 'OK'::text
            WHEN aggr_info.periodicl::text = 'РД'::text AND CURRENT_DATE = (( SELECT next_or_current_working_day.next_or_current_working_day
               FROM next_or_current_working_day)) AND date_part('day'::text, CURRENT_TIMESTAMP - aggr_info.time_up) < (( SELECT days_number_from_last_working_day.ok_interval
               FROM days_number_from_last_working_day))::double precision THEN 'OK'::text
            WHEN aggr_info.periodicl::text = 'X'::text THEN 'X'::text
            WHEN aggr_info.periodicl::text = '5МИН'::text AND (CURRENT_TIMESTAMP - aggr_info.time_up) < '00:05:00'::interval THEN 'OK'::text
            WHEN aggr_info.periodicl::text = '10МИН'::text AND (CURRENT_TIMESTAMP - aggr_info.time_up) < '00:10:00'::interval THEN 'OK'::text
            WHEN aggr_info.periodicl::text = '20МИН'::text AND (CURRENT_TIMESTAMP - aggr_info.time_up) < '00:20:00'::interval THEN 'OK'::text
            WHEN aggr_info.periodicl::text = 'ЧС'::text AND (CURRENT_TIMESTAMP - aggr_info.time_up) < '01:00:00'::interval THEN 'OK'::text
            WHEN aggr_info.table_name::text = 'currency_rate'::text AND (date_part('dow'::text, CURRENT_TIMESTAMP) = ANY (ARRAY[0::double precision, 1::double precision])) THEN 'OK'::text
            ELSE 'WARNING'::text
        END AS status,
    date_part('year'::text, aggr_info.update_timestamp::timestamp without time zone) AS update_year,
    date_part('month'::text, aggr_info.update_timestamp::timestamp without time zone) AS update_month,
    date_part('day'::text, aggr_info.update_timestamp::timestamp without time zone) AS update_day,
    date_part('hour'::text, aggr_info.update_timestamp::timestamp without time zone) AS update_hour,
    to_char(aggr_info.update_timestamp::timestamp without time zone, 'dd.MM.YYYY'::text) AS update_timestamp_text,
    date_trunc('day'::text, aggr_info.time_up) AS time_up_without_time,
    aggr_info.nifi_url
   FROM aggr_info;