-- DROP PROCEDURE tech.update_ct_log(varchar, varchar, varchar);

CREATE OR REPLACE PROCEDURE tech.update_ct_log(table_schema character varying, table_name character varying, proc_group character varying DEFAULT ''::character varying)
 LANGUAGE plpgsql
AS $procedure$
	declare date_col_name varchar;
	declare uts_col_name varchar;
	declare max_date timestamptz:= NULL;
	declare max_updatestamp timestamptz:= NULL;
	declare cur_count int8;
	begin
		begin 
			execute format(
			'select maxrecdate_column_name 
			from tech.tech_cfg_control_table 
			where table_name = %1$L', table_name
			) into date_col_name;
			execute format(
			'select max(%1$I) 
			from %2$I.%3$I', date_col_name, table_schema, table_name
			) into max_date;			
		exception 
			when others then
		end;
		begin 
			execute format(
			'select update_timestamp_column_name 
			from tech.tech_cfg_control_table
			where table_name = %1$L and schema_name = %2$L', table_name, table_schema
			) into uts_col_name;
			execute format(
			'select max(%1$I) 
			from %2$I.%3$I', uts_col_name, table_schema, table_name
			) into max_updatestamp;
			execute format(
			'select count(%1$I) 
			from %2$I.%3$I', uts_col_name, table_schema, table_name
			) into cur_count;
		exception 
			when others then
		end;
		if max_updatestamp is NULL then 
			begin 
				execute format(
				'select max(update_timestamp) 
				from %1$I.%2$I', table_schema, table_name
				) into max_updatestamp;
				execute format(
				'select count(update_timestamp) 
				from %1$I.%2$I', table_schema, table_name
				) into cur_count;
			exception 
				when others then
			end;
		END IF;		
		if max_updatestamp is NULL then 
			begin 
				execute format(
				'select count(*) 
				from %1$I.%2$I', table_schema, table_name
				) into cur_count;
				CALL tech.new_error(schema_name:=table_schema::varchar, 
											table_name:=table_name::varchar, 
											error_code:=''::varchar, 
											system_message:=''::varchar, 
											description:='Нет update_timestamp. Произведен подсчёт строк по всем столбцам'::varchar,
											proc_group:=proc_group::varchar);
			exception 
				when others then
			end;
		END IF;
		begin 
			EXECUTE format('insert into tech.tech_control_table_log 
							(table_name, rowscount, time_up, maxrecdate, schema_name) values 
							(%1$L, %2$L, %3$L, %4$L, %5$L)', 
							table_name, cur_count, max_updatestamp, max_date, table_schema);
		exception 
			when others then 
			CALL tech.new_error(schema_name:=table_schema::varchar, 
										table_name:=table_name::varchar, 
										error_code:=''::varchar, 
										system_message:=''::varchar, 
										description:='Ошибка в момент записи в лог'::varchar,
										proc_group:=proc_group::varchar);
		end;
	END;
$procedure$
;
