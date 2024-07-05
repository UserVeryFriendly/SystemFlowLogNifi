CREATE TABLE tech.tech_cfg_control_table (
	table_name varchar NOT NULL,
	incident_id varchar(9) NOT NULL,
	dataflow_id varchar(9) NOT NULL,
	dataflow_desc varchar NOT NULL,
	update_timestamp_column_name varchar NOT NULL,
	maxrecdate_column_name varchar NULL,
	periodicl varchar NOT NULL,
	shuptime time NULL,
	data_engineer varchar NOT NULL,
	schema_name varchar NOT NULL,
	frequency varchar NULL,
	deprecated bool NULL,
	nifi_url varchar NULL,
	CONSTRAINT tech_cfg_control_table_pk PRIMARY KEY (table_name)
);

CREATE TABLE tech.logs_control_task (
	schema_name varchar NULL,
	table_name varchar NULL,
	incident_id varchar NULL,
	dataflow_id varchar NULL,
	periodicl varchar NULL,
	last_timestamp varchar NULL,
	status varchar NULL,
	stage varchar NULL,
	data_engineer varchar NULL,
	id_task varchar NULL,
	update_timestamp timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	assignee_name varchar NULL,
	type_of_problem varchar NULL
);