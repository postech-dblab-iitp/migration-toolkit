/*
 * Copyright (C) 2009 Search Solution Corporation. All rights reserved by Search Solution. 
 *
 * Redistribution and use in source and binary forms, with or without modification, 
 * are permitted provided that the following conditions are met: 
 *
 * - Redistributions of source code must retain the above copyright notice, 
 *   this list of conditions and the following disclaimer. 
 *
 * - Redistributions in binary form must reproduce the above copyright notice, 
 *   this list of conditions and the following disclaimer in the documentation 
 *   and/or other materials provided with the distribution. 
 *
 * - Neither the name of the <ORGANIZATION> nor the names of its contributors 
 *   may be used to endorse or promote products derived from this software without 
 *   specific prior written permission. 
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND 
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. 
 * IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, 
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, 
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, 
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY 
 * OF SUCH DAMAGE. 
 *
 */
package com.cubrid.cubridmigration.core.engine.template;

/**
 * TemplateTags Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-9-30 created by Kevin Cao
 */
public final class TemplateTags {

	private TemplateTags() {
		//Hide the constructor
	}

	public static final String ATTR_AUTO_INCREMENT = "auto_increment";
	public static final String ATTR_BASE_TYPE = "base_type";
	public static final String ATTR_CACHE = "cache";
	public static final String ATTR_CACHE_SIZE = "cache_size";
	public static final String ATTR_CHARSET = "charset";
	public static final String ATTR_COMMIT_COUNT = "commit_count";
	public static final String ATTR_CREATE = "create";
	//public static final String ATTR_CREATE_DB = "create_db";
	public static final String ATTR_CYCLE = "cycle";
	public static final String ATTR_DB_TYPE = "db_type";
	public static final String ATTR_DBA_PASSWORD = "dba_password";
	public static final String ATTR_DEFAULT = "default";
	public static final String ATTR_DRIVER = "driver";
	public static final String ATTR_EXPORT_THREAD = "export_thread";
	public static final String ATTR_EXPRESSION = "expression";
	public static final String ATTR_FIELDS = "fields";
	public static final String ATTR_HOST = "host";
	public static final String ATTR_IMPORT_THREAD = "import_thread";
	public static final String ATTR_IS_SELECTED = "is_selected";
	public static final String ATTR_INCREMENT = "increment";
	public static final String ATTR_LOAD_ONLY = "load_only";
	public static final String ATTR_LOCATION = "location";
	public static final String ATTR_MAX = "max";
	public static final String ATTR_MIGRATE_DATA = "migrate_data";
	public static final String ATTR_MIN = "min";
	public static final String ATTR_NAME = "name";
	public static final String ATTR_NO_LOGGING = "no_logging";
	public static final String ATTR_NO_MAX = "no_max";
	public static final String ATTR_NO_MIN = "no_min";
	public static final String ATTR_NO_OID = "no_oid";
	public static final String ATTR_NO_STATISTICS = "no_statistics";
	public static final String ATTR_NULL = "null";
	public static final String ATTR_ON_CACHE_OBJECT = "on_cache_object";
	public static final String ATTR_ON_DELETE = "on_delete";
	public static final String ATTR_ON_UPDATE = "on_update";
	public static final String ATTR_ONLINE = "online";
	public static final String ATTR_OPTIMIZE_DB = "optimize_db";
	public static final String ATTR_ORDER = "order";
	public static final String ATTR_ORDER_RULE = "order_rule";
	public static final String ATTR_PAGE_SIZE = "page_size";
	public static final String ATTR_PARTITION = "partition";
	public static final String ATTR_PASSWORD = "password";
	public static final String ATTR_PATH = "path";
	public static final String ATTR_PK = "pk";
	public static final String ATTR_PORT = "port";
	//public static final String ATTR_PRE_FIX = "pre_fix";
	public static final String ATTR_REF_FIELDS = "ref_fields";
	public static final String ATTR_REF_TABLE = "ref_table";
	public static final String ATTR_REPLACE = "replace";
	public static final String ATTR_REVERSE = "reverse";
	public static final String ATTR_SIZE = "size";
	public static final String ATTR_START = "start";
	public static final String ATTR_SUB_TYPE = "sub_type";
	public static final String ATTR_TARGET = "target";
	public static final String ATTR_TIMEZONE = "timezone";
	public static final String ATTR_TYPE = "type";
	public static final String ATTR_UNIQUE = "unique";
	public static final String ATTR_USER = "user";
	public static final String ATTR_VALUE = "value";
	public static final String ATTR_VERSION = "version";
	//public static final String ATTR_WARNING_RATE = "warning_rate";
	public static final String ATTR_DIR = "dir";
	public static final String ATTR_SCHEMA = "schema";
	public static final String ATTR_DATA = "data";
	public static final String ATTR_INDEX = "index";

	public static final String ATTR_TRIM = "trim";
	public static final String ATTR_REPLACE_EXPRESSION = "rep_exp";
	public static final String ATTR_CONDITION = "condition";

	//public static final String TAG_AUTOADDDATAVOLUME = "autoAddDataVolume";
	//public static final String TAG_AUTOADDINDEXVOLUME = "autoAddIndexVolume";
	public static final String TAG_CMSERVER = "cmServer";
	public static final String TAG_COLUMN = "column";
	public static final String TAG_COLUMNS = "columns";
	public static final String TAG_CONSTRAINTS = "constraints";
	public static final String TAG_CREATEDB = "createDB";
	public static final String TAG_CREATEVIEWSQL = "createViewSQL";
	public static final String TAG_EXISTDB = "existDB";
	public static final String TAG_FILE = "file";
	public static final String TAG_FK = "fk";
	public static final String TAG_FUNCTION = "function";
	public static final String TAG_FUNCTIONS = "functions";
	//public static final String TAG_GENERICVOLUME = "genericVolume";
	public static final String TAG_HASH = "hash";
	public static final String TAG_INDEX = "index";
	public static final String TAG_JDBC = "jdbc";
	public static final String TAG_LIST = "list";
	public static final String TAG_LOADDBSETTING = "loadDBSetting";
	//public static final String TAG_LOGVOLUME = "logVolume";
	public static final String TAG_MIGRATION = "migration";
	//public static final String TAG_NEWDBUSER = "newDBUser";
	public static final String TAG_PARAMS = "params";
	public static final String TAG_PARTITIONS = "partitions";
	public static final String TAG_PK = "pk";
	public static final String TAG_PROCEDURE = "procedure";
	public static final String TAG_PROCEDURES = "procedures";
	public static final String TAG_RANGE = "range";
	public static final String TAG_SEQUENCE = "sequence";
	public static final String TAG_SEQUENCES = "sequences";
	public static final String TAG_SOURCE = "source";
	public static final String TAG_SQLTABLE = "sqlTable";
	public static final String TAG_SQLTABLES = "sqlTables";
	public static final String TAG_STATEMENT = "statement";
	public static final String TAG_TABLE = "table";
	public static final String TAG_TABLES = "tables";
	public static final String TAG_TARGET = "target";
	public static final String TAG_TRIGGER = "trigger";
	public static final String TAG_TRIGGERS = "triggers";
	public static final String TAG_VIEW = "view";
	public static final String TAG_VIEWCOLUME = "viewColume";
	public static final String TAG_VIEWCOLUMN = "viewColumn";
	public static final String TAG_VIEWCOLUMNS = "viewColumns";
	public static final String TAG_VIEWQUERYSQL = "viewQuerySQL";
	public static final String TAG_VIEWS = "views";
	public static final String TAG_VOLUME = "volume";
	//public static final String TAG_ADDITIONALVOLUMES = "additionalVolumes";
	public static final String TAG_FILE_REPOSITORY = "fileRepository";
	public static final String TAG_PARTITION_DDL = "partitionDDL";

	public static final String TAG_SQL = "sql_source";
	public static final String TAG_SQL_FILE = "sql_file";

	public static final String VALUE_HASH = "hash";
	public static final String VALUE_LIST = "list";
	public static final String VALUE_NO = "no";
	public static final String VALUE_RANGE = "range";
	public static final String VALUE_YES = "yes";

	//graph tags
	public static final String TAG_VERTEXES = "vertexes";
	public static final String TAG_VERTEX = "vertex";
	public static final String TAG_PROPERTY = "property";
	public static final String TAG_EDGES = "edges";
	public static final String TAG_EDGE = "edge";
	
	//graph attributes
	public static final String ATTR_LABEL = "label";
	public static final String ATTR_START_VERTEX = "start_vertex";
	public static final String ATTR_END_VERTEX = "end_vertex";
	
	public static final String VALUE_ONLINE = "online";
	public static final String VALUE_GRAPH = "graph";
	public static final String VALUE_OFFLINE = "offline";
	public static final String VALUE_DIR = "dir";
	public static final String ATTR_ONETABLEONEFILE = "one_tale_one_file";
	public static final String ATTR_SHARED = "shared";
	public static final String ATTR_SHARED_VALUE = "shared_value";
	public static final String ATTR_DATA_FILE_FORMAT = "data_file_format";
	public static final String ATTR_CSV_SEPARATE = "csv_separate";
	public static final String ATTR_CSV_QUOTE = "csv_quote";
	public static final String ATTR_CSV_ESCAPE = "csv_escape";
	public static final String ATTR_CREATE_CONSTRAINT_NOW = "create_constraints_before_data";
	public static final String ATTR_REUSE_OID = "reuse_oid";
	public static final String ATTR_WRITE_ERROR_RECORDS = "write_error_records";
	public static final String ATTR_BEFORE_SQL = "sql_before";
	public static final String ATTR_AFTER_SQL = "sql_after";
	public static final String ATTR_DEFAULT_EXPRESSION = "expression_default";
	public static final String ATTR_USER_JDBC_URL = "user_jdbc_url";
	public static final String ATTR_IMPLICIT_ESTIMATE_PROGRESS = "implicit_estimate_progress";
	public static final String ATTR_FILE_MAX_SIZE = "file_max_size";
	public static final String TAG_CSVS = "csvs";
	public static final String TAG_CSV = "csv";
	public static final String TAG_CSV_COLUMNS = "csv_columns";
	public static final String TAG_CSV_COLUMN = "csv_column";
	public static final String ATTR_IMPORT_FIRST_ROW = "import_first_row";
	public static final String ATTR_CSV_NULL_VALUE = "csv_null_value";
	public static final String ATTR_OUTPUT_FILE_PREFIX = "file_prefix";
	public static final String ATTR_USER_DATA_HANDLER = "data_handler";
	public static final String ATTR_PAGE_FETCH_COUNT = "page_fetch_count";
	public static final String ATTR_LOB_ROOT_DIR = "lob_root_dir";
	public static final String ATTR_EXP_OPT_COL = "exp_opt_col";
	public static final String ATTR_START_TAR_MAX = "start_target_max";
	public static final String ATTR_OWNER = "owner";
	public static final String TAG_SCHEMA = "schema";
	public static final String TAG_SQL_SCHEMA = "sql_schema";
	public static final String ATTR_UPDATE_STATISTICS = "update_statistics";
	public static final String ATTR_AUTO_SYNCHRONIZE_START_VALUE = "auto_synchronize_start_value";
}
