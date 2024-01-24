package com.cubrid.cubridmigration.core.engine.task.imp;

import com.cubrid.cubridmigration.core.engine.task.ImportTask;

public class QuickScriptImportTask extends ImportTask{
	protected void executeImport() {
		importer.importQuickScript();
	}
}
