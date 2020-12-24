package com.neucloud.presto.IotDB_connector.util;

import java.util.HashMap;

public class IotDBRow {

	private HashMap<String, String> columnMap;

	public IotDBRow() {
	}

	public IotDBRow(HashMap<String, String> columnMap) {
		this.columnMap = columnMap;
	}

	public HashMap<String, String> getColumnMap() {
		return columnMap;
	}

	public void setColumnMap(HashMap<String, String> columnMap) {
		this.columnMap = columnMap;
	}

}
