package com.neucloud.presto.IotDB_connector;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

public class IotDBHandleResolver implements ConnectorHandleResolver {

	public Class<? extends ConnectorTableHandle> getTableHandleClass() {
		return IotDBTableHandle.class;
	}

	public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass() {
		return IotDBTableLayoutHandle.class;
	}

	public Class<? extends ColumnHandle> getColumnHandleClass() {
		return IotDBColumnHandle.class;
	}

	public Class<? extends ConnectorSplit> getSplitClass() {
		return IotDBSplit.class;
	}
	
	public Class<? extends ConnectorTransactionHandle> getTransactionHandleClass() {
		return IotDBTransactionHandle.class;
	}


}
