package com.neucloud.presto.IotDB_connector;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.neucloud.presto.IotDB_connector.util.C3P0Util;

public class IotDBConnector implements Connector{

	private static final Logger log = LoggerFactory.getLogger(IotDBConnector.class);
	
	private final IotDBMetadata metadata;
	private final IotDBSplitManager splitManager;
	private final IotDBRecordSetProvider recordSetProvider;

	public IotDBConnector(String catalogName, Map<String, String> config) {
		log.debug("初始化IotDBConnector");
		C3P0Util.init(config);
		this.metadata = IotDBMetadata.getInstance(catalogName);
		this.splitManager = IotDBSplitManager.getInstance();
		this.recordSetProvider = IotDBRecordSetProvider.getInstance();
	}
	
	public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly) {
		return IotDBTransactionHandle.INSTANCE;
	}

	public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle) {
		return metadata;
	}

	public ConnectorSplitManager getSplitManager() {
		return splitManager;
	}
	
	
	public ConnectorRecordSetProvider getRecordSetProvider() {
		return recordSetProvider;
	}
}
