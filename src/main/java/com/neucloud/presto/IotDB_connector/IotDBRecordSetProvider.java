package com.neucloud.presto.IotDB_connector;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.neucloud.presto.IotDB_connector.IotDBRecordSet;

public class IotDBRecordSetProvider implements ConnectorRecordSetProvider {
	private static Logger log = LoggerFactory.getLogger(IotDBRecordSetProvider.class);

	private static IotDBRecordSetProvider single;

	public static IotDBRecordSetProvider getInstance() {
		log.debug("初始化IotDBRecordSetProvider");
		if (single == null) {
			single = new IotDBRecordSetProvider();
		}
		return single;
	}

	public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
			ConnectorSplit split, List<? extends ColumnHandle> columns) {
		IotDBSplit IotDBSplit = (IotDBSplit) split;
		log.debug("------------------------IotDBRecordSetProvider:{}", IotDBSplit.getTableName());
			
		ImmutableList.Builder<IotDBColumnHandle> handles = ImmutableList.builder();
		for (ColumnHandle handle : columns) {
			IotDBColumnHandle IotDBHandle = (IotDBColumnHandle) handle;
			log.debug("------------------------handleName:{},handleType:{}", IotDBHandle.getColumnName(),IotDBHandle.getColumnType());
			handles.add(IotDBHandle);
		}

		return new IotDBRecordSet(IotDBSplit, handles.build());
	}

}
