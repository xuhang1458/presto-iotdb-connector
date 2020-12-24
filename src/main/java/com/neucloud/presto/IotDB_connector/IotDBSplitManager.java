package com.neucloud.presto.IotDB_connector;

import java.util.ArrayList;
import java.util.List;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

public class IotDBSplitManager implements ConnectorSplitManager {

	public static IotDBSplitManager single;

	public static IotDBSplitManager getInstance() {
		if (single == null) {
			single = new IotDBSplitManager();
		}
		return single;
	}

	private IotDBSplitManager() {
	}

	public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
			ConnectorTableLayoutHandle layout, SplitSchedulingContext splitSchedulingContext) {
		IotDBTableLayoutHandle layoutHandle = (IotDBTableLayoutHandle) layout;
		IotDBTableHandle tableHandle = layoutHandle.getTable();
		List<ConnectorSplit> splits = new ArrayList<>();
		splits.add(new IotDBSplit(tableHandle.getSchemaName(), tableHandle.getTableName()));

		return new FixedSplitSource(splits);
	}

}
