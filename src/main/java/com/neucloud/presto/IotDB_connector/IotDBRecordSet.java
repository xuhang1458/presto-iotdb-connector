package com.neucloud.presto.IotDB_connector;

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;
import com.neucloud.presto.IotDB_connector.IotDBColumnHandle;
import com.neucloud.presto.IotDB_connector.IotDBRecordSet;
import com.neucloud.presto.IotDB_connector.IotDBSplit;

public class IotDBRecordSet implements RecordSet {
	
	private Logger log = LoggerFactory.getLogger(IotDBRecordSet.class);
	private final List<IotDBColumnHandle> columnHandles;
	private final List<Type> columnTypes;
	private final IotDBSplit split;

	public IotDBRecordSet(IotDBSplit split, List<IotDBColumnHandle> columnHandles) {
		log.debug("IotDBRecordSet-------------");
		this.split = requireNonNull(split, "split is null");
		this.columnHandles = requireNonNull(columnHandles, "column handles is null");
		ImmutableList.Builder<Type> types = ImmutableList.builder();
		for (IotDBColumnHandle column : columnHandles) {
			types.add(column.getColumnType());
		}
		this.columnTypes = types.build();

	}

	@Override
	public List<Type> getColumnTypes() {
		return columnTypes;
	}

	@Override
	public RecordCursor cursor() {
		log.debug("cursor-------------");
		return new IotDBRecordCursor(columnHandles, split);
	}

}
