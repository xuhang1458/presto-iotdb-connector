package com.neucloud.presto.IotDB_connector;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.RecordCursor;
import com.google.common.base.Strings;
import com.neucloud.presto.IotDB_connector.util.IotDBRow;
import com.neucloud.presto.IotDB_connector.util.IotDBUtil;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

public class IotDBRecordCursor implements RecordCursor {

	private Logger log = LoggerFactory.getLogger(IotDBRecordCursor.class);

	private final List<IotDBColumnHandle> columnHandles;
	private final Iterator<IotDBRow> iterator;
	private IotDBRow row;
	
	
	public IotDBRecordCursor(List<IotDBColumnHandle> columnHandles, IotDBSplit split) {
		this.columnHandles = columnHandles;
		this.iterator = IotDBUtil.select(split.getTableName(), columnHandles);
	}

	@Override
	public long getCompletedBytes() {
		return 0;
	}

	@Override
	public long getReadTimeNanos() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Type getType(int field) {
		checkArgument(field < columnHandles.size(), "Invalid field index");
		return columnHandles.get(field).getColumnType();
	}

	@Override
	public boolean advanceNextPosition() {
		log.debug("advanceNextPosition--------------------------");
		if (!iterator.hasNext()) {
			return false;
		}
		log.debug("advanceNextPosition-------------------------hasNext");
		this.row = iterator.next();
		return true;
	}

	@Override
	public boolean getBoolean(int field) {
		log.debug("getBoolean-------------------------");
		checkFieldType(field, BooleanType.BOOLEAN);
		String columnName = columnHandles.get(field).getColumnName();
		return Boolean.parseBoolean(row.getColumnMap().get(columnName));
	}

	@Override
	public long getLong(int field) {
		log.debug("getLong-------------------------");
		checkFieldType(field, BigintType.BIGINT);
		String columnName = columnHandles.get(field).getColumnName();
		String value = row.getColumnMap().get(columnName);
		return Long.parseLong(value);
	}

	@Override
	public double getDouble(int field) {
		log.debug("getDouble-------------------------");
		checkFieldType(field, DoubleType.DOUBLE);
		String columnName = columnHandles.get(field).getColumnName();
		String value = row.getColumnMap().get(columnName);
		return Double.parseDouble(value);
	}

	@Override
	public Slice getSlice(int field) {
		log.debug("getSlice-------------------------");
		checkFieldType(field, VarcharType.createUnboundedVarcharType());
		String columnName = columnHandles.get(field).getColumnName();
		String value = row.getColumnMap().get(columnName);
		return Slices.utf8Slice(value);
	}

	@Override
	public Object getObject(int field) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isNull(int field) {
		log.debug("isNull-------------------------");
		checkArgument(field < columnHandles.size(), "Invalid field index");
		String columnName = columnHandles.get(field).getColumnName();
		String value = row.getColumnMap().get(columnName);
		return Strings.isNullOrEmpty(value);
	}

	@Override
	public void close() {
		log.debug("close-------------------------");
	}

	private void checkFieldType(int field, Type expected) {
		log.debug("checkFieldType-------------------------");
		Type actual = getType(field);
		checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
	}
}
