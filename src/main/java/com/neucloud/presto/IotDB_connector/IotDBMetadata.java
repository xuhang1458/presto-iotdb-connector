package com.neucloud.presto.IotDB_connector;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.neucloud.presto.IotDB_connector.util.IotDBUtil;

public class IotDBMetadata implements ConnectorMetadata {

	private static final Logger log = LoggerFactory.getLogger(IotDBMetadata.class);

	private static IotDBMetadata single;

	private static String connectorId;

	public static IotDBMetadata getInstance(String catalogName) {
		if (single == null) {
			single = new IotDBMetadata(catalogName);
		}
		return single;
	}

	public IotDBMetadata(String catalogName) {
		connectorId = new IotDBConnectorId(catalogName).toString();
	}

	public List<String> listSchemaNames(ConnectorSession session) {
		log.debug("查询所有schemas");
		return IotDBUtil.getSchemaNames();
	}

	public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
		return new IotDBTableHandle(connectorId, tableName.getSchemaName(), tableName.getTableName());
	}

	public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table,
			Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns) {
		IotDBTableHandle tableHandle = (IotDBTableHandle) table;
		ConnectorTableLayout layout = new ConnectorTableLayout(new IotDBTableLayoutHandle(tableHandle));
		return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
	}

	public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle) {
		return new ConnectorTableLayout(handle);
	}

	public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
		IotDBTableHandle iotDBTable = (IotDBTableHandle) table;
		checkArgument(iotDBTable.getConnectorId().equals(connectorId), "tableHandle is not for this connector");
		List<ColumnMetadata> list = IotDBUtil.getColumn(iotDBTable.getTableName());
		SchemaTableName tableName = new SchemaTableName(iotDBTable.getSchemaName(), iotDBTable.getTableName());
		return new ConnectorTableMetadata(tableName, list);
	}

	public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
		IotDBTableHandle IotDBTable = (IotDBTableHandle) tableHandle;
		log.debug("调用getColumnHandles");
		checkArgument(IotDBTable.getConnectorId().equals(connectorId), "tableHandle is not for this connector");
		
		Map<String, ColumnHandle> map = new HashMap<String, ColumnHandle>();
		List<ColumnMetadata> list = IotDBUtil.getColumn(IotDBTable.getTableName());
		for (int i = 0; i < list.size(); i++) {
			ColumnMetadata meta = list.get(i);
			log.debug("准备meta： Name：{}， Type：{}", meta.getName(),meta.getType());
			map.put(meta.getName(), new IotDBColumnHandle(connectorId, meta.getName(), meta.getType(), i));
		}
		return map;
	}

	public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle,
			ColumnHandle columnHandle) {
		return ((IotDBColumnHandle) columnHandle).getColumnMetadata();
	}

	public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session,
			SchemaTablePrefix prefix) {
		requireNonNull(prefix, "prefix is null");
		Map<SchemaTableName, List<ColumnMetadata>> columns = new HashMap<SchemaTableName, List<ColumnMetadata>>();
		Optional<String> optional = null;
		List<SchemaTableName> list = listTables(session, optional);
		for (SchemaTableName table : list) {
			if (table.getTableName().startsWith(prefix.getTableName())) {
				columns.put(table, IotDBUtil.getColumn(table.getTableName()));
			}
		}
		return columns;
	}

	public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull) {
		log.debug("查所有表，schemaOrNull"+schemaNameOrNull);
		ArrayList<String> schemaNames;
		if (schemaNameOrNull != null) {
			schemaNames = new ArrayList<String>();
			schemaNames.add(schemaNameOrNull);
		} else {
			schemaNames = IotDBUtil.getSchemaNames();
		}

		ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
		for (String schemaName : schemaNames) {
			for (String tableName : IotDBUtil.getTableNames(schemaName)) {
				builder.add(new SchemaTableName(schemaName, tableName));
			}
		}
		return builder.build();
	}

	public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
		log.debug("查所有表，Optional "+schemaName.get());
		List<SchemaTableName> listTable = new ArrayList<SchemaTableName>();
		String schema = schemaName.get();
		for (String tableName : IotDBUtil.getTableNames(schema)) {
			listTable.add(new SchemaTableName(schema, tableName));
		}
		return listTable;
	}

}
