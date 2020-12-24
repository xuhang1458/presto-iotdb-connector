package com.neucloud.presto.IotDB_connector.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnMetadata;
import com.neucloud.presto.IotDB_connector.IotDBColumnHandle;

public class IotDBUtil {

	private static final Logger log = LoggerFactory.getLogger(IotDBUtil.class);

	public static ArrayList<String> getSchemaNames() {
		return executeQuery("SHOW STORAGE GROUP");
	}

	public static ArrayList<String> getTableNames(String schemaName) {
		String sql = "show devices " + schemaName;
		return executeQuery(sql);
	}

	public static ArrayList<ColumnMetadata> getColumn(String tableName) {
		log.debug("查找Column:"+tableName);
		ArrayList<ColumnMetadata> reslutList = null;
		String sql = "SHOW TIMESERIES " + tableName;
		Connection connection = C3P0Util.getConnection();
		if (connection == null) {
			log.error("get connection defeat");
			return reslutList;
		}
		Statement statement;
		try {
			statement = connection.createStatement();
			statement.execute(sql);
			ResultSet resultSet = statement.getResultSet();
			reslutList = outputColumn(resultSet);
			C3P0Util.release(connection, statement, resultSet);
		} catch (SQLException e1) {
			e1.printStackTrace();
		}

		return reslutList;
	}

	public static Iterator<IotDBRow> select(String tableName, List<IotDBColumnHandle> columnHandles) {
		String sql = "select * from " + tableName;
		log.debug("执行sql："+sql);
		ArrayList<IotDBRow> reslutList = null;
		Connection connection = C3P0Util.getConnection();
		if (connection == null) {
			log.error("get connection defeat");
			return null;
		}

		try {
			Statement statement = connection.createStatement();
			statement.execute(sql);
			ResultSet resultSet = statement.getResultSet();
			log.debug("执行结果："+resultSet);
			reslutList = outputData(resultSet, columnHandles);
			C3P0Util.release(connection, statement, resultSet);
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		return reslutList.iterator();
	}

	private static ArrayList<ColumnMetadata> outputColumn(ResultSet resultSet) throws SQLException {
		ArrayList<ColumnMetadata> resultList = new ArrayList<ColumnMetadata>();
		if (resultSet != null) {
			while (resultSet.next()) {
				String columnName = resultSet.getString("timeseries");
				String columnType = resultSet.getString("dataType");

				switch (columnType) {
				case "BOOLEAN":
					resultList.add(new ColumnMetadata(columnName, BooleanType.BOOLEAN));
					break;
				case "INT32":
					resultList.add(new ColumnMetadata(columnName, IntegerType.INTEGER));
					break;
				case "INT64":
					resultList.add(new ColumnMetadata(columnName, BigintType.BIGINT));
					break;
				case "FLOAT":
					resultList.add(new ColumnMetadata(columnName, DoubleType.DOUBLE));
					break;
				case "DOUBLE":
					resultList.add(new ColumnMetadata(columnName, DoubleType.DOUBLE));
					break;
				case "TEXT":
					resultList.add(new ColumnMetadata(columnName, VarcharType.createUnboundedVarcharType()));
					break;
				}

			}
			resultList.add(new ColumnMetadata("timestamp", VarcharType.createUnboundedVarcharType()));
		}
		return resultList;
	}

	private static ArrayList<String> executeQuery(String sql) {
		ArrayList<String> reslutList = null;
		Connection connection = C3P0Util.getConnection();
		if (connection == null) {
			log.error("get connection defeat");
			return reslutList;
		}
		Statement statement;
		try {
			statement = connection.createStatement();
			statement.execute(sql);
			ResultSet resultSet = statement.getResultSet();
			reslutList = outputResult(resultSet);
			C3P0Util.release(connection, statement, resultSet);
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		return reslutList;
	}

	private static ArrayList<String> outputResult(ResultSet resultSet) throws SQLException {
		ArrayList<String> resultList = new ArrayList<String>();
		if (resultSet != null) {
			final ResultSetMetaData metaData = resultSet.getMetaData();
			final int columnCount = metaData.getColumnCount();

			while (resultSet.next()) {
				StringBuilder row = new StringBuilder();
				for (int i = 1;; i++) {
					row.append(resultSet.getString(i));
					if (i < columnCount) {
						row.append(",");
					} else {
						break;
					}
				}

				resultList.add(row.toString());
			}
		}
		return resultList;
	}

	private static ArrayList<IotDBRow> outputData(ResultSet resultSet, List<IotDBColumnHandle> columnHandles)
			throws SQLException {
		ArrayList<IotDBRow> resultList = new ArrayList<IotDBRow>();
		if (resultSet != null) {
			while (resultSet.next()) {
				HashMap<String, String> map = new HashMap<String, String>();
				for (IotDBColumnHandle column : columnHandles) {
					String columnName = column.getColumnName();
					log.debug("当前column："+columnName);
					String value = "";
					if(columnName.contentEquals("timestamp")) {
						value = resultSet.getString("Time");
					}else {
						value = resultSet.getString(columnName);
					}
					map.put(columnName, value);
				}
				IotDBRow row = new IotDBRow(map);
				resultList.add(row);
			}
		}
		return resultList;
	}
	
	
}
