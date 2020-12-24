package com.neucloud.presto.IotDB_connector;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class IotDBSplit  implements ConnectorSplit{
	private final String schemaName;
	private final String tableName;

	@JsonCreator
	public IotDBSplit(@JsonProperty("schemaName") String schemaName, @JsonProperty("tableName") String tableName) {

		this.schemaName = requireNonNull(schemaName, "schema name is null");
		this.tableName = requireNonNull(tableName, "table name is null");
	}

	@JsonProperty
	public String getSchemaName() {
		return schemaName;
	}

	@JsonProperty
	public String getTableName() {
		return tableName;
	}

	public NodeSelectionStrategy getNodeSelectionStrategy() {
		return NodeSelectionStrategy.NO_PREFERENCE;
	}

	public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates) {
		return new ArrayList<HostAddress>();
	}

	public Object getInfo() {
		return this;
	}

}
