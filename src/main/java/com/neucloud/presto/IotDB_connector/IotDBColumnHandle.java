package com.neucloud.presto.IotDB_connector;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import java.util.Objects;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class IotDBColumnHandle implements ColumnHandle {
	private final String connectorId;
	private final String columnName;
	private final Type columnType;
	private final int ordinalPosition;

	@JsonCreator
	public IotDBColumnHandle(@JsonProperty("connectorId") String connectorId,
			@JsonProperty("columnName") String columnName, @JsonProperty("columnType") Type columnType,
			@JsonProperty("ordinalPosition") int ordinalPosition) {
		this.connectorId = requireNonNull(connectorId, "connectorId is null");
		this.columnName = requireNonNull(columnName, "columnName is null");
		this.columnType = requireNonNull(columnType, "columnType is null");
		this.ordinalPosition = ordinalPosition;
	}

	@JsonProperty
	public String getConnectorId() {
		return connectorId;
	}

	@JsonProperty
	public String getColumnName() {
		return columnName;
	}

	@JsonProperty
	public Type getColumnType() {
		return columnType;
	}

	@JsonProperty
	public int getOrdinalPosition() {
		return ordinalPosition;
	}

	public ColumnMetadata getColumnMetadata() {
		return new ColumnMetadata(columnName, columnType);
	}

	@Override
	public int hashCode() {
		return Objects.hash(connectorId, columnName);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if ((obj == null) || (getClass() != obj.getClass())) {
			return false;
		}

		IotDBColumnHandle other = (IotDBColumnHandle) obj;
		return Objects.equals(this.connectorId, other.connectorId) && Objects.equals(this.columnName, other.columnName);
	}

	@Override
	public String toString() {
		return toStringHelper(this).add("connectorId", connectorId).add("columnName", columnName)
				.add("columnType", columnType).add("ordinalPosition", ordinalPosition).toString();
	}
}
