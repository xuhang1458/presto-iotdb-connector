package com.neucloud.presto.IotDB_connector;

import java.util.Objects;

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class IotDBTableLayoutHandle implements ConnectorTableLayoutHandle {
	private final IotDBTableHandle table;

	@JsonCreator
	public IotDBTableLayoutHandle(@JsonProperty("table") IotDBTableHandle table) {
		this.table = table;
	}

	@JsonProperty
	public IotDBTableHandle getTable() {
		return table;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		IotDBTableLayoutHandle that = (IotDBTableLayoutHandle) o;
		return Objects.equals(table, that.table);
	}

	@Override
	public int hashCode() {
		return Objects.hash(table);
	}

	@Override
	public String toString() {
		return table.toString();
	}
}
