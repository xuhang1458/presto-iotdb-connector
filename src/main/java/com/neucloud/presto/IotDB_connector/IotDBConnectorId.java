package com.neucloud.presto.IotDB_connector;

import static java.util.Objects.requireNonNull;

import java.util.Objects;

public class IotDBConnectorId {
	private final String id;

	public IotDBConnectorId(String id) {
		this.id = requireNonNull(id, "id is null");
	}

	@Override
	public String toString() {
		return id;
	}

	@Override
	public int hashCode() {
		return Objects.hash(id);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if ((obj == null) || (getClass() != obj.getClass())) {
			return false;
		}

		IotDBConnectorId other = (IotDBConnectorId) obj;
		return Objects.equals(this.id, other.id);
	}
}
