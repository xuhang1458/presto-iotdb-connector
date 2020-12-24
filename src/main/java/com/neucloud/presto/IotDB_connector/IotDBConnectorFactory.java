package com.neucloud.presto.IotDB_connector;

import java.util.Map;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;

public class IotDBConnectorFactory  implements ConnectorFactory{

	public String getName() {
		return "IotDB-connector";
	}

	public ConnectorHandleResolver getHandleResolver() {
		return new IotDBHandleResolver();
	}

	public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
		return new IotDBConnector(catalogName, config);
	}




}
