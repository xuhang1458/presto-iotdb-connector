package com.neucloud.presto.IotDB_connector;

import java.util.ArrayList;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;


public class IotDBPlugin  implements Plugin{

	public Iterable<ConnectorFactory> getConnectorFactories() {
		ArrayList<ConnectorFactory> list = new ArrayList<ConnectorFactory>();
		list.add(new IotDBConnectorFactory());
		return list;
	}

	
}
