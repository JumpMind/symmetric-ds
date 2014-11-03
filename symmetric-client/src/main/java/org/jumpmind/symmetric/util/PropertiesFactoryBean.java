package org.jumpmind.symmetric.util;

import java.util.Properties;

public class PropertiesFactoryBean extends org.springframework.beans.factory.config.PropertiesFactoryBean {

	private static Properties localProperties;

	public PropertiesFactoryBean() {
		this.setLocalOverride(true);
		if (localProperties != null) {
			this.setProperties(localProperties);
		}
	}

	public static void setLocalProperties(Properties localProperties) {
		PropertiesFactoryBean.localProperties = localProperties;
	}

	public static void clearLocalProperties() {
		PropertiesFactoryBean.localProperties = null;
	}
}
