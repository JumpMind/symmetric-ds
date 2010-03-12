package org.jumpmind.symmetric.grails;

import java.io.Serializable;

import org.codehaus.groovy.grails.commons.ConfigurationHolder
import org.jumpmind.symmetric.model.DataEventType;

class Data implements Serializable {
	static transients = [ 'triggerHistory', 'eventType']
	static hasMany = [ dataEvents : DataEvent ]
	                   
	private @Delegate org.jumpmind.symmetric.model.Data delegate = new org.jumpmind.symmetric.model.Data()

	static mapping = {
		def config = ConfigurationHolder.config
		table config.symmetric.sync.table.prefix + '_data'
		version false
		id name : 'dataId', generator: 'assigned'
		
		autoTimestamp false
	}
}
