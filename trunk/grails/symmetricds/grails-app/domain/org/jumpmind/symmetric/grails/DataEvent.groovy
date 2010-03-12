package org.jumpmind.symmetric.grails;

import java.io.Serializable;

import org.codehaus.groovy.grails.commons.ConfigurationHolder

class DataEvent implements Serializable {
	Data data
	OutgoingBatch batch
	Router router
	
	static mapping = {
		def config = ConfigurationHolder.config
		table config.symmetric.sync.table.prefix + '_data_event'
		version false
		
		data column: 'data_id'
		batch column: 'batch_id'
		router column: 'router_id'
		id composite: ['data', 'batch', 'router'], generator: 'assigned'
		
		autoTimestamp false
	}
}
