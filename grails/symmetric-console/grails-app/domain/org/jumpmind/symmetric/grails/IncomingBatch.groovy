package org.jumpmind.symmetric.grails;

import org.codehaus.groovy.grails.commons.ConfigurationHolder

class IncomingBatch implements Serializable {
	static transients = ['persistable', 'nodeBatchId', 'retry']
	private @Delegate org.jumpmind.symmetric.model.IncomingBatch delegate = new org.jumpmind.symmetric.model.IncomingBatch()
	
	static mapping = {
		def config = ConfigurationHolder.config
		table config.symmetric.sync.table.prefix + '_incoming_batch'
		version false
		id name: 'batchId', generator: 'assigned'
		
		lastUpdatedTime column: 'LAST_UPDATE_TIME'
		lastUpdatedHostName column: 'LAST_UPDATE_HOSTNAME'
		autoTimestamp false
	}
}
