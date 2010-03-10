package org.jumpmind.symmetric.grails;

import org.codehaus.groovy.grails.commons.ConfigurationHolder

class OutgoingBatch implements Serializable {
	static transients = [ 'batchInfo', 'nodeBatchId' ]
	
	private @Delegate org.jumpmind.symmetric.model.OutgoingBatch delegate = new org.jumpmind.symmetric.model.OutgoingBatch()
	
	static mapping = {
		def config = ConfigurationHolder.config
		table config.symmetric.sync.table.prefix + '_outgoing_batch'
		version false
		id name: 'batchId', generator: 'assigned'
		autoTimestamp false
	}

	static constraints = {
		nodeId(nullable: true)
		channelId(nullable: true)
		status(nullable: true)
		routerMillis(nullable: true)
		networkMillis(nullable: true)
		filterMillis(nullable: true)
		loadMillis(nullable: true)
		extractMillis(nullable: true)
		byteCount(nullable: true)
		sentCount(nullable: true)
		dataEventCount(nullable: true)
		failedDataId(nullable: true)
		sqlState(nullable: true)
		sqlCode(nullable: true)
		sqlMessage(nullable: true)
		lastUpdatedHostName(nullable: true)
		lastUpdatedTime(nullable: true)
		createTime(nullable: true)
	}
}
