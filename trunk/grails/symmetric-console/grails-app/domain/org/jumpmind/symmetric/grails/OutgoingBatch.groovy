package org.jumpmind.symmetric.grails;

import java.util.Date;

import org.codehaus.groovy.grails.commons.ConfigurationHolder
import org.jumpmind.symmetric.model.OutgoingBatch.Status;

class OutgoingBatch implements Serializable {
	static transients = [ 'batchInfo', 'nodeBatchId']

	Long batchId
	String nodeId
	String channelId
	Status status
	Long routerMillis
	Long networkMillis
	Long filterMillis
	Long loadMillis
    Long extractMillis;
	Long byteCount;
	Long sentCount;
	Long dataEventCount;
	Long failedDataId;
	String sqlState;
	Integer sqlCode;
	String sqlMessage;
	String lastUpdatedHostName;
	Date lastUpdatedTime;
	Date createTime;

	                     
	// Removed delegate until primitive fields have a default value in the database and are not nullable
	//private @Delegate org.jumpmind.symmetric.model.OutgoingBatch delegate = new org.jumpmind.symmetric.model.OutgoingBatch()
	
	static mapping = {
		def config = ConfigurationHolder.config
		table config.symmetric.sync.table.prefix + '_outgoing_batch'
		version false
		id name: 'batchId', generator: 'assigned'
		
		lastUpdatedTime column: 'LAST_UPDATE_TIME'
		lastUpdatedHostName column: 'LAST_UPDATE_HOSTNAME'

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
