package org.jumpmind.symmetric.grails

import org.codehaus.groovy.grails.commons.ConfigurationHolder

class Node {
	static transients = [ 'nodeGroupId', 'batchStatus']
	private @Delegate org.jumpmind.symmetric.model.Node delegate = new org.jumpmind.symmetric.model.Node()
	
	String id
	NodeGroup nodeGroup

  static mapping = {
	def config = ConfigurationHolder.config
    table config.symmetric.sync.table.prefix + '_node'
    version false
    id name: 'nodeId', generator: 'assigned'
	nodeGroup column: 'node_group_id'
    autoTimestamp false
    syncUrl column: 'sync_url'

  }

  static constraints = {
    timezoneOffset(nullable: true)
    heartbeatTime(nullable: true)
    createdAtNodeId(nullable: true)
    syncUrl(nullable: true)
    schemaVersion(nullable: true)
    databaseType(nullable: true)
    symmetricVersion(nullable: true)
    databaseVersion(nullable: true)
  }
	
	public String getBatchStatus() {
		if (batchInErrorCount > 0) {
			return "ER"
		}
		else if (batchInErrorCount == 0 && batchToSendCount > 0) {
			return "PT"
		}
		else if (batchInErrorCount == 0 && batchToSendCount == 0) {
			return "OK"
		}
		else {
			return ""
		}
	}
}
