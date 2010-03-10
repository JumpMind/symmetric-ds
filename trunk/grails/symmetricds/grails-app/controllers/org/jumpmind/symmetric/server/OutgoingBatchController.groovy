package org.jumpmind.symmetric.server;

import org.jumpmind.symmetric.grails.OutgoingBatch

class OutgoingBatchController {
	
	static allowedMethods = [save: "POST", update: "POST", delete: "POST"]
	
	def index = {
		redirect(action: "list", params: params)
	}

	def list = {
		session.menu="monitor-outgoing"
		
		params.max = 20
		
		def total = 0
		def list = null
		
		if (params.nodeId != null && params.status != null) {
			list = OutgoingBatch.findAllByNodeIdAndStatus(params.nodeId, params.status, params)
		}
		else if (params.status != null) {
			list = OutgoingBatch.findAllByStatus(params.status, params)
		}
		else if (params.nodeId != null) {
			list = OutgoingBatch.findAllByNodeId(params.nodeId, params)
		}
		else {
			list = OutgoingBatch.list(params)
			total = OutgoingBatch.count()
		}
		println "SIZE " + list.size()
		[outgoingBatchInstanceList: list, outgoingBatchInstanceTotal: total]
	}
		
}
