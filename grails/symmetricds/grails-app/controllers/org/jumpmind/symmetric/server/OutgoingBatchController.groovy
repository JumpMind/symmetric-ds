package org.jumpmind.symmetric.server

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
			total = OutgoingBatch.findAllByNodeIdAndStatus(params.nodeId, params.status).size()
		}
		else if (params.status != null) {
			list = OutgoingBatch.findAllByStatus(params.status, params)
			total = OutgoingBatch.findAllByStatus(params.status).size()
		}
		else if (params.nodeId != null) {
			list = OutgoingBatch.findAllByNodeId(params.nodeId, params)
			total = OutgoingBatch.findAllByNodeId(params.nodeId).size()
		}
		else {
			list = OutgoingBatch.list(params)
			total = OutgoingBatch.count()
		}
		[outgoingBatchInstanceList: list, outgoingBatchInstanceTotal: total]
	}

    def show = {
        def outgoingBatchInstance = OutgoingBatch.get(params.id)
        if (!outgoingBatchInstance) {
            flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'outgoingBatch.label', default: 'OutgoingBatch'), params.id])}"
            redirect(action: "list")
        }
        else {
            [outgoingBatchInstance: outgoingBatchInstance]
        }
    }

    def edit = {
        def outgoingBatchInstance = OutgoingBatch.get(params.id)
        if (!outgoingBatchInstance) {
            flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'outgoingBatch.label', default: 'OutgoingBatch'), params.id])}"
            redirect(action: "list")
        }
        else {
            return [outgoingBatchInstance: outgoingBatchInstance]
        }
    }

    def delete = {
        def outgoingBatchInstance = OutgoingBatch.get(params.id)
        if (outgoingBatchInstance) {
            try {
                outgoingBatchInstance.delete(flush: true)
                flash.message = "${message(code: 'default.deleted.message', args: [message(code: 'outgoingBatch.label', default: 'OutgoingBatch'), params.id])}"
                redirect(action: "list")
            }
            catch (org.springframework.dao.DataIntegrityViolationException e) {
                flash.message = "${message(code: 'default.not.deleted.message', args: [message(code: 'outgoingBatch.label', default: 'OutgoingBatch'), params.id])}"
                redirect(action: "show", id: params.id)
            }
        }
        else {
            flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'outgoingBatch.label', default: 'OutgoingBatch'), params.id])}"
            redirect(action: "list")
        }
    }
}
