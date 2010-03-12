package org.jumpmind.symmetric.server

import org.jumpmind.symmetric.grails.IncomingBatch

class IncomingBatchController {

    static allowedMethods = [save: "POST", update: "POST", delete: "POST"]

    def index = {
        redirect(action: "list", params: params)
    }

    def list = {
        params.max = Math.min(params.max ? params.int('max') : 10, 100)
        [incomingBatchInstanceList: IncomingBatch.list(params), incomingBatchInstanceTotal: IncomingBatch.count()]
    }

    def create = {
        def incomingBatchInstance = new IncomingBatch()
        incomingBatchInstance.properties = params
        return [incomingBatchInstance: incomingBatchInstance]
    }

    def save = {
        def incomingBatchInstance = new IncomingBatch(params)
        if (incomingBatchInstance.save(flush: true)) {
            flash.message = "${message(code: 'default.created.message', args: [message(code: 'incomingBatch.label', default: 'IncomingBatch'), incomingBatchInstance.id])}"
            redirect(action: "show", id: incomingBatchInstance.id)
        }
        else {
            render(view: "create", model: [incomingBatchInstance: incomingBatchInstance])
        }
    }

    def show = {
        def incomingBatchInstance = IncomingBatch.get(params.id)
        if (!incomingBatchInstance) {
            flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'incomingBatch.label', default: 'IncomingBatch'), params.id])}"
            redirect(action: "list")
        }
        else {
            [incomingBatchInstance: incomingBatchInstance]
        }
    }

    def edit = {
        def incomingBatchInstance = IncomingBatch.get(params.id)
        if (!incomingBatchInstance) {
            flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'incomingBatch.label', default: 'IncomingBatch'), params.id])}"
            redirect(action: "list")
        }
        else {
            return [incomingBatchInstance: incomingBatchInstance]
        }
    }

    def update = {
        def incomingBatchInstance = IncomingBatch.get(params.id)
        if (incomingBatchInstance) {
            if (params.version) {
                def version = params.version.toLong()
                if (incomingBatchInstance.version > version) {
                    
                    incomingBatchInstance.errors.rejectValue("version", "default.optimistic.locking.failure", [message(code: 'incomingBatch.label', default: 'IncomingBatch')] as Object[], "Another user has updated this IncomingBatch while you were editing")
                    render(view: "edit", model: [incomingBatchInstance: incomingBatchInstance])
                    return
                }
            }
            incomingBatchInstance.properties = params
            if (!incomingBatchInstance.hasErrors() && incomingBatchInstance.save(flush: true)) {
                flash.message = "${message(code: 'default.updated.message', args: [message(code: 'incomingBatch.label', default: 'IncomingBatch'), incomingBatchInstance.id])}"
                redirect(action: "show", id: incomingBatchInstance.id)
            }
            else {
                render(view: "edit", model: [incomingBatchInstance: incomingBatchInstance])
            }
        }
        else {
            flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'incomingBatch.label', default: 'IncomingBatch'), params.id])}"
            redirect(action: "list")
        }
    }

    def delete = {
        def incomingBatchInstance = IncomingBatch.get(params.id)
        if (incomingBatchInstance) {
            try {
                incomingBatchInstance.delete(flush: true)
                flash.message = "${message(code: 'default.deleted.message', args: [message(code: 'incomingBatch.label', default: 'IncomingBatch'), params.id])}"
                redirect(action: "list")
            }
            catch (org.springframework.dao.DataIntegrityViolationException e) {
                flash.message = "${message(code: 'default.not.deleted.message', args: [message(code: 'incomingBatch.label', default: 'IncomingBatch'), params.id])}"
                redirect(action: "show", id: params.id)
            }
        }
        else {
            flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'incomingBatch.label', default: 'IncomingBatch'), params.id])}"
            redirect(action: "list")
        }
    }
}
