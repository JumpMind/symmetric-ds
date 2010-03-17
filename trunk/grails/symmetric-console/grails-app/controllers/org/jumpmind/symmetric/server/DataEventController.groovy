package org.jumpmind.symmetric.server

import org.jumpmind.symmetric.grails.DataEvent
import org.jumpmind.symmetric.grails.OutgoingBatch

class DataEventController {

    static allowedMethods = [save: "POST", update: "POST", delete: "POST"]

    def index = {
        redirect(action: "list", params: params)
    }

    def list = {
        params.max = Math.min(params.max ? params.int('max') : 10, 100)

        def batch = OutgoingBatch.get(params.batchId)
		//def data = DataEvent.findAllByBatch(batch)
		
		
		if (!params.max) params.max = 10
		if (!params.offset) params.offset = 0
		//if (!params.sort) params.sort = "dataDataId"
		//if (!params.order) params.order = "asc"
		def data = DataEvent.withCriteria {
			maxResults(params.max?.toInteger())
			firstResult(params.offset?.toInteger())
			eq('batch', batch)
			if (params.sort == 'dataDataId') {
				data {
					order('dataId', params.order)
				}
			} 
			else if (params.sort == 'dataTableName') {
				data {
					order('tableName', params.order)
				}
			} 
			else if (params.sort == 'dataEventType') {
				data {
					order('evenType', params.order)
				}
			} 
			else if (params.sort == 'dataRowData') {
				data {
					order('rowData', params.order)
				}
			} 
			else if (params.sort == 'dataPkData') {
				data {
					order('pkData', params.order)
				}
			} 
			else if (params.sort == 'dataOldData') {
				data {
					order('oldData', params.order)
				}
			} 
			//else {
			//	order(params.sort, params.order)
			//}
		}

        [dataEventInstanceList: data, dataEventInstanceTotal: data.size()]
    }

    def create = {
        def dataEventInstance = new DataEvent()
        dataEventInstance.properties = params
        return [dataEventInstance: dataEventInstance]
    }

    def save = {
        def dataEventInstance = new DataEvent(params)
        if (dataEventInstance.save(flush: true)) {
            flash.message = "${message(code: 'default.created.message', args: [message(code: 'dataEvent.label', default: 'DataEvent'), dataEventInstance.id])}"
            redirect(action: "show", id: dataEventInstance.id)
        }
        else {
            render(view: "create", model: [dataEventInstance: dataEventInstance])
        }
    }

    def show = {
        def dataEventInstance = DataEvent.get(params.id)
        if (!dataEventInstance) {
            flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'dataEvent.label', default: 'DataEvent'), params.id])}"
            redirect(action: "list")
        }
        else {
            [dataEventInstance: dataEventInstance]
        }
    }

    def edit = {
        def dataEventInstance = DataEvent.get(params.id)
        if (!dataEventInstance) {
            flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'dataEvent.label', default: 'DataEvent'), params.id])}"
            redirect(action: "list")
        }
        else {
            return [dataEventInstance: dataEventInstance]
        }
    }

    def update = {
        def dataEventInstance = DataEvent.get(params.id)
        if (dataEventInstance) {
            if (params.version) {
                def version = params.version.toLong()
                if (dataEventInstance.version > version) {
                    
                    dataEventInstance.errors.rejectValue("version", "default.optimistic.locking.failure", [message(code: 'dataEvent.label', default: 'DataEvent')] as Object[], "Another user has updated this DataEvent while you were editing")
                    render(view: "edit", model: [dataEventInstance: dataEventInstance])
                    return
                }
            }
            dataEventInstance.properties = params
            if (!dataEventInstance.hasErrors() && dataEventInstance.save(flush: true)) {
                flash.message = "${message(code: 'default.updated.message', args: [message(code: 'dataEvent.label', default: 'DataEvent'), dataEventInstance.id])}"
                redirect(action: "show", id: dataEventInstance.id)
            }
            else {
                render(view: "edit", model: [dataEventInstance: dataEventInstance])
            }
        }
        else {
            flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'dataEvent.label', default: 'DataEvent'), params.id])}"
            redirect(action: "list")
        }
    }

    def delete = {
        def dataEventInstance = DataEvent.get(params.id)
        if (dataEventInstance) {
            try {
                dataEventInstance.delete(flush: true)
                flash.message = "${message(code: 'default.deleted.message', args: [message(code: 'dataEvent.label', default: 'DataEvent'), params.id])}"
                redirect(action: "list")
            }
            catch (org.springframework.dao.DataIntegrityViolationException e) {
                flash.message = "${message(code: 'default.not.deleted.message', args: [message(code: 'dataEvent.label', default: 'DataEvent'), params.id])}"
                redirect(action: "show", id: params.id)
            }
        }
        else {
            flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'dataEvent.label', default: 'DataEvent'), params.id])}"
            redirect(action: "list")
        }
    }
}
