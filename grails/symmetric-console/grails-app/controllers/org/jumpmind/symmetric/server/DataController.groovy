package org.jumpmind.symmetric.server

import org.jumpmind.symmetric.grails.Data

class DataController {

    static allowedMethods = [save: "POST", update: "POST", delete: "POST"]

    def index = {
        redirect(action: "show", params: params)
    }

    def show = {
        def dataInstance = Data.findByDataId(params.id)
        if (!dataInstance) {
            flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'data.label', default: 'Data'), params.id])}"
            redirect(action: "list")
        }
        else {
            [dataInstance: dataInstance]
        }
    }
	
	def list = {
		if (params.batchId != null) {
			
		}
	}

}
