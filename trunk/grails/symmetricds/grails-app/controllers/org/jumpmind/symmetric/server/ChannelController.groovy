package org.jumpmind.symmetric.server

class ChannelController {

  static allowedMethods = [save: "POST", update: "POST", delete: "GET"]

  def symmetricEngine

  def index = {
    redirect(action: "list", params: params)
  }

  def list = {
	session.menu="control-channels"
	
    params.max = Math.min(params.max ? params.max.toInteger() : 10, 100)
    [channelInstanceList: org.jumpmind.symmetric.grails.Channel.list(params), channelInstanceTotal: org.jumpmind.symmetric.grails.Channel.count()]
  }

  def create = {
    def channelInstance = new org.jumpmind.symmetric.grails.Channel()
    // TODO pull list of possible algorithms from spring
    channelInstance.batchAlgorithm = 'default'
    channelInstance.processingOrder = 1
    channelInstance.enabled = true
    channelInstance.maxBatchToSend = 1
    channelInstance.maxBatchSize = 10000
    channelInstance.extractPeriodMillis = 0
    channelInstance.properties = params
    return [channelInstance: channelInstance, batchAlgorithms: symmetricEngine.applicationContext.getBean('batchAlgorithms')]
  }

  def save = {
    def channelInstance = new org.jumpmind.symmetric.grails.Channel(params)
    if (channelInstance.save(insert: true, flush: true)) {
      flash.message = "${message(code: 'default.created.message', args: [message(code: 'channel.label', default: 'Channel'), channelInstance.channelId])}"
      redirect(action: "show", id: channelInstance.channelId)
    }
    else {
      render(view: "create", model: [channelInstance: channelInstance])
    }
  }

  def show = {
    def channelInstance = org.jumpmind.symmetric.grails.Channel.get(params.id)
    if (!channelInstance) {
      flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'channel.label', default: 'Channel'), params.id])}"
      redirect(action: "list")
    }
    else {
      [channelInstance: channelInstance]
    }
  }

  def edit = {
    def channelInstance = org.jumpmind.symmetric.grails.Channel.get(params.id)
    if (!channelInstance) {
      flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'channel.label', default: 'Channel'), params.id])}"
      redirect(action: "list")
    }
    else {
      return [channelInstance: channelInstance, batchAlgorithms: symmetricEngine.applicationContext.getBean('batchAlgorithms')]
    }
  }

  def update = {
    def channelInstance = org.jumpmind.symmetric.grails.Channel.get(params.channelId)
    if (channelInstance) {
      if (params.version) {
        def version = params.version.toLong()
        if (channelInstance.version > version) {

          channelInstance.errors.rejectValue("version", "default.optimistic.locking.failure", [message(code: 'channel.label', default: 'Channel')] as Object[], "Another user has updated this Channel while you were editing")
          render(view: "edit", model: [channelInstance: channelInstance])
          return
        }
      }
      channelInstance.properties = params
      if (!channelInstance.hasErrors() && channelInstance.save(flush: true)) {
        flash.message = "${message(code: 'default.updated.message', args: [message(code: 'channel.label', default: 'Channel'), channelInstance.channelId])}"
        redirect(action: "show", id: channelInstance.channelId)
      }
      else {
        render(view: "edit", model: [channelInstance: channelInstance])
      }
    }
    else {
      flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'channel.label', default: 'Channel'), params.id])}"
      redirect(action: "list")
    }
  }

  def delete = {
    def channelInstance = org.jumpmind.symmetric.grails.Channel.get(params.id)
    if (channelInstance) {
      try {
        if (org.jumpmind.symmetric.grails.Trigger.countByChannelId(params.id) > 0 || org.jumpmind.symmetric.grails.Channel.isSystemChannel(params.id)) {
          throw new org.springframework.dao.DataIntegrityViolationException('Cannot delete channel')
        }
        channelInstance.delete(flush: true)
        flash.message = "${message(code: 'default.deleted.message', args: [message(code: 'channel.label', default: 'Channel'), params.id])}"
        redirect(action: "list")
      }
      catch (org.springframework.dao.DataIntegrityViolationException e) {
        flash.message = "${message(code: 'default.not.deleted.message', args: [message(code: 'channel.label', default: 'Channel'), params.id])}"
        redirect(action: "show", id: params.id)
      }
    }
    else {
      flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'channel.label', default: 'Channel'), params.id])}"
      redirect(action: "list")
    }
  }
}
