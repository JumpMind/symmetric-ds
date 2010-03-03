package org.jumpmind.symmetric.server

class TriggerController {

  static allowedMethods = [save: "POST", update: "POST", delete: "GET"]

  def userService

  def index = {
    redirect(action: "list", params: params)
  }

  def list = {
    params.max = Math.min(params.max ? params.max.toInteger() : 10, 100)
    [triggerInstanceList: org.jumpmind.symmetric.grails.Trigger.list(params), triggerInstanceTotal: org.jumpmind.symmetric.grails.Trigger.count()]
  }

  def create = {
    def triggerInstance = new org.jumpmind.symmetric.grails.Trigger()
    triggerInstance.triggerId = null
    triggerInstance.properties = params
    return [triggerInstance: triggerInstance]
  }

  def save = {
    def triggerInstance = new org.jumpmind.symmetric.grails.Trigger(params)
    triggerInstance.createTime = new Date()
    triggerInstance.lastUpdateTime = new Date()
    triggerInstance.lastUpdateBy = userService.currentUser()
    if (triggerInstance.save(flush: true)) {
      flash.message = "${message(code: 'default.created.message', args: [message(code: 'trigger.label', default: 'Trigger'), triggerInstance.id])}"
      redirect(action: "show", id: triggerInstance.id)
    }
    else {
      render(view: "create", model: [triggerInstance: triggerInstance])
    }
  }

  def show = {
    def triggerInstance = org.jumpmind.symmetric.grails.Trigger.get(params.id)
    if (!triggerInstance) {
      flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'trigger.label', default: 'Trigger'), params.id])}"
      redirect(action: "list")
    }
    else {
      [triggerInstance: triggerInstance]
    }
  }

  def edit = {
    def triggerInstance = org.jumpmind.symmetric.grails.Trigger.get(params.id)
    if (!triggerInstance) {
      flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'trigger.label', default: 'Trigger'), params.id])}"
      redirect(action: "list")
    }
    else {
      return [triggerInstance: triggerInstance]
    }
  }

  def update = {
    def triggerInstance = org.jumpmind.symmetric.grails.Trigger.get(params.id)
    triggerInstance.lastUpdateTime = new Date()
    triggerInstance.lastUpdateBy = userService.currentUser()
    if (triggerInstance) {
      if (params.version) {
        def version = params.version.toLong()
        if (triggerInstance.version > version) {

          triggerInstance.errors.rejectValue("version", "default.optimistic.locking.failure", [message(code: 'trigger.label', default: 'Trigger')] as Object[], "Another user has updated this Trigger while you were editing")
          render(view: "edit", model: [triggerInstance: triggerInstance])
          return
        }
      }
      triggerInstance.properties = params
      if (!triggerInstance.hasErrors() && triggerInstance.save(flush: true)) {
        flash.message = "${message(code: 'default.updated.message', args: [message(code: 'trigger.label', default: 'Trigger'), triggerInstance.id])}"
        redirect(action: "show", id: triggerInstance.id)
      }
      else {
        render(view: "edit", model: [triggerInstance: triggerInstance])
      }
    }
    else {
      flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'trigger.label', default: 'Trigger'), params.id])}"
      redirect(action: "list")
    }
  }

  def delete = {
    def triggerInstance = org.jumpmind.symmetric.grails.Trigger.get(params.id)
    if (triggerInstance) {
      try {
        triggerInstance.delete(flush: true)
        flash.message = "${message(code: 'default.deleted.message', args: [message(code: 'trigger.label', default: 'Trigger'), params.id])}"
        redirect(action: "list")
      }
      catch (org.springframework.dao.DataIntegrityViolationException e) {
        flash.message = "${message(code: 'default.not.deleted.message', args: [message(code: 'trigger.label', default: 'Trigger'), params.id])}"
        redirect(action: "show", id: params.id)
      }
    }
    else {
      flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'trigger.label', default: 'Trigger'), params.id])}"
      redirect(action: "list")
    }
  }
}
