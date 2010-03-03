package org.jumpmind.symmetric.server

class NodeGroupLinkController {

  static allowedMethods = [save: "POST", update: "POST", delete: "GET"]

  def nodeGroupLinkService

  def index = {
    redirect(action: "list", params: params)
  }

  def list = {
    params.max = Math.min(params.max ? params.max.toInteger() : 10, 100)
    [nodeGroupLinkInstanceList: org.jumpmind.symmetric.grails.NodeGroupLink.list(params), nodeGroupLinkInstanceTotal: org.jumpmind.symmetric.grails.NodeGroupLink.count()]
  }

  def create = {
    def nodeGroupLinkInstance = new org.jumpmind.symmetric.grails.NodeGroupLink()
    nodeGroupLinkInstance.properties = params
    return [nodeGroupLinkInstance: nodeGroupLinkInstance]
  }

  def save = {
    def nodeGroupLinkInstance = new org.jumpmind.symmetric.grails.NodeGroupLink(params)
    if (nodeGroupLinkInstance.save(flush: true)) {
      flash.message = "${message(code: 'default.created.message', args: [message(code: 'nodeGroupLink.label', default: 'NodeGroupLink'), nodeGroupLinkInstance.id])}"
      redirect(action: "show", id: nodeGroupLinkInstance.id)
    }
    else {
      render(view: "create", model: [nodeGroupLinkInstance: nodeGroupLinkInstance])
    }
  }

  def show = {
    def nodeGroupLinkInstance = nodeGroupLinkService.lookupNodeGroupLink(params)
    if (nodeGroupLinkInstance) {
      return [nodeGroupLinkInstance: nodeGroupLinkInstance]
    }

    flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'nodeGroupLink.label', default: 'NodeGroupLink'), params['sourceNodeGroup.nodeGroupId'] + '-' + params['targetNodeGroup.nodeGroupId']])}"
    redirect(action: "list")
  }

  def edit = {
    def nodeGroupLinkInstance = nodeGroupLinkService.lookupNodeGroupLink(params)
    if (nodeGroupLinkInstance) {
      return [nodeGroupLinkInstance: nodeGroupLinkInstance]
    }

    flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'nodeGroupLink.label', default: 'NodeGroupLink'), params.id])}"
    redirect(action: "list")

  }

  def update = {
    def nodeGroupLinkInstance = nodeGroupLinkService.lookupNodeGroupLink(params)
    if (nodeGroupLinkInstance) {
      if (params.version) {
        def version = params.version.toLong()
        if (nodeGroupLinkInstance.version > version) {

          nodeGroupLinkInstance.errors.rejectValue("version", "default.optimistic.locking.failure", [message(code: 'nodeGroupLink.label', default: 'NodeGroupLink')] as Object[], "Another user has updated this NodeGroupLink while you were editing")
          render(view: "edit", model: [nodeGroupLinkInstance: nodeGroupLinkInstance])
          return
        }
      }
      nodeGroupLinkInstance.properties = params
      if (!nodeGroupLinkInstance.hasErrors() && nodeGroupLinkInstance.save(flush: true)) {
        flash.message = "${message(code: 'default.updated.message', args: [message(code: 'nodeGroupLink.label', default: 'NodeGroupLink'), nodeGroupLinkInstance.id])}"
        redirect(action: "show", id: nodeGroupLinkInstance.id)
      }
      else {
        render(view: "edit", model: [nodeGroupLinkInstance: nodeGroupLinkInstance])
      }
    }
    else {
      flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'nodeGroupLink.label', default: 'NodeGroupLink'), params.id])}"
      redirect(action: "list")
    }
  }

  def delete = {
    def nodeGroupLinkInstance = nodeGroupLinkService.lookupNodeGroupLink(params)
    if (nodeGroupLinkInstance) {
      try {
        nodeGroupLinkInstance.delete(flush: true)
        flash.message = "${message(code: 'default.deleted.message', args: [message(code: 'nodeGroupLink.label', default: 'NodeGroupLink'), params.id])}"
        redirect(action: "list")
      }
      catch (org.springframework.dao.DataIntegrityViolationException e) {
        flash.message = "${message(code: 'default.not.deleted.message', args: [message(code: 'nodeGroupLink.label', default: 'NodeGroupLink'), params.id])}"
        redirect(action: "show", id: params.id)
      }
    }
    else {
      flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'nodeGroupLink.label', default: 'NodeGroupLink'), params.id])}"
      redirect(action: "list")
    }
  }
}
