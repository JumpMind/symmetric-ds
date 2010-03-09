package org.jumpmind.symmetric.server

class NodeGroupController {

  static allowedMethods = [save: "POST", update: "POST", delete: "GET"]

  def index = {
    redirect(action: "list", params: params)
  }

  def list = {
	session.menu="configuration-nodeGroups"
    params.max = Math.min(params.max ? params.max.toInteger() : 10, 100)
    [nodeGroupInstanceList: org.jumpmind.symmetric.grails.NodeGroup.list(params), nodeGroupInstanceTotal: org.jumpmind.symmetric.grails.NodeGroup.count()]
  }

  def create = {
    def nodeGroupInstance = new org.jumpmind.symmetric.grails.NodeGroup()
    nodeGroupInstance.properties = params
    return [nodeGroupInstance: nodeGroupInstance]
  }

  def save = {
    def nodeGroupInstance = new org.jumpmind.symmetric.grails.NodeGroup(params)
    if (nodeGroupInstance.save(insert: true, flush: true)) {
      flash.message = "${message(code: 'default.created.message', args: [message(code: 'nodeGroup.label', default: 'NodeGroup'), nodeGroupInstance.nodeGroupId])}"
      redirect(action: "show", id: nodeGroupInstance.nodeGroupId)
    }
    else {
      render(view: "create", model: [nodeGroupInstance: nodeGroupInstance])
    }
  }

  def show = {
    def nodeGroupInstance = org.jumpmind.symmetric.grails.NodeGroup.get(params.id)
    if (!nodeGroupInstance) {
      flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'nodeGroup.label', default: 'NodeGroup'), params.id])}"
      redirect(action: "list")
    }
    else {
      [nodeGroupInstance: nodeGroupInstance]
    }
  }

  def edit = {
    def nodeGroupInstance = org.jumpmind.symmetric.grails.NodeGroup.get(params.id)
    if (!nodeGroupInstance) {
      flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'nodeGroup.label', default: 'NodeGroup'), params.id])}"
      redirect(action: "list")
    }
    else {
      return [nodeGroupInstance: nodeGroupInstance]
    }
  }

  def update = {
    def nodeGroupInstance = org.jumpmind.symmetric.grails.NodeGroup.get(params.id)
    if (nodeGroupInstance) {
      nodeGroupInstance.properties = params
      if (!nodeGroupInstance.hasErrors() && nodeGroupInstance.save(flush: true)) {
        flash.message = "${message(code: 'default.updated.message', args: [message(code: 'nodeGroup.label', default: 'NodeGroup'), nodeGroupInstance.nodeGroupId])}"
        redirect(action: "show", id: nodeGroupInstance.nodeGroupId)
      }
      else {
        render(view: "edit", model: [nodeGroupInstance: nodeGroupInstance])
      }
    }
    else {
      flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'nodeGroup.label', default: 'NodeGroup'), params.id])}"
      redirect(action: "list")
    }
  }

  def delete = {
    def nodeGroupInstance = org.jumpmind.symmetric.grails.NodeGroup.get(params.id)
    if (nodeGroupInstance) {
      try {
        if (org.jumpmind.symmetric.grails.NodeGroupLink.countBySourceNodeGroup(nodeGroupInstance) > 0 || org.jumpmind.symmetric.grails.NodeGroupLink.countByTargetNodeGroup(nodeGroupInstance) > 0) {
          throw new org.springframework.dao.DataIntegrityViolationException("Cannot delete node group that is referenced")
        }
        nodeGroupInstance.delete(flush: true)
        flash.message = "${message(code: 'default.deleted.message', args: [message(code: 'nodeGroup.label', default: 'NodeGroup'), params.id])}"
        redirect(action: "list")
      }
      catch (org.springframework.dao.DataIntegrityViolationException e) {
        flash.message = "${message(code: 'default.not.deleted.message', args: [message(code: 'nodeGroup.label', default: 'NodeGroup'), params.id])}"
        redirect(action: "show", id: params.id)
      }
    }
    else {
      flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'nodeGroup.label', default: 'NodeGroup'), params.id])}"
      redirect(action: "list")
    }
  }
}
