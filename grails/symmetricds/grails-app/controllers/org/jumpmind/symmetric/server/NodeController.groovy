package org.jumpmind.symmetric.server

import org.jumpmind.symmetric.grails.Node

class NodeController {

  static allowedMethods = [save: "POST", update: "POST", delete: "POST"]

  def index = {
    redirect(action: "list", params: params)
  }

  def list = {
	session.menu="monitor-nodes"
	
	params.max = Integer.MAX_VALUE
    def list = Node.list(params).sort {a, b ->
      if (a.createdAtNodeId && a.nodeId == a.createdAtNodeId) {
        return -1
      } else if (b.createdAtNodeId && b.nodeId == b.createdAtNodeId) {
        return 1
      } else if (b.createdAtNodeId && a.nodeId == b.createdAtNodeId) {
        return -1
      } else if (a.createdAtNodeId && b.nodeId == a.createdAtNodeId) {
        return 1
      } else if (a.createdAtNodeId && b.createdAtNodeId) {
        return a.createdAtNodeId.compareTo(b.createdAtNodeId)
      } else if (!a.createdAtNodeId || !b.createdAtNodeId) {
        return -1
      } else {
        return a.nodeId.compareTo(b.nodeId)
      }
    }
    [nodeInstanceList: list, nodeInstanceTotal: Node.count()]
  }

  def create = {
    def nodeInstance = new Node()
    nodeInstance.properties = params
    return [nodeInstance: nodeInstance]
  }

  def save = {
    def nodeInstance = new Node(params)
    if (nodeInstance.save(flush: true)) {
      flash.message = "${message(code: 'default.created.message', args: [message(code: 'node.label', default: 'Node'), nodeInstance.id])}"
      redirect(action: "show", id: nodeInstance.id)
    }
    else {
      render(view: "create", model: [nodeInstance: nodeInstance])
    }
  }

  def show = {
	def nodeInstance = Node.findById(params.id)
	if (!nodeInstance) {
      flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'node.label', default: 'Node'), params.id])}"
      redirect(action: "list")
    }
    else {
      [nodeInstance: nodeInstance]
    }
  }

  def edit = {
    def nodeInstance = Node.get(params.id)
    if (!nodeInstance) {
      flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'node.label', default: 'Node'), params.id])}"
      redirect(action: "list")
    }
    else {
      return [nodeInstance: nodeInstance]
    }
  }

  def update = {
    def nodeInstance = Node.get(params.id)
    if (nodeInstance) {
      if (params.version) {
        def version = params.version.toLong()
        if (nodeInstance.version > version) {

          nodeInstance.errors.rejectValue("version", "default.optimistic.locking.failure", [message(code: 'node.label', default: 'Node')] as Object[], "Another user has updated this Node while you were editing")
          render(view: "edit", model: [nodeInstance: nodeInstance])
          return
        }
      }
      nodeInstance.properties = params
      if (!nodeInstance.hasErrors() && nodeInstance.save(flush: true)) {
        flash.message = "${message(code: 'default.updated.message', args: [message(code: 'node.label', default: 'Node'), nodeInstance.id])}"
        redirect(action: "show", id: nodeInstance.id)
      }
      else {
        render(view: "edit", model: [nodeInstance: nodeInstance])
      }
    }
    else {
      flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'node.label', default: 'Node'), params.id])}"
      redirect(action: "list")
    }
  }

  def delete = {
    def nodeInstance = Node.get(params.id)
    if (nodeInstance) {
      try {
        nodeInstance.delete(flush: true)
        flash.message = "${message(code: 'default.deleted.message', args: [message(code: 'node.label', default: 'Node'), params.id])}"
        redirect(action: "list")
      }
      catch (org.springframework.dao.DataIntegrityViolationException e) {
        flash.message = "${message(code: 'default.not.deleted.message', args: [message(code: 'node.label', default: 'Node'), params.id])}"
        redirect(action: "show", id: params.id)
      }
    }
    else {
      flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'node.label', default: 'Node'), params.id])}"
      redirect(action: "list")
    }
  }
}
