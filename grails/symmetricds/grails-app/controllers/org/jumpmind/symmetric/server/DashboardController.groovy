package org.jumpmind.symmetric.server

import org.jumpmind.symmetric.ISymmetricEngine
import org.jumpmind.symmetric.grails.Node
import org.jumpmind.symmetric.grails.NodeIdentity
import org.jumpmind.symmetric.grails.NodeHost

class DashboardController {

  def ISymmetricEngine symmetricEngine

  def index = {
    redirect(action: "show", params: params)
  }

  def show = {
    [command: createCommand()]
  }

  def start = {}

  def stop = {}

  def createCommand() {
    def nodeId = NodeIdentity.findAll()?.get(0).nodeId
    new DashboardCommand(
            started: symmetricEngine.started || symmetricEngine.starting,
            nodeGroupId: symmetricEngine.parameterService.nodeGroupId,
            nodeId: nodeId,
            numberOfNodes: Node.count(),
            numberOfClients: Node.countByCreatedAtNodeId(nodeId),
            nodeHosts: NodeHost.findByNodeId(nodeId)

    )
  }
}

class DashboardCommand {
  boolean started
  String nodeId
  String nodeGroupId
  int numberOfNodes
  int numberOfClients
  def nodeHosts
}
