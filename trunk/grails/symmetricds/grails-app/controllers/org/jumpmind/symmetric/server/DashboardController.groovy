package org.jumpmind.symmetric.server

import org.jumpmind.symmetric.ISymmetricEngine

import org.jumpmind.symmetric.grails.Node
import org.jumpmind.symmetric.grails.NodeIdentity
import org.jumpmind.symmetric.grails.NodeHost
import org.jumpmind.symmetric.grails.OutgoingBatch

class DashboardController {

  def ISymmetricEngine symmetricEngine

  def index = {
	redirect(action: "show", params: params)
  }

  def show = {
	session.menu="monitor-dashboard"
	session.overview = createCommand()

	def batches = getOutgoingBatchSummary()

	[batches: batches, maxBatch: batches?.max{ it.totalBatches }?.totalBatches]
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
	
  def getOutgoingBatchSummary() {
	def c = OutgoingBatch.createCriteria()
	def results = c.list { 
		projections { 
			groupProperty("nodeId") 
			groupProperty("status")
			count("batchId")
		} 
		order("nodeId", "asc")
		order("status", "asc")
	}
	def prevNodeId = null
	def batches = []
	def batchRow = null
	results.each {
		if (it[0] != prevNodeId) {
			if (batchRow != null) {
				batches.add(batchRow)
			}
			batchRow = new OutgoingBatchCommand()
		}
		batchRow.nodeLabel= it[0] == "-1" ? "Not routed" : it[0]
		batchRow.nodeId = it[0]
		batchRow.statusList.add(it[1])
		batchRow.statusListCount.add(it[2])
		batchRow.totalBatches += it[2]
		prevNodeId = batchRow.nodeId
	}
	batches.add(batchRow)
	return batches
  }
}

class OutgoingBatchCommand {
	String nodeId
	String nodeLabel
	List statusList
	List statusListCount
	int totalBatches
	
	public OutgoingBatchCommand() {
		statusList = []
		statusListCount = []
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
