package org.jumpmind.symmetric.server


import java.util.Map;

import org.jumpmind.symmetric.ISymmetricEngine

import org.jumpmind.symmetric.grails.Node
import org.jumpmind.symmetric.grails.NodeIdentity
import org.jumpmind.symmetric.grails.NodeHost
import org.jumpmind.symmetric.grails.OutgoingBatch
import org.jumpmind.symmetric.grails.IncomingBatch

class DashboardController {

  def ISymmetricEngine symmetricEngine

  def index = {
	redirect(action: "show", params: params)
  }

  def show = {
	session.menu="monitor-dashboard"
	session.overview = createCommand()

	def outBatches = getOutgoingBatchSummary()
	def inBatches = getIncomingBatchSummary()
	def myNodeGroups = getMyNodeGroups()
	
	[outBatches: outBatches, 
	 maxOutBatch: outBatches?.max{ it.totalBatches }?.totalBatches,
	 inBatches : inBatches,
	 maxInBatch: inBatches?.max{ it.totalBatches }?.totalBatches,
	 myNodeGroups : myNodeGroups]
  }

  def start = {}

  def stop = {}

  def createCommand() {
    def nodeId = NodeIdentity.find("from NodeIdentity")?.nodeId
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
			batchRow = new BatchCommand()
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
	
  def getIncomingBatchSummary() {
	def c = IncomingBatch.createCriteria()
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
			batchRow = new BatchCommand()
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
	
  def getMyNodeGroups() {
		def c = Node.createCriteria()
		def errorList = c.list { 
			gt("batchInErrorCount", 0)
			projections { 
				groupProperty("nodeGroup") 
				count("nodeId")
			} 
		}
		def c2 = Node.createCriteria()
		def partialList = c2.list { 
			eq("batchInErrorCount", 0)
			gt("batchToSendCount", 0)
			projections { 
				groupProperty("nodeGroup") 
				count("nodeId")
			} 
		}
		def c3 = Node.createCriteria()
		def okList = c3.list { 
			eq("batchInErrorCount", 0)
			eq("batchToSendCount", 0)
			projections { 
				groupProperty("nodeGroup") 
				count("nodeId")
			} 
		}
		
		Map summary = new HashMap()
		summary = buildNodeGroupSummary(errorList, summary, "ER")
		summary = buildNodeGroupSummary(partialList, summary, "PA")
		summary = buildNodeGroupSummary(okList, summary, "OK")
		return summary
  }
	
  	def buildNodeGroupSummary(List statusList, Map summary, String key) {
		statusList.each {
			if (summary.get(it[0].nodeGroupId) == null) {
				summary.put(it[0].nodeGroupId, new HashMap())
			}
			def counter = summary.get(it[0].nodeGroupId).get(key)
			summary.get(it[0].nodeGroupId).put(key, counter == null ? it[1] : counter + it[i])
		}
		return summary
	}
}


class NodeGroupCommand {
	String nodeGroupDescription
	String nodeGroupId
	Map status
}

class BatchCommand {
	String nodeId
	String nodeLabel
	List statusList
	List statusListCount
	int totalBatches
	
	public BatchCommand() {
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
