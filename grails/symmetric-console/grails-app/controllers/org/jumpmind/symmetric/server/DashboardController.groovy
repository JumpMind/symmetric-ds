package org.jumpmind.symmetric.server


import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Map;

import java.util.Calendar;

import org.jumpmind.symmetric.ISymmetricEngine

import org.jumpmind.symmetric.grails.Node
import org.jumpmind.symmetric.grails.NodeIdentity
import org.jumpmind.symmetric.grails.NodeHost
import org.jumpmind.symmetric.grails.OutgoingBatch
import org.jumpmind.symmetric.grails.IncomingBatch

import jofc2.model.Chart

import jofc2.model.elements.LineChart
import jofc2.model.axis.YAxis
import jofc2.model.axis.XAxis
import jofc2.model.axis.Label
import jofc2.model.elements.BarChart
import jofc2.OFC
import jofc2.model.axis.Label.Rotation

class DashboardController {

  def ISymmetricEngine symmetricEngine
  def dashboardService

  def static SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("hh:mm a");
  def static SimpleDateFormat DAY_FORMAT = new SimpleDateFormat("MM/dd");
  def static SimpleDateFormat DATE_TIME_FORMAT = new SimpleDateFormat("MM/dd/yy hh:mm a");
  
  def index = {
	redirect(action: "show", params: params)
  }

  def show = {
	session.menu="monitor-dashboard"
	session.overview = createCommand()

	def outBatches = getOutgoingBatchSummary()
	def inBatches = getIncomingBatchSummary()
	def myNodeGroups = getMyNodeGroups()
	def intervals = [ "Minutes":Calendar.MINUTE, "Hours":Calendar.HOUR,"Days":Calendar.DATE ]
	def interval = params.interval == "null" || params.interval == null ? Calendar.HOUR : new Integer(params.interval)
	
	[outBatches: outBatches, 
	 maxOutBatch: outBatches?.max{ it.totalBatches }?.totalBatches,
	 inBatches : inBatches,
	 maxInBatch: inBatches?.max{ it.totalBatches }?.totalBatches,
	 myNodeGroups : myNodeGroups,
	 intervals : intervals,
	 interval : interval]
  }

  def start = {}

  def stop = {}

  def throughput = {
		def interval = params.interval == "null" || params.interval == null ? Calendar.HOUR : new Integer(params.interval)
		
		def throughput = dashboardService.getThroughput(interval)
		
		def c = new Chart(new Date().toString())
		def extractBar = new BarChart().setColour("#FF0000").setTooltip("Extract ms<br>#top#");
		def filterBar = new BarChart().setColour("#00FF00").setTooltip("Filter ms<br>#top#");
		def loadBar = new BarChart().setColour("#0000FF").setTooltip("Load ms<br>#top#");
		def networkBar = new BarChart().setColour("#990066").setTooltip("Network ms<br>#top#");
		def routerBar = new BarChart().setColour("#CC6600").setTooltip("Router ms<br>#top#");
		def dataEventLine = new BarChart().setColour("#666666").setTooltip("Router ms<br>#top#");
		
		def xAxisLabels = []
		def yMax = 0
		def dataSum = throughput?.sum() { it.totalDataEventCount != null ? it.totalDataEventCount : 0}

		throughput.each {
			if (interval == Calendar.MINUTE || interval == Calendar.HOUR) {
				xAxisLabels.add(TIME_FORMAT.format(it.begin))
			}
			else if (interval == Calendar.DATE) {
				xAxisLabels.add(DAY_FORMAT.format(it.end))
			}
			
			if (it.extractMs > yMax) { yMax = it.extractMs }
			if (it.filterMs > yMax) { yMax = it.filterMs }
			if (it.loadMs > yMax) { yMax = it.loadMs }
			if (it.networkMs > yMax) { yMax = it.networkMs }
			if (it.routerMs > yMax) { yMax = it.routerMs }
			
			extractBar.addValues(it.extractMs  == null ? 0 : it.extractMs)
			filterBar.addValues(it.filterMs  == null ? 0 : it.filterMs)
			loadBar.addValues(it.loadMs  == null ? 0 : it.loadMs)
			networkBar.addValues(it.networkMs  == null ? 0 : it.networkMs)
			routerBar.addValues(it.routerMs  == null ? 0 : it.routerMs)
		}
		
		throughput.each {
			def dataEventRatio = 0
			def eventCount = (it.totalDataEventCount == null) ? 0 : it.totalDataEventCount
			
			if (dataSum != null && dataSum > 0) {
				dataEventRatio = new BigDecimal( (eventCount / dataSum) * yMax ).setScale(2, BigDecimal.ROUND_UP)
			}
									
			dataEventLine.addBars(new BarChart.Bar(dataEventRatio?.floatValue(), "#000000")
				.setTooltip("Data Events<br>" + java.text.NumberFormat.getInstance().format(eventCount)))

		}
		
		c.setXAxis(new XAxis().setLabels(xAxisLabels));
		c.getXAxis().getLabels().each {
			it.setRotation(Rotation.DIAGONAL)
			it.setSize(12)
		}
		//c.getXAxis().set3D(5);
		c.getXAxis().setColour("#909090");
		
		c.setYAxis(new YAxis().setRange(0, yMax, 1))
		c.getYAxis().setLabels(["             ."])
		
		c.addElements(extractBar);
		c.addElements(filterBar);
		c.addElements(loadBar);
		c.addElements(networkBar);
		c.addElements(routerBar);
		c.addElements(dataEventLine);
		render c;
  } 

  def createCommand() {
    def nodeId = NodeIdentity.find("from NodeIdentity")?.nodeId
	def heartbeat = Node.get(nodeId)?.heartbeatTime == null ? "NA" : DATE_TIME_FORMAT.format(Node.get(nodeId)?.heartbeatTime)
    new DashboardCommand(
            started: symmetricEngine.started || symmetricEngine.starting,
            nodeGroupId: symmetricEngine.parameterService.nodeGroupId,
            nodeId: nodeId,
            numberOfNodes: Node.count(),
            numberOfClients: nodeId ? Node.countByCreatedAtNodeId(nodeId) : 0,
            nodeHosts: nodeId ? NodeHost.findByNodeId(nodeId) : [],
			heartbeat: heartbeat

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
	if (batchRow) {
	    batches.add(batchRow)
	}
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
	if (batchRow) {
	    batches.add(batchRow)
	}
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
  String heartbeat
}
