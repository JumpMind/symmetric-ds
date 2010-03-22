package org.jumpmind.symmetric.server;

class ThroughputBean implements Serializable {
	String channelId
	Integer totalDataEventCount
	Float extractMs
	Float filterMs
	Float loadMs
	Float networkMs
	Float routerMs
	Date begin
	Date end
	
}
