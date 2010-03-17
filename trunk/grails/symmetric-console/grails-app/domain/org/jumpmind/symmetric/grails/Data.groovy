package org.jumpmind.symmetric.grails;

import java.io.Serializable;


import org.codehaus.groovy.grails.commons.ConfigurationHolder
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.TriggerReBuildReason;

class Data implements Serializable {
	static transients = [ 'triggerHistory', 'eventType', 'htmlRowData', 'htmlOldData']
	static hasMany = [ dataEvents : DataEvent ]
	TriggerHistory triggerHist
	String strEventType
	
	private @Delegate org.jumpmind.symmetric.model.Data delegate = new org.jumpmind.symmetric.model.Data()

	static mapping = {
		triggerHist ignoreNotFound:true
		
		def config = ConfigurationHolder.config
		table config.symmetric.sync.table.prefix + '_data'
		version false
		id name : 'dataId', generator: 'assigned'
		triggerHist column : 'trigger_hist_id'
		strEventType column : 'event_type'
		autoTimestamp false
	}
	
	public String toString() {
		return "This is a trigger history object."
	}
	
    public DataEventType getEventType() {
        return DataEventType.getEventType(strEventType)
    }
	
	public String getHtmlRowData() {
		StringBuffer html = new StringBuffer()
		html.append("\n<div class='data-row-data'>")
		def columns = toParsedRowData()
		columns.each {
			html.append("\n\t<div class='data-row-data-field'>").append(it).append("</div>")
			
		}
		html.append("\n\t<div class='clearie' >&nbsp;</div>")
		html.append("\n</div>")
		
	}
	
	public String getHtmlOldData() {
		StringBuffer html = new StringBuffer()
		html.append("\n<div class='data-old-data'>")
		def columns = toParsedOldData()
		columns.each {
			html.append("\n\t<div class='data-old-data-field'>").append(it).append("</div>")
			
		}
		html.append("\n\t<div class='clearie' >&nbsp;</div>")
		html.append("\n</div>")
		
	}
}
