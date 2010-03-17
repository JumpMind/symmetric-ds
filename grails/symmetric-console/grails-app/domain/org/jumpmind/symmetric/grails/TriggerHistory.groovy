package org.jumpmind.symmetric.grails;

import org.codehaus.groovy.grails.commons.ConfigurationHolder
import org.jumpmind.symmetric.model.TriggerReBuildReason

class TriggerHistory implements Serializable {
	static transients = [ 'parsedColumnNames', 'parsedPkColumnNames', 'triggerHistoryId', 'lastTriggerBuildReason', 'htmlColumnNames']
	String strLastTriggerBuildReason
	
	private @Delegate org.jumpmind.symmetric.model.TriggerHistory delegate = new org.jumpmind.symmetric.model.TriggerHistory()
	
	static mapping = {
		def config = ConfigurationHolder.config
		table config.symmetric.sync.table.prefix + '_trigger_hist'
		version false
		
		id column : 'trigger_hist_id', generator: 'assigned'
		strLastTriggerBuildReason column : 'last_trigger_build_reason'
		autoTimestamp false
	}
	
	public String toHtmlString() {
		StringBuffer html = new StringBuffer()
		html.append("\n<div class='trigger-history-container'>")
		html.append(buildHtmlTag("triggerId", "ID", triggerId, true))
		html.append(buildHtmlTag("sourceTableName", "Source Table", sourceTableName, false))
		html.append(buildHtmlTag("sourceSchemaName", "Source Schema", sourceSchemaName, true))
		html.append(buildHtmlTag("sourceCatalogName", "Source Catalog", sourceCatalogName, false))
		html.append(buildHtmlTag("columnNames", "Columns", columnNames, true))
		html.append(buildHtmlTag("pkColumnNames", "PK Columns", pkColumnNames, false))
		html.append(buildHtmlTag("nameForInsertTrigger", "Insert Trigger", nameForInsertTrigger, true))
		html.append(buildHtmlTag("nameForUpdateTrigger", "Update Trigger", nameForUpdateTrigger, false))
		html.append("\n\t<div class='clearie' >&nbsp;</div>")
		html.append("\n</div>")

		return html.toString()
	}

	private String buildHtmlTag(String name, String label, String val, boolean alt) {
		val = val?.replaceAll(",", "<br>")
		StringBuffer html = new StringBuffer()
		html.append("\n\t<div id='").append(name).append("' class='trigger-history-element").append(alt ? " alt" : "").append("'>")
			.append("\n\t\t<div id='").append(name).append("-label' class='trigger-history-label'>").append(label).append("</div>")
			.append("\n\t\t<div id='").append(name).append("-value' class='trigger-history-value'>").append(val).append("</div>")
			.append("\n\t\t<div class='clearie' >&nbsp;</div>")
			.append("\n\t</div>")	
		return html.toString()
	}
	
	public String getHtmlColumnNames() {
		StringBuffer html = new StringBuffer()
		html.append("\n<div class='trigger-history-column-names'>")
		def columns = columnNames.split(",")
		columns.each {
			html.append("\n\t<div class='trigger-history-column-name'>").append(it).append("</div>")
			
		}
		html.append("\n\t<div class='clearie' >&nbsp;</div>")
		html.append("\n</div>")
		
	}
	
    public TriggerReBuildReason getLastTriggerBuildReason() {
        return TriggerReBuildReason.fromCode(strLastTriggerBuildReason)
    }

}
