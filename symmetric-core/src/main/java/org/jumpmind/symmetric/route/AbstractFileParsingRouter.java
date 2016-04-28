package org.jumpmind.symmetric.route;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.stage.IStagedResource;
import org.jumpmind.symmetric.io.stage.IStagedResource.State;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerReBuildReason;
import org.jumpmind.symmetric.model.TriggerRouter;

public abstract class AbstractFileParsingRouter extends AbstractDataRouter {

	public abstract List<String> parse(File file, int lineNumber);
	public abstract String getColumnNames();
	
	public abstract ISymmetricEngine getEngine();
	
	public final static String TRIGGER_ID_FILE_PARSER = "SYM_VIRTUAL_FILE_PARSE_TRIGGER";
	public final static String STAGING_DIR = "parsers/fileParseRouter.txt";
	
	@Override
	public Set<String> routeToNodes(SimpleRouterContext context, DataMetaData dataMetaData, Set<Node> nodes,
			boolean initialLoad, boolean initialLoadSelectUsed, TriggerRouter triggerRouter) {
		
		Map<String, String> newData = getNewDataAsString(null, dataMetaData,
                getEngine().getSymmetricDialect());
		
		String targetTableName = dataMetaData.getRouter().getTargetTableName();
		String fileName = newData.get("FILE_NAME");
		String relativeDir = newData.get("RELATIVE_DIR");
		String triggerId = newData.get("TRIGGER_ID");
		
		if (triggerId != null) {
			String baseDir = getEngine().getFileSyncService().getFileTrigger(triggerId).getBaseDir();
			File file = createSourceFile(baseDir, relativeDir, fileName);
			
			IStagedResource resource = getEngine().getStagingManager().find(STAGING_DIR);
			if (resource == null || !resource.exists()) {
				resource = getEngine().getStagingManager().create(0, STAGING_DIR);
			}
			
			Map<String, Integer> bookmarkMap = readStagingFile(resource);
			String filePath = relativeDir + "/" + fileName;
			
			Integer lineNumber = bookmarkMap.get(filePath) == null ? 0 : bookmarkMap.get(filePath);
			
			List<String> dataRows = parse(file, lineNumber);
			String columnNames = getColumnNames();
			
			String nodeList = buildNodeList(nodes);
			
			for (String row : dataRows) {
				Data data = new Data();
				data.setChannelId("default");
				data.setDataEventType(DataEventType.INSERT);
				data.setRowData(row);
				data.setTableName(targetTableName);
				data.setNodeList(nodeList);
				data.setTriggerHistory(getTriggerHistory(targetTableName, columnNames));
				data.setExternalData(dataMetaData.getRouter().getRouterId());
				data.setDataId(getEngine().getDataService().insertData(data));
				lineNumber++;
			}
			if (!dataRows.isEmpty()) {
				bookmarkMap.put(filePath, lineNumber);
				
				try {
					resource.delete();
					resource = getEngine().getStagingManager().create(0, STAGING_DIR);
					for (Map.Entry<String, Integer> entry : bookmarkMap.entrySet()) {
						resource.getWriter().write("\n" + entry.getKey() + "=" + entry.getValue());
					}
					resource.getWriter().close();
					resource.setState(State.DONE);
				}
				catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		return new HashSet<String>();

	}

	public String buildNodeList(Set<Node> nodes) {
		StringBuffer sb = new StringBuffer();
		for (Node n : nodes) {
			if (sb.length() > 0) {
				sb.append(",");
			}
			sb.append(n.getNodeId());
		}
		return sb.toString();
	}
	
	public Map<String, Integer> readStagingFile(IStagedResource resource) {
		Map<String, Integer> bookmarkMap = new HashMap<String, Integer>();
		
		try{
			String thisLine = null;
			if (resource.exists()) {
	         while ((thisLine = resource.getReader().readLine()) != null) {
	            String[] split = thisLine.split("=");
	            if (split.length == 2) {
	            	bookmarkMap.put(split[0].trim(), new Integer(split[1].trim()));
	            }
	         }     
			}
	      }catch(Exception e){
	         e.printStackTrace();
	      }
		return bookmarkMap;
	}
	
	public File createSourceFile(String baseDir, String relativeDir, String fileName) {
        File sourceBaseDir = new File(baseDir);
        if (!relativeDir.equals(".")) {
            String sourcePath = relativeDir + "/";
            sourceBaseDir = new File(sourceBaseDir, sourcePath);
        }
        return new File(sourceBaseDir, fileName);
    }
	
	protected TriggerHistory getTriggerHistory(String tableName, String columnNames) {
		List<TriggerHistory> triggerHistories = getEngine().getTriggerRouterService().getActiveTriggerHistories(tableName);
		for (TriggerHistory history : triggerHistories) {
			if (history.getTriggerId().equals(TRIGGER_ID_FILE_PARSER)) {
				return history;
			}
		}
		TriggerHistory newTriggerHist = new TriggerHistory(tableName, "", columnNames);
		newTriggerHist.setTriggerId(TRIGGER_ID_FILE_PARSER);
		newTriggerHist.setTableHash(0);
		newTriggerHist.setTriggerRowHash(0);
		newTriggerHist.setTriggerTemplateHash(0);
		newTriggerHist.setLastTriggerBuildReason(TriggerReBuildReason.NEW_TRIGGERS);
		newTriggerHist.setColumnNames(columnNames);
		
		getEngine().getTriggerRouterService().insert(newTriggerHist);
		
		return newTriggerHist;
        
	}

}
