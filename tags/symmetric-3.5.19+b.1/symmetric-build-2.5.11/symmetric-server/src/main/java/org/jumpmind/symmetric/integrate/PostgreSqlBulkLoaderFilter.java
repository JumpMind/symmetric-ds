package org.jumpmind.symmetric.integrate;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.ext.INodeGroupExtensionPoint;
import org.jumpmind.symmetric.load.IBatchListener;
import org.jumpmind.symmetric.load.IDataLoader;
import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.load.IDataLoaderFilter;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.util.CsvUtils;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.springframework.jdbc.support.nativejdbc.NativeJdbcExtractor;

public class PostgreSqlBulkLoaderFilter implements IBatchListener, IDataLoaderFilter,
	INodeGroupExtensionPoint {

	final Log log = LogFactory.getLog(getClass());
	
	private IParameterService parameterService;

    private boolean autoRegister = true;
    private static final String COPY_IN_KEY = "COPYIN";
    private static final String COPY_MGR_KEY = "COPYMGR";
    private static final String COPY_TABLE_KEY = "COPYTABLE";
    private static final String NBR_PENDING_BULK_LOAD_ROWS_KEY = "PENDING_BULK_LOAD_ROWS";
    private static final int BULK_LOAD_FLUSH_INTERVAL = 10000;

    private String[] nodeGroupIdsToApplyTo;
    private Set<String> tablesToApplyTo;
    
    public void setTablesToApplyTo(Set<String> tablesToApplyTo) {
		this.tablesToApplyTo = tablesToApplyTo;
	}
        
    public String[] getNodeGroupIdsToApplyTo() {
		return nodeGroupIdsToApplyTo;
	}

	public void setNodeGroupIdsToApplyTo(String[] nodeGroupIdsToApplyTo) {
		this.nodeGroupIdsToApplyTo = nodeGroupIdsToApplyTo;
	}

	public boolean isAutoRegister() {
		return autoRegister;
	}

    public void setAutoRegister(boolean autoRegister) {
        this.autoRegister = autoRegister;
    }

	public boolean filterInsert(IDataLoaderContext context,
			String[] columnValues) {
		//if the table we are now doing an insert for insn't the same as 
		//a copy command we already have running, then commit and cleanup 
		//the old one first
		String copyTable = (String) context.getContextCache().get(COPY_TABLE_KEY); 
		if (copyTable != null && !copyTable.equalsIgnoreCase(context.getTableName())) {
			commitAndCleanup(context);
		}
		
		Integer nbrPendingBulkLoadRows = (Integer) context.getContextCache().get(NBR_PENDING_BULK_LOAD_ROWS_KEY);
		if (nbrPendingBulkLoadRows != null && nbrPendingBulkLoadRows > 0 && nbrPendingBulkLoadRows % 5000==0) {
			log.debug(String.format("Bulk loaded %d rows into %s so far ...", nbrPendingBulkLoadRows, context.getTableName()));
		}
		
		//see if we should do a bulk load for the given table
		if (tablesToApplyTo == null || tablesToApplyTo.contains(context.getTableName())) {		
			//init the copyManager if it hasn't already been set up
			initCopyManager(context);
			//write the data string to the copy manager
			writeDataString(context, columnValues);
			//flush if necessary
			flushIfNecessary(context);
			
			//false tells SymmetricDS not to work the insert by standard mechanism
			return false;
		} else {
			return true;
		}			
	}

	public boolean filterUpdate(IDataLoaderContext context,
			String[] columnValues, String[] keyValues) {
		//with any other connection action, we must first commit
		//and cleanup a copy if one is in motion
		commitAndCleanup(context);
		//handle updates with standard mechanism
		return true;
	}

	public boolean filterDelete(IDataLoaderContext context, String[] keyValues) {
		//with any other connection action, we must first commit
		//and cleanup a copy if one is in motion
		commitAndCleanup(context);
		//handle deletes with standard mechanism
		return true;
	}

	public void earlyCommit(IDataLoader loader, IncomingBatch batch) {
		log.debug("early commit: " + batch.getBatchId());
		commitAndCleanup(loader.getContext());
	}

	public void batchComplete(IDataLoader loader, IncomingBatch batch) {
		log.debug("batch complete: " + batch.getBatchId());
        commitAndCleanup(loader.getContext());
	}

	public void batchCommitted(IDataLoader loader, IncomingBatch batch) {
		log.debug("batch committed: " + batch.getBatchId());
	}

	public void batchRolledback(IDataLoader loader, IncomingBatch batch,
			Exception ex) {
		log.debug("batch rolledback: " + batch.getBatchId());
		if (loader.getContext().getContextCache().get(COPY_MGR_KEY) != null) {
				
			CopyIn copyIn = (CopyIn) loader.getContext().getContextCache().get(COPY_IN_KEY);
			try {
				copyIn.cancelCopy();
			} catch (SQLException sqlex) {
				throw new RuntimeException("Error in PostgreSqlBulkLoaderFilter.cancelCopy. ", sqlex);
			} finally {
				cleanup(loader.getContext());
			}
		}
	}

	private void initCopyManager(IDataLoaderContext context) {
				
		try {
			Map<String, Object> contextCache = context.getContextCache();
			if (contextCache.get(COPY_MGR_KEY) == null) {
				//get the connection
			    Class<? extends NativeJdbcExtractor> clazz = Class.forName(parameterService
			            .getString(ParameterConstants.DB_NATIVE_EXTRACTOR)).asSubclass(NativeJdbcExtractor.class);
			    NativeJdbcExtractor nativeJdbcExtractor = (NativeJdbcExtractor) clazz.newInstance();
				Connection conn = nativeJdbcExtractor.getNativeConnection(context.getJdbcTemplate().getDataSource().getConnection());
								
				//create the copy manager
				CopyManager copyManager = new CopyManager((BaseConnection) conn);
				contextCache.put(COPY_MGR_KEY, copyManager);
				String sql = createCopyMgrSql(context);
				log.debug("starting bulk copy using: " + sql);
				CopyIn copyIn = copyManager.copyIn(sql);
				contextCache.put(COPY_TABLE_KEY, context.getTableName());
				contextCache.put(COPY_IN_KEY, copyIn);
			} 
		} catch (RuntimeException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new RuntimeException("Error in PostgreSqlBulkLoaderFilter.initCopyManager. ", ex);
		}
	}
	
	private String createCopyMgrSql(IDataLoaderContext context) {				
		String sql = "COPY ";
		sql += context.getSchemaName() + "." + context.getTableName() + " ";
		String columns = "";
		//specify column names in case we have exclude columns
		for (String column:context.getColumnNames()) {
			if (!columns.equals("")) {
				columns +=",";
			}
			columns += column;
		}
		sql += "(" + columns + ") ";
		sql += "FROM STDIN with delimiter ',' csv quote ''''";

		return sql;
	}
	
	private void writeDataString(IDataLoaderContext context, String[] columnValues) {
		
	   CopyIn copyIn = (CopyIn) context.getContextCache().get(COPY_IN_KEY);
	   String data = CsvUtils.escapeCsvData(columnValues, '\n', '\'');
	   byte[] dataToLoad = data.getBytes(); 
	   try {
		   copyIn.writeToCopy(dataToLoad, 0, dataToLoad.length);
	   } catch (SQLException ex) {
			throw new RuntimeException("Error in PostgreSqlBulkLoaderFilter.writeDataString. ", ex);
	   }

	}
	
	private void flushIfNecessary(IDataLoaderContext context) {
		
		Integer nbrPendingBulkLoadRows = (Integer) context.getContextCache().get(NBR_PENDING_BULK_LOAD_ROWS_KEY);
		if (nbrPendingBulkLoadRows != null) {
			if (nbrPendingBulkLoadRows >= BULK_LOAD_FLUSH_INTERVAL) {
				CopyIn copyIn = (CopyIn) context.getContextCache().get(COPY_IN_KEY);
				try {
					log.debug("flushing bulk copy...");
					copyIn.flushCopy();
				} catch (SQLException ex) {
					throw new SymmetricException("Error in initCopyManager. " + ex.getMessage());
				}
				nbrPendingBulkLoadRows = 0;
			} else {
				nbrPendingBulkLoadRows = Integer.valueOf(nbrPendingBulkLoadRows.intValue() + 1);			
			}
		} else {
			nbrPendingBulkLoadRows=0;
		}
		context.getContextCache().put(NBR_PENDING_BULK_LOAD_ROWS_KEY, nbrPendingBulkLoadRows);		
	}
	
	private void cleanup(IDataLoaderContext context) {
		context.getContextCache().remove(COPY_MGR_KEY);
		context.getContextCache().remove(COPY_IN_KEY);		
		context.getContextCache().remove(NBR_PENDING_BULK_LOAD_ROWS_KEY);
		context.getContextCache().remove(COPY_TABLE_KEY);
	}
	
	private void commitAndCleanup(IDataLoaderContext context) {

		if (context.getContextCache().get(COPY_MGR_KEY) != null) {
			CopyIn copyIn = (CopyIn) context.getContextCache().get(COPY_IN_KEY);		
			if (copyIn != null)
			try {
				log.info("ending copy");
				copyIn.endCopy();			
			} catch (SQLException ex) {
				throw new RuntimeException("Error in PostgreSqlBulkLoaderFilter.batchComplete.", ex);
			} 
			cleanup(context);				
		}
	}

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }

}
