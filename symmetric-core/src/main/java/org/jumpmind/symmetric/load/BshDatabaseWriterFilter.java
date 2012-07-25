package org.jumpmind.symmetric.load;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.jumpmind.symmetric.model.LoadFilter;
import org.jumpmind.symmetric.service.impl.LoadFilterService.LoadFilterNodeGroupLink;
import org.jumpmind.util.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bsh.EvalError;
import bsh.Interpreter;


public class BshDatabaseWriterFilter implements IDatabaseWriterFilter {
	
    private static final String OLD_ = "OLD_";
    private static final String CONTEXT = "context";
    private static final String TABLE = "table";
    private static final String DATA = "data";
    private static final String ENGINE = "engine";

    protected final Logger log = LoggerFactory.getLogger(getClass());

    final String INTERPRETER_KEY = String.format("%d.BshInterpreter", hashCode());
    ISymmetricEngine symmetricEngine = null;
    List<LoadFilterNodeGroupLink> loadFilters = null;
	public enum WriteMethod { BEFORE_WRITE, AFTER_WRITE, BATCH_COMPLETE, BATCH_COMMIT, BATCH_ROLLBACK };
       
    public BshDatabaseWriterFilter(ISymmetricEngine symmetricEngine, List<LoadFilterNodeGroupLink> loadFilters) {
        this.symmetricEngine = symmetricEngine;
        this.loadFilters = loadFilters;
    }	
	
	public boolean beforeWrite(DataContext context, Table table, CsvData data) {

		boolean writeRow = true;		
        try {
	
	        Interpreter interpreter = getInterpreter(context); 
	        bind(interpreter, context, table, data);	
	        writeRow = processLoadFilters(interpreter, table, loadFilters, WriteMethod.BEFORE_WRITE);

        }  catch (EvalError evalEx) {
        	processError(null, table, evalEx.getErrorText());        	  
        }        
        return writeRow;
	}

	public void afterWrite(DataContext context, Table table, CsvData data) {

        try {
	
	        Interpreter interpreter = getInterpreter(context); 
	        bind(interpreter, context, table, data);	
	        processLoadFilters(interpreter, table, loadFilters, WriteMethod.AFTER_WRITE);

        }  catch (EvalError evalEx) {
        	processError(null, table, evalEx.getErrorText());        	  
        }        

	}

	public boolean handlesMissingTable(DataContext context, Table table) {
		// TODO Auto-generated method stub
		return false;
	}

	public void earlyCommit(DataContext context) {
		// TODO Auto-generated method stub

	}

	public void batchComplete(DataContext context) {
		// TODO Auto-generated method stub

	}

	public void batchCommitted(DataContext context) {
		// TODO Auto-generated method stub

	}

	public void batchRolledback(DataContext context) {
		// TODO Auto-generated method stub

	}

    protected Interpreter getInterpreter(Context ctx) {
        Interpreter interpreter = (Interpreter) ctx.get(INTERPRETER_KEY);
        if (interpreter == null) {
            interpreter = new Interpreter();
            ctx.put(INTERPRETER_KEY, interpreter);
        }
        return interpreter;
    }

    protected void bind(Interpreter interpreter, DataContext context, Table table, CsvData data) throws EvalError {
    	
    	interpreter.set(ENGINE, this.symmetricEngine);
        
    	Map<String, String> sourceValues = data.toColumnNameValuePairs(table.getColumnNames(),
                CsvData.ROW_DATA);
        for (String columnName : sourceValues.keySet()) {
            interpreter.set(columnName.toUpperCase(), sourceValues.get(columnName));
        }
    	
    	Map<String, String> oldValues = data.toColumnNameValuePairs(table.getColumnNames(),
                CsvData.OLD_DATA);
        for (String columnName : oldValues.keySet()) {
            interpreter.set(OLD_ + columnName.toUpperCase(), sourceValues.get(columnName));
        }
        
        interpreter.set(CONTEXT, context);
        interpreter.set(TABLE, table);
        interpreter.set(DATA, data);
    	
    }   
    
    protected void processError(LoadFilter currentFilter, Table table, String errorMsg) {
        log.error("Beanshell script error for load filter {} on table {} error {}", 
        		new Object[] {currentFilter.getLoadFilterId(), table.getName(), errorMsg});
        if (currentFilter.isFailOnError()) {
        	throw new SymmetricException("Error executing beanshell script for load filter {} on table {} error {}.", 
        			new Object[] {currentFilter != null ? currentFilter.getLoadFilterId():"N/A", table.getName(), errorMsg});
        			
        }
    }
    
    protected boolean processLoadFilters(Interpreter interpreter, Table table, List<LoadFilterNodeGroupLink> loadFilters, 
    		WriteMethod writeMethod) throws EvalError {
    	    	
    	boolean writeRow = true;
    	LoadFilter currentFilter = null;

    	try {    	
	        for (LoadFilter filter:loadFilters) {
	        	currentFilter = filter;
	        		        	
	        	if (StringUtils.equals(filter.getTargetTableName(), table.getName()) &&
	        			StringUtils.equals(filter.getTargetCatalogName(), table.getCatalog()) &&
	        			StringUtils.equals(filter.getTargetSchemaName(), table.getSchema())) {

	        		Object result = null;
	        		if (writeMethod.equals(WriteMethod.BEFORE_WRITE) && filter.getBeforeWriteScript() != null) {	        		
	                   result = interpreter.eval(filter.getBeforeWriteScript());
	        		} 
	        		else if (writeMethod.equals(WriteMethod.AFTER_WRITE) && filter.getAfterWriteScript() != null) {
	        			result = interpreter.eval(filter.getAfterWriteScript());
	        		}
	        		else if (writeMethod.equals(WriteMethod.BATCH_COMMIT) && filter.getBatchCommitScript() != null) {
	        			result = interpreter.eval(filter.getBatchCommitScript());
	        		}
	        		else if (writeMethod.equals(WriteMethod.BATCH_COMPLETE) && filter.getBatchCompleteScript() != null) {
	        			result = interpreter.eval(filter.getBatchCompleteScript());
	        		}
	        		else if (writeMethod.equals(WriteMethod.BATCH_ROLLBACK) && filter.getBatchRollbackScript() != null) {
	        			result = interpreter.eval(filter.getBatchRollbackScript());
	        		}
	        		
	        		if (result !=null && result.equals(Boolean.FALSE)) {
	                	writeRow = false;
	                }
	        	}
	        } 

	    }  catch (EvalError evalEx) {
	    	processError(currentFilter, table, evalEx.getErrorText());        	  
	    }
                
        return writeRow;
    }
}
