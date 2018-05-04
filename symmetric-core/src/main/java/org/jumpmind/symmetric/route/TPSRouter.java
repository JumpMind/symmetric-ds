package org.jumpmind.symmetric.route;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ISymmetricEngine;

import nl.cad.tpsparse.tps.TpsFile;
import nl.cad.tpsparse.tps.record.DataRecord;
import nl.cad.tpsparse.tps.record.FieldDefinitionRecord;
import nl.cad.tpsparse.tps.record.TableDefinitionRecord;

public class TPSRouter extends AbstractFileParsingRouter implements IDataRouter, IBuiltInExtensionPoint {

    private ISymmetricEngine engine;

    private List<String> fields = new ArrayList<String>();
    
    public TPSRouter(ISymmetricEngine engine) {
        this.engine = engine;
    }

    @Override
    public ISymmetricEngine getEngine() {
    		return this.engine;
    }
    
	@Override
	public List<String> parse(File file, int lineNumber) {
		List<String> rows = new ArrayList<String>();
		try {
			TpsFile tpsFile = new TpsFile(file);
		
		    //
		    // TPS files can contain multiple tables (commonly only one is used).
		    //
		    Map<Integer, TableDefinitionRecord> tables = tpsFile.getTableDefinitions(false);
		    for (Map.Entry<Integer, TableDefinitionRecord> entry : tables.entrySet()) {
		    		TableDefinitionRecord table = entry.getValue();
		        //
		        // For each table get the field definition (columns).
		        //
		        for (FieldDefinitionRecord field : table.getFields()) {
		            fields.add(field.getFieldName());
		        }
		        //
		        // And data records (rows).
		        //
		        for (DataRecord rec : tpsFile.getDataRecords(entry.getKey(), entry.getValue(), false)) {
		        		StringBuilder row = new StringBuilder();
		        		for (int i = 0; i < rec.getValues().size(); i++) {
		        			Object val =  rec.getValues().get(i);
						if (i > 0) { 
							row.append(","); 
						}
						row.append(val.toString());
		        		}
		        		rows.add(row.toString());
		        }
		    }
		} catch (IOException ioe) {
			log.error("Unable to load TPS file");
		}
		return rows;
	}

	@Override
	public String getColumnNames() {
		StringBuffer columns = new StringBuffer();
		try {
			for (int i = 0; i < fields.size(); i++) {
				if (i > 0) { columns.append(","); }
				columns.append(fields.get(i));
			}
		}
		catch (Exception e) {
			log.error("Unable to read column names for TPS file ", e);
		}
		return columns.toString();
	}

	


}
