package org.jumpmind.symmetric.route;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.route.parse.DBFReader;

public class DBFRouter extends AbstractFileParsingRouter implements IDataRouter, IBuiltInExtensionPoint {

    private ISymmetricEngine engine;
    private DBFReader dbfReader = null;
    
    public DBFRouter(ISymmetricEngine engine) {
        this.engine = engine;
    }

    public ISymmetricEngine getEngine() {
    	return this.engine;
    }
    
	@Override
	public List<String> parse(File file, int lineNumber) {
		List<String> rows = new ArrayList<String>();
		
		try {
			dbfReader = new DBFReader(new FileInputStream(file));
			int currentLine = 1;
			while (dbfReader.hasNextRecord()) {
				StringBuffer row = new StringBuffer();
				Object[] record = dbfReader.nextRecord();
				if (currentLine > lineNumber) {
					for (int i = 0; i < record.length; i++) {
						if (i > 0) { row.append(","); }
						row.append(record[i]);
					}
					
					rows.add(row.toString());
				}
				currentLine++;
			}
		}
		catch (Exception e) {
			
		}
		return rows;
	}
	
	@Override
	public String getColumnNames() {
		StringBuffer columns = new StringBuffer();
		try {
			for (int i = 0; i < dbfReader.getFieldCount(); i++) {
				if (i > 0) { columns.append(","); }
				columns.append(dbfReader.getField(i));
			}
		}
		catch (Exception e) {
			
		}
		return columns.toString();
	}
}
