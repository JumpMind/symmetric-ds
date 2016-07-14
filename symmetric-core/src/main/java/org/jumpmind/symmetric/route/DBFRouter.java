package org.jumpmind.symmetric.route;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
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
			boolean validateHeader = engine.getParameterService()
					.is(ParameterConstants.DBF_ROUTER_VALIDATE_HEADER, true);
			
			dbfReader = new DBFReader(Files.newInputStream(file.toPath(), StandardOpenOption.READ), validateHeader);
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
			log.error("Unable to parse DBF file " + file.getName(), e);
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
			log.error("Unable to read column names for DBF file ", e);
		}
		return columns.toString();
	}
}
