package org.jumpmind.symmetric.route;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ISymmetricEngine;

public class CSVRouter extends AbstractFileParsingRouter implements IDataRouter, IBuiltInExtensionPoint{
    private ISymmetricEngine engine;
    private String columns;
    
    public CSVRouter(ISymmetricEngine engine) {
        this.engine = engine;
    }

    @Override
    public ISymmetricEngine getEngine() {
        return this.engine;
    }
    
    @Override
    public List<String> parse(File file, int lineNumber, int tableIndex) {
        List<String> rows = new ArrayList<String>();
        int currentLine = 1;
        
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line;
            while((line = reader.readLine()) != null){
                if(currentLine == 1){
                    columns = line;
                }else{
                    if(currentLine > lineNumber){
                        rows.add(line);
                    }       
                }
                currentLine++;
            }
            reader.close();  
        } catch (IOException e) {
            log.error("Unable to parse CSV file " + file.getName() + " line number " + currentLine, e);
        }

        return rows;
    }

    @Override
    public String getColumnNames() {
           return this.columns;
    }



}
