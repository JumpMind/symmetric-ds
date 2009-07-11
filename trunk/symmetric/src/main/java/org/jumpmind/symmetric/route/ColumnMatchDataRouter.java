package org.jumpmind.symmetric.route;

import java.io.StringReader;
import java.util.Set;

import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.Trigger;

import com.csvreader.CsvReader;

public class ColumnMatchDataRouter extends AbstractDataRouter implements IDataRouter {

    public Set<String> routeToNodes(Data data, Trigger trigger, Set<org.jumpmind.symmetric.model.Node> nodes,
            NodeChannel channel, boolean initialLoad) {
        CsvReader csvReader = new CsvReader(new StringReader(data.getRowData()));
        csvReader.setEscapeMode(CsvReader.ESCAPE_MODE_BACKSLASH);
        csvReader.setSafetySwitch(false);
        
        return null;
    }

}
