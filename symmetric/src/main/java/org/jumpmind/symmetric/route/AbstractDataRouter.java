package org.jumpmind.symmetric.route;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.util.CsvUtils;

import com.csvreader.CsvReader;

public abstract class AbstractDataRouter implements IDataRouter {

    private boolean autoRegister = true;

    private boolean applyToInitialLoad = true;

    public boolean isAutoRegister() {
        return autoRegister;
    }

    public void setAutoRegister(boolean autoRegister) {
        this.autoRegister = autoRegister;
    }

    public void setApplyToInitialLoad(boolean applyToInitialLoad) {
        this.applyToInitialLoad = applyToInitialLoad;
    }

    public boolean isApplyToInitialLoad() {
        return applyToInitialLoad;
    }

    protected Set<String> toNodeIds(Set<Node> nodes) {
        Set<String> nodeIds = new HashSet<String>(nodes.size());
        for (Node node : nodes) {
            nodeIds.add(node.getNodeId());
        }
        return nodeIds;
    }
    
    // TODO This could probably become more efficient
    protected String[] parseData(String data) throws IOException {
        CsvReader csvReader = CsvUtils.getCsvReader(new StringReader(data));
        if (csvReader.readRecord()) {
            return csvReader.getValues();
        } else {
            throw new IOException(String.format("Could not parse the data passed in: %s", data));
        }
    }
}
