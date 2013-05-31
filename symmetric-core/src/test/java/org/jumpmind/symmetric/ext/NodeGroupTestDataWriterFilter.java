package org.jumpmind.symmetric.ext;

import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriterFilterAdapter;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;

public class NodeGroupTestDataWriterFilter extends DatabaseWriterFilterAdapter implements
        IDatabaseWriterFilter, INodeGroupExtensionPoint {

    protected int numberOfTimesCalled = 0;

    String[] nodeGroups;

    public String[] getNodeGroupIdsToApplyTo() {
        return nodeGroups;
    }

    @Override
    public boolean beforeWrite(
            DataContext context, Table table, CsvData data) {
        numberOfTimesCalled++;
        return true;
    }

    public void setNodeGroups(String[] nodeGroups) {
        this.nodeGroups = nodeGroups;
    }

    public int getNumberOfTimesCalled() {
        return numberOfTimesCalled;
    }

}