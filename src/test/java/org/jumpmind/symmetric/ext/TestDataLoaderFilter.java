package org.jumpmind.symmetric.ext;

import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.load.IDataLoaderFilter;

public class TestDataLoaderFilter implements IDataLoaderFilter {

    private boolean autoRegister = true;

    private int numberOfTimesCalled = 0;

    public boolean filterDelete(IDataLoaderContext context, String[] keyValues) {
        numberOfTimesCalled++;
        return true;
    }

    public boolean filterInsert(IDataLoaderContext context, String[] columnValues) {
        numberOfTimesCalled++;
        return true;
    }

    public boolean filterUpdate(IDataLoaderContext context, String[] columnValues, String[] keyValues) {
        numberOfTimesCalled++;
        return true;
    }

    public boolean isAutoRegister() {
        return this.autoRegister;
    }

    public void setAutoRegister(boolean autoRegister) {
        this.autoRegister = autoRegister;
    }

    public int getNumberOfTimesCalled() {
        return numberOfTimesCalled;
    }

}
