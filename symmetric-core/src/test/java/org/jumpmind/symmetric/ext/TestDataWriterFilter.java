package org.jumpmind.symmetric.ext;

import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriterFilterAdapter;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.junit.Ignore;

@Ignore
public class TestDataWriterFilter extends DatabaseWriterFilterAdapter implements
        IDatabaseWriterFilter {

    private int numberOfTimesCalled = 0;

    private static int numberOfTimesCreated;

    public TestDataWriterFilter() {
        numberOfTimesCreated++;
    }

    @Override
    public boolean beforeWrite(
            DataContext context, Table table, CsvData data) {
        numberOfTimesCalled++;
        return true;
    }

    public static int getNumberOfTimesCreated() {
        return numberOfTimesCreated;
    }

    public int getNumberOfTimesCalled() {
        return numberOfTimesCalled;
    }

}