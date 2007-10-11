package org.jumpmind.symmetric.load;

import org.jumpmind.symmetric.load.StatementBuilder.DmlType;

public interface IColumnFilter {

    public String[] filterColumnsNames(DmlType dml, String[] columnNames);
    public Object[] filterColumnsValues(DmlType dml, Object[] columnValues);
}
