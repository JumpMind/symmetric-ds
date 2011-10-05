package org.jumpmind.symmetric.core.db;

import java.util.List;
import java.util.Set;

import org.jumpmind.symmetric.core.SymmetricTables;
import org.jumpmind.symmetric.core.common.BinaryEncoding;
import org.jumpmind.symmetric.core.db.DmlStatement.DmlType;
import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.Database;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.model.Table;

public interface IDbDialect {    

    public DbDialectInfo getDbDialectInfo();
    
    public SymmetricTables getSymmetricTables();

    public Parameters getParameters();

    public String getAlterScriptFor(Table... tables);

    public void alter(boolean failOnError, Table... tables);

    public Database findDatabase(String catalogName, String schemaName);

    public Table findTable(String tableName, boolean useCached);

    public Table findTable(String catalogName, String schemaName, String tableName,
            boolean useCached);

    public List<Table> findTables(String catalogName, String schemaName, boolean useCached);

    public Object[] getObjectValues(BinaryEncoding encoding, String[] values,
            Column[] orderedMetaData);

    public String getDefaultCatalog();

    public String getDefaultSchema();

    public ISqlTemplate getSqlTemplate();        

    public IDataCaptureBuilder getDataCaptureBuilder();

    public boolean isLob(int type);

    /**
     * Returns the constraint name. This method takes care of length limitations
     * imposed by some databases.
     * 
     * @param prefix
     *            The constraint prefix, can be <code>null</code>
     * @param table
     *            The table that the constraint belongs to
     * @param secondPart
     *            The second name part, e.g. the name of the constraint column
     * @param suffix
     *            The constraint suffix, e.g. a counter (can be
     *            <code>null</code>)
     * @return The constraint name
     */
    public String getConstraintName(String prefix, Table table, String secondPart, String suffix);

    /**
     * Generates a version of the name that has at most the specified length.
     * 
     * @param name
     *            The original name
     * @param desiredLength
     *            The desired maximum length
     * @return The shortened version
     */
    public String shortenName(String name, int desiredLength);

    public boolean isDataIntegrityException(Exception ex);
    
    public boolean supportsBatchUpdates();
    
    public Query createQuery(int expectedNumberOfArgs, Table... tables);

    public Query createQuery(Table... tables);    
    
    public DmlStatement createDmlStatement(DmlType dmlType, Table table);
    
    public DmlStatement createDmlStatement(DmlType dmlType, Table table, Set<String> columnFilter);
    
    public DmlStatement createDmlStatement(DmlType dmlType, String catalogName, String schemaName, String tableName, Column[] keys, Column[] columns);
    
    public DmlStatement createDmlStatement(DmlType dmlType, String catalogName, String schemaName, String tableName, Column[] keys, Column[] columns,
            Column[] preFilteredColumns);
    
    public void refreshParameters(Parameters parameters);
    
    public void removeSymmetric();
        
    public boolean requiresSavepoints();
       
}
