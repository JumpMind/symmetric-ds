package org.jumpmind.symmetric.core.db;

import java.util.List;

import org.jumpmind.symmetric.core.common.BinaryEncoding;
import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.model.Table;

public interface IPlatform {

    public PlatformInfo getPlatformInfo();

    public Table findTable(String catalogName, String schemaName, String tableName, boolean useCached, Parameters parameters);

    public List<Table> findTables(String catalogName, String schemaName, Parameters parameters);
    
    public Object[] getObjectValues(BinaryEncoding encoding, String[] values,
            Column[] orderedMetaData);
    
    public String getDefaultCatalog();
    
    public String getDefaultSchema();

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
}
