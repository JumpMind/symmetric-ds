package org.jumpmind.symmetric.ddlutils.oracle;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Map;

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Index;
import org.apache.ddlutils.model.IndexColumn;
import org.apache.ddlutils.model.NonUniqueIndex;
import org.apache.ddlutils.model.UniqueIndex;
import org.apache.ddlutils.platform.DatabaseMetaDataWrapper;
import org.apache.ddlutils.platform.oracle.Oracle10ModelReader;

public class OracleModelReader extends Oracle10ModelReader {

    public OracleModelReader(Platform platform) {
        super(platform);
    }
    
    @SuppressWarnings("unchecked")
    protected void readIndex(DatabaseMetaDataWrapper metaData, Map values, Map knownIndices) throws SQLException
    {
        Short indexType = (Short)values.get("TYPE");

        // we're ignoring statistic indices
        if ((indexType != null) && (indexType.shortValue() == DatabaseMetaData.tableIndexStatistic) &&
                (indexType.shortValue() == DatabaseMetaData.tableIndexClustered)  &&
                (indexType.shortValue() == DatabaseMetaData.tableIndexHashed))
        {
            return;
        }
        
        String indexName = (String)values.get("INDEX_NAME");

        if (indexName != null)
        {
            Index index = (Index)knownIndices.get(indexName);
    
            if (index == null)
            {
                if (((Boolean)values.get("NON_UNIQUE")).booleanValue())
                {
                    index = new NonUniqueIndex();
                }
                else
                {
                    index = new UniqueIndex();
                }

                index.setName(indexName);
                knownIndices.put(indexName, index);
            }
    
            IndexColumn indexColumn = new IndexColumn();
    
            indexColumn.setName((String)values.get("COLUMN_NAME"));
            if (values.containsKey("ORDINAL_POSITION"))
            {
                indexColumn.setOrdinalPosition(((Short)values.get("ORDINAL_POSITION")).intValue());
            }
            index.addColumn(indexColumn);
        }
    }

}
