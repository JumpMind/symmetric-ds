package org.jumpmind.db.platform.bigquery;

import java.sql.Types;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.AbstractDdlBuilder;
import org.jumpmind.db.platform.DatabaseNamesConstants;

import com.google.cloud.bigquery.BigQuery;

public class BigQueryDdlBuilder extends AbstractDdlBuilder {

    BigQuery bigQuery;
    
    public BigQueryDdlBuilder(BigQuery bq) {
        super(DatabaseNamesConstants.BIGQUERY);
        this.delimitedIdentifierModeOn=false;
        this.bigQuery = bq;
        
        getDatabaseInfo().setDelimitedIdentifiersSupported(false);
        
        databaseInfo.addNativeTypeMapping(Types.INTEGER, "INT64");
        databaseInfo.addNativeTypeMapping(Types.BIT, "INT64");
        databaseInfo.addNativeTypeMapping(Types.BIGINT, "NUMERIC");
        databaseInfo.addNativeTypeMapping(Types.TINYINT, "NUMERIC");
        databaseInfo.addNativeTypeMapping(Types.DOUBLE, "NUMERIC");
        databaseInfo.addNativeTypeMapping(Types.DECIMAL, "NUMERIC");
        
        databaseInfo.addNativeTypeMapping(Types.BINARY, "BYTES");
        databaseInfo.addNativeTypeMapping(Types.BLOB, "BYTES");
         
        databaseInfo.addNativeTypeMapping(Types.VARCHAR, "STRING");
        databaseInfo.addNativeTypeMapping(Types.CHAR, "STRING");
        databaseInfo.addNativeTypeMapping(Types.LONGVARBINARY, "STRING");
        databaseInfo.addNativeTypeMapping(Types.LONGVARCHAR, "STRING");
        databaseInfo.addNativeTypeMapping(Types.VARBINARY, "STRING");
        databaseInfo.addNativeTypeMapping(Types.NVARCHAR, "STRING");
        databaseInfo.addNativeTypeMapping(Types.CLOB, "STRING");
        
        databaseInfo.setForeignKeysSupported(false);
        databaseInfo.setPrimaryKeyEmbedded(true);
        databaseInfo.setIndicesSupported(false);
        
        databaseInfo.setHasSize(Integer.valueOf(Types.CHAR), false);
        databaseInfo.setHasSize(Integer.valueOf(Types.VARCHAR), false);
        databaseInfo.setHasSize(Integer.valueOf(Types.BINARY), false);
        databaseInfo.setHasSize(Integer.valueOf(Types.VARBINARY), false);
        databaseInfo.setHasSize(Integer.valueOf(Types.NCHAR), false);
        databaseInfo.setHasSize(Integer.valueOf(Types.NVARCHAR), false);
        databaseInfo.setHasSize(Integer.valueOf(Types.NUMERIC), false);
        databaseInfo.setHasSize(Integer.valueOf(Types.DECIMAL), false);
        
        databaseInfo.setHasPrecisionAndScale(Types.NUMERIC, false);
        databaseInfo.setHasPrecisionAndScale(Types.DECIMAL, false);
    }
    
    @Override
    protected void mergeOrRemovePlatformTypes(Database currentModel, Database desiredModel) {
        super.mergeOrRemovePlatformTypes(currentModel, desiredModel);
        for (Table table : desiredModel.getTables()) {
            for (Column col : table.getColumns()) {
                col.setPrimaryKey(false);
                col.setAutoIncrement(false);
                col.setRequired(false);
                col.setDefaultValue(null);
                
                if (col.getMappedTypeCode() == Types.CHAR) {
                    col.setMappedTypeCode(Types.VARCHAR);
                } else if (col.getMappedTypeCode() == Types.BIT) {
                    col.setMappedTypeCode(Types.INTEGER);
                } else if (col.getMappedTypeCode() == Types.LONGVARCHAR) {
                    col.setMappedTypeCode(Types.VARCHAR);
                } else if (col.getMappedTypeCode() == Types.BLOB) {
                    col.setMappedTypeCode(Types.BINARY);
                } else if (col.getMappedTypeCode() == Types.DECIMAL ) {
                    col.setMappedTypeCode(Types.NUMERIC);
                }
            }
        }
    }

    @Override
    protected void writePrimaryKeyStmt(Table table, Column[] primaryKeyColumns, StringBuilder ddl) {
    }

    @Override
    protected void writeEmbeddedPrimaryKeysStmt(Table table, StringBuilder ddl) {
    }
    
    @Override
    protected void writeExternalPrimaryKeysCreateStmt(Table table, Column[] primaryKeyColumns, StringBuilder ddl) {
    }
    
    @Override
    protected void writeColumnAutoIncrementStmt(Table table, Column column, StringBuilder ddl) {
    }
    
    @Override
    protected void writeEmbeddedIndexCreateStmt(Table table, IIndex index, StringBuilder ddl) {
    }
    
    @Override
    protected void writeEmbeddedIndicesStmt(Table table, StringBuilder ddl) {
    }
    
    @Override
    protected void writeColumnUniqueStmt(StringBuilder ddl) {
    }
    
    @Override
    protected void writeColumnDefaultValueStmt(Table table, Column column, StringBuilder ddl) {
    }
    
    
}
