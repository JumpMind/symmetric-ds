package org.jumpmind.symmetric.io.data.transform;

import java.math.BigDecimal;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.io.data.DataContext;

public class AdditiveColumnTransform implements ISingleValueColumnTransform, IBuiltInExtensionPoint {

    public static final String NAME = "additive";
    
    public String getName() {
        return NAME;
    }
    
    public boolean isExtractColumnTransform() {
        return false;
    }
    
    public boolean isLoadColumnTransform() {
        return true;
    }
    
    public String getFullyQualifiedTableName(IDatabasePlatform platform, String schema, String catalog, String tableName) {
        String quote = platform.getDdlBuilder().isDelimitedIdentifierModeOn() ? platform
            .getDatabaseInfo().getDelimiterToken() : "";
        tableName = quote + tableName + quote;
        if (!StringUtils.isBlank(schema)) {
            tableName = schema + "." + tableName;
        }
        if (!StringUtils.isBlank(catalog)) {
            tableName = catalog + "." + tableName;
        }
        return tableName;
    }

    public String transform(IDatabasePlatform platform, DataContext context,
            TransformColumn column, TransformedData data, Map<String, String> sourceValues, String newValue, String oldValue) throws IgnoreColumnException,
            IgnoreRowException {
        
        BigDecimal multiplier = new BigDecimal(1.00);
        
        if (StringUtils.isNotBlank(column.getTransformExpression())) {
            multiplier = new BigDecimal(column.getTransformExpression());            
        }
        
        BigDecimal delta = new BigDecimal(newValue);
        
        Table table = platform.getTableFromCache(data.getCatalogName(), data.getSchemaName(),
                data.getTableName(), false);
        if (table != null) {
            if (!StringUtils.isNotBlank(newValue)) {
                newValue="0.00";
            }
            
            if (!StringUtils.isNotBlank(oldValue)) {
                oldValue="0.00";
            }
            
            delta = delta.subtract(new BigDecimal(oldValue));
            delta = delta.multiply(multiplier);
            newValue = delta.toString();
            
            String quote = platform.getDdlBuilder().isDelimitedIdentifierModeOn() ? platform
                    .getDatabaseInfo().getDelimiterToken() : "";
            StringBuilder sql = new StringBuilder(String.format("update %s set %s=%s+(%s) where ",
                    getFullyQualifiedTableName(platform, data.getSchemaName(), data.getCatalogName(), data.getTableName()), 
                    quote + column.getTargetColumnName() + quote,
                    quote + column.getTargetColumnName() + quote,
                    newValue));

            String[] keyNames = data.getKeyNames();
            Column[] columns = new Column[keyNames.length];
            for (int i = 0; i < keyNames.length; i++) {
                if (i > 0) {
                    sql.append("and ");
                }
                columns[i] = table.getColumnWithName(keyNames[i]);
                if (columns[i] == null) {
                    throw new NullPointerException("Could not find a column named: " + keyNames[i] + " on the target table: " + table.getName());
                }
                sql.append(quote);
                sql.append(keyNames[i]);
                sql.append(quote);
                sql.append("=? ");
            }

            if (0 < platform.getSqlTemplate().update(
                    sql.toString(),
                    platform.getObjectValues(context.getBatch().getBinaryEncoding(),
                            data.getKeyValues(), columns))) {
                throw new IgnoreColumnException();
            }

        }
        
        return newValue;
    }

}
