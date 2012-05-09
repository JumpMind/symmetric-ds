package org.jumpmind.symmetric.io.data.transform;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.io.data.DataContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdditiveColumnTransform implements ISingleValueColumnTransform, IBuiltInExtensionPoint {

    static final Logger log = LoggerFactory.getLogger(AdditiveColumnTransform.class);
    
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
        
        Table table = platform.getTableFromCache(data.getCatalogName(), data.getSchemaName(),
                data.getTableName(), false);
        if (table == null) {
            if (log.isDebugEnabled()) {
                log.debug("Could not find the target table {}" , data.getFullyQualifiedTableName());
            }
            throw new IgnoreColumnException();
        } else if (table.getColumnWithName(column.getTargetColumnName()) == null) {
            if (log.isDebugEnabled()) {
                log.debug("Could not find the target column {}" , column.getTargetColumnName());
            }
            throw new IgnoreColumnException();
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Old and new as received: "+oldValue+", "+newValue);
            }
            
            if (!StringUtils.isNotBlank(newValue)) {
                newValue="0";
            }
            if (!StringUtils.isNotBlank(oldValue)) {
                oldValue="0";
            }
            
            BigDecimal delta = new BigDecimal(newValue);
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
            List<Column> columns = new ArrayList<Column>();
            List<String> keyValuesList = new ArrayList<String>();
            boolean addedFirstKey = false;
            for (int i = 0; i < keyNames.length; i++) {
                Column targetCol = table.getColumnWithName(keyNames[i]);
                if (targetCol != null) {
                    columns.add(targetCol);
                    keyValuesList.add(sourceValues.get(keyNames[i]));
                    if (addedFirstKey) {
                        sql.append("and ");
                    } else {
                        addedFirstKey = true;
                    }
                    sql.append(quote);
                    sql.append(keyNames[i]);
                    sql.append(quote);
                    sql.append("=? ");
                }
            }

            if (log.isDebugEnabled()) {
                log.debug("SQL: "+sql);
            }
            if (0 < platform.getSqlTemplate().update(
                    sql.toString(),
                    platform.getObjectValues(context.getBatch().getBinaryEncoding(),
                            keyValuesList.toArray(new String[keyValuesList.size()]),
                            columns.toArray(new Column[columns.size()])))) {
                throw new IgnoreColumnException();
            }

        } 
        
        return newValue;
    }
}
