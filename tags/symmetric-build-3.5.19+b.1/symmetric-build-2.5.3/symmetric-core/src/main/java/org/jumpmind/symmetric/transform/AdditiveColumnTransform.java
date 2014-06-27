package org.jumpmind.symmetric.transform;

import java.math.BigDecimal;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.ddl.model.Column;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.ext.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ext.ICacheContext;
import org.springframework.jdbc.core.JdbcTemplate;

public class AdditiveColumnTransform implements ISingleValueColumnTransform, IBuiltInExtensionPoint {

    public static final String NAME = "additive";
    
    protected IDbDialect dbDialect;

    public boolean isAutoRegister() {
        return true;
    }

    public String getName() {
        return NAME;
    }
    
    public String getFullyQualifiedTableName(String schema, String catalog, String tableName) {
        String quote = dbDialect.getPlatform().isDelimitedIdentifierModeOn() ? dbDialect.getPlatform()
            .getPlatformInfo().getDelimiterToken() : "";
        tableName = quote + tableName + quote;
        if (!StringUtils.isBlank(schema)) {
            tableName = schema + "." + tableName;
        }
        if (!StringUtils.isBlank(catalog)) {
            tableName = catalog + "." + tableName;
        }
        return tableName;
    }

    public String transform(ICacheContext context, TransformColumn column,
            TransformedData data, Map<String, String> sourceValues, String value, String oldValue) throws IgnoreColumnException,
            IgnoreRowException {
        if (StringUtils.isNotBlank(value)) {
            BigDecimal newValue = new BigDecimal(value);
            Table table = dbDialect.getTable(data.getCatalogName(), data.getSchemaName(),
                    data.getTableName(), true);
            if (table != null) {
                if (StringUtils.isNotBlank(oldValue)) {
                    newValue = newValue.subtract(new BigDecimal(oldValue));
                    value = newValue.toString();
                }
                
                String quote = dbDialect.getPlatform().isDelimitedIdentifierModeOn() ? dbDialect.getPlatform()
                        .getPlatformInfo().getDelimiterToken() : "";
                JdbcTemplate template = context.getJdbcTemplate();
                StringBuilder sql = new StringBuilder(String.format("update %s set %s=%s+%s where ",
                        getFullyQualifiedTableName(data.getSchemaName(), data.getCatalogName(), data.getTableName()), 
                        quote + column.getTargetColumnName() + quote,
                        quote + column.getTargetColumnName() + quote,
                        value));

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

                if (0 < template.update(
                        sql.toString(),
                        dbDialect.getObjectValues(context.getBinaryEncoding(),
                                data.getKeyValues(), columns))) {
                    throw new IgnoreColumnException();
                }

            }
        }
        return value;
    }

    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

}
