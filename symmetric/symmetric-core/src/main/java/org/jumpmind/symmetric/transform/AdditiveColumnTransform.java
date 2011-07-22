package org.jumpmind.symmetric.transform;

import java.math.BigDecimal;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.ddl.model.Column;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.ext.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.springframework.jdbc.core.JdbcTemplate;

public class AdditiveColumnTransform implements IColumnTransform, IBuiltInExtensionPoint {

    protected IDbDialect dbDialect;

    public boolean isAutoRegister() {
        return true;
    }

    public String getName() {
        return "additive";
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

    public String transform(IDataLoaderContext context, TransformColumn column,
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
                StringBuilder sql = new StringBuilder(String.format("update %s set %s=%s+? where ",
                        getFullyQualifiedTableName(data.getSchemaName(), data.getCatalogName(), data.getTableName()), 
                        quote + column.getTargetColumnName() + quote,
                        quote + column.getTargetColumnName() + quote));

                String[] keyNames = data.getKeyNames();
                Column[] columns = new Column[keyNames.length + 1];
                columns[0] = table.getColumnWithName(column.getTargetColumnName());
                for (int i = 0; i < keyNames.length; i++) {
                    if (i > 0) {
                        sql.append("and ");
                    }
                    columns[i + 1] = table.getColumnWithName(keyNames[i]);
                    sql.append(quote);
                    sql.append(keyNames[i]);
                    sql.append(quote);
                    sql.append("=? ");
                }

                if (0 < template.update(
                        sql.toString(),
                        dbDialect.getObjectValues(context.getBinaryEncoding(),
                                prepend(value, data.getKeyValues()), columns))) {
                    throw new IgnoreColumnException();
                }

            }
        }
        return value;
    }

    protected String[] prepend(String v, String[] array) {
        String[] dest = new String[array.length + 1];
        dest[0] = v;
        for (int i = 0; i < array.length; i++) {
            dest[i + 1] = array[i];
        }
        return dest;
    }

    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

}
