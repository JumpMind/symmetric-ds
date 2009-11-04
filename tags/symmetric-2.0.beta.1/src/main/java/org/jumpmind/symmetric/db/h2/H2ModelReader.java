package org.jumpmind.symmetric.db.h2;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.ForeignKey;
import org.apache.ddlutils.model.Index;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.model.TypeMap;
import org.apache.ddlutils.platform.DatabaseMetaDataWrapper;
import org.apache.ddlutils.platform.JdbcModelReader;
import org.apache.ddlutils.platform.MetaDataColumnDescriptor;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;

/**
 * Reads a database model from a H2 database. From patch <a
 * href="https://issues.apache.org/jira/browse/DDLUTILS-185"
 * >https://issues.apache.org/jira/browse/DDLUTILS-185</a>
 */
public class H2ModelReader extends JdbcModelReader {

    final ILog logger = LogFactory.getLog(getClass());

    /**
     * Creates a new model reader for H2 databases.
     * 
     * @param platform
     *            The platform that this model reader belongs to
     */
    public H2ModelReader(Platform platform) {
        super(platform);
        setDefaultCatalogPattern(null);
        setDefaultSchemaPattern(null);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Map readColumns(ResultSet resultSet, List columnDescriptors) throws SQLException {
        if (logger.isDebugEnabled()) {
            int count = resultSet.getMetaData().getColumnCount();
            for (int i = 1; i <= count; i++) {
                logger.debug(resultSet.getMetaData().getColumnName(i) + "=" + resultSet.getString(i));
            }
        }
        return super.readColumns(resultSet, columnDescriptors);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Column readColumn(DatabaseMetaDataWrapper metaData, Map values) throws SQLException {
        Column column = super.readColumn(metaData, values);
        if (values.get("CHARACTER_MAXIMUM_LENGTH") != null) {
            column.setSize(values.get("CHARACTER_MAXIMUM_LENGTH").toString());
        }
        if (values.get("COLUMN_DEFAULT") != null) {
            column.setDefaultValue(values.get("COLUMN_DEFAULT").toString());
        }
        if (values.get("NUMERIC_SCALE") != null) {
            column.setScale((Integer) values.get("NUMERIC_SCALE"));
        }
        if (TypeMap.isTextType(column.getTypeCode()) && (column.getDefaultValue() != null)) {
            column.setDefaultValue(unescape(column.getDefaultValue(), "'", "''"));
        }
        return column;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected List initColumnsForColumn() {
        List result = new ArrayList();
        result.add(new MetaDataColumnDescriptor("COLUMN_DEF", 12));
        result.add(new MetaDataColumnDescriptor("COLUMN_DEFAULT", 12));
        result.add(new MetaDataColumnDescriptor("TABLE_NAME", 12));
        result.add(new MetaDataColumnDescriptor("COLUMN_NAME", 12));
        result.add(new MetaDataColumnDescriptor("DATA_TYPE", 4, new Integer(1111)));
        result.add(new MetaDataColumnDescriptor("NUM_PREC_RADIX", 4, new Integer(10)));
        result.add(new MetaDataColumnDescriptor("DECIMAL_DIGITS", 4, new Integer(0)));
        result.add(new MetaDataColumnDescriptor("NUMERIC_SCALE", 4, new Integer(0)));
        result.add(new MetaDataColumnDescriptor("COLUMN_SIZE", 12));
        result.add(new MetaDataColumnDescriptor("CHARACTER_MAXIMUM_LENGTH", 12));
        result.add(new MetaDataColumnDescriptor("IS_NULLABLE", 12, "YES"));
        result.add(new MetaDataColumnDescriptor("REMARKS", 12));
        return result;
    }

    @Override
    protected boolean isInternalForeignKeyIndex(DatabaseMetaDataWrapper metaData, Table table, ForeignKey fk,
            Index index) {
        String name = index.getName();
        return name != null && name.startsWith("CONSTRAINT_INDEX_");
    }

    @Override
    protected boolean isInternalPrimaryKeyIndex(DatabaseMetaDataWrapper metaData, Table table, Index index) {
        String name = index.getName();
        return name != null && name.startsWith("PRIMARY_KEY_");
    }
}
