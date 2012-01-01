package org.jumpmind.db.platform.sybase;

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

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.PatternCompiler;
import org.apache.oro.text.regex.PatternMatcher;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.Reference;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.platform.AbstractJdbcDdlReader;
import org.jumpmind.db.platform.DatabaseMetaDataWrapper;
import org.jumpmind.db.platform.DdlException;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.log.Log;

/*
 * Reads a database model from a Sybase database.
 */
public class SybaseDdlReader extends AbstractJdbcDdlReader {
    /* The regular expression pattern for the ISO dates. */
    private Pattern _isoDatePattern;

    /* The regular expression pattern for the ISO times. */
    private Pattern _isoTimePattern;

    public SybaseDdlReader(Log log, IDatabasePlatform platform) {
        super(log, platform);
        setDefaultCatalogPattern(null);
        setDefaultSchemaPattern(null);
        setDefaultTablePattern("%");

        PatternCompiler compiler = new Perl5Compiler();

        try {
            _isoDatePattern = compiler.compile("'(\\d{4}\\-\\d{2}\\-\\d{2})'");
            _isoTimePattern = compiler.compile("'(\\d{2}:\\d{2}:\\d{2})'");
        } catch (MalformedPatternException ex) {
            throw new DdlException(ex);
        }
    }

    @Override
    protected Table readTable(Connection connection, DatabaseMetaDataWrapper metaData, Map values)
            throws SQLException {
        Table table = super.readTable(connection, metaData, values);

        if (table != null) {
            // Sybase does not return the auto-increment status via the database
            // metadata
            determineAutoIncrementFromResultSetMetaData(connection, table, table.getColumns());
        }
        return table;
    }

    @Override
    protected Integer overrideJdbcTypeForColumn(Map<String, Object> values) {
        String typeName = (String) values.get("TYPE_NAME");
        if (typeName != null && typeName.toUpperCase().startsWith("TEXT")) {
            return Types.LONGVARCHAR;
        } else {
            return super.overrideJdbcTypeForColumn(values);
        }
    }

    @Override
    protected Column readColumn(DatabaseMetaDataWrapper metaData, Map values) throws SQLException {
        Column column = super.readColumn(metaData, values);

        if ((column.getTypeCode() == Types.DECIMAL) && (column.getSizeAsInt() == 19)
                && (column.getScale() == 0)) {
            // Back-mapping to BIGINT
            column.setTypeCode(Types.BIGINT);
        } else if (column.getDefaultValue() != null) {
            if (column.getTypeCode() == Types.TIMESTAMP) {
                // Sybase maintains the default values for DATE/TIME jdbc types,
                // so we have to
                // migrate the default value to TIMESTAMP
                PatternMatcher matcher = new Perl5Matcher();
                Timestamp timestamp = null;

                if (matcher.matches(column.getDefaultValue(), _isoDatePattern)) {
                    timestamp = new Timestamp(Date.valueOf(matcher.getMatch().group(1)).getTime());
                } else if (matcher.matches(column.getDefaultValue(), _isoTimePattern)) {
                    timestamp = new Timestamp(Time.valueOf(matcher.getMatch().group(1)).getTime());
                }
                if (timestamp != null) {
                    column.setDefaultValue(timestamp.toString());
                }
            } else if (TypeMap.isTextType(column.getTypeCode())) {
                column.setDefaultValue(unescape(column.getDefaultValue(), "'", "''"));
            }
        }
        return column;
    }

    @Override
    protected void readIndex(DatabaseMetaDataWrapper metaData, Map values, Map knownIndices)
            throws SQLException {
        if (getPlatform().isDelimitedIdentifierModeOn()) {
            String indexName = (String) values.get("INDEX_NAME");

            // Sometimes, Sybase keeps the delimiter quotes around the index
            // names
            // when returning them in the metadata, so we strip them
            if (indexName != null) {
                String delimiter = getPlatformInfo().getDelimiterToken();

                if ((indexName != null) && indexName.startsWith(delimiter)
                        && indexName.endsWith(delimiter)) {
                    indexName = indexName.substring(delimiter.length(), indexName.length()
                            - delimiter.length());
                    values.put("INDEX_NAME", indexName);
                }
            }
        }
        super.readIndex(metaData, values, knownIndices);
    }

    @Override
    protected Collection readForeignKeys(Connection connection, DatabaseMetaDataWrapper metaData,
            String tableName) throws SQLException {
        // Sybase (or jConnect) does not return the foreign key names, thus we
        // have to
        // read the foreign keys manually from the system tables
        StringBuffer query = new StringBuffer();

        query.append("SELECT refobjs.name, localtables.id, remotetables.name, remotetables.id");
        for (int idx = 1; idx <= 16; idx++) {
            query.append(", refs.fokey");
            query.append(idx);
            query.append(", refs.refkey");
            query.append(idx);
        }
        query.append(" FROM sysreferences refs, sysobjects refobjs, sysobjects localtables, sysobjects remotetables");
        query.append(" WHERE refobjs.type = 'RI' AND refs.constrid = refobjs.id AND");
        query.append(" localtables.type = 'U' AND refs.tableid = localtables.id AND localtables.name = '");
        query.append(tableName);
        query.append("' AND remotetables.type = 'U' AND refs.reftabid = remotetables.id");

        Statement stmt = connection.createStatement();
        PreparedStatement prepStmt = connection
                .prepareStatement("SELECT name FROM syscolumns WHERE id = ? AND colid = ?");
        ArrayList result = new ArrayList();

        try {
            ResultSet fkRs = stmt.executeQuery(query.toString());

            while (fkRs.next()) {
                ForeignKey fk = new ForeignKey(fkRs.getString(1));
                int localTableId = fkRs.getInt(2);
                int remoteTableId = fkRs.getInt(4);

                fk.setForeignTableName(fkRs.getString(3));
                for (int idx = 0; idx < 16; idx++) {
                    short fkColIdx = fkRs.getShort(5 + idx + idx);
                    short pkColIdx = fkRs.getShort(6 + idx + idx);
                    Reference ref = new Reference();

                    if (fkColIdx == 0) {
                        break;
                    }

                    prepStmt.setInt(1, localTableId);
                    prepStmt.setShort(2, fkColIdx);

                    ResultSet colRs = prepStmt.executeQuery();

                    if (colRs.next()) {
                        ref.setLocalColumnName(colRs.getString(1));
                    }
                    colRs.close();

                    prepStmt.setInt(1, remoteTableId);
                    prepStmt.setShort(2, pkColIdx);

                    colRs = prepStmt.executeQuery();

                    if (colRs.next()) {
                        ref.setForeignColumnName(colRs.getString(1));
                    }
                    colRs.close();

                    fk.addReference(ref);
                }
                result.add(fk);
            }

            fkRs.close();
        } finally {
            stmt.close();
            prepStmt.close();
        }

        return result;
    }

    @Override
    protected boolean isInternalPrimaryKeyIndex(Connection connection,
            DatabaseMetaDataWrapper metaData, Table table, IIndex index) throws SQLException {
        // We can simply check the sysindexes table where a specific flag is set
        // for pk indexes
        StringBuffer query = new StringBuffer();

        query.append("SELECT name = sysindexes.name FROM sysindexes, sysobjects WHERE sysobjects.name = '");
        query.append(table.getName());
        query.append("' AND sysindexes.name = '");
        query.append(index.getName());
        query.append("' AND sysobjects.id = sysindexes.id AND (sysindexes.status & 2048) > 0");

        Statement stmt = connection.createStatement();

        try {
            ResultSet rs = stmt.executeQuery(query.toString());
            boolean result = rs.next();

            rs.close();
            return result;
        } finally {
            stmt.close();
        }
    }
}
