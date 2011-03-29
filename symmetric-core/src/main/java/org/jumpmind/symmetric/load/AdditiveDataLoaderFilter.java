/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.  */

package org.jumpmind.symmetric.load;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * The AdditiveDataLoaderFilter uses column-level sync-ing to allow data loads
 * which are either column-level additive or override in nature. Additive
 * columns use the incoming new and old values to compute the delta to be
 * applied to the node's current value. Override columns simply override (write
 * over) whatever value the node currently has.
 */
public class AdditiveDataLoaderFilter implements INodeGroupDataLoaderFilter {

    private static final ILog log = LogFactory.getLog(AdditiveDataLoaderFilter.class);

    private String tableName;

    private String[] additiveColumnNames;

    private String[] overrideColumnNames;

    private String[] nodeGroups;

    protected JdbcTemplate jdbcTemplate;

    private boolean autoRegister = true;

    public boolean filterDelete(IDataLoaderContext context, String[] keyValues) {
        if (!tableName.equalsIgnoreCase(context.getTableName())) {
            return true;
        } else {
            // The correct behavior here would seem to be to use the "old"
            // values to back out the node's overall
            // contribution to the summary columns, much like a reverse update.
            throw new RuntimeException("delete not supported for AdditiveDataLoaderFilter, table: "
                    + context.getTableName() + ", key(s): " + keyValues);
        }

    }

    public boolean filterInsert(IDataLoaderContext context, String[] columnValues) {
        if (!tableName.equalsIgnoreCase(context.getTableName())) {
            return true;
        } else {
            // We first attempt an update, since the row may already exist from
            // a different node. If the update returns 'false', then we need to
            // do a traditional insert as we are the first to insert the
            // additive table row.

            // Build the list of key values for the update call
            String[] keyValues = new String[context.getKeyNames().length];
            for (int i = 0; i < context.getKeyNames().length; i++) {
                keyValues[i] = columnValues[context.getColumnIndex(context.getKeyNames()[i])];
            }

            boolean result = !update(context, columnValues, keyValues);
            return result;
        }
    }

    public boolean filterUpdate(IDataLoaderContext context, String[] columnValues, String[] keyValues) {
        if (!tableName.equalsIgnoreCase(context.getTableName())) {
            return true;
        } else {
            boolean result = !update(context, columnValues, keyValues);
            return result;
        }
    }

    /**
     * Updates only the columns specified in the additive column list.
     * 
     * @return Returns true if updated one or more rows
     */
    protected boolean update(IDataLoaderContext context, String[] columnValues, String[] keyValues) {
        Object[] colData = context.getObjectValues(columnValues);
        Object[] keyData = context.getObjectKeyValues(keyValues);
        StringBuilder s = new StringBuilder();
        s.append("update " + context.getTableName());
        List<Object> values = new ArrayList<Object>();
        String setClause = buildSetClause(context, colData, values);

        // Nothing to update or set? If so, return true since we don't need the
        // caller to do any work.

        if (StringUtils.trimToNull(setClause) == null) {
            return false;
        }

        s.append(" set ").append(setClause);
        s.append(buildWhereClause(context, keyData, values));
        log.debug("Sql", s);
        return (jdbcTemplate.update(s.toString(), values.toArray()) > 0);
    }

    /**
     * We only include columns listed as "additive" all others are ignored
     * 
     * @param context
     * @param columnValues
     * @return
     */
    protected String buildSetClause(IDataLoaderContext context, Object[] columnValues, List<Object> values) {
        StringBuilder s = new StringBuilder();

        if (overrideColumnNames != null || additiveColumnNames != null) {

            // Track the moment when we add our first name=value pair to the set
            // list.
            boolean firstSet = false;

            // Override columns...
            if (overrideColumnNames != null) {
                for (int i = 0; i < overrideColumnNames.length; i++) {
                    int overrideColumnIndex = context.getColumnIndex(overrideColumnNames[i]);
                    Object newData = columnValues[overrideColumnIndex];

                    if (newData != null) {
                        if (firstSet) {
                            s.append(", ");
                        }
                        firstSet = true;
                        s.append(overrideColumnNames[i]);
                        s.append("=?");

                        values.add(newData);

                    }
                }
            }
            if (additiveColumnNames != null) {
                Object[] oldValues = context.getOldObjectValues();
                // Additive columns...
                for (int i = 0; i < additiveColumnNames.length; i++) {
                    int additiveColumnIndex = context.getColumnIndex(additiveColumnNames[i]);
                    Object oldData = oldValues == null ? null : oldValues[additiveColumnIndex];
                    Object newData = columnValues[additiveColumnIndex];

                    if (newData != null) {
                        if (oldData == null || !newData.equals(oldData)) {
                            if (firstSet) {
                                s.append(", ");
                            }

                            firstSet = true;
                            s.append(additiveColumnNames[i]);
                            s.append("=");
                            s.append(additiveColumnNames[i]);
                            s.append("+");
                            s.append(newData);

                            if (oldData != null) {
                                s.append("-(");
                                s.append(oldData);
                                s.append(")");
                            }
                        }
                    }
                }
            }
        }
        return s.toString();
    }

    protected String buildWhereClause(IDataLoaderContext context, Object[] keyValues, List<Object> values) {

        StringBuilder s = new StringBuilder();
        s.append(" where ");

        for (int i = 0; i < context.getKeyNames().length; i++) {
            String key = context.getKeyNames()[i];
            s.append(key);
            s.append("=?");

            values.add(keyValues[i]);

            if (i != context.getKeyNames().length - 1) {
                s.append(" and ");
            }
        }
        return s.toString();
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setNodeGroups(String[] nodeGroups) {
        this.nodeGroups = nodeGroups;
    }

    public boolean isAutoRegister() {
        return autoRegister;
    }

    public void setAutoRegister(boolean autoRegister) {
        this.autoRegister = autoRegister;
    }

    public String[] getNodeGroupIdsToApplyTo() {
        return nodeGroups;
    }

    public String[] getAdditiveColumnNames() {
        return additiveColumnNames;
    }

    public void setAdditiveColumnNames(String[] additiveColumnNames) {
        this.additiveColumnNames = additiveColumnNames;
    }

    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public String[] getOverrideColumnNames() {
        return overrideColumnNames;
    }

    public void setOverrideColumnNames(String[] overrideColumnNames) {
        this.overrideColumnNames = overrideColumnNames;
    }
}