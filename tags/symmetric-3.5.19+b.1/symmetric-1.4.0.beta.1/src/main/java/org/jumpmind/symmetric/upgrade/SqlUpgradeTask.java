/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Eric Long <erilong@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

package org.jumpmind.symmetric.upgrade;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.Node;

public class SqlUpgradeTask extends AbstractSqlUpgradeTask {

    private static final Log logger = LogFactory.getLog(SqlUpgradeTask.class);

    protected IDbDialect dbDialect;

    protected String dialectName;

    protected List<String> sqlList;

    protected boolean ignoreFailure;

    public void upgrade(int[] fromVersion) {
        for (String sql : sqlList) {
            logger.warn("Upgrade: " + sql);
            jdbcTemplate.update(sql);
        }
    }

    public void upgrade(Node node, int[] fromVersion) {
        if (dialectName == null || (dbDialect != null && dbDialect.getName().equalsIgnoreCase((dialectName)))) {
            for (String sql : sqlList) {
                sql = prepareSql(node, sql);
                logger.warn("Upgrade: " + sql);
                if (ignoreFailure) {
                    try {
                        jdbcTemplate.update(sql);
                    } catch (Exception e) {
                        logger.warn("Ignoring failure of last upgrade statement: " + e.getMessage());
                    }
                } else {
                    jdbcTemplate.update(sql);
                }
            }
        }
    }

    public void setSqlList(List<String> sqlList) {
        this.sqlList = sqlList;
    }

    public void setIgnoreFailure(boolean ignoreFailure) {
        this.ignoreFailure = ignoreFailure;
    }

    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

    public void setDialectName(String dialectName) {
        this.dialectName = dialectName;
    }

}
