/*
 * SymmetricDS is an open source database synchronization solution.
 *
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
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
package org.jumpmind.symmetric.db;

import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerRouter;

abstract public class AbstractEmbeddedDbDialect extends AbstractDbDialect implements IDbDialect {

    @Override
    protected void initForSpecificDialect() {
    }
    
    /**
     * All the templates have ' escaped because the SQL is inserted into a view.
     * When returning the raw SQL for use as SQL it needs to be un-escaped.
     */
    @Override
    public String createInitalLoadSqlFor(Node node, TriggerRouter trigger) {
        String sql = super.createInitalLoadSqlFor(node, trigger);
        sql = sql.replace("''", "'");
        return sql;
    }

    @Override
    public String createCsvDataSql(Trigger trigger, String whereClause) {
        String sql = super.createCsvDataSql(trigger, whereClause);
        sql = sql.replace("''", "'");
        return sql;
    }

    @Override
    public String createCsvPrimaryKeySql(Trigger trigger, String whereClause) {
        String sql = super.createCsvPrimaryKeySql(trigger, whereClause);
        sql = sql.replace("''", "'");
        return sql;
    }

  
    public void purge() {
    }

    public String getDefaultCatalog() {
        return null;
    }

    @Override
    public String getInitialLoadTableAlias() {
        return "t.";
    }

    @Override
    public String preProcessTriggerSqlClause(String sqlClause) {
        sqlClause = sqlClause.replace("$(newTriggerValue).", "$(newTriggerValue)");
        sqlClause = sqlClause.replace("$(oldTriggerValue).", "$(oldTriggerValue)");
        sqlClause = sqlClause.replace("$(curTriggerValue).", "$(curTriggerValue)");
        return sqlClause.replace("'", "''");
    }

}
