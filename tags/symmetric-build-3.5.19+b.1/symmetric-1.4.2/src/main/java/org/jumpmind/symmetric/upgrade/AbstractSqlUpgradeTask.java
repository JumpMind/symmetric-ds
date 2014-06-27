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

package org.jumpmind.symmetric.upgrade;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.model.Node;
import org.springframework.jdbc.core.JdbcTemplate;

abstract public class AbstractSqlUpgradeTask implements IUpgradeTask {

    protected JdbcTemplate jdbcTemplate;

    protected boolean isUpgradeRegistrationServer = true;

    protected boolean isUpgradeNonRegistrationServer = true;

    protected boolean useReplacement = true;

    public void upgrade(int[] fromVersion) {
    }

    protected String prepareSql(Node node, String sql) {
        if (useReplacement) {
            sql = replace("groupId", node.getNodeGroupId(), sql);
            sql = replace("externalId", node.getExternalId(), sql);
            sql = replace("nodeId", node.getNodeId(), sql);
        }
        return sql;
    }

    protected String replace(String prop, String replaceWith, String sourceString) {
        return StringUtils.replace(sourceString, "$(" + prop + ")", replaceWith);
    }

    public void setJdbcTemplate(JdbcTemplate jdbc) {
        this.jdbcTemplate = jdbc;
    }

    public boolean isUpgradeNonRegistrationServer() {
        return isUpgradeNonRegistrationServer;
    }

    public boolean getUpgradeNonRegistrationServer() {
        return isUpgradeNonRegistrationServer;
    }

    public void setUpgradeNonRegistrationServer(boolean isUpgradeNonRegistrationServer) {
        this.isUpgradeNonRegistrationServer = isUpgradeNonRegistrationServer;
    }

    public boolean isUpgradeRegistrationServer() {
        return isUpgradeRegistrationServer;
    }

    public boolean getUpgradeRegistrationServer() {
        return isUpgradeRegistrationServer;
    }

    public void setUpgradeRegistrationServer(boolean isUpgradeRegistrationServer) {
        this.isUpgradeRegistrationServer = isUpgradeRegistrationServer;
    }

    public boolean getUseReplacement() {
        return useReplacement;
    }

    public void setUseReplacement(boolean useReplacement) {
        this.useReplacement = useReplacement;
    }

}
