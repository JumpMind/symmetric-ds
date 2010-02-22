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
package org.jumpmind.symmetric.db.h2;

import java.sql.Connection;
import java.sql.SQLException;

import org.jumpmind.symmetric.db.AbstractEmbeddedTrigger;

public class H2Trigger extends AbstractEmbeddedTrigger implements org.h2.api.Trigger {

    /**
     * This method is called by the database engine once when initializing the
     * trigger.
     * 
     * @param conn
     *            a connection to the database
     * @param schemaName
     *            the name of the schema
     * @param triggerName
     *            the name of the trigger used in the CREATE TRIGGER statement
     * @param tableName
     *            the name of the table
     * @param before
     *            whether the fire method is called before or after the
     *            operation is performed
     * @param type
     *            the operation type: INSERT, UPDATE, or DELETE
     */
    public void init(Connection conn, String schemaName, String triggerName, String tableName, boolean before, int type)
            throws SQLException {
        this.init(conn, triggerName, tableName);
    }

}
