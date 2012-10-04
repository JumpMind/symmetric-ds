/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Eric Long <erilong@users.sourceforge.net>,
 *               Chris Henson <chenson42@users.sourceforge.net>
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

package org.jumpmind.symmetric.io.data;

final public class CsvConstants {

    private CsvConstants() {
    }
    
    public static final String BINARY = "binary";

    public static final String NODEID = "nodeid";

    public static final String SCHEMA = "schema";
    
    public static final String CATALOG = "catalog";
    
    public static final String TABLE = "table";

    public static final String KEYS = "keys";

    public static final String COLUMNS = "columns";

    public static final String BATCH = "batch";

    public static final String INSERT = "insert";

    public static final String UPDATE = "update";

    public static final String OLD = "old";

    public static final String DELETE = "delete";

    public static final String COMMIT = "commit";

    public static final String SQL = "sql";
    
    public static final String BSH = "bsh";

    public static final String CREATE = "create";

    public static final String CHANNEL = "channel";
    
    public static final String IGNORE = "ignore";
}
