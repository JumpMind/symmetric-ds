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

package org.jumpmind.symmetric.model;

import org.jumpmind.symmetric.util.ICoded;

public enum DataEventType implements ICoded {

    /**
     * Insert DML type.
     */
    INSERT("I"),

    /**
     * Update DML type.
     */
    UPDATE("U"),

    /**
     * Delete DML type.
     */
    DELETE("D"),

    /**
     * An event that indicates that table validation needs to be done.
     */
    VALIDATE("V"),

    /**
     * An event that indicates that a table needs to be reloaded.
     */
    RELOAD("R"),

    /**
     * An event that indicates that the data payload has a sql statement that
     * needs to be executed. This is more of a remote control feature (that
     * would have been very handy in past lives).
     */
    SQL("S"),

    /**
     * An event that indicates that the data payload is a table creation.
     */
    CREATE("C");

    private String code;

    DataEventType(String code) {
        this.code = code;
    }

    public String getCode() {
        return this.code;
    }

    public static DataEventType getEventType(String s) {
        if (s.equals("I")) {
            return DataEventType.INSERT;
        } else if (s.equals("U")) {
            return DataEventType.UPDATE;
        } else if (s.equals("D")) {
            return DataEventType.DELETE;
        } else if (s.equals("R")) {
            return DataEventType.RELOAD;
        } else if (s.equals("S")) {
            return DataEventType.SQL;
        } else if (s.equals("C")) {
            return DataEventType.CREATE;
        }
        return null;
    }
}
