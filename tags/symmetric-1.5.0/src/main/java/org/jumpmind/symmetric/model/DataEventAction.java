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

/**
 * Identifies the action to take when the event watcher sees events in the event
 * table.
 */
public enum DataEventAction implements ICoded {

    PUSH("P"), WAIT_FOR_POLL("W");

    private String code;

    DataEventAction(String code) {
        this.code = code;
    }

    public String getCode() {
        return this.code;
    }

    public static DataEventAction fromCode(String code) {
        if (code != null && code.length() > 0) {
            if (PUSH.code.equals(code)) {
                return PUSH;
            } else if (WAIT_FOR_POLL.code.equals(code)) {
                return WAIT_FOR_POLL;
            }
        }
        return null;
    }

}
