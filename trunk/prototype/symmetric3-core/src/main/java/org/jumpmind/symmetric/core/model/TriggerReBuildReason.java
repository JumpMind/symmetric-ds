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

package org.jumpmind.symmetric.core.model;

/**
 * {@link TriggerHistory}
 */
public enum TriggerReBuildReason {

    NEW_TRIGGERS("N"), TABLE_SCHEMA_CHANGED("S"), TABLE_SYNC_CONFIGURATION_CHANGED("C"), FORCED("F"), TRIGGERS_MISSING(
            "T");

    private String code;

    TriggerReBuildReason(String code) {
        this.code = code;
    }

    public String getCode() {
        return this.code;
    }

    public static TriggerReBuildReason fromCode(String code) {
        if (code != null && code.length() > 0) {
            if (code.equals(NEW_TRIGGERS.code)) {
                return NEW_TRIGGERS;
            } else if (code.equals(TABLE_SCHEMA_CHANGED.code)) {
                return TABLE_SCHEMA_CHANGED;
            } else if (code.equals(TABLE_SYNC_CONFIGURATION_CHANGED.code)) {
                return TABLE_SYNC_CONFIGURATION_CHANGED;
            } else if (code.equals(FORCED.code)) {
                return FORCED;
            }
        }
        return null;
    }

}
