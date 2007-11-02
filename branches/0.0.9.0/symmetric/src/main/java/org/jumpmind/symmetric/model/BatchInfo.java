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

public class BatchInfo {
    public static final String OK = "OK";
    
    public static final int UNDEFINED_ERROR_LINE_NUMBER = 0;

    private String batchId;
    
    private boolean isOk;

    private long errorLine;

    public BatchInfo(String batchId) {
        this.batchId = batchId;
        isOk = true;
    }

    public BatchInfo(String batchId, long errorLineNumber) {
        this.batchId = batchId;
        isOk = false;
        errorLine = errorLineNumber;
    }

    public String getBatchId() {
        return batchId;
    }

    public long getErrorLine() {
        return errorLine;
    }

    public boolean isOk() {
        return isOk;
    }

}
