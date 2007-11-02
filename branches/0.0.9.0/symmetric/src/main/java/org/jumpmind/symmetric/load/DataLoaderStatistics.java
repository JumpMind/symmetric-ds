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

package org.jumpmind.symmetric.load;

import java.util.Date;

public class DataLoaderStatistics implements IDataLoaderStatistics {

    private Date startTime;
    
    private long lineCount;
    
    private long statementCount;
    
    private long fallbackInsertCount;
    
    private long fallbackUpdateCount;
    
    private long missingDeleteCount;

    public DataLoaderStatistics() {
        this.startTime = new Date();
    }
    
    public long incrementLineCount() {
        return ++lineCount;
    }

    public long incrementFallbackInsertCount() {
        return ++fallbackInsertCount;
    }

    public long incrementFallbackUpdateCount() {
        return ++fallbackUpdateCount;
    }

    public long incrementMissingDeleteCount() {
        return ++missingDeleteCount;
    }

    public long incrementStatementCount() {
        return ++statementCount;
    }

    public long getFallbackInsertCount() {
        return fallbackInsertCount;
    }

    public void setFallbackInsertCount(long fallbackInsertCount) {
        this.fallbackInsertCount = fallbackInsertCount;
    }

    public long getFallbackUpdateCount() {
        return fallbackUpdateCount;
    }

    public void setFallbackUpdateCount(long fallbackUpdateCount) {
        this.fallbackUpdateCount = fallbackUpdateCount;
    }

    public long getLineCount() {
        return lineCount;
    }

    public void setLineCount(long lineCount) {
        this.lineCount = lineCount;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public long getStatementCount() {
        return statementCount;
    }

    public void setStatementCount(long statementCount) {
        this.statementCount = statementCount;
    }

    public long getMissingDeleteCount() {
        return missingDeleteCount;
    }

    public void setMissingDeleteCount(long missingDeleteCount) {
        this.missingDeleteCount = missingDeleteCount;
    }

}
