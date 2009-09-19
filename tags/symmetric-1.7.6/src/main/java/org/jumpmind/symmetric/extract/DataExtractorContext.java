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

package org.jumpmind.symmetric.extract;

import java.util.ArrayList;
import java.util.List;

import org.jumpmind.symmetric.model.OutgoingBatch;

public class DataExtractorContext implements Cloneable {

    private List<String> auditRecordsWritten = new ArrayList<String>();
    private String lastTableName;
    private OutgoingBatch batch;
    private IDataExtractor dataExtractor;

    public DataExtractorContext copy(IDataExtractor extractor) {
        this.dataExtractor = extractor;
        DataExtractorContext newVersion;
        try {
            newVersion = (DataExtractorContext) super.clone();
            newVersion.auditRecordsWritten = new ArrayList<String>();
            return newVersion;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> getAuditRecordsWritten() {
        return auditRecordsWritten;
    }

    public void setLastTableName(String tableName) {
        lastTableName = tableName;
    }

    public boolean isLastTable(String tableName) {
        return lastTableName.equals(tableName);
    }

    public OutgoingBatch getBatch() {
        return batch;
    }

    public void setBatch(OutgoingBatch batch) {
        this.batch = batch;
    }

    public IDataExtractor getDataExtractor() {
        return dataExtractor;
    }

}
