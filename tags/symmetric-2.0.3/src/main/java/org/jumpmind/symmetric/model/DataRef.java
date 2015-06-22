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

import java.util.Date;

public class DataRef {

    private long refDataId;
    private Date refTime;

    public DataRef(long refDataid, Date refTime) {
        super();
        this.refDataId = refDataid;
        this.refTime = refTime;
    }

    public void setRefDataId(long refDataid) {
        this.refDataId = refDataid;
    }

    public long getRefDataId() {
        return refDataId;
    }

    public void setRefTime(Date refTime) {
        this.refTime = refTime;
    }

    public Date getRefTime() {
        return refTime;
    }

}