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

import java.util.Calendar;

public class Lease {
    private String resourceId;

    private String leaseType;

    private Calendar leased;

    private Calendar leaseExpires;

    // public getters

    public Calendar getLeased() {
        return leased;
    }

    public Calendar getLeaseExpires() {
        return leaseExpires;
    }

    public String getResourceId() {
        return resourceId;
    }

    public String getLeaseType() {
        return leaseType;
    }

    // protected setters

    public void setLeased(Calendar leased) {
        this.leased = leased;
    }

    public void setLeaseExpires(Calendar leaseExpires) {
        this.leaseExpires = leaseExpires;
    }

    public void setResourceId(String resourceId) {
        this.resourceId = resourceId;
    }

    public void setLeaseType(String leaseType) {
        this.leaseType = leaseType;
    }

}
