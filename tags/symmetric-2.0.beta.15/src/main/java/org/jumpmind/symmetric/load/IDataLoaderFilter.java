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

import org.jumpmind.symmetric.ext.IExtensionPoint;
import org.springframework.transaction.annotation.Transactional;

@Transactional
public interface IDataLoaderFilter extends IExtensionPoint {

    /**
     * @return True if the row should be loaded. False if the filter has handled
     *         the row and it should be ignored.
     */
    public boolean filterInsert(IDataLoaderContext context, String[] columnValues);

    /**
     * @param columnValues Contains 'new' values for both key and non-key columns
     * @param keyValues Contains the 'old' values for the key columns
     * @return True if the row should be loaded. False if the filter has handled
     *         the row and it should be ignored.
     */
    public boolean filterUpdate(IDataLoaderContext context, String[] columnValues, String[] keyValues);

    /**
     * @return True if the row should be loaded. False if the filter has handled
     *         the row and it should be ignored.
     */
    public boolean filterDelete(IDataLoaderContext context, String[] keyValues);

}
