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
package org.jumpmind.symmetric.load;

import org.apache.ddlutils.model.Table;
import org.jumpmind.symmetric.ext.IExtensionPoint;
import org.jumpmind.symmetric.load.StatementBuilder.DmlType;

/**
 * This is an extension point that can be implemented to filter out columns from
 * use by the dataloader. One column filter may be added per target table.
 * </p>
 * Please implement {@link ITableColumnFilter} instead of this class directly if
 * you want the extension to be auto discovered.
 */
public interface IColumnFilter extends IExtensionPoint {

    /**
     * This method is always called first. Typically, you must cache the column
     * index you are interested in order to be able to filter the column value
     * as well.
     * 
     * @return The columnName that the data loader will use to build its dml.
     */
    public String[] filterColumnsNames(IDataLoaderContext ctx, DmlType dml, Table table, String[] columnNames);

    /**
     * This method is always called after
     * {@link IColumnFilter#filterColumnsNames(DmlType, String[])}. It should
     * perform the same filtering under the same conditions for the values as
     * was done for the column names.
     * 
     * @return the column values
     */
    public Object[] filterColumnsValues(IDataLoaderContext ctx, DmlType dml, Table table, Object[] columnValues);
}
