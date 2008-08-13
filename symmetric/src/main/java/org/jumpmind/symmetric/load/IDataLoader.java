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

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.springframework.transaction.annotation.Transactional;

public interface IDataLoader extends Cloneable {

    public void open(BufferedReader in) throws IOException;

    public void open(BufferedReader in, List<IDataLoaderFilter> filters, Map<String, IColumnFilter> columnFilters)
            throws IOException;

    public boolean hasNext() throws IOException;

    @Transactional
    public void load() throws IOException;

    public void skip() throws IOException;

    public void close();

    public IDataLoader clone();

    public IDataLoaderContext getContext();

    public IDataLoaderStatistics getStatistics();

}
