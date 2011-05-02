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

import java.io.BufferedWriter;
import java.io.IOException;

import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.OutgoingBatch;

public interface IDataExtractor {

    public void init(BufferedWriter writer, DataExtractorContext context) throws IOException;

    public void begin(OutgoingBatch batch, BufferedWriter writer) throws IOException;

    public void preprocessTable(Data data, BufferedWriter out, DataExtractorContext context) throws IOException;

    public void commit(OutgoingBatch batch, BufferedWriter writer) throws IOException;

    public void write(BufferedWriter writer, Data data, DataExtractorContext context) throws IOException;

}
