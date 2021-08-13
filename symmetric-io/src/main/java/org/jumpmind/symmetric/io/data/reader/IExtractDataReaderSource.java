/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.io.data.reader;

import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;

public interface IExtractDataReaderSource {
    public Batch getBatch();

    /**
     * Return the table with the catalog, schema, and table name of the target table for the last {@link CsvData} retrieved by {@link #next()}
     */
    public Table getTargetTable();

    /**
     * Return the table with the catalog, schema, and table name of the source table for the last {@link CsvData} retrieved by {@link #next()}
     */
    public Table getSourceTable();

    public CsvData next();

    public boolean requiresLobsSelectedFromSource(CsvData data);

    public void close();
}
