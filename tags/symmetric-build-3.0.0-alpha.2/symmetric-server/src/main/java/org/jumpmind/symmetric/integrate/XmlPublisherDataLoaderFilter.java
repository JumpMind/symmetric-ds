/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License. 
 */

package org.jumpmind.symmetric.integrate;

import java.util.List;
import java.util.Set;

import org.jdom.Element;
import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.ext.INodeGroupExtensionPoint;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.IDataReader;
import org.jumpmind.symmetric.io.data.IDataWriter;

/**
 * This is an optional {@link IDataLoaderFilter} and {@link IBatchListener} that
 * is capable of translating table data to XML and publishing it for consumption
 * by the enterprise. It uses JDOM internally to create an XML representation of
 * SymmetricDS data.
 * <p>
 * This filter is typically configured as a Spring bean. The table names that
 * should be published are identified by
 * {@link #setTableNamesToPublishAsGroup(Set)}. Rows from tables can be grouped
 * together (which get synchronized in the same batch) by identifying columns
 * that are the same that act as a 'key' by setting
 * {@link #setGroupByColumnNames(List)}
 * <p>
 * The {@link IPublisher} is typically configured and injected onto this bean as
 * well. Provided is a {@link SimpleJmsPublisher}.
 * <p>
 * An example of the XML that is published is as follows:
 * 
 * <pre>
 * &lt;batch id="2TEST2" nodeid="00001" time="12345678910"&gt;
 *   &lt;row entity="TABLE_NAME" dml="I"&gt;
 *     &lt;data key="id1"&gt;2&lt;/data&gt;
 *     &lt;data key="id2"&gt;TEST&lt;/data&gt;
 *     &lt;data key="id3"&gt;2&lt;/data&gt;
 *     &lt;data key="data1"&gt;Me&lt;/data&gt;
 *     &lt;data key="data2" nil="true"/&gt;
 *   &lt;/row&gt;
 * &lt;/batch&gt;
 * </pre>
 */
public class XmlPublisherDataLoaderFilter extends AbstractXmlPublisherExtensionPoint implements
        IPublisherFilter, INodeGroupExtensionPoint {

    protected boolean loadDataInTargetDatabase = true;

    public <R extends IDataReader, W extends IDataWriter> boolean beforeWrite(
            DataContext context, Table table, CsvData data) {
        if (tableNamesToPublishAsGroup == null
                || tableNamesToPublishAsGroup.contains(table.getName())) {
            Element xml = getXmlFromCache(context, table.getColumnNames(),
                    data.getParsedData(CsvData.ROW_DATA), table.getPrimaryKeyColumnNames(),
                    data.getParsedData(CsvData.PK_DATA));
            if (xml != null) {
                toXmlElement(data.getDataEventType(), xml, table.getName(), table.getColumnNames(),
                        data.getParsedData(CsvData.ROW_DATA), table.getPrimaryKeyColumnNames(),
                        data.getParsedData(CsvData.PK_DATA));
            }
        } else if (log.isDebugEnabled()) {
            log.debug("'{}' not in list to publish", table.getName());
        }
        return loadDataInTargetDatabase;
    }

    public <R extends IDataReader, W extends IDataWriter> void batchComplete(
            DataContext context) {
        if (doesXmlExistToPublish(context)) {
            finalizeXmlAndPublish(context);
        }
    }

    public void setLoadDataInTargetDatabase(boolean loadDataInTargetDatabase) {
        this.loadDataInTargetDatabase = loadDataInTargetDatabase;
    }

    public <R extends IDataReader, W extends IDataWriter> void afterWrite(
            DataContext context, Table table, CsvData data) {
    }

    public <R extends IDataReader, W extends IDataWriter> boolean handlesMissingTable(
            DataContext context, Table table) {
        return false;
    }

    public <R extends IDataReader, W extends IDataWriter> void earlyCommit(
            DataContext context) {
    }

    public <R extends IDataReader, W extends IDataWriter> void batchCommitted(
            DataContext context) {
    }

    public <R extends IDataReader, W extends IDataWriter> void batchRolledback(
            DataContext context) {
    }

}
