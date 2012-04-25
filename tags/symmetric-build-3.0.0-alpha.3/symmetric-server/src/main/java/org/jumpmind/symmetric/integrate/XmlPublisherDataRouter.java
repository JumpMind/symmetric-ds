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

import java.util.Collections;
import java.util.Set;

import org.jdom.Element;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.route.IDataRouter;
import org.jumpmind.symmetric.route.SimpleRouterContext;

/**
 * This is an {@link IDataRouter} that can be configured as an extension point.
 * Instead of routing data to other nodes, it publishes data to the
 * {@link IPublisher} interface. The most common implementation of the
 * {@link IPublisher} is the {@link SimpleJmsPublisher}.
 */
public class XmlPublisherDataRouter extends AbstractXmlPublisherExtensionPoint implements
        IDataRouter {

    boolean onePerBatch = false;

    public void contextCommitted(SimpleRouterContext context) {
        if (doesXmlExistToPublish(context)) {
            finalizeXmlAndPublish(context);
        }
    }

    public void completeBatch(SimpleRouterContext context, OutgoingBatch batch) {
        if (onePerBatch && doesXmlExistToPublish(context)) {
            finalizeXmlAndPublish(context);
        }
    }

    public Set<String> routeToNodes(SimpleRouterContext context, DataMetaData dataMetaData,
            Set<Node> nodes, boolean initialLoad) {
        if (tableNamesToPublishAsGroup == null
                || tableNamesToPublishAsGroup.contains(dataMetaData.getData().getTableName())) {
            Element xml = getXmlFromCache(context, dataMetaData.getTriggerHistory()
                    .getParsedColumnNames(), dataMetaData.getData().toParsedRowData(), dataMetaData
                    .getTriggerHistory().getParsedPkColumnNames(), dataMetaData.getData()
                    .toParsedPkData());
            if (xml != null) {
                toXmlElement(dataMetaData.getData().getDataEventType(), xml, dataMetaData.getData()
                        .getTableName(), dataMetaData.getTriggerHistory().getParsedColumnNames(),
                        dataMetaData.getData().toParsedRowData(), dataMetaData.getTriggerHistory()
                                .getParsedPkColumnNames(), dataMetaData.getData().toParsedPkData());
            }
        } else if (log.isDebugEnabled()) {
            log.debug("'{}' not in list to publish", dataMetaData.getData().getTableName());
        }
        return Collections.emptySet();
    }

    /**
     * Indicates that one message should be published per batch. If this is set
     * to false, then only one message will be published once for each set of
     * data that is routed (even though it may have been routed to several nodes
     * across several different batches).
     * 
     * @param onePerBatch
     */
    public void setOnePerBatch(boolean onePerBatch) {
        this.onePerBatch = onePerBatch;
    }

}
