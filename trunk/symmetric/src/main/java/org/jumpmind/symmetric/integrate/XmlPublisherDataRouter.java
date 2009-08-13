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
package org.jumpmind.symmetric.integrate;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.jdom.Element;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.route.IDataRouter;
import org.jumpmind.symmetric.route.IRouterContext;

/**
 * This is an {@link IDataRouter} that can be configured as an extension point. Instead of routing data to other nodes,
 * it publishes data to the {@link IPublisher} interface. The most common implementation of the {@link IPublisher} is
 * the {@link SimpleJmsPublisher}.
 */
public class XmlPublisherDataRouter extends AbstractXmlPublisherExtensionPoint implements IDataRouter {

    public void completeBatch(IRouterContext context, OutgoingBatch batch) {
        if (doesXmlExistToPublish(context)) {
            finalizeXmlAndPublish(context);
        }
    }

    public Collection<String> routeToNodes(IRouterContext context, DataMetaData dataMetaData, Set<Node> nodes,
            boolean initialLoad) {
        if (tableNamesToPublishAsGroup == null
                || tableNamesToPublishAsGroup.contains(dataMetaData.getData().getTableName())) {
            Element xml = getXmlFromCache(context, dataMetaData.getTriggerHistory().getParsedColumnNames(),
                    dataMetaData.getData().getParsedRowData(), dataMetaData.getTriggerHistory()
                            .getParsedPkColumnNames(), dataMetaData.getData().getParsedPkData());
            if (xml != null) {
                toXmlElement(dataMetaData.getData().getEventType(), xml, dataMetaData.getData().getTableName(),
                        dataMetaData.getTriggerHistory().getParsedColumnNames(), dataMetaData.getData()
                                .getParsedRowData(), dataMetaData.getTriggerHistory().getParsedPkColumnNames(),
                        dataMetaData.getData().getParsedPkData());
            }
        }
        return Collections.emptySet();
    }

}
