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
package org.jumpmind.symmetric.transform;

import java.util.ArrayList;
import java.util.List;

import org.jumpmind.symmetric.ext.ICacheContext;
import org.jumpmind.symmetric.extract.DataExtractorContext;
import org.jumpmind.symmetric.load.StatementBuilder.DmlType;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.Router;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.util.CsvUtils;

public class TransformDataExtractor extends AbstractTransformer {

    private ITriggerRouterService triggerRouterService;

    public List<Data> transformData(Data data, String routerId, DataExtractorContext context)
            throws IgnoreRowException {
        DataEventType eventType = data.getEventType();
        DmlType dmlType = toDmlType(eventType);
        if (dmlType != null) {
            TriggerHistory triggerHistory = data.getTriggerHistory();
            Router router = triggerRouterService.getRouterById(routerId, false);
            if (router != null) {
                List<TransformedData> transformedData = transform(dmlType, context,
                        router.getNodeGroupLink(), triggerHistory.getSourceCatalogName(),
                        triggerHistory.getSourceSchemaName(), data.getTableName(),
                        triggerHistory.getParsedColumnNames(), data.toParsedRowData(),
                        dmlType == DmlType.INSERT ? null : triggerHistory.getParsedPkColumnNames(),
                        dmlType == DmlType.INSERT ? null : data.toParsedPkData(),
                        data.toParsedOldData());
                if (transformedData != null) {
                    return apply(context, transformedData);
                }
            }
        }

        return null;
    }

    @Override
    protected TransformPoint getTransformPoint() {
        return TransformPoint.EXTRACT;
    }

    protected List<Data> apply(ICacheContext context,
            List<TransformedData> dataThatHasBeenTransformed) {
        List<Data> datas = new ArrayList<Data>(dataThatHasBeenTransformed.size());
        for (TransformedData transformedData : dataThatHasBeenTransformed) {
            DmlType targetDmlType = transformedData.getTargetDmlType();
            if (targetDmlType != null) {
                Data data = new Data();
                TriggerHistory triggerHistory = new TriggerHistory(transformedData.getTableName(),
                        CsvUtils.escapeCsvData(transformedData.getKeyNames()),
                        CsvUtils.escapeCsvData(transformedData.getColumnNames()));
                triggerHistory.setTriggerHistoryId(triggerHistory.toVirtualTriggerHistId());
                triggerHistory.setSourceCatalogName(transformedData.getCatalogName());
                triggerHistory.setSourceSchemaName(transformedData.getSchemaName());
                data.setTriggerHistory(triggerHistory);
                data.setEventType(toDataEventType(transformedData.getTargetDmlType()));
                data.setTableName(transformedData.getTableName());
                data.setRowData(CsvUtils.escapeCsvData(transformedData.getColumnValues()));
                data.setPkData(CsvUtils.escapeCsvData(transformedData.getKeyValues()));
                data.setOldData(null);
                datas.add(data);
            }
        }
        return datas;
    }

    protected DmlType toDmlType(DataEventType eventType) {
        switch (eventType) {
        case INSERT:
            return DmlType.INSERT;
        case UPDATE:
            return DmlType.UPDATE;
        case DELETE:
            return DmlType.DELETE;
        default:
            return null;
        }
    }

    protected DataEventType toDataEventType(DmlType dmlType) {
        switch (dmlType) {
        case INSERT:
            return DataEventType.INSERT;
        case UPDATE:
            return DataEventType.UPDATE;
        case DELETE:
            return DataEventType.DELETE;
        default:
            return null;
        }
    }

    public void setTriggerRouterService(ITriggerRouterService triggerRouterService) {
        this.triggerRouterService = triggerRouterService;
    }

}
