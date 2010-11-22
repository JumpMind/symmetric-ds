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
 * under the License.  */

package org.jumpmind.symmetric.route;

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.ISqlProvider;
import org.jumpmind.symmetric.service.impl.AbstractService;

/**
 * 
 */
public class DataToRouteReaderFactory extends AbstractService implements ISqlProvider {

    public static final String GAP_DETECTOR_TYPE_GAP = "gap";
    public static final String GAP_DETECTOR_TYPE_REF = "ref";
    
    private IDataService dataService;

    public IDataToRouteReader getDataToRouteReader(ChannelRouterContext context) {
        String type = parameterService.getString(ParameterConstants.ROUTING_DATA_READER_TYPE);
        if (type == null || type.equals(GAP_DETECTOR_TYPE_REF)) {
            return new DataRefRouteReader(jdbcTemplate.getDataSource(),
                    jdbcTemplate.getQueryTimeout(), dbDialect.getRouterDataPeekAheadCount(), this,
                    dbDialect.getStreamingResultsFetchSize(), context, dataService, dbDialect.requiresAutoCommitFalseToSetFetchSize(), dbDialect);
        } else if (type == null || type.equals(GAP_DETECTOR_TYPE_GAP)) {
            return new DataGapRouteReader(jdbcTemplate.getDataSource(),
                    jdbcTemplate.getQueryTimeout(), dbDialect.getRouterDataPeekAheadCount(), this,
                    dbDialect.getStreamingResultsFetchSize(), context, dataService, dbDialect.requiresAutoCommitFalseToSetFetchSize(), dbDialect);
        } else {
            throw unsupportedType(type);
        }
    }

    public IDataToRouteGapDetector getDataToRouteGapDetector() {
        String type = parameterService.getString(ParameterConstants.ROUTING_DATA_READER_TYPE);
        if (type == null || type.equals(GAP_DETECTOR_TYPE_REF)) {
            return new DataRefGapDetector(dataService, parameterService, jdbcTemplate, dbDialect,
                    this);
        } else if (type == null || type.equals(GAP_DETECTOR_TYPE_GAP)) {
            return new DataGapDetector(dataService, parameterService, jdbcTemplate, dbDialect, this);
        } else {
            throw unsupportedType(type);
        }
    }
    
    public boolean isUsingDataRef() {
        String type = parameterService.getString(ParameterConstants.ROUTING_DATA_READER_TYPE);
        return type == null || type.equals(GAP_DETECTOR_TYPE_REF);
    }

    private RuntimeException unsupportedType(String type) {
        return new UnsupportedOperationException("The data to route type of '" + type
                + "' is not supported");
    }

    public void setDataService(IDataService dataService) {
        this.dataService = dataService;
    }

}