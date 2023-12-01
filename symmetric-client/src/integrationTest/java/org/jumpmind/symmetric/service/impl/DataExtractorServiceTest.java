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
package org.jumpmind.symmetric.service.impl;

import org.junit.Assert;
import java.lang.reflect.Field;
import java.util.Date;

import org.jumpmind.db.sql.ISqlReadCursor;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.ProcessInfo;
import org.jumpmind.symmetric.model.Router;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerReBuildReason;
import org.jumpmind.symmetric.route.AbstractFileParsingRouter;
import org.jumpmind.symmetric.service.impl.DataExtractorService.SelectFromSymDataSource;
import org.junit.Test;
import org.mockito.Mockito;

public class DataExtractorServiceTest {
    @SuppressWarnings("unchecked")
    @Test
    public void selectFromSymDataSource_csvValuesAreExtracted_triggerRouterIsNotMarkedAsMissing()
            throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        DataExtractorService dataExtractorService = Mockito.mock(DataExtractorService.class);
        TriggerRouterService triggerRouterService = Mockito.mock(TriggerRouterService.class);
        Mockito.when(triggerRouterService.getTriggerRoutersByTriggerHist("target", false)).thenReturn(null);
        Mockito.when(triggerRouterService.getRouterById("fooCsvRouter")).thenReturn(new Router());
        ISymmetricDialect symmetricDialect = Mockito.mock(ISymmetricDialect.class);
        Mockito.when(symmetricDialect.getBinaryEncoding()).thenReturn(BinaryEncoding.NONE);
        Mockito.when(symmetricDialect.getName()).thenReturn("H2");
        // mock a cursor with some data from csv router
        ISqlReadCursor<Data> mockedCursor = Mockito.mock(ISqlReadCursor.class);
        Data csvCapturedData = new Data(1, "1", "1", DataEventType.INSERT, "foo", new Date(), buildVirtualTriggerHistoryForParsedFile(),
                "default", null, null);
        Mockito.when(mockedCursor.next()).thenReturn(csvCapturedData);

        setPrivateField(DataExtractorService.class.getSuperclass(), dataExtractorService, "symmetricDialect", symmetricDialect);
        setPrivateField(DataExtractorService.class, dataExtractorService, "triggerRouterService", triggerRouterService);

        SelectFromSymDataSource selectFromSymDataSource = dataExtractorService.new SelectFromSymDataSource(new OutgoingBatch(), new Node(),
                new Node(), new ProcessInfo(), false);
        setPrivateField(SelectFromSymDataSource.class, selectFromSymDataSource, "cursor", mockedCursor);
        Assert.assertTrue(selectFromSymDataSource.next().equals(csvCapturedData));
    }

    protected TriggerHistory buildVirtualTriggerHistoryForParsedFile() {
        TriggerHistory virtualTriggerHistory = new TriggerHistory();
        virtualTriggerHistory.setColumnNames("foo");
        virtualTriggerHistory.setCreateTime(new Date());
        virtualTriggerHistory.setLastTriggerBuildReason(TriggerReBuildReason.NEW_TRIGGERS);
        virtualTriggerHistory.setPkColumnNames("foo");
        virtualTriggerHistory.setSourceTableName("foo");
        virtualTriggerHistory.setTriggerHistoryId(1);
        virtualTriggerHistory.setTriggerId(AbstractFileParsingRouter.TRIGGER_ID_FILE_PARSER);
        return virtualTriggerHistory;
    }

    protected void setPrivateField(Class<?> objectClass, Object objectWithPrivateField, String fieldName, Object value)
            throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        Field field = objectClass.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(objectWithPrivateField, value);
    }
}
