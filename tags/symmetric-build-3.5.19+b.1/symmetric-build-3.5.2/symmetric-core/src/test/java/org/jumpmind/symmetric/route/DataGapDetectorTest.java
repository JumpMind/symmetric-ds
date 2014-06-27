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
package org.jumpmind.symmetric.route;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.jumpmind.symmetric.model.DataGap;
import org.junit.Test;

public class DataGapDetectorTest {

    @Test
    public void testRemoveAbandonedGaps() {
        DataGapDetector detector = new DataGapDetector();
        List<DataGap> gaps = new ArrayList<DataGap>();
        
        gaps.add(new DataGap(1,1000));
        gaps.add(new DataGap(2000,3000));
        gaps.add(new DataGap(3001,3001));
        gaps.add(new DataGap(3002,3002));
        gaps.add(new DataGap(3003,3003));
        gaps.add(new DataGap(3004,3004));
        int expectedSize = gaps.size();
        
        List<DataGap> evaluatedList = detector.removeAbandonedGaps(gaps);
        Assert.assertEquals(expectedSize, evaluatedList.size());
        
        gaps.add(new DataGap(2000,2001));
        gaps.add(new DataGap(2010,2022));        
        gaps.add(new DataGap(2899,3000));
        
        Assert.assertTrue(gaps.size() > expectedSize);
        
        evaluatedList = detector.removeAbandonedGaps(gaps);
        Assert.assertEquals(expectedSize, evaluatedList.size());
        
        
    }
}
