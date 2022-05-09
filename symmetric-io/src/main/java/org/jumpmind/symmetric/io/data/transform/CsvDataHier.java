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
package org.jumpmind.symmetric.io.data.transform;

import java.util.List;

import org.jumpmind.symmetric.io.data.CsvData;

public class CsvDataHier {
    private CsvData csvData;
    private List<CsvDataHier> children;

    public CsvDataHier(CsvData csvData) {
        this.csvData = csvData;
    }

    public CsvData getCsvData() {
        return csvData;
    }

    public void setCsvData(CsvData csvData) {
        this.csvData = csvData;
    }

    public List<CsvDataHier> getChildren() {
        return children;
    }

    public void setChildren(List<CsvDataHier> children) {
        this.children = children;
    }

    public void addChild(CsvDataHier csvDataHier) {
        children.add(csvDataHier);
    }
}
