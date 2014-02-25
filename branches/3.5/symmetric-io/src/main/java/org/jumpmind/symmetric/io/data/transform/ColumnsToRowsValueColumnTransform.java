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

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.symmetric.io.data.DataContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColumnsToRowsValueColumnTransform  implements ISingleValueColumnTransform {

	private static final Logger logger = LoggerFactory.getLogger(ColumnsToRowsValueColumnTransform.class);

	public final static String NAME = "columnsToRowsValueColumnTransform";

	
	public String getName() {
		return NAME;
	}

	public boolean isExtractColumnTransform() {
		return true;
	}

	public boolean isLoadColumnTransform() {
		return true;
	}

	public String transform(IDatabasePlatform platform, DataContext context, TransformColumn column,
			TransformedData data, Map<String, String> sourceValues, String newValue, String oldValue)
			throws IgnoreRowException {
		
		String contextBase = ColumnsToRowsKeyColumnTransform.getContextBase(column.getTransformId());
				
		
		@SuppressWarnings("unchecked")
		Map<String, String> reverseMap = (Map<String, String>) context.get(contextBase+ColumnsToRowsKeyColumnTransform.CONTEXT_MAP);
		String pkColumnName = (String) context.get(contextBase+ColumnsToRowsKeyColumnTransform.CONTEXT_PK_COLUMN);

		if (reverseMap == null) {
			throw new RuntimeException("Reverse map not found in context as key " + contextBase+ColumnsToRowsKeyColumnTransform.CONTEXT_MAP+ "  Unable to transform.");
		}
		if (pkColumnName == null) {
			throw new RuntimeException("Primary key column name not found in context as key " + contextBase+ColumnsToRowsKeyColumnTransform.CONTEXT_PK_COLUMN+"  Unable to transform.");
		}

		String pkValue = data.getTargetKeyValues().get(pkColumnName);
		String value = null;
		
		if (pkValue!=null) {
			 value = reverseMap.get(pkValue);
			 if (value!=null) {
				 return data.getSourceValues().get(value);
			 } else {
				 throw new RuntimeException("Unable to locate column name for pk value "+pkValue);
			 }
		} else {
			throw new RuntimeException("Unable to locate column with pk name "+pkColumnName+" in target values.");
		}
	}
}
