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
package org.jumpmind.db.platform;

import java.util.Map;
import java.util.TreeMap;

public class PermissionResult {

	public enum Status {
		PASS, FAIL, UNIMPLEMENTED, UNSUPPORTED, NOT_APPLICABLE
	}
	
	public enum PermissionCategory {
	    TABLE_MODIFICATION, TRIGGERS, ADDITIONAL
	}
	
	private PermissionType permissionType;
	
	private Status status;
	
	private Exception exception;
	
	private String solution;
	
	private PermissionCategory category;
	
	private static Map<PermissionType, PermissionCategory> categories = new TreeMap<PermissionType, PermissionCategory>();
	
	static {
	    categories.put(PermissionType.CREATE_TABLE, PermissionCategory.TABLE_MODIFICATION);
	    categories.put(PermissionType.DROP_TABLE, PermissionCategory.TABLE_MODIFICATION);
	    categories.put(PermissionType.ALTER_TABLE, PermissionCategory.TABLE_MODIFICATION);
	    categories.put(PermissionType.CREATE_TRIGGER, PermissionCategory.TRIGGERS);
	    categories.put(PermissionType.DROP_TRIGGER, PermissionCategory.TRIGGERS);
	    categories.put(PermissionType.CREATE_FUNCTION, PermissionCategory.ADDITIONAL);
	    categories.put(PermissionType.CREATE_ROUTINE, PermissionCategory.ADDITIONAL);
	    categories.put(PermissionType.EXECUTE, PermissionCategory.ADDITIONAL);
	}
		
	public PermissionResult(PermissionType permissionType, Status status, Exception exception, String solution) {
		this.setPermissionType(permissionType);
		this.setStatus(status);
		this.setException(exception);
		this.setSolution(solution);
		this.category = categories.get(permissionType);
	}
	
	public PermissionResult(PermissionType permissionType, Status status) {
		this.setPermissionType(permissionType);
		this.setStatus(status);
	    this.category = categories.get(permissionType);
	}

	public PermissionType getPermissionType() {
		return permissionType;
	}

	public void setPermissionType(PermissionType permissionType) {
		this.permissionType = permissionType;
	}

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

	public Exception getException() {
		return exception;
	}

	public void setException(Exception exception) {
		this.exception = exception;
	}

	public String getSolution() {
		return solution;
	}

	public void setSolution(String solution) {
		this.solution = solution;
	}

	public PermissionCategory getCategory() {
        return category;
    }

    public void setCategory(PermissionCategory category) {
        this.category = category;
    }

    public String toString() {
		return "Permission Type: " + permissionType + ", Status: " + status; 
	}
}
