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
package org.jumpmind.symmetric.fs;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.filefilter.AndFileFilter;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.NotFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;

public class DirectorySpec {

    protected String directory;
    protected boolean recursive;
    protected String[] includes;
    protected String[] excludes;
    
    public DirectorySpec(String directory, boolean recursive, String[] includes, String[] excludes) {
        this.directory = directory;
        this.recursive = recursive;
        this.includes = includes;
        this.excludes = excludes;
    }
    
    public IOFileFilter createIOFileFilter() {
        IOFileFilter filter = new WildcardFileFilter(includes == null ? new String[] {"*"} : includes);
        if (excludes != null && excludes.length > 0) {
            List<IOFileFilter> fileFilters = new ArrayList<IOFileFilter>();
            fileFilters.add(filter);
            fileFilters.add(new NotFileFilter(new WildcardFileFilter(excludes)));
            filter = new AndFileFilter(fileFilters);
        }
        if (!recursive) {
            List<IOFileFilter> fileFilters = new ArrayList<IOFileFilter>();
            fileFilters.add(filter);
            fileFilters.add(new NotFileFilter(FileFilterUtils.directoryFileFilter()));
            filter = new AndFileFilter(fileFilters);            
        }
        return filter;
    }

    public String getDirectory() {
        return directory;
    }

    public void setDirectory(String directory) {
        this.directory = directory;
    }

    public boolean isRecursive() {
        return recursive;
    }

    public void setRecursive(boolean recursive) {
        this.recursive = recursive;
    }

    public String[] getIncludes() {
        return includes;
    }

    public void setIncludes(String[] includes) {
        this.includes = includes;
    }

    public String[] getExcludes() {
        return excludes;
    }

    public void setExcludes(String[] excludes) {
        this.excludes = excludes;
    }
    
    
    
    
    
}
