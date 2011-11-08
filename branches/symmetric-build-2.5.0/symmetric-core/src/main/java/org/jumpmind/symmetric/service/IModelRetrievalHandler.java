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
package org.jumpmind.symmetric.service;

import java.io.IOException;

/**
 * A callback interface for retrieving data that allows the handler to interrupt the
 * retrieval process.
 */
public interface IModelRetrievalHandler<T,S> {

    /**
     * @param model1
     *            The model object that has just been retrieved
     * @param model2
     *            Supplemental model information that has just been retrieved            
     * @param count
     *            The number of model items that have been retrieved.  The count starts at 1.
     * @return true if the client should continue to retrieve model data, false
     *         if it should stop processing.
     */
    public boolean retrieved(T model1, S model2, int count) throws IOException;

}
