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
#include "service/DataExtractorService.h"
#include "common/Log.h"

SymList * SymDataExtractorService_extract(SymDataExtractorService *this, SymNode *node, SymOutgoingTransport *transport) {
	SymLog_info("SymDataExtractorService_extract");
	return NULL;
}

void SymDataExtractorService_destroy(SymDataExtractorService *this) {
    free(this);
}

SymDataExtractorService * SymDataExtractorService_new(SymDataExtractorService *this) {
    if (this == NULL) {
        this = (SymDataExtractorService *) calloc(1, sizeof(SymDataExtractorService));
    }
    this->extract = (void *) &SymDataExtractorService_extract;
    this->destroy = (void *) &SymDataExtractorService_destroy;
    return this;
}
