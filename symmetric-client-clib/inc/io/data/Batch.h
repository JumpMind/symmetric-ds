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
#ifndef SYM_BATCH_H
#define SYM_BATCH_H

#include <stdio.h>
#include <stdlib.h>

typedef struct {
    long batchId;
    char *sourceNodeId;
    char *targetNodeId;
    int initialLoad;
    char *channelId;
    unsigned short isIgnore;
    void (*set)(char **strField, char *str);
    void (*destroy)(void *this);
} SymBatch;

SymBatch * SymBatch_new(SymBatch *this);

SymBatch * SymBatch_new_with_settings(SymBatch *this, long batchId, char *channelId, char *sourceNodeId, char *targetNodeId);

#endif
