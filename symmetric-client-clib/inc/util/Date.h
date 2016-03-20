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
#ifndef SYM_DATE_H
#define SYM_DATE_H

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "util/StringBuilder.h"

#define SYM_DATE_FORMAT "%Y-%m-%d %H:%M:%S"

typedef struct SymDate {
    char *dateTimeString;
    time_t time;
    unsigned short (*before)(struct SymDate *this, struct SymDate *otherDate);
    unsigned short (*after)(struct SymDate *this, struct SymDate *otherDate);
    unsigned short (*equals)(struct SymDate *this, struct SymDate *otherDate);
    void (*destroy)(struct SymDate *this);
} SymDate;

SymDate * SymDate_new();

SymDate * SymDate_newWithString(char *dateTimeString);

SymDate * SymDate_newWithTime(time_t time);

#endif
