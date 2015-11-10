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
#include "util/Date.h"

void SymDate_destroy(SymDate *this) {
    free(this->dateTimeString);
    free(this);
}

SymDate * SymDate_newWithString(char *dateTimeString) {
    SymDate *this = SymDate_new();
    this->dateTimeString = SymStringBuilder_copy(dateTimeString);
    SymStringBuilder *sb = SymStringBuilder_newWithString(dateTimeString);
    char *year = sb->substring(sb, 0, 4);
    char *month = sb->substring(sb, 5, 7);
    char *day = sb->substring(sb, 8, 10);
    char *hour = sb->substring(sb, 11, 13);
    char *minute = sb->substring(sb, 14, 16);
    char *second = sb->substring(sb, 17, 19);

    struct tm timeInfo;
    timeInfo.tm_year = atoi(year);
    timeInfo.tm_mon = atoi(month) - 1;
    timeInfo.tm_mday = atoi(day);
    timeInfo.tm_hour = atoi(hour);
    timeInfo.tm_min = atoi(minute);
    timeInfo.tm_sec = atoi(second);
    this->time = mktime(&timeInfo);

    free(year);
    free(month);
    free(day);
    free(hour);
    free(minute);
    free(second);
    sb->destroy(sb);
    return this;
}

unsigned short SymDate_before(SymDate *this, SymDate *otherDate) {
    return this->time < otherDate->time;
}

unsigned short SymDate_after(SymDate *this, SymDate *otherDate) {
    return this->time > otherDate->time;
}

unsigned short SymDate_equals(SymDate *this, SymDate *otherDate) {
    return this->time == otherDate->time;
}

SymDate * SymDate_newWithTime(time_t time) {
    SymDate *this = (SymDate *) calloc(1, sizeof(SymDate));
    this->destroy = (void *) &SymDate_destroy;
    this->before = (void *) &SymDate_before;
    this->after = (void *) &SymDate_after;
    this->equals = (void *) &SymDate_equals;
    this->dateTimeString = calloc(24, sizeof(char));
    this->time = time;
    struct tm *timeInfo = localtime(&time);
    strftime(this->dateTimeString, 24, SYM_DATE_FORMAT, timeInfo);
    return this;
}

SymDate * SymDate_new() {
    return SymDate_newWithTime(time(NULL));
}
