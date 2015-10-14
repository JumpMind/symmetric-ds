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
#include "util/StringUtils.h"

char *Sym_trim(char *str)
{
    int length = strlen(str);

    int leadingWhitespaces = 0;
    while (isspace(str[leadingWhitespaces]) && leadingWhitespaces < length) {
        leadingWhitespaces++;
    }

    if (leadingWhitespaces == length) { // all whitespace.
        return "";
    }

    int trailingWhitespaces = length-1;
    while (isspace(str[trailingWhitespaces]) && trailingWhitespaces >=0) {
        trailingWhitespaces--;
    }

    trailingWhitespaces = ((length-1)-trailingWhitespaces);

    int newLength = length - leadingWhitespaces - trailingWhitespaces;
    char *newString = malloc(newLength+1);

    int i;
    for (i = leadingWhitespaces; i < length-trailingWhitespaces; i++) {
        newString[i-leadingWhitespaces] = str[i];
    }

    newString[i] = '\0';

    return newString;
}

char *Sym_toUpperCase(char *str) {
    int length = strlen(str);
    char *newString = malloc(length+1);

    int i;
    for (i = 0; i < length; i++) {
        newString[i] = toupper(str[i]);
    }

    newString[i] = '\0';

    return newString;
}

char *Sym_toLowerCase(char *str) {
    int length = strlen(str);
    char *newString = malloc(length+1);

    int i;
    for (i = 0; i < length; i++) {
        newString[i] = tolower(str[i]);
    }

    newString[i] = '\0';

    return newString;
}

unsigned short Sym_isBlank(char *str) {
    if (str == NULL) {
        return 1;
    }

    int strLen = strlen(str);
    if (strLen == 0) {
        return 1;
    }

    int i;
    for (i = 0; i < strLen; i++) {
        if (!isspace(str[i])) {
            return 0;
        }
    }

    return 1;
}
