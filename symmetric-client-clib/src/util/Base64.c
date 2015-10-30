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
#include "util/Base64.h"

static char encodingTable[] = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
    'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
    'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/'};

static char *decodingTable = NULL;
static int mod_table[] = {0, 2, 1};

static void SymBase64_buildDecodingTable() {
    decodingTable = malloc(256);

    int i;
    for (i = 0; i < 64; i++) {
        decodingTable[(unsigned char) encodingTable[i]] = i;
    }
}

char * SymBase64_encode(const unsigned char *data, int inSize) {

    int outSize = 4 * ((inSize + 2) / 3);

    char *encodedData = malloc(outSize + 1);
    if (encodedData == NULL) {
        return NULL;
    }
    encodedData[outSize] = '\0';

    int i, j;
    for (i = 0, j = 0; i < inSize;) {
        uint32_t octet_a = i < inSize ? (unsigned char) data[i++] : 0;
        uint32_t octet_b = i < inSize ? (unsigned char) data[i++] : 0;
        uint32_t octet_c = i < inSize ? (unsigned char) data[i++] : 0;

        uint32_t triple = (octet_a << 0x10) + (octet_b << 0x08) + octet_c;

        encodedData[j++] = encodingTable[(triple >> 3 * 6) & 0x3F];
        encodedData[j++] = encodingTable[(triple >> 2 * 6) & 0x3F];
        encodedData[j++] = encodingTable[(triple >> 1 * 6) & 0x3F];
        encodedData[j++] = encodingTable[(triple >> 0 * 6) & 0x3F];
    }

    for (i = 0; i < mod_table[inSize % 3]; i++) {
        encodedData[outSize - 1 - i] = '=';
    }

    return encodedData;
}

unsigned char * SymBase64_decode(const char *data, int *outSize) {

    if (decodingTable == NULL) SymBase64_buildDecodingTable();

    int inSize = strlen(data);
    *outSize = 0;
    if (inSize % 4 != 0) {
        return NULL;
    }

    *outSize = inSize / 4 * 3;
    if (data[inSize - 1] == '=') (*outSize)--;
    if (data[inSize - 2] == '=') (*outSize)--;

    unsigned char *decodedData = malloc(*outSize + 1);
    if (decodedData == NULL) {
        return NULL;
    }
    decodedData[*outSize] = '\0';

    int i, j;
    for (i = 0, j = 0; i < inSize;) {
        uint32_t sextet_a = data[i] == '=' ? 0 & i++ : decodingTable[(int) data[i++]];
        uint32_t sextet_b = data[i] == '=' ? 0 & i++ : decodingTable[(int) data[i++]];
        uint32_t sextet_c = data[i] == '=' ? 0 & i++ : decodingTable[(int) data[i++]];
        uint32_t sextet_d = data[i] == '=' ? 0 & i++ : decodingTable[(int) data[i++]];

        uint32_t triple = (sextet_a << 3 * 6) + (sextet_b << 2 * 6) + (sextet_c << 1 * 6) + (sextet_d << 0 * 6);

        if (j < *outSize) decodedData[j++] = (triple >> 2 * 8) & 0xFF;
        if (j < *outSize) decodedData[j++] = (triple >> 1 * 8) & 0xFF;
        if (j < *outSize) decodedData[j++] = (triple >> 0 * 8) & 0xFF;
    }

    return decodedData;
}
