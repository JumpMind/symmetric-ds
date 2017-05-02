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
#include "util/CsvUtils.h"

//struct csv_parser *SymCsvUtils_csvParser = NULL;

static void SymCsvUtils_parseField(void *data, size_t size, void *userData) {
    SymStringArray *fields = (SymStringArray*) userData;
    char* string = (char*)data;
    fields->addn(fields, data, size);
}

static void SymCsvUtils_parseLine(int eol, void *userData) {
    SymStringArray *fields = (SymStringArray*) userData;
    // TODO support mulit-line CSV.
}

SymStringArray* SymCsvUtils_tokenizeCsvData(char * csvData) {
    if (csvData == NULL) {
        return NULL;
    }

    char * dataWithReplacedQuotes = NULL;

    // The last field is dropped unless the CSV ends with a comma.
    if (!SymStringUtils_endsWith(csvData, ",")) {
        // TODO getting unexplained, seemingly random string problems when using  SymStringUtils_replace, and
        // SymStringUtils_replace together - need to verify in unit testing.  Problematic string example:
        //  char* csvData = SymStringUtils_format("%s", "\"MANUALS\",\"store_2_corp\",,\"1.txt\",\"filesync\",\"filesync_reload\",\"C\",\"0\",\"6\",\"1481126242\",\"1533-08-04 12:00:00.000\",,\"1570-11-28 12:00:00.000\", ");
        // dataWithReplacedQuotes = SymStringUtils_replace(csvData, "\\\"", "\"\"");
        dataWithReplacedQuotes = SymStringUtils_format("%s,\"\"", csvData);
    }
//    else {
//        dataWithReplacedQuotes = SymStringUtils_replace(csvData, "\\\"", "\"\"");
//    }


    struct csv_parser SymCsvUtils_csvParser;

    int rc = csv_init(&SymCsvUtils_csvParser, 0);
    if (rc != 0) {
        SymLog_warn("Failed to init csvParser with error code %d", rc);
    }

    SymStringArray* tokenizedCsvData = SymStringArray_new(NULL);

    int length = strlen(dataWithReplacedQuotes)*sizeof(char);

//    size_t resultLength = csv_parse(this->csvParser, dataWithReplacedQuotes, length,
//            SymProtocolDataReader_parseField, SymProtocolDataReader_parseLine, this);
    size_t resultLength = csv_parse(&SymCsvUtils_csvParser, dataWithReplacedQuotes, length,
            SymCsvUtils_parseField, SymCsvUtils_parseLine, tokenizedCsvData);

    if (resultLength != length) {
        SymLog_error("Error from CSV parser: %s", csv_strerror(csv_error(&SymCsvUtils_csvParser)));
        rc = 0;
    }

    free(dataWithReplacedQuotes);
    csv_free(&SymCsvUtils_csvParser);
    return tokenizedCsvData;
}
