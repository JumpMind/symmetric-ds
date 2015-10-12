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
#include "common/Log.h"

static char* logLevelDescription(SymLogLevel logLevel) {
	switch (logLevel) {
	case DEBUG:
		return SYM_LOG_LEVEL_DEBUG;
	case INFO:
		return SYM_LOG_LEVEL_INFO;
	case WARN:
		return SYM_LOG_LEVEL_WARN;
	case ERROR:
		return SYM_LOG_LEVEL_ERROR;
	default:
		return SYM_LOG_LEVEL_UNKNOWN;
	}
}

/** This is the central place where all logging funnels through. */
void SymLog_log(SymLogLevel logLevel, const char *functionName, const char *fileName, int lineNumber, const char* message, ...) {
	FILE *destination;
	if (logLevel <= INFO) {
		destination = stdout;
	}
	else {
		destination = stderr;
	}

	char* levelDescription = logLevelDescription(logLevel);

	SymStringBuilder *messageBuffer = SymStringBuilder_new();

	SymDate *date = SymDate_new();

	messageBuffer->append(messageBuffer, date->dateTimeString);
	messageBuffer->append(messageBuffer, " [");
	messageBuffer->append(messageBuffer, levelDescription);
	messageBuffer->append(messageBuffer, "] [");
	messageBuffer->append(messageBuffer, functionName);
//	messageBuffer->append(messageBuffer, " ");
//	messageBuffer->append(messageBuffer, fileName);
//	messageBuffer->append(messageBuffer, ":");
//	messageBuffer->appendInt(messageBuffer, lineNumber);
	messageBuffer->append(messageBuffer, "] ");
    va_list varargs;
    va_start(varargs, message);
	messageBuffer->appendfv(messageBuffer, message, varargs);
    va_end(varargs);
	messageBuffer->append(messageBuffer, "\n");

	fprintf(destination, "%s", messageBuffer->toString(messageBuffer));

	// stdout may not flush before stderr does.
	// Do this to keep log messages more or less in order.
	fflush(destination);

	date->destroy(date);
	messageBuffer->destroy(messageBuffer);
}

