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
#include "symclient_test.h"

int SymEngineTest_setup() {
    return 0;
}

int SymEngineTest_teardown() {
    return 0;
}

void SymEngineTest_test1() {
    SymProperties *prop = SymProperties_new(NULL);
    prop->put(prop, SYM_PARAMETER_DB_URL, "sqlite:file:./Debug/data.db");
    prop->put(prop, SYM_PARAMETER_GROUP_ID, "store");
    prop->put(prop, SYM_PARAMETER_EXTERNAL_ID, "003");
    prop->put(prop, SYM_PARAMETER_REGISTRATION_URL, "http://localhost:31415/sync/corp-000");
    //prop->put(prop, "auto.sync.triggers.at.startup", "0");

    SymEngine *engine = SymEngine_new(NULL, prop);
    CU_ASSERT(engine->start(engine) == 0);
    engine->heartbeat(engine, 0);
    engine->purge(engine);
    engine->syncTriggers(engine);

    engine->pullService->pullData(engine->pullService);

    CU_ASSERT(engine->stop(engine) == 0);
    engine->destroy(engine);
    prop->destroy(prop);
}

int SymEngineTest_CUnit() {
    CU_pSuite suite = CU_add_suite("SymEngineTest", SymEngineTest_setup, SymEngineTest_teardown);
    if (suite == NULL) {
        return CUE_NOSUITE;
    }

    if (CU_add_test(suite, "SymEngineTest_test1", SymEngineTest_test1) == NULL) {
        return CUE_NOTEST;
    }
    return CUE_SUCCESS;
}
