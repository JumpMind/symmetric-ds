SymmetricDS C Library Client
============================

This is the SymmetricDS client data synchronization engine implemented as
a C library.


How to Build
------------
The project was built with the Eclipse CDT, which generates makefiles that
can be run from command line.  Choose either the Debug (larger size, with
symbols for debugging) or Release version and run the makefile from
that directory.

# cd symmetric-client-clib/Release
# make

Using the API
-------------
This example client will connect to a SQLite database, create its runtime
tables, and attempt to register and pull data from a server. 

#include "libsymclient.h"

int main() {
    SymProperties *prop = SymProperties_new(NULL);
    prop->put(prop, SYM_PARAMETER_DB_URL, "sqlite:file:data.db");
    prop->put(prop, SYM_PARAMETER_GROUP_ID, "client");
    prop->put(prop, SYM_PARAMETER_EXTERNAL_ID, "001");
    prop->put(prop, SYM_PARAMETER_REGISTRATION_URL, "http://localhost:31415/sync/server");

    SymEngine *engine = SymEngine_new(NULL, prop);
    engine->start(engine);

    engine->pullService->pull_data(engine->pullService);

    engine->stop(engine);
    engine->destroy(engine);
}


Required software for compile/runtime:
--------------------------------------
Dependency: Curl
License: MIT/X derivate license
Website: http://curl.haxx.se/libcurl/
APT: sudo apt-get install libcurl4-gnutls-dev

Dependency: SQLite
License: Public Domain
Website: https://www.sqlite.org/
APT: sudo apt-get install sqlite3 libsqlite3 libsqlite3-dev

Dependency: LibCSV
License: LGPL v2
APT: https://sourceforge.net/projects/libcsv/
