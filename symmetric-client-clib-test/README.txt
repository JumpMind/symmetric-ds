SymmetricDS C Library Client
============================

This is a test project that links to the SymmetricDS client library and 
performs unit testing of the data synchronization engine. 

How to Build
------------
The project was built with the Eclipse CDT, which generates makefiles that
can be run from command line.  Choose either the Debug (larger size, with
symbols for debugging) or Release version and run the makefile from
that directory.

# cd symmetric-client-clib/Release
# make


How to Run
----------

Run the executable produced from the build, which will run all the unit
tests and output the results to standard output.  If the libsymclient.so
was built but not installed, use the LD_LIBRARY_PATH variable to find
the shared library.

# cd symmetric-client-clib/Release
# LD_LIBRARY_PATH=../../symmetric-client-clib/Debug/ ./symclient_test


Required software for compile/runtime:
--------------------------------------
Dependency: CUnit library
License: LGPL version 2
Download: http://cunit.sourceforge.net/
APT: sudo apt-get install libcunit1 libcunit1-dev
Homebrew: brew install cunit

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
Website: https://sourceforge.net/projects/libcsv/
APT:
