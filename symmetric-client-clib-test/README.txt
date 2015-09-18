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


Required software for testing:
------------------------------

Dependency: CUnit library
License: LGPL version 2
Download: http://cunit.sourceforge.net/
Install: sudo apt-get install libcunit1 libcunit1-dev
