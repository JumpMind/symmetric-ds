This folder contains a user defined function (UDF) library that must be installed in the Firebird 2.0
database for SymmetricDS to work properly.  It includes a sym_escape() function used by the database
triggers to escape strings.

The following files are included:

* sym_udf.c   - The C program implementing the user defined functions
* build.sh    - A shell program that compiles and links the sym_udf library
* sym_udf.so  - A compiled Linux shared object library
* sym_udf.dll - A compiled Windows shared object library

Linux users will copy the sym_udf.so file into the Firebird installation directory, while Windows
users will copy the sym_udf.dll file.  The file is copied to the "UDF" folder under the Firebird
installation home directory.
