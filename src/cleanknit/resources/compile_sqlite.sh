# script to build sqlite3 shell from the amalgamation with
# a bunch of features enabled.
# Inspired by https://github.com/nalgeon/sqlite

# The list of sqlite features was extracted out from a sqlite3 shell
# via hand-editing the result of this query
#    select group_concat('-DSQLITE_'||compile_options, ' \'||CHAR(10)) 
#    FROM pragma_compile_options 
#    where compile_options like 'ENABLE%';
#
# This is meant to be run in an unpacked SQLite amalgamation directory
# xref https://www.sqlite.org/amalgamation.html

gcc -Os -g -I. \
-DSQLITE_ENABLE_DBPAGE_VTAB \
-DSQLITE_ENABLE_DBSTAT_VTAB \
-DSQLITE_ENABLE_EXPLAIN_COMMENTS \
-DSQLITE_ENABLE_FTS3 \
-DSQLITE_ENABLE_FTS4 \
-DSQLITE_ENABLE_FTS5 \
-DSQLITE_ENABLE_GEOPOLY \
-DSQLITE_ENABLE_MATH_FUNCTIONS \
-DSQLITE_ENABLE_OFFSET_SQL_FUNC \
-DSQLITE_ENABLE_RTREE \
-DSQLITE_ENABLE_STAT4 \
-DSQLITE_ENABLE_STMTVTAB \
-DSQLITE_ENABLE_UNKNOWN_SQL_FUNCTION \
-DSQLITE_ENABLE_EXPLAIN_COMMENTS \
-DHAVE_USLEEP \
-DHAVE_READLINE \
-DSQLITE_THREADSAFE=0 \
-DUSE_URI \
shell.c sqlite3.c \
        -ldl \
        -lm  \
        -lreadline \
        -lncurses \
  -o sqlite3
