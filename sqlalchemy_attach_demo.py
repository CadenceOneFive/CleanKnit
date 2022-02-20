
import sys

import sqlalchemy
from sqlalchemy import create_engine, MetaData

# This is a hand-rolled pysqlite3 from https://github.com/coleifer/pysqlite3
# The setup.py is edited to make the list of compilation options mutually consistent
# wrt https://github.com/nalgeon/sqlite#sqlite-shell-builder as I like to
# prototype features using the sqlite3 *shell* and then only 'fall up' to Python when there
# is something 'fancy' to be done.
import pysqlite3

# Create an empty, memory-resident database
# https://docs.sqlalchemy.org/en/14/dialects/sqlite.html#connect-strings
engine_memory = sqlalchemy.create_engine("sqlite://", module=pysqlite3)

# We need to do some messing around with the low-level database handle
raw_connection_memory = engine_memory.raw_connection()

# Temporarily emable extension
raw_connection_memory.enable_load_extension(True)
if sys.platform=='win32':
    raw_connection_memory.load_extension("C:/tools/vsv")
else:
    raw_connection_memory.load_extension("/home/phrrngtn/bin/vsv")
raw_connection_memory.enable_load_extension(False)

r = engine_memory.execute("select sqlite_version()").fetchall()
print(r)
r = engine_memory.execute("PRAGMA compile_options;").fetchall()
print(r)
# See which modules are loaded. Some are compiled in, some will be dynamically loaded
# like we did with 'vsv'
r = engine_memory.execute("PRAGMA module_list;").fetchall()
print(r)
# This is the same way of getting at the metadata but using
# a table-like interface. We can -- and should -- have a SQLAlchemy model
# for these catalog/rule4 objects so that we can use high-level
# SQLAlchemy constructs to manipulate them (as opposed to this 'embedded SQL as strings' which
# is not as robust)
r = engine_memory.execute("SELECT name from pragma_module_list;").fetchall()
print(r)

# Although SQLite does not support 'schemas', per se, you can emulate them
# via ATTACH DATABASE. Here, I am taking the database whose schema I generated
# from the Socrata metadata API and attaching it as 'nyc'
if sys.platform == 'win32':
    engine_memory.execute("ATTACH DATABASE 'C:/data/Socrata/nyc_backup.db3' as nyc")
else:
    engine_memory.execute("ATTACH DATABASE '/home/phrrngtn/nyc_backup.db3' as nyc")
m = MetaData(bind=engine_memory, schema="nyc")
# This is not as high-performance as a custom, set-oriented query against sqlite_schema would be
# but it has the advantage of being documented and supported. The reflection takes about 8 seconds on
# my laptop and I think it could be made to run in milliseconds using a set-based metadata/rule4 technique
m.reflect()

# Note how we can use schema.object name to refer to the table directly in the table collection.
# Also note that although the table name contains a hyphen and will need to be quoted when
# the object is used in a SQL query. SQLAlchemy will take care of all that for us.
q = m.tables["nyc.vsv_8wi4-bsy4"].select()
# The nyc.vsv_8wi4-bsy4 is a virtual table creating using the vsv module and is a wrapper
# over the delimited file downloaded by curl. I code-generated (via the sqlite3 shell) the SQL
# code to map in all the tables for which I had downloaded data.

# sqlite> .schema "vsv_8wi4-bsy4"
# CREATE VIRTUAL TABLE "vsv_8wi4-bsy4" USING vsv(filename="/mnt/c/data/socrata/8wi4-bsy4.csv",header=yes,nulls=on,affinity=numeric)
# /* "vsv_8wi4-bsy4"(borough,boro,block,lot,bbl,hnum_lo,hnum_hi,str_name,crfn,grantee,deed_date,price,cap_rate,borough_cap_rate,yearqtr,Postcode,Latitude,Longitude,"Community Board","Council District","Census Tract",BIN,NTA,Location1) */;

# Although the table/file is small (359 rows including the header), we use a limit clause to
# just take 3 records.
q = q.limit(3)
r = engine_memory.execute(q)
# iterate over the result and print each row as a dict
for row in r:
    print(dict(row))
