import sqlalchemy
from sqlalchemy import MetaData, Table, Column, sql, ForeignKey
from sqlalchemy.engine import URL
from sqlalchemy.orm import (
    registry,
    relationship,
    Session,
    session,
    sessionmaker,
    query,
    joinedload,
)

import pdb
from itertools import chain

# See https://github.com/coleifer/pysqlite3
import pysqlite3
from sqlalchemy.types import String, Text, Integer, JSON, Date, DateTime, Float
from sqlalchemy import create_engine
import json

m = MetaData(schema="socrata")
mapper_registry = registry()


# add in tables for 'domain'. Start with string/JSON
SocrataDomain = String(512)

domain = Table(
    "domain",
    m,
    Column("domain", SocrataDomain, primary_key=True),
    Column("_resources", JSON, nullable=False),
)
resource = Table(
    "resource",
    m,
    Column("domain", SocrataDomain, ForeignKey(domain.c.domain)),
    Column("resource_id", String(9), primary_key=True),
    Column("name", String),
    Column("permalink", String),
    Column("metadata", JSON),
    Column("resource", JSON, nullable=False),
)

resource_column = Table(
    "resource_column",
    m,
    Column(
        "resource_id", String(9), ForeignKey(resource.c.resource_id), primary_key=True
    ),
    Column("field_number", Integer, primary_key=True),
    Column("field_name", String),
    Column("data_type", String, nullable=False),
    Column("name", String, nullable=False),
    Column("description", String, nullable=False),
)


@mapper_registry.mapped
class Domain:
    __table__ = domain

    __mapper_args__ = {  # type: ignore
        "properties": {"resources": relationship("Resource")}
    }


@mapper_registry.mapped
class Resource:
    __table__ = resource
    __mapper_args__ = {  # type: ignore
        "properties": {
            "columns": relationship(
                "ResourceColumn", order_by="ResourceColumn.field_number"
            )
        }
    }

    def as_sa_table(self, metadata, schema=None):
        t = Table(self.resource_id, metadata, schema=schema)
        for c in self.columns:
            t.append_column(c.as_sa_column())
        return t


_type_map = {
    "Text": String,
    "Date": DateTime,
    "Calendar date": Date,
    "Number": Integer,
    # These are GeoAlchemy2 type and should work with PostgreSQL and SpatiaLite.
    # However, SpatiaLite is a pain in the neck to build as a dynamically loadable
    # extension so will not include it for the moment. It may be possible to
    # rig SQLAlchemy to render the column as TEXT (for example) when emitting DDL to SQLite but
    # I am not sure what the 'right' thing to do is.
    # "Polygon": Geometry("POLYGON"),
    # "MultiPoint": Geometry("MULTIPOINT"),
    # "MultiLine": Geometry("MULTILINESTRING"),
    # "Point": Geometry("POINT"),
    "URL": Text,
}

# The Socrata domains have dots/periods in the name and that
# is not database-friendly so we map the domains to something
# that works as a schema. The object will get created in the default schema
# if there is no mapping for the domain.
_domain_to_schema_map = {
    "data.cityofnewyork.us": "city_of_newyork_us",
}


@mapper_registry.mapped
class ResourceColumn:
    __table__ = resource_column

    def as_sa_column_hack(self):
        return Column(
            "f_%d_%s" % (self.field_number, self.field_name),
            type_=Text,
            comment=self.description,
        )

    def as_sa_column(self):
        return Column(
            "%s" % (self.field_name),
            type_=_type_map.get(self.data_type, Text),
            comment=self.description,
        )


# get rid of the password. See about using certificates instead.
# e = create_engine("postgresql+psycopg2://tgrid:tgrid4all@localhost:5432/tgrid")
e = create_engine("sqlite://", module=pysqlite3)
# connection_string = "Driver={ODBC Driver 17 for SQL Server};Server=LAPTOP-M6BP34C4.local\TGRID4All;UID=tgrid4all;PWD=tgrid"
# connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
# e = create_engine(connection_url)
# This is a workaround to the problem with schema-scoped names as they are not directly
# supported in SQLite.
e.execute("ATTACH DATABASE ':memory:' AS socrata")
e.echo = False


def resource2dict(resources):
    """
    assumes appropriately named json files are in the current directory"""
    d = {}
    for r in resource_list["results"]:
        print(r)
        if r["count"] == 0:
            continue
        try:
            payload = open(r["domain"] + ".json", "r").read()
        except:
            continue

        d[r["domain"]] = dict(domain=r["domain"], _resources=payload)

    return d


# SQLite does not have schemas so map it to None.
# However, I was not able to get this workaround to work correctly so use the ATTACH DATABASE
# trick above.
# xref https://docs.sqlalchemy.org/en/14/core/connections.html#translation-of-schema-names
# schema_map = {"socrata": None}
# e.connect().execution_options(schema_translate_map=schema_map)
# e.execution_options(schema_translate_map=schema_map)

Session = sessionmaker(bind=e)

if True:
    e.echo = True
    m.drop_all(bind=e)
    m.create_all(bind=e)
    e.echo = False

    # TODO: show how to retrieve the domains programatically and also the resources
    # within each domain programatically.

    # how to get the domains and the resource counts.
    # xref: https://socratadiscovery.docs.apiary.io/#reference/0/count-by-domain/count-by-domain
    # GET http://api.us.socrata.com/api/catalog/v1/domains
    # curl --include 'http://api.us.socrata.com/api/catalog/v1/domains'

    # resources within a domain. I got the 4350 from resultSetSize
    # see https://socratadiscovery.docs.apiary.io/#reference/0/find-by-domain
    #    Field	Description
    # results	an array of result objects
    # resultSetSize	the total number of results that could be returned were they not paged
    # curl "https://api.us.socrata.com/api/catalog/v1?domains=data.cityofnewyork.us&offset=0&limit=3450" --out newyork.json

    resource_list = json.load(open("socrata_domains.json"))
    ins = domain.insert()
    resource_map = resource2dict(resource_list)
    # pdb.set_trace()
    e.execute(ins, list(resource_map.values()))

    ins = resource.insert()

    for l in resource_map.values():
        try:

            e.execute(
                ins,
                [
                    dict(
                        resource_id=r["resource"]["id"],
                        domain=r["metadata"]["domain"],
                        name=r["resource"]["name"],
                        metadata=r["metadata"],
                        permalink=r["permalink"],
                        resource=r,
                    )
                    for r in json.loads(l["_resources"])["results"]
                ],
            )
        except json.JSONDecodeError:
            print("problem with %s" % (l["domain"]))
            continue

    # ins = resource_column.insert()

    # for r in domain_resources["results"]:
    #     res = r["resource"]
    #     if not "columns_field_name" in res:
    #         continue

    #     if not "id" in res:
    #         print("no id in %s" % (res))
    #         continue

    #     number_of_columns = len(res["columns_field_name"])
    #     if number_of_columns == 0:
    #         continue

    #     tuples = list(
    #         [
    #             tuple(z)
    #             for z in zip(
    #                 (res["id"],) * number_of_columns,
    #                 range(1, number_of_columns + 1),
    #                 res["columns_field_name"],
    #                 res["columns_datatype"],
    #                 res["columns_name"],
    #                 res["columns_description"],
    #             )
    #         ]
    #     )

    #     cols = list([c.name for c in resource_column.columns])
    #     e.execute(ins, [dict(zip(cols, t)) for t in tuples])
    #     # Can't seem to get the tuples to work.
    #     # e.execute(ins,[tuple(z) for z in zip((res['id'],)*number_of_columns,
    #     #                   range(1,number_of_columns+1),
    #     #                   res['columns_field_name'],
    #     #                   res['columns_datatype'],
    #     #                   res['columns_name'],
    #     #                   res['columns_description'])])

print("Done persisting metadata")
# with Session() as session:
#     # q = session.query(Domain).options(joinedload(Domain.resources).joinedload(Resource.columns))
#     new_m = MetaData()
#     for dom in session.query(Domain):
#         target_schema = _domain_to_schema_map.get(dom.domain, None)
#         print(dom.domain, target_schema)
#         for r in dom.resources:
#             skip_table = False
#             if len(r.columns) == 0:
#                 print("resource %s has zero columns! %s" % (r.name, r.permalink))
#                 continue
#             else:
#                 # print("%d columns in resource %s" % (len(r.columns), r.name))
#                 for c in r.columns:
#                     if len(c.field_name) >= 100:
#                         print(
#                             "Column %s in table %s too long (%d). Skipping table"
#                             % (c.field_name, r.name, len(c.field_name))
#                         )
#                         skip_table = True
#                         break
#                 pass
#             # SQLite can deal with such long identifiers as appear in this metadata
#             # PostgreSQL accepts the identifiers but truncates them. This may cause duplicate column-names
#             # SQL Server 2019 has a maximum length of 128
#             if not skip_table:
#                 t = r.as_sa_table(new_m, schema=target_schema)
#             # print("adding table %s %s" % (t.name, t.columns))

#     # Posgresql can run out of memory if we try and drop a whole bunch of tables
#     # at once within the same transaction
#     print("dropping resource tables")
#     for tn in new_m.tables:
#         tab = new_m.tables.get(tn)
#         tab.drop(bind=session.bind, checkfirst=True)

#     print("Creating resource tables")
#     new_m.create_all(bind=session.bind)


# Copy the 'main' database from our application that contains
# all the newly created tables to a file-based database
# xref https://stackoverflow.com/a/67162137/40387
engine_backup_file = create_engine(
    "sqlite:////home/phrrngtn/socrata_backup.db3", module=pysqlite3
)
raw_connection_backup_file = engine_backup_file.raw_connection()

raw_connection_socrata_resource = e.raw_connection()

print("Backing up resource tables to disk")
raw_connection_socrata_resource.backup(
    raw_connection_backup_file.connection, name="socrata"
)
print("done")
