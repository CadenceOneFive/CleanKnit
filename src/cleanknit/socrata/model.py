import logging
import structlog
from structlog import get_logger

# https://stackoverflow.com/a/66537038
structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
)
log = get_logger('cleanknit.socrata')

import sqlalchemy
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    sql,
    ForeignKey,
    event,
)
from sqlalchemy.engine import URL, Engine
from sqlalchemy.orm import (
    registry,
    relationship,
    Session,
    session,
    sessionmaker,
    query,
    joinedload,
)


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    log.info("hello from set_sqlite_pragma", connection_record=connection_record)
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


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
