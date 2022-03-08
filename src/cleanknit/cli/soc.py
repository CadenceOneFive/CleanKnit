from sqlalchemy.orm import sessionmaker
from ..socrata.model import log, domain,resource,resource_column
import json

def resource2dict(resource_list):
    """
    assumes appropriately named json files are in the current directory"""
    d = {}
    for r in resource_list["results"]:
        if r["count"] == 0:
            continue
        try:
            payload = open(r["domain"] + ".json", "r").read()
        except:
            continue

        d[r["domain"]] = dict(domain=r["domain"], _resources=payload)

    return d





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


def create_socrata_rule4(S, resource_list):
    resource_map = resource2dict(resource_list)
    ins = domain.insert()
    with S.begin():
        S.execute(ins, list(resource_map.values()))
    log.info("Done persisting domains")

    # This might take up a fair bit of memory
    all_resources = []
    for l in resource_map.values():
        try:
            for r in json.loads(l["_resources"])["results"]:
                all_resources.append(
                    dict(
                        resource_id=r["resource"]["id"],
                        domain=r["metadata"]["domain"],
                        name=r["resource"]["name"],
                        metadata=r["metadata"],
                        permalink=r["permalink"],
                        resource=r,
                    )
                )
        except json.JSONDecodeError:
            log.error("problem decoding JSON", domain=l["domain"])
            continue

    all_resource_columns = []

    ins = resource.insert()
    with S.begin():
        S.execute(ins, all_resources)
    log.info("Done persisting resources")

    rc_cols = list([c.name for c in resource_column.columns])
    for r in all_resources:
        if not "resource_id" in r:
            log.debug("no resource_id", **r)
            continue
        resource_id = r["resource_id"]
        if not "metadata" in r:
            log.debug("no metadata", **r)
            continue

        res = r["resource"]["resource"]
        if not "columns_field_name" in res:
            log.debug(
                "no columns_field_name",
                domain=r["metadata"]["domain"],
                resource_id=resource_id,
                name=res["name"],
            )
            continue

        number_of_columns = len(res["columns_field_name"])
        if number_of_columns == 0:
            log.debug(
                "zero columns",
                domain=r["metadata"]["domain"],
                resource_id=resource_id,
                name=res["name"],
            )
            continue

        tuples = list(
            [
                tuple(z)
                for z in zip(
                    (res["id"],) * number_of_columns,
                    range(1, number_of_columns + 1),
                    res["columns_field_name"],
                    res["columns_datatype"],
                    res["columns_name"],
                    res["columns_description"],
                )
            ]
        )
        for rc in [dict(zip(rc_cols, t)) for t in tuples]:
            all_resource_columns.append(rc)

    log.info("Done preparing resource-columns")

    with S.begin():
        S.execute(resource_column.insert(), all_resource_columns)

    log.info("Done persisting resource-columns")
    log.info("Done persisting metadata")
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