-- This is the DDL for the tables that contains the column definitions
-- for tabular resources hosted by Socrata.
PRAGMA foreign_keys = ON;

create table nums (i integer primary key);

-- from https://stackoverflow.com/a/24662818
WITH RECURSIVE cte(x) AS (
    SELECT
        random()
    UNION
    ALL
    SELECT
        random()
    FROM
        cte
    LIMIT
        10000
)
INSERT INTO
    nums(i)
select
    ROW_NUMBER() OVER (
        ORDER BY
            x
    ) - 1 -- we want ours to start at 0
FROM
    cte;

-- we get the overall list of domains managed by socrata from since single HTTP GET
-- curl 'http://api.us.socrata.com/api/catalog/v1/domains' --output socrata_domains.json
-- for convenience, we code-generate the URLS and curl invocations to download
-- the metadata for each domain (in JSON format)
-- ./socrata2curl.sh < socrata_domains.json  | sh -x 
-- this table will hold the blob of JSON metadata plus a little bit of 
-- metadata that can help with incremental update. At the moment, there is a bunch of volatile
-- data in the catalog JSON blobs (e.g. how many times the resource was accessed in the last week)
-- so some more work is needed on fixing the 'cache busting' behavior.
CREATE TABLE socrata_blob([path] primary key, mtime, [blob], blob_checksum);

-- this is a simple table of socrata domains.
CREATE TABLE socrata_domain(domain varchar(512) primary key);

-- we have a resource table that has metadata for each resource pluse
-- the blob of JSON that contains the definition of the resource. 
-- Note the foreign
CREATE TABLE socrata_resource_tabular(
    domain VARCHAR(512),
    resource_id VARCHAR(9) NOT NULL,
    [name] VARCHAR,
    [description] VARCHAR,
    permalink VARCHAR,
    metadata JSON,
    classification JSON,
    [owner] JSON,
    [creator] JSON,
    [resource] JSON NOT NULL,
    PRIMARY KEY (resource_id),
    FOREIGN KEY(domain) REFERENCES socrata_domain (domain)
);

-- see https://www.sqlite.org/fts5.html for general background on FTS
-- we want to have a very high performance index that is suitable for use within interactive
-- environments like Excel.
CREATE VIRTUAL TABLE socrata_resource_tabular_fts USING fts5(
    [name],
    [description],
    content = socrata_resource_tabular
);

-- this is some boilerplate adapted from https://kimsereylam.com/sqlite/2020/03/06/full-text-search-with-sqlite.html
-- for auto-maintaining the full-text indexes in the face of data modification (DML) on the tables.
CREATE TRIGGER resource_ai
AFTER
INSERT
    ON socrata_resource_tabular BEGIN
INSERT INTO
    socrata_resource_tabular_fts (rowid, [name], [description])
VALUES
    (new.rowid, new.[name], new.[description]);

END;

CREATE TRIGGER resource_ad
AFTER
    DELETE ON socrata_resource_tabular BEGIN
INSERT INTO
    socrata_resource_tabular_fts (
        socrata_resource_tabular_fts,
        rowid,
        [name],
        [description]
    )
VALUES
    (
        'delete',
        old.rowid,
        old.[name],
        old.[description]
    );

END;

CREATE TRIGGER resource_au
AFTER
UPDATE
    ON socrata_resource_tabular BEGIN
INSERT INTO
    socrata_resource_tabular_fts (
        socrata_resource_tabular_fts,
        rowid,
        [name],
        [description]
    )
VALUES
    (
        'delete',
        old.rowid,
        old.[name],
        old.[description]
    );

INSERT INTO
    socrata_resource_tabular_fts (rowid, name, description)
VALUES
    (new.rowid, new.name, new.description);

END;

CREATE TABLE socrata_resource_column (
    resource_id VARCHAR(9) NOT NULL,
    field_number INTEGER NOT NULL,
    field_name VARCHAR,
    data_type VARCHAR NOT NULL,
    [name] VARCHAR NOT NULL,
    [description] VARCHAR NOT NULL,
    PRIMARY KEY (resource_id, field_number),
    FOREIGN KEY(resource_id) REFERENCES socrata_resource_tabular (resource_id)
);

CREATE VIRTUAL TABLE socrata_resource_column_fts USING fts5(
    field_name,
    [name],
    [description],
    content = socrata_resource_column
);

CREATE TRIGGER resource_column_ai
AFTER
INSERT
    ON socrata_resource_column BEGIN
INSERT INTO
    socrata_resource_column_fts (rowid, field_name, [name], [description])
VALUES
    (
        new.rowid,
        new.field_name,
        new.[name],
        new.[description]
    );

END;

CREATE TRIGGER resource_column_ad
AFTER
    DELETE ON socrata_resource_column BEGIN
INSERT INTO
    socrata_resource_column_fts (
        socrata_resource_column_fts,
        rowid,
        field_name,
        [name],
        [description]
    )
VALUES
    (
        'delete',
        old.rowid,
        old.field_name,
        old.[name],
        old.[description]
    );

END;

CREATE TRIGGER resource_column_au
AFTER
UPDATE
    ON socrata_resource_column BEGIN
INSERT INTO
    socrata_resource_column_fts (
        resource_column_fts,
        rowid,
        field_name,
        [name],
        [description]
    )
VALUES
    (
        'delete',
        old.rowid,
        old.field_name,
        old.[name],
        old.[description]
    );

INSERT INTO
    socrata_resource_column_fts (rowid, field_name, name, description)
VALUES
    (
        new.rowid,
        new.field_name,
        new.name,
        new.description
    );

END;

-- this has some room for improvement: it reads the file-system from the current working directory
-- it also does not do anything to knock out the volatile elements of the JSON definition of the resource
-- (see above comments). It would be nice if we could figure out how to ask Socrata for metadata entries
-- that have changed since a fixed timestamp (which we would read out of the database)
WITH F(name, mtime) AS (
    select
        d.name,
        d.mtime as mtime
    from
        lsdir('.', true) as d -- note that we have a table-valued function to
        -- return the list of directory entries as a table.
        -- https://github.com/nalgeon/sqlean/blob/main/docs/fileio.md
    where
        d.name like '%.json'
        and d.name not like '%metadata%'
        and length(d.name) <> 9
),
NEW_BLOBS(name, mtime, blob) AS (
    SELECT
        F.name,
        F.mtime,
        readfile(F.name) as blob -- https://github.com/nalgeon/sqlean/blob/main/docs/fileio.md
    FROM
        F
        LEFT OUTER JOIN socrata_blob as existing ON (F.name = existing.path)
    WHERE
        F.mtime > COALESCE(existing.mtime, 0)
),
-- note that there may be ever-changing data in the blob that will bust caching based on checksum.
BLOBS_WITH_CHECKSUMS (name, mtime, blob, blob_checksum) AS (
    SELECT
        name,
        mtime,
        blob,
        hex(sha1(blob)) as blob_checksum -- https://github.com/nalgeon/sqlean/blob/main/docs/crypto.md
    FROM
        NEW_BLOBS
)
INSERT INTO
    socrata_blob(path, mtime, blob, blob_checksum)
SELECT
    name as path,
    mtime,
    blob as blob,
    blob_checksum as blob_checksum
FROM
    BLOBS_WITH_CHECKSUMS
WHERE
    true ON conflict(path) DO
UPDATE
SET
    blob = excluded.blob,
    mtime = excluded.mtime,
    blob_checksum = excluded.blob_checksum
WHERE
    socrata_blob.blob_checksum <> excluded.blob_checksum;

-- populate domain .. we should make this into an UPSERT.
WITH T(domain) AS (
    select
        E.value ->> '$.metadata.domain' as domain
    FROM
        socrata_blob as b,
        JSON_EACH(b.blob, '$.results') as E
)
insert into
    socrata_domain(domain)
select
    distinct domain
from
    T;

-- likewise, this should be made into an UPSERT
WITH T AS (
    select
        json_extract(
            E.value,
            '$.metadata.domain',
            '$.resource.id',
            '$.resource.name',
            '$.resource.description',
            '$.permalink',
            '$.link'
        ) as flat_row,
        E.value -> '$.metadata' as metadata,
        E.value -> '$.owner' as [owner],
        E.value -> '$.creator' as [creator],
        E.value -> '$.classification' as classification,
        E.value -> '$.resource' as [resource]
    FROM
        socrata_blob as b,
        JSON_EACH(b.blob, '$.results') as E
),
_RESOURCE AS (
    SELECT
        T.flat_row ->> 0 as domain,
        T.flat_row ->> 1 as resource_id,
        T.flat_row ->> 2 as [name],
        T.flat_row ->> 3 as [description],
        T.flat_row ->> 4 as permalink,
        T.metadata as metadata,
        T.owner as [owner],
        T.creator as creator,
        T.classification as classification,
        T.resource as [resource]
    FROM
        T
)
INSERT INTO
    [socrata_resource_tabular] (
        domain,
        resource_id,
        [name],
        [description],
        permalink,
        metadata,
        [owner],
        creator,
        classification,
        [resource]
    )
SELECT
    domain,
    resource_id,
    name,
    description,
    permalink,
    metadata,
    [owner],
    [creator],
    classification,
    resource
FROM
    _RESOURCE;

-- now do the columns; likewise, another need for an UPSERT.
WITH T AS (
    select
        r.resource_id,
        i + 1 as field_number,
        -- the JSON is zero-based but we want the fields to be 1-based
        r.resource -> '$.columns_field_name' ->> i AS field_name,
        r.resource -> '$.columns_datatype' ->> i AS data_type,
        r.resource -> '$.columns_name' ->> i AS [name],
        r.resource -> '$.columns_description' ->> i AS [description]
    FROM
        socrata_resource_tabular as r -- this contains the resource blobs as shredded from the catalog blob for a domain
        JOIN nums ON (
            -- note the < .. nums is zero-based
            nums.i < json_array_length(r.resource, '$.columns_name')
        )
    where
        json_array_length(r.resource, '$.columns_name') <> 0 -- want to pick out the resources that have column
        -- we might be able to use $.lens_view_type = 'tabular'
)
INSERT INTO
    socrata_resource_column(
        resource_id,
        field_number,
        field_name,
        data_type,
        [name],
        [description]
    )
SELECT
    resource_id,
    field_number,
    field_name,
    data_type,
    [name],
    [description]
FROM
    T;

-- demo query 
select
    highlight(socrata_resource_column_fts, 2, '<b>', '</b>'),
    -- note column number should be consistent
    -- with the column used for the MATCH
    r.resource_id,
    r.name,
    rc.name,
    s.rowid,
    s.field_name,
    s.name,
    s.description
FROM
    socrata_resource_column_fts as s
    LEFT OUTER JOIN socrata_resource_column AS rc ON (s.rowid = rc.rowid)
    JOIN socrata_resource_tabular AS r ON (rc.resource_id = r.resource_id)
where
    s.[description] match 'sqft'
limit
    5;