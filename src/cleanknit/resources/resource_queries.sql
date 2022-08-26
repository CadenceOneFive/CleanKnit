-- This is the DDL for the table that contains the column definitions
-- for tabular resources.
CREATE TABLE socrata_blob([path] primary key, mtime, [blob], blob_checksum);

CREATE TABLE domain(domain varchar(512) primary key);

CREATE TABLE resource(
    domain VARCHAR(512),
    resource_id VARCHAR(9) NOT NULL,
    [name] VARCHAR,
    [description] VARCHAR,
    permalink VARCHAR,
    metadata JSON,
    [resource] JSON NOT NULL,
    PRIMARY KEY (resource_id),
    FOREIGN KEY(domain) REFERENCES domain (domain)
);

-- see https://www.sqlite.org/fts5.html
CREATE VIRTUAL TABLE resource_fts USING fts5([name], [description], content = resource);

-- adapted from https://kimsereylam.com/sqlite/2020/03/06/full-text-search-with-sqlite.html
CREATE TRIGGER resource_ai
AFTER
INSERT
    ON resource BEGIN
INSERT INTO
    resource_fts (rowid, [name], [description])
VALUES
    (new.rowid, new.[name], new.[description]);

END;

CREATE TRIGGER resource_ad
AFTER
    DELETE ON resource BEGIN
INSERT INTO
    resource_fts (resource_fts, rowid, [name], [description])
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
    ON resource BEGIN
INSERT INTO
    resource_fts (resource_fts, rowid, [name], [description])
VALUES
    (
        'delete',
        old.rowid,
        old.[name],
        old.[description]
    );

INSERT INTO
    resource_fts (rowid, name, description)
VALUES
    (new.rowid, new.name, new.description);

END;

CREATE TABLE resource_column (
    resource_id VARCHAR(9) NOT NULL,
    field_number INTEGER NOT NULL,
    field_name VARCHAR,
    data_type VARCHAR NOT NULL,
    [name] VARCHAR NOT NULL,
    [description] VARCHAR NOT NULL,
    PRIMARY KEY (resource_id, field_number),
    FOREIGN KEY(resource_id) REFERENCES resource (resource_id)
);

CREATE VIRTUAL TABLE resource_column_fts USING fts5(
    field_name,
    [name],
    [description],
    content = resource_column
);

CREATE TRIGGER resource_column_ai
AFTER
INSERT
    ON resource BEGIN
INSERT INTO
    resource_column_fts (rowid, field_name, [name], [description])
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
    DELETE ON resource BEGIN
INSERT INTO
    resource_column_fts (
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

END;

CREATE TRIGGER resource_column_au
AFTER
UPDATE
    ON resource BEGIN
INSERT INTO
    resource_column_fts (
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
    resource_column_fts (rowid, field_name, name, description)
VALUES
    (
        new.rowid,
        new.field_name,
        new.name,
        new.description
    );

END;

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

-- populate domain
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
        ) as row,
        E.value -> '$.metadata' as metadata,
        E.value -> '$.resource' as resource
    FROM
        socrata_blob as b,
        JSON_EACH(b.blob, '$.results') as E
),
_RESOURCE AS (
    SELECT
        T.row -> 0 as domain,
        T.row -> 1 as resource_id,
        T.row -> 2 as name,
        T.row -> 3 as description,
        T.row -> 4 as permalink,
        T.metadata as metadata,
        T.resource as resource
    FROM
        T
)
insert into
    domain(domain)
select
    distinct domain
from
    _RESOURCE;

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
        E.value -> '$.resource' as [resource]
    FROM
        socrata_blob as b,
        JSON_EACH(b.blob, '$.results') as E
),
_RESOURCE AS (
    SELECT
        T.flat_row -> 0 as domain,
        T.flat_row -> 1 as resource_id,
        T.flat_row -> 2 as [name],
        T.flat_row -> 3 as [description],
        T.flat_row -> 4 as permalink,
        T.metadata as metadata,
        T.resource as [resource]
    FROM
        T
)
INSERT INTO
    [resource]
SELECT
    *
FROM
    _RESOURCE;

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
        resource as r -- this contains the resource blobs as shredded from the catalog blob for a domain
        JOIN nums ON (
            -- note the < .. nums is zero-based
            nums.i < json_array_length(r.resource, '$.columns_name')
        )
    where
        json_array_length(r.resource, '$.columns_name') <> 0 -- want to pick out the resources that have column
        -- we might be able to use $.lens_view_type = 'tabular'
)
INSERT INTO
    resource_column(
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
    highlight(resource_column_idx, 2, '<b>', '</b>'),
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
    resource_column_fts as s
    LEFT OUTER JOIN resource_column AS rc ON (s.rowid = rc.rowid)
    JOIN resource AS r ON (rc.resource_id = r.resource_id)
where
    s.[description] match 'water'
limit
    100;