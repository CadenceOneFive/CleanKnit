-- use the SQLite file-access extension to read the directory and
-- find all files with a .json extension that do not contain the word
-- metadata and are not 9 characters long (as those are Socrata data files and we want to processs
-- the *meta*data files)
WITH F(name) AS (
    select d.name as name
    from lsdir('.') as d -- note that we have a table-valued function to 
        -- return the list of directory entries as a table.
    where d.name like '%.json'
        and d.name not like '%metadata%'
        and length(d.name) <> 9
),
BLOBS(name, blob) AS (
    SELECT f.name,
        readfile(F.name) as blob
),
BLOBS_WITH_CHECKSUMS (name, blob, sha1) AS (
    SELECT name,
        blob,
        hex(sha1(blob)) as sha1
    FROM BLOBS
),
T AS (
    select -- note the JSON path expressions to pluck out scalars from the JSON
        json_extract(j.value, '$.resource.id') as resource_id,
        json_extract(j.value, '$.metadata.domain') as resource_domain,
        datetime(json_extract(j.value, '$.resource.updatedAt')) as updated_at,
        datetime(json_extract(j.value, '$.resource.createdAt')) as created_at,
        datetime(
            json_extract(j.value, '$.resource.metadata_updated_at')
        ) as metadata_updated_at,
        datetime(
            json_extract(j.value, '$.resource.data_updated_at')
        ) as data_updated_at,
        json_extract(j.value, '$.resource.name') as resource_name,
        json_extract(j.value, '$.permalink') as permalink
    FROM BLOBS_WITH_CHECKSUMS as bwc,
        json_each(bwc.blob, '$.results') as j -- 
    where json_valid(bwc.blob) -- we slurp in the file to confirm that it is valid JSON
) -- it looks like there are 156,859 resources managed by Socrata as of the time the metadata files were downloaded.
-- the runtime for this query is about 12 seconds so it is doing better than 10,000 resources/second
SELECT *
FROM T
where resource_domain IS NOT NULL;