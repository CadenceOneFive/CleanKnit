WITH F(name, mtime) AS (
    select d.name as name,
        d.mtime as mtime
    from lsdir('.') as d -- note that we have a table-valued function to 
        -- return the list of directory entries as a table.
        -- https://github.com/nalgeon/sqlean/blob/main/docs/fileio.md
    where d.name like '%.json'
        and d.name not like '%metadata%'
        and length(d.name) <> 9
),
NEW_BLOBS(name, mtime, blob) AS (
    SELECT F.name,
        F.mtime,
        readfile(F.name) as blob -- https://github.com/nalgeon/sqlean/blob/main/docs/fileio.md
    FROM F
        LEFT OUTER JOIN socrata_blob as existing ON (F.name = existing.path)
    WHERE F.mtime > COALESCE(existing.mtime, 0)
),
BLOBS_WITH_CHECKSUMS (name, mtime, blob, blob_checksum) AS (
    SELECT name,
        mtime,
        blob,
        hex(sha1(blob)) as blob_checksum -- https://github.com/nalgeon/sqlean/blob/main/docs/crypto.md
    FROM NEW_BLOBS
)
INSERT INTO socrata_blob(path, mtime, blob, blob_checksum)
SELECT name as path,
    mtime,
    blob as blob,
    blob_checksum as blob_checksum
FROM BLOBS_WITH_CHECKSUMS
WHERE true ON conflict(path) DO
UPDATE
SET blob = excluded.blob,
    mtime = excluded.mtime,
    blob_checksum = excluded.blob_checksum
WHERE socrata_blob.blob_checksum <> excluded.blob_checksum;