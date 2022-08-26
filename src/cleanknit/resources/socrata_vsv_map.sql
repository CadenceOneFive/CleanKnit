-- xref:
-- VSV allows access to delimited files
--   https://github.com/nalgeon/sqlean/blob/main/docs/vsv.md
-- 
-- fileio provides the lsdir function
--  https://github.com/nalgeon/sqlean/blob/main/docs/fileio.md
--
-- re provides regular expressions
--    https://github.com/nalgeon/sqlean/blob/main/docs/re.md
--
-- Here is the query to create virtual tables off the Socrata downloaded CSV using the column
-- names from the first line of the file
select
    'CREATE VIRTUAL TABLE "vsv_' || substring(substring(d.name, length(d.name) -12), 1, 9) || '" IF NOT EXISTS USING vsv(filename="' || d.name || '",fsep="\t",header=yes,nulls=on,affinity=numeric);'
FROM
    -- lsdir is part of a SQLite extension that allows access to the file-system
    -- SQLite is running on WSL2 which is a way of running Linux on a Windows PC
    -- I downloaded the data to Windows to C:\data\socrata using curl (on Windows). This
    -- filesystem is accessible via /mnt/c on WSL
    --lsdir('/mnt/c/data/socrata') as d
    lsdir('.') as d
where
    -- this is yet another useful SQLite extension that allows us to filter out
    -- filenames that are not in the 4-4.tsv form (although note that the regexp package
    -- provided by this extension does not allow you to specify number of matches of the character class. This
    -- regexp should by right be [a-z0-9][a-z0-9][a-z0-9][a-z0-9]-[a-z0-9][a-z0-9][a-z0-9][a-z0-9]\.csv
    regexp_like(d.name, '[a-z0-9]+-[a-z0-9]+.tsv$');