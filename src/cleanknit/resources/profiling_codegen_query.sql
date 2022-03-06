-- codegen query for profiling Socrata datasets that have been mapped in as virtual tables
-- via the VSV extension https://github.com/nalgeon/sqlean/blob/main/docs/vsv.md
--
-- The basic idea is to use the wonderfully flexible type system of SQLite to
-- survey the data based on the groupings of the observed *types* of the first N
-- columns of the table
select
    'SELECT ' || quote(s.name) || ',' || group_concat('typeof("' || c.name || '")') || ',COUNT(*) AS n FROM "' || s.name || '" GROUP BY ' || group_concat('typeof("' || c.name || '")') || ';'
FROM
    sqlite_schema as s,
    pragma_table_info(s.name) as c
where
    -- VSV is the name of the extension I use to process the files. It creates a SQLite
    -- virtual table which makes it possible to query the contents of the delimited file
    -- without actually physically 'loading' it into a database. I chose to use the
    -- prefix 'vsv' for the virtual table name.
    s.name like 'vsv%' -- this profiles just the first 10 columns. This may be sufficient
    -- for candidate keys as people tend to put them to the 'left' of the
    -- table
    and c.cid < 10
    and c.name <> '{' -- some of the files may not actually be in CSV format
group by
    s.name;