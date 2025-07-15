# How to run ducklake-gigapi

1. `cd gigapi_env`
2. `docker compose up`
3. Open your browser on http://localhost:7971
4. Use `my_ducklake` database
5. Run the following commands:

```
-- create table test (time TIMESTAMP,message VARCHAR); -- create table
-- alter table test set partitioned by (year(time), month(time), day(time), hour(time)); -- set partitioning
-- COMMENT ON COLUMN test.time IS 'orderby.1'; -- set order key
-- INSERT INTO test SELECT now(), 'abc' from range(100); -- this one may be run 3-4 times to insert extra rows 
-- SELECT * FROM ducklake_list_files('my_ducklake', 'test'); -- check files, check if they are merged
-- SELECT count() from test; -- check if no rows lost
```

Uncomment the commands one by one to run each of them.
