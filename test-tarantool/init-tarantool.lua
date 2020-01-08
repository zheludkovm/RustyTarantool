#! /usr/bin/tarantool
-- This is default tarantool initialization file
-- with easy to use configuration examples including
-- replication, sharding and all major features
-- Complete documentation available in:  http://tarantool.org/doc/
--
-- To start this instance please run `systemctl start tarantool@example` or
-- use init scripts provided by binary packages.
-- To connect to the instance, use "sudo tarantoolctl enter example"
-- Features:
-- 1. Database configuration
-- 2. Binary logging and snapshots
-- 3. Replication
-- 4. Automatinc sharding
-- 5. Message queue
-- 6. Data expiration

-----------------
-- Configuration
-----------------
box.cfg {
    ------------------------
    -- Network configuration
    ------------------------

    -- The read/write data port number or URI
    -- Has no default value, so must be specified if
    -- connections will occur from remote clients
    -- that do not use “admin address”
    listen = 3301;
    -- listen = '*:3301';

    -- The server is considered to be a Tarantool replica
    -- it will try to connect to the master
    -- which replication_source specifies with a URI
    -- for example konstantin:secret_password@tarantool.org:3301
    -- by default username is "guest"
    -- replication_source="127.0.0.1:3102";

    -- The server will sleep for io_collect_interval seconds
    -- between iterations of the event loop
    io_collect_interval = nil;

    -- The size of the read-ahead buffer associated with a client connection
    readahead = 16320;

    ----------------------
    -- Memtx configuration
    ----------------------

    -- An absolute path to directory where snapshot (.snap) files are stored.
    -- If not specified, defaults to /var/lib/tarantool/INSTANCE
    -- memtx_dir = nil;

    -- How much memory Memtx engine allocates
    -- to actually store tuples, in bytes.
    memtx_memory = 128 * 1024 * 1024;

    -- Size of the smallest allocation unit, in bytes.
    -- It can be tuned up if most of the tuples are not so small
    memtx_min_tuple_size = 16;

    -- Size of the largest allocation unit, in bytes.
    -- It can be tuned up if it is necessary to store large tuples
    memtx_max_tuple_size = 10 * 1024 * 1024; -- 10Mb
    vinyl_max_tuple_size = 10 * 1024 * 1024; -- 10Mb

    ----------------------
    -- Vinyl configuration
    ----------------------

    -- An absolute path to directory where Vinyl files are stored.
    -- If not specified, defaults to /var/lib/tarantool/INSTANCE
    -- vinyl_dir = nil;

    -- How much memory Vinyl engine can use for in-memory level, in bytes.
    vinyl_memory = 128 * 1024 * 1024; -- 128 mb

    -- How much memory Vinyl engine can use for caches, in bytes.
    vinyl_cache = 64 * 1024 * 1024; -- 64 mb

    -- The maximum number of background workers for compaction.
    vinyl_write_threads = 2;

    ------------------------------
    -- Binary logging and recovery
    ------------------------------

    -- An absolute path to directory where write-ahead log (.xlog) files are
    -- stored. If not specified, defaults to /var/lib/tarantool/INSTANCE
    -- wal_dir = nil;

    -- Specify fiber-WAL-disk synchronization mode as:
    -- "none": write-ahead log is not maintained;
    -- "write": fibers wait for their data to be written to the write-ahead log;
    -- "fsync": fibers wait for their data, fsync follows each write;
    --    wal_mode = "none";
    wal_mode = "write";

    -- The maximal size of a single write-ahead log file
    wal_max_size = 256 * 1024 * 1024;

    -- The interval between actions by the snapshot daemon, in seconds
    checkpoint_interval = 60 * 60; -- one hour

    -- The maximum number of snapshots that the snapshot daemon maintans
    checkpoint_count = 6;

    -- Reduce the throttling effect of box.snapshot() on
    -- INSERT/UPDATE/DELETE performance by setting a limit
    -- on how many megabytes per second it can write to disk
    snap_io_rate_limit = nil;

    -- Don't abort recovery if there is an error while reading
    -- files from the disk at server start.
    force_recovery = true;

    ----------
    -- Logging
    ----------

    -- How verbose the logging is. There are six log verbosity classes:
    -- 1 – SYSERROR
    -- 2 – ERROR
    -- 3 – CRITICAL
    -- 4 – WARNING
    -- 5 – INFO
    -- 6 – DEBUG
    log_level = 5;

    -- By default, the log is sent to /var/log/tarantool/INSTANCE.log
    -- If logger is specified, the log is sent to the file named in the string
    log = "./tarantool.log",
    wal_dir = './db/wal',
    memtx_dir = './db/memtx',
    vinyl_dir = './db/vinyl',
    --work_dir = './work',

    -- If true, tarantool does not block on the log file descriptor
    -- when it’s not ready for write, and drops the message instead
    log_nonblock = true;

    -- If processing a request takes longer than
    -- the given value (in seconds), warn about it in the log
    too_long_threshold = 0.5;

    -- Inject the given string into server process title
    -- custom_proc_title = 'example';
    background = false;
    pid_file = 'rust.pid';
}

local function bootstrap()
    box.schema.user.grant('guest', 'read,write,execute', 'universe')

    box.schema.user.create('rust', { password = 'rust' })
    box.schema.user.grant('rust', 'read,write,execute', 'universe')
end
box.once('grants2', bootstrap)
local json = require('json')
local log = require('log')
local open = io.open

local function init_spaces()
    if(box.space.target_space ~=nil) then
       box.space.target_space:drop();
    end

    box.schema.create_space('target_space', {id = 1000})
    box.space.target_space:create_index('primary', {type='tree', parts={1,'unsigned', 2, "string"}})
    box.space.target_space:put({1,'test-row'})
    box.space.target_space:put({2,'test-row2'})

    --init table with currenies
    if(box.space.countries ~=nil) then
        box.space.countries:drop();
    end

    local countriesSpace = box.schema.create_space('countries', {id = 1001})
    countriesSpace:create_index('primary', {type='tree', parts={1,'unsigned'}})

    local file = open("countries.json", "rb") -- r read mode and b binary mode
    local content = file:read "*a"
    file:close()
    local currencies = json.decode(content);
    for i,row in pairs(currencies) do
       countriesSpace:insert({
           row["country-code"],
           string.lower(row["name"]),
           string.lower(row["region"]),
           string.lower(row["sub-region"]),
       })
    end

end

init_spaces()

json=require('json')
fiber = require('fiber')
function test(a,b)
--   fiber.sleep(2)
   return a,b,11
end

local function check_attr(value, search_str)
   return  search_str==nil or string.find(value,  string.lower(search_str));
end

function test_search(country_name, region, sub_region)
    local result = {};
    for i,row in box.space.countries:pairs() do
       if( check_attr(row[2], country_name) and
           check_attr(row[3], region) and
           check_attr(row[4], sub_region) ) then

           table.insert(result,{
              ["country-code"]=row[1],
              ["name"]=row[2],
              ["region"]=row[3],
              ["sub-region"]=row[4],
           })
       end
    end
    return result;
end
