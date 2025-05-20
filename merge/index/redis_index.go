package index

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/gigapi/gigapi/v2/merge/shared"
	"github.com/gigapi/gigapi/v2/utils"
	"github.com/redis/go-redis/v9"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type RedisIndex struct {
	url      *url.URL
	c        *redis.Client
	patchSha string
}

func NewRedisIndex(URL string) (shared.Index, error) {
	u, err := url.Parse(URL)
	if err != nil {
		return nil, err
	}

	strDb := strings.Trim(u.Path, "/")
	iDb, err := strconv.Atoi(strDb)
	if err != nil {
		return nil, fmt.Errorf("invalid Redis DB number: %s", strDb)
	}

	idx := &RedisIndex{url: u}
	opts := &redis.Options{
		Addr: fmt.Sprintf("%s", u.Host),
		DB:   iDb,
	}

	if u.User != nil && u.User.Username() != "" {
		opts.Username = u.User.Username()
		if pwd, ok := u.User.Password(); ok {
			opts.Password = pwd
		}
	}
	if u.Scheme == "rediss" {
		opts.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
			tlsConfig := &tls.Config{
				InsecureSkipVerify: true,
			}
			return tls.Dial(network, addr, tlsConfig)
		}
	}

	idx.c = redis.NewClient(opts)

	err = idx.initFuncs()
	if err != nil {
		return nil, err
	}

	return idx, nil
}

const SCRIPT_PATCH_INDEX = `
-- Function to create and push a new merge object
local function create_and_push_new_merge(merge_key, path, size)
    local new_merge = cjson.encode({
        state = "idle",
        paths = {path},
        size = size
    })
    redis.call("RPUSH", merge_key, new_merge)
    return new_merge
end

local function delete_file(entry)
    redis.call("DEL", "files:" .. entry.path)
    return true
end

-- Function to process a single file
local function process_file(entry)
    -- Extract the index from the file path
    local path, index = string.match(entry.path, "(.+)/[^/]+%.(%d+)%.parquet$")

    if not index then
        return {success = false, error = "Invalid file path format: " .. entry.path}
    end

	local index_num = tonumber(index)
    if entry.cmd == "DELETE" then
        return {success = delete_file(entry)}
    end

    if index_num > #KEYS then
        return {success = true}
    end

    -- Create a Redis entry for the file
    redis.call("SET", "files:" .. entry.path, cjson.encode(entry))

    -- Get the last value from the merge list
    local merge_key = "merge:" .. index .. ":" .. path
    local last_merge = redis.call("LINDEX", merge_key, -1)

    if not last_merge then
        -- Create and push a new merge object
        create_and_push_new_merge(merge_key, entry.path, entry.size_bytes)
        return {success = true}
    end

    -- Parse JSON from the last merge entry
    local last_merge_data = cjson.decode(last_merge)
    if last_merge_data.state ~= "idle" then
        -- Create and push a new merge object
        create_and_push_new_merge(merge_key, entry.path, entry.size_bytes)
        return {success = true}
    end

    if last_merge_data.size + entry.size_bytes > tonumber(KEYS[index_num]) then
        -- Create and push a new merge object
        create_and_push_new_merge(merge_key, entry.path, entry.size_bytes)
        return {success = true}
    end

    -- Update the last merge entry
    last_merge_data.size = last_merge_data.size + entry.size_bytes
    table.insert(last_merge_data.paths, entry.path)
    local updated_merge = cjson.encode(last_merge_data)
    redis.call("LSET", merge_key, -1, updated_merge)
    return {success = true}
end

-- Process all files
local results = {
    processed_count = 0
}

for i = 1, #ARGV do
    local entry = cjson.decode(ARGV[i])
    local result = process_file(entry)
    if result.success then
        results.processed_count = results.processed_count + 1
    else
		return redis.error_reply("Error processing file: ".. entry.path.. " - ".. result.error)
    end
end

return results.processed_count
`

type redisIndexEntry struct {
	*shared.IndexEntry
	Cmd string `json:"cmd"`
}

func (r *RedisIndex) initFuncs() error {
	var err error
	r.patchSha, err = r.c.ScriptLoad(context.Background(), SCRIPT_PATCH_INDEX).Result()
	if err != nil {
		return err
	}

	return nil
}

func (r *RedisIndex) Batch(add []*shared.IndexEntry, rm []string) utils.Promise[int32] {
	var cmds []any
	for _, entry := range add {
		cmd, err := json.Marshal(redisIndexEntry{IndexEntry: entry, Cmd: "ADD"})
		if err != nil {
			return utils.Fulfilled[int32](err, 0)
		}
		cmds = append(cmds, string(cmd))
	}
	for _, path := range rm {
		cmd, err := json.Marshal(redisIndexEntry{IndexEntry: &shared.IndexEntry{Path: path}, Cmd: "DELETE"})
		if err != nil {
			return utils.Fulfilled[int32](err, 0)
		}
		cmds = append(cmds, string(cmd))
	}
	res := utils.New[int32]()

	var keys []string
	for _, c := range shared.GetMergeConfigurations() {
		keys = append(keys, strconv.FormatInt(c[1], 10))
	}

	go func() {
		_, err := r.c.EvalSha(context.Background(), r.patchSha, keys, cmds...).Result()
		res.Done(0, err)
	}()
	return res
}

func (r *RedisIndex) Get(path string) *shared.IndexEntry {
	res, err := r.c.Get(context.Background(), "files:"+path).Result()
	if err != nil {
		return nil
	}
	e := &shared.IndexEntry{}
	err = json.Unmarshal([]byte(res), e)
	if err != nil {
		return nil
	}
	return e
}

func (r *RedisIndex) Run() {
	//TODO implement me
	panic("implement me")
}

func (r *RedisIndex) Stop() {
	//TODO implement me
	panic("implement me")
}

type QEntry struct {
	Path  string `json:"path"`
	TimeS int32  `json:"time"`
}

func (r *RedisIndex) AddToDropQueue(files []string) utils.Promise[int32] {
	_files := make([]any, len(files))
	for i, file := range files {
		_file, err := json.Marshal(QEntry{
			Path:  file,
			TimeS: int32(time.Now().Unix()),
		})
		if err != nil {
			return utils.Fulfilled[int32](err, 0)
		}
		_files[i] = string(_file)
	}
	r.c.LPush(context.Background(), "drop", _files...)
	return utils.Fulfilled[int32](nil, 0)
}

func (r *RedisIndex) RmFromDropQueue(files []string) utils.Promise[int32] {
	res := utils.New[int32]()
	go func() {
		for _, file := range files {
			_, err := r.c.LRem(context.Background(), "drop", 1, file).Result()
			if err != nil {
				res.Done(0, err)
				return
			}
		}
		res.Done(0, nil)
	}()
	return res
}

func (r *RedisIndex) GetDropQueue() []string {
	res, err := r.c.LRange(context.Background(), "drop", 0, -1).Result()
	if err != nil {
		return nil
	}
	return res
}
