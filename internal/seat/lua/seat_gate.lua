-- seat_gate.lua
-- Atomic hash-compare → conditional write → stream append.
--
-- KEYS[1] = seat:hash:{tripID}:{date}
-- KEYS[2] = seat:map:{tripID}:{date}
-- KEYS[3] = seat:ts:{tripID}:{date}
-- KEYS[4] = seat:dirty_stream
--
-- ARGV[1] = new canonical hash
-- ARGV[2] = new seat JSON
-- ARGV[3] = current timestamp (float string)
-- ARGV[4] = trip:date identifier for stream entry

local cur_hash = redis.call('GET', KEYS[1])

if cur_hash ~= ARGV[1] then
    -- Data changed: update map and hash, append to dirty stream
    redis.call('SET', KEYS[2], ARGV[2])
    redis.call('SET', KEYS[1], ARGV[1])
    redis.call('XADD', KEYS[4], 'MAXLEN', '~', '200000', '*',
               'trip', ARGV[4], 'changed_at', ARGV[3])
end

-- Always update timestamp regardless of change
redis.call('SET', KEYS[3], ARGV[3])

return cur_hash ~= ARGV[1] and 1 or 0
