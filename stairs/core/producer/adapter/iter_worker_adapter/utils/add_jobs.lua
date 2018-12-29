

local step_key = KEYS[1]


for i, value in ipairs(ARGV) do
    redis.call("lpush", step_key, value)
end



