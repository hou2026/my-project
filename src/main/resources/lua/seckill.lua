local voucherId = ARGV[1]
local userId = ARGV[2]
local orderId =ARGV[3]

-- 定义 Redis Key
local stockKey = "seckill:stock:" .. voucherId
local orderKey = "seckill:order:" .. userId

-- 检查库存是否充足，不足返回1
if tonumber(redis.call("GET", stockKey) or 0) <= 0 then
    return 1
end

-- 检查用户是否已购买，已购买返回2（一人一单）
if redis.call("SISMEMBER", orderKey, voucherId) == 1 then
    return 2
end

-- 扣减库存并记录用户购买信息
redis.call("DECR", stockKey)
redis.call("SADD", orderKey, voucherId)
-- 将voucherId,userId,orderId加入消息队列
redis.call("xadd","stream.orders","*","voucherId",voucherId,"userId",userId,"id",orderId)
return 0
