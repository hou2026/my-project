-- 分布式锁释放脚本
-- KEYS[1]: 锁的key
-- ARGV[1]: 锁的标识(通常是UUID或线程标识)
-- 返回值: 1表示释放成功, 0表示释放失败(不是锁的持有者)

if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
else
    return 0
end
