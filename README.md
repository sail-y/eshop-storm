# eshop-storm


从kafka读取原生数据，清洗数据后进行实事热点数据计算


## 基于Storm统计热点数据

1. 用Spout从kafka读取消息
2. Bolt提取productId发射到下一个Bolt
3. 基于LRUMap统计热点访问的productId
4. 将热点数据存入zookeeper

## 缓存预热

https://github.com/sail-y/eshop-cache

1. 服务启动的时候，进行缓存预热
2. 从zk中读取taskid列表
3. 依次遍历每个taskid，尝试获取分布式锁，如果获取不到，快速报错，不要等待，因为说明已经有其他服务实例在预热了
4. 直接尝试获取下一个taskid的分布式锁
5. 即使获取到了分布式锁，也要检查一下这个taskid的预热状态，如果已经被预热过了，就不再预热了
6. 执行预热操作，遍历productid列表，查询数据，然后写ehcache和redis
7. 预热完成后，设置taskid对应的预热状态
