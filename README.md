# ARP4G-redis
这是[ARP4G](https://github.com/framework-arp/ARP4G)的Redis实现。

## 功能
1. 实现基于Redis的持久化
2. 实现基于[redsync](https://github.com/go-redsync/redsync)的互斥锁
## 如何使用
```go
//定义仓库
type OrderRepository interface {
	Take(ctx context.Context, id any) (order *Order, found bool)
}
//定义Service
type OrderService struct {
	orderRepository OrderRepository
}
//获得redis客户端
redisClient := redis.NewClient(&redis.Options{
	Addr: "127.0.0.1:6379",
	DB:   0,
})
//生成仓库的redis实现
redisOrderRepo := redisrepo.NewRedisRepository(redisClient, "Order", func() *aggregate.MySession { return &Order{} })
//使用仓库生成Service
orderService := &OrderService{redisOrderRepo}
```
