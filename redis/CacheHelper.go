package cache

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/***REMOVED***/go-web-archetype/dal"
	"time"
)

type MyCacheHelper struct {
	client     *redis.Client
	context    context.Context
	genericDao *dal.GenericDao
}

func New(client *redis.Client, ctx context.Context, dao *dal.GenericDao) *MyCacheHelper {
	helper := MyCacheHelper{
		client:     client,
		context:    ctx,
		genericDao: dao,
	}
	return &helper
}

func (cacheHelper *MyCacheHelper) Set(prefix string, key string, val interface{}, timeout time.Duration) (string, error) {
	return cacheHelper.client.Set(cacheHelper.context, prefix + `::` + key, val, timeout).Result()
}

func (cacheHelper *MyCacheHelper) Del(prefix string, key string) (int64, error) {
	return cacheHelper.client.Del(cacheHelper.context, prefix + `::` + key).Result()
}

func (cacheHelper *MyCacheHelper) Get(prefix string, key string) (string, error){
	return cacheHelper.client.Get(cacheHelper.context, prefix + `::` + key).Result()
}