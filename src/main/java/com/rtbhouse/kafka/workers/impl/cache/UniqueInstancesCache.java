package com.rtbhouse.kafka.workers.impl.cache;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class UniqueInstancesCache<T> {

    private static final CacheLoader IDENTITY_CACHE_LOADER = new CacheLoader() {
        @Override
        public Object load(Object key) {
            return key;
        }
    };

    private final LoadingCache<T, T> cache;

    public UniqueInstancesCache() {
        //noinspection unchecked
        this.cache = CacheBuilder.newBuilder()
                .weakValues()
                .build((CacheLoader<T, T>) IDENTITY_CACHE_LOADER);
    }

    public UniqueInstancesCache(int concurrencyLevel) {
        //noinspection unchecked
        this.cache = CacheBuilder.newBuilder()
                .concurrencyLevel(concurrencyLevel)
                .weakValues()
                .build((CacheLoader<T, T>) IDENTITY_CACHE_LOADER);
    }

    public T saveUnique(T object) {
        return cache.getUnchecked(object);
    }
}
