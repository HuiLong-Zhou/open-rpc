package com.zhl.factory;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 *  单例池工厂
 * @author zhl
 * @since 2024-07-15 14:11
 */
public class SingletonFactory {
    private static final Map<String, Object> OBJECT_MAP = new ConcurrentHashMap<>();

    private SingletonFactory() {
    }

    /**
     * 获取单例对象，如果没有则创建单例对象并存入 map 缓存中
     * @param clazz
     * @return
     * @param <T>
     */
    public static <T> T getInstance(Class<T> clazz) {
        if(clazz==null) {
            throw new IllegalArgumentException("Class cannot be null.");
        }
        String key = clazz.getName();
        if(OBJECT_MAP.containsKey(key)) {
            return clazz.cast(OBJECT_MAP.get(key));
        }else {
            return clazz.cast(OBJECT_MAP.computeIfAbsent(key, k -> {
                try {
                    return clazz.getDeclaredConstructor().newInstance();
                } catch (InstantiationException | NoSuchMethodException | InvocationTargetException |
                         IllegalAccessException e) {
                    throw new RuntimeException("Failed to create singleton instance for class: " + clazz.getName(), e);
                }
            }));
        }
    }
}
