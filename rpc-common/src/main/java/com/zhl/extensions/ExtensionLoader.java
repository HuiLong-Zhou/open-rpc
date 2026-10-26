package com.zhl.extensions;

import com.zhl.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 *  https://cn.dubbo.apache.org/zh-cn/overview/mannual/java-sdk/reference-manual/spi/description/dubbo-spi/
 * @author zhl
 * @since 2024-07-16 17:41
 */
@Slf4j
public class ExtensionLoader<T> {
    private static final String SERVICE_DIRECTORY = "META-INF/extensions/";
    /**
     * 缓存了该 ExtensionLoader 加载的扩展名与扩展实现对象之间的映射关系。
     */
    private final Map<String, Holder<Object>> cacheInstances = new ConcurrentHashMap<>();
    private static final Map<Class<?>, ExtensionLoader<?>> EXTENSION_LOADERS = new ConcurrentHashMap<>();
    private static final Map<Class<?>, Object> EXTENSION_INSTANCES = new ConcurrentHashMap<>();
    private final Class<?> type;
    /**
     * 缓存了该 ExtensionLoader 加载的扩展名与扩展实现对象之间的映射关系。
     */
    private final Holder<Map<String, Class<?>>> cachedClasses = new Holder<>();

    public ExtensionLoader(Class<?> type) {
        this.type = type;
    }

    public static  <S>ExtensionLoader<S> getExtensionLoader(Class<S> type) {
        if(type==null) {
            throw new IllegalArgumentException("Extension type should not be null.");
        }
        if (!type.isInterface()) {
            throw new IllegalArgumentException("Extension type must be an interface.");
        }
        if (type.getAnnotation(SPI.class) == null) {
            throw new IllegalArgumentException("Extension type must be annotated by @SPI");
        }
        // 确保创建单例对象
        ExtensionLoader<S> extensionLoader = (ExtensionLoader<S>) EXTENSION_LOADERS.get(type);
        if(extensionLoader==null) {
            EXTENSION_LOADERS.putIfAbsent(type, new ExtensionLoader<S>(type));
            extensionLoader= (ExtensionLoader<S>) EXTENSION_LOADERS.get(type);
        }
        return extensionLoader;
    }

    public T getExtension(String name) {
        if(StringUtil.isBlank(name)) {
            throw new IllegalArgumentException("Extension name should not be null or empty.");
        }
        Holder<Object> holder = cacheInstances.get(name);
        if(holder==null) {
            cacheInstances.putIfAbsent(name, new Holder<Object>());
            holder=cacheInstances.get(name);
        }
        Object instance =holder.get();
        // 双重检查锁
        if(instance==null) {
            synchronized (holder) {
                instance = holder.get();
                if(instance==null) {
                    // 创建扩展示例
                    instance = createExtension(name);
                    // 设置示例到 holder 中
                    holder.set(instance);
                }
            }
        }
        return (T) instance;
    }

    private T createExtension(String name) {
        // 从配置文件中加载所有的拓展类，可得到“配置项名称”到“配置类”的映射关系表
        Class<?> clazz = getExtensionClasses().get(name);
        if (clazz==null) {
            throw new RuntimeException("No such extension of name " + name);
        }
        // 这段代码保证了扩展类只会被构造一次，也就是单例的.
        T instance = (T) EXTENSION_INSTANCES.get(clazz);
        if(instance==null) {
            try {
                EXTENSION_INSTANCES.putIfAbsent(clazz, clazz.newInstance());
                instance = (T) EXTENSION_INSTANCES.get(clazz);
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        }
        return instance;
    }

    /**
     * 获取所有的扩展类
     */
    private Map<String, Class<?>> getExtensionClasses() {
        // 从缓存中获取已加载的扩展类
        Map<String, Class<?>> classes = cachedClasses.get();
        // 双重检查
        if(classes==null) {
            synchronized (cachedClasses) {
                classes = cachedClasses.get();
                if (classes==null) {
                    // 加载扩展类
                    classes=loadExtensionClasses();
                    cachedClasses.set(classes);
                }
            }
        }
        return classes;
    }

    /**
     * 加载扩展类<p>
     * 目前只有一种存放的策略，读取 META-INF/extensions<br>
     * 1. 对 SPI 注解进行解析。注意在我的SPI 中没有任何属性，只是标志的作用，所以不必解析 SPI ，交由 {@link ExtensionLoader#getExtensionLoader}解析即可<br>
     * 2. 加载指定文件夹中的配置文件
     */
    private Map<String, Class<?>> loadExtensionClasses() {
        Map<String, Class<?>> extensionClasses = new HashMap<>();
        loadDirectory(extensionClasses);
        return extensionClasses;
    }

    private void loadDirectory(Map<String, Class<?>> extensionClasses) {
        String fileName = ExtensionLoader.SERVICE_DIRECTORY + type.getName();
        // Enumeration 在 Properties 中有应用，类似于 Iterator 迭代器
        try {
            Enumeration<URL> urls;
            ClassLoader classLoader = ExtensionLoader.class.getClassLoader();
            // 通过 classLoader 获取所有资源连接
            urls = classLoader.getResources(fileName);
            if(urls!=null) {
                while (urls.hasMoreElements()) {
                    URL resourceUrl = urls.nextElement();
                    loadResource(extensionClasses, resourceUrl, classLoader);
                }
            }
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    /**
     * 加载相关资源
     * 扩展文件中的算法解析
     */
    private void loadResource(Map<String, Class<?>> extensionClasses, URL resourceURL, ClassLoader classLoader) {
        try(BufferedReader reader = new BufferedReader(new InputStreamReader(resourceURL.openStream(), StandardCharsets.UTF_8))) {
            String line;
            // 按行读取配置内容
            while ((line = reader.readLine()) != null) {
                // 定位 # 字符
                final int ci = line.indexOf('#');
                if (ci >= 0) {
                    // 截取 # 之前的字符串，# 之后的内容为注释，需要忽略
                    line = line.substring(0, ci);
                }
                line = line.trim();
                if (line.length() > 0) {
                    try {
                        // 以等于号 = 为界，截取键与值
                        int i = line.indexOf('=');
                        String name = line.substring(0, i).trim();
                        String clazzName = line.substring(i + 1).trim();
                        if(name.length() > 0 && clazzName.length() > 0) {
                            Class<?> clazz = classLoader.loadClass(clazzName);
                            extensionClasses.put(name, clazz);
                        }
                    } catch (ClassNotFoundException e) {
                        log.error(e.getMessage());
                    }
                }
            }
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }
}
