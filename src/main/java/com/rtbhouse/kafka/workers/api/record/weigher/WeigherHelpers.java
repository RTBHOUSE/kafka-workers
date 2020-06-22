package com.rtbhouse.kafka.workers.api.record.weigher;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.reflect.Modifier.isStatic;

import java.lang.reflect.Field;
import java.util.Map;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WeigherHelpers {

    private static final Logger logger = LoggerFactory.getLogger(WeigherHelpers.class);

    private static final long HEAP_MAX_SIZE = Runtime.getRuntime().maxMemory();

    private static final long LARGE_HEAP_SIZE = 32L * 1024 * 1024 * 1024;

    private static final Map<Class<?>, Integer> TYPE_SIZES = Map.of(
            Boolean.TYPE, 1,
            Byte.TYPE, Byte.BYTES,
            Character.TYPE, Character.BYTES,
            Short.TYPE, Short.BYTES,
            Integer.TYPE, Integer.BYTES,
            Long.TYPE, Long.BYTES,
            Float.TYPE, Float.BYTES,
            Double.TYPE, Double.BYTES
    );

    //TODO: remove path arg and debug
    public static int estimateInstanceSize(Class<?> clazz, String path) {
        checkState(!clazz.isPrimitive());
        int shallowSize  = headerSize(clazz);
        int fieldsInstanceSize = 0;
        for (Field field : FieldUtils.getAllFieldsList(clazz)) {
            if (!isStatic(field.getModifiers())) {
                String fieldName = field.getName();
                Class<?> fieldType = field.getType();
                String isEnumPrefix = fieldType.isEnum() ? "enum." : "";
                String fieldPath = path + String.format(".%s(%s%s)", fieldName, isEnumPrefix, fieldType.getSimpleName());
                int fieldSize = fieldSize(fieldType);
                logger.debug("{} fieldSize = {}", fieldPath, fieldSize);
                shallowSize += fieldSize;
                if (!fieldType.isPrimitive() && !fieldType.isEnum()) {
                    fieldsInstanceSize += estimateInstanceSize(fieldType, fieldPath);
                }
            }
        }

        int padding = padding(shallowSize);
        int totalSize = shallowSize + padding + fieldsInstanceSize;
        logger.debug("{}: {} + {} + {} = {}", path, shallowSize, padding, fieldsInstanceSize, totalSize);
        return totalSize;
    }

    private static int headerSize(Class<?> clazz) {
        return (clazz.isArray() && isHeapLarge()) ? 24 : 16;
    }

    private static boolean isHeapLarge() {
        return HEAP_MAX_SIZE >= LARGE_HEAP_SIZE;
    }

    private static int padding(int size) {
        final int m = 8;
        final int r = size % m;
        return r == 0 ? 0 : m - r;
    }

    private static int fieldSize(Class<?> clazz) {
        if (clazz.isPrimitive()) {
            return TYPE_SIZES.get(clazz);
        } else {
            return isHeapLarge() ? 8 : 4;
        }
    }

    static int paddedSize(int size) {
        return size + padding(size);
    }
}
