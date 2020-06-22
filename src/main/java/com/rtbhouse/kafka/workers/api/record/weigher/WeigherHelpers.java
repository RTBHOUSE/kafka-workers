package com.rtbhouse.kafka.workers.api.record.weigher;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.reflect.Modifier.isStatic;

import java.lang.reflect.Field;
import java.util.Map;

import org.apache.commons.lang3.reflect.FieldUtils;

public class WeigherHelpers {

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

    public static int estimateInstanceSize(Class<?> clazz) {
        checkState(!clazz.isPrimitive());
        int shallowSize  = headerSize(clazz);
        int fieldsInstanceSize = 0;
        for (Field field : FieldUtils.getAllFieldsList(clazz)) {
            if (!isStatic(field.getModifiers())) {
                Class<?> fieldType = field.getType();
                int fieldSize = fieldSize(fieldType);
                shallowSize += fieldSize;
                if (!fieldType.isPrimitive() && !fieldType.isEnum()) {
                    fieldsInstanceSize += estimateInstanceSize(fieldType);
                }
            }
        }

        int padding = padding(shallowSize);
        return shallowSize + padding + fieldsInstanceSize;
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
