package com.rtbhouse.kafka.workers.impl.record.weigher;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.reflect.Modifier.isStatic;

import java.lang.reflect.Field;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WeigherHelpers {

    private static final Logger logger = LoggerFactory.getLogger(WeigherHelpers.class);

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

    public static int shallowSize(Class<?> clazz, String path) {
        checkState(!clazz.isPrimitive());
        int header = clazz.isArray() ? 16 + 4 : 16;
        int fieldsSize  = 0;
        int fieldsShallowSize = 0;
        for (Field field : clazz.getDeclaredFields()) {
            if (!isStatic(field.getModifiers())) {
                String fieldName = field.getName();
                Class<?> fieldType = field.getType();
                String fieldPath = path + String.format(".%s(%s)", fieldName, fieldType.getSimpleName());
                int fieldSize = fieldSize(fieldType);
                logger.debug("{} fieldSize = {}", fieldPath, fieldSize);
                fieldsSize += fieldSize;
                if (!fieldType.isPrimitive()) {
                    fieldsShallowSize += shallowSize(fieldType, fieldPath);
                }
            }
        }

        int padding = padding(header + fieldsSize);
        int totalSize = header + fieldsSize + padding + fieldsShallowSize;
        logger.debug("{}: {} + {} + {} + {} = {}", path, header, fieldsSize, padding, fieldsShallowSize, totalSize);
        return totalSize;
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
            //TODO: for heap >32G 8 bytes
            return 4;
        }
    }
}
