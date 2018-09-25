package com.rtbhouse.kafka.workers.integration.utils;

import static java.nio.file.Files.walkFileTree;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public final class FileUtils {

    public static void deleteRecursively(final File dir) throws IOException {
        walkFileTree(dir.toPath(),
                new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(final Path file,
                            final BasicFileAttributes attrs) throws IOException {
                        Files.delete(file);
                        return FileVisitResult.CONTINUE;
                    };

                    @Override
                    public FileVisitResult postVisitDirectory(final Path dir,
                            final IOException exc) throws IOException {
                        Files.delete(dir);
                        return FileVisitResult.CONTINUE;
                    };

                });
    }

}
