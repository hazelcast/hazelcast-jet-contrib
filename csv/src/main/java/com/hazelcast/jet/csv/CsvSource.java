/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.csv;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.pipeline.BatchSource;
import org.simpleflatmapper.csv.CsvMapper;
import org.simpleflatmapper.csv.CsvMapperFactory;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.pipeline.Sources.batchFromProcessor;

/**
 * Reads CSV files and maps lines to Java Beans.
 *
 * Please note, that Java Bean should be a public class, in order to work correctly with SimpleFlatMapper.
 */
public final class CsvSource<R> extends AbstractProcessor {

    private final Path directory;
    private final String glob;
    private final boolean sharedFileSystem;

    private int processorIndex;
    private int parallelism;
    private DirectoryStream<Path> directoryStream;
    private Traverser<R> outputTraverser;
    private Stream<R> currentStream;

    private final Class<R> itemClass;

    private CsvSource(
            @Nonnull Class<R> itemClass,
            @Nonnull String directory,
            @Nonnull String glob,
            boolean sharedFileSystem
    ) {
        this.itemClass = itemClass;
        this.directory = Paths.get(directory);
        this.glob = glob;
        this.sharedFileSystem = sharedFileSystem;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    protected void init(@Nonnull Context context) throws IOException {
        processorIndex = sharedFileSystem ? context.globalProcessorIndex() : context.localProcessorIndex();
        parallelism = sharedFileSystem ? context.totalParallelism() : context.localParallelism();

        directoryStream = Files.newDirectoryStream(directory, glob);
        outputTraverser = Traversers.traverseIterator(directoryStream.iterator())
                .filter(this::shouldProcessEvent)
                .flatMap(this::processFile);
    }

    @Override
    public boolean complete() {
        return emitFromTraverser(outputTraverser);
    }

    private boolean shouldProcessEvent(Path file) {
        if (Files.isDirectory(file)) {
            return false;
        }
        int hashCode = file.hashCode();
        return ((hashCode & Integer.MAX_VALUE) % parallelism) == processorIndex;
    }

    private Traverser<R> processFile(Path file) {
        if (getLogger().isFinestEnabled()) {
            getLogger().finest("Processing file " + file);
        }
        assert currentStream == null : "currentStream != null";

        currentStream = createCsvMapper(file);
        return traverseStream(currentStream)
                .onFirstNull(() -> {
                    currentStream.close();
                    currentStream = null;
                });
    }

    @SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
    private Stream<R> createCsvMapper(Path file) {
        try {
            FileReader fileReader = new FileReader(file.toFile());
            CsvMapperFactory csvMapperFactory = CsvMapperFactory.newInstance()
                    .defaultDateFormat("yyyy-MM-dd");
            CsvMapper<R> mapper = csvMapperFactory.newMapper(itemClass);

            return mapper.stream(new BufferedReader(fileReader));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void close() throws IOException {
        IOException ex = null;
        if (directoryStream != null) {
            try {
                directoryStream.close();
            } catch (IOException e) {
                ex = e;
            }
        }
        if (currentStream != null) {
            currentStream.close();
        }
        if (ex != null) {
            throw ex;
        }
    }

    private static <W> ProcessorMetaSupplier metaSupplier(
            @Nonnull Class<W> itemClass,
            @Nonnull String directory,
            @Nonnull String glob,
            boolean sharedFileSystem
    ) {
        return ProcessorMetaSupplier.of(() -> new CsvSource<>(
                        itemClass, directory, glob, sharedFileSystem),
                2);
    }

    /**
     * Creates CSV File source, that reads files from given {@code directory} and {@code glob}, then maps it to Java Bean of type {@code itemClass}.
     * @param itemClass class of the item in CSV file
     * @param directory directory from which files will be read
     * @param glob regular expression to select files to be read
     * @param sharedFileSystem true if files are on shared file system. Default value is false.
     * @return BatchSource that reads CSV files into Java Bean objects.
     */
    @SuppressWarnings({ "BooleanParameter", "WeakerAccess" })
    public static <E> BatchSource<E> csv(Class<E> itemClass, String directory, String glob, boolean sharedFileSystem) {
        return batchFromProcessor("csvSource(" + new File(directory, glob) + ')',
                metaSupplier(itemClass, directory, glob, sharedFileSystem)
        );
    }

    public static <E> BatchSource<E> csv(Class<E> itemClass, String directory, String glob) {
        return csv(itemClass, directory, glob, false);
    }
}
