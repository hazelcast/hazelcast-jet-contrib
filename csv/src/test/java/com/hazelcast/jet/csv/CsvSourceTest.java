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

import com.hazelcast.jet.IListJet;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.util.UuidUtil;
import org.junit.Test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

public class CsvSourceTest {

    @Test
    public void csv_source_read_and_maps_files() throws IOException {
        // Given
        JetInstance jet = Jet.newJetInstance();

        File directory = createTempDirectory();
        File file1 = new File(directory, randomName());
        appendToFile(file1,
                "greeting,secondPart",
                "hello1,world1",
                "hello,there"
        );

        File file2 = new File(directory, randomName());
        appendToFile(file2,
                "greeting,secondPart",
                "hello2,world2",
                "General Kenobi,You are bold one"
        );

        HelloClass[] expected = {
                new HelloClass("hello", "there"),
                new HelloClass("hello1", "world1"),
                new HelloClass("hello2", "world2"),
                new HelloClass("General Kenobi", "You are bold one")
        };

        // When
        BatchSource<HelloClass> source = CsvSource.csv(HelloClass.class, directory.getPath(), "*");

        IListJet<HelloClass> resultList = jet.getList("resultList");
        Sink<HelloClass> resultListSink = Sinks.list(resultList);

        // Then
        Pipeline p = Pipeline.create()
                .drawFrom(source)
                .drainTo(resultListSink)
                .getPipeline();
        jet.newJob(p).join();

        HelloClass[] result = resultList.toArray(new HelloClass[0]);

        assertThat(result).containsExactlyInAnyOrder(expected);
    }

    @SuppressWarnings({ "unused", "WeakerAccess" }) // all getters and setters are public for SimpleFlatMapper
    public static class HelloClass implements Serializable {
        private String greeting;
        private String secondPart;

        public HelloClass() {}
        public HelloClass(String greeting, String secondPart) {
            this.greeting = greeting;
            this.secondPart = secondPart;
        }

        public String getGreeting() {
            return this.greeting;
        }

        public String getSecondPart() {
            return this.secondPart;
        }

        public void setGreeting(String greeting) {
            this.greeting = greeting;
        }

        public void setSecondPart(String secondPart) {
            this.secondPart = secondPart;
        }

        public boolean equals(final Object o) {
            if (o == this) return true;
            if (!(o instanceof HelloClass)) return false;
            final HelloClass other = (HelloClass) o;

            return Objects.equals(this.greeting, other.greeting)
                    && Objects.equals(this.secondPart, other.secondPart);
        }
        @Override
        public int hashCode() {
            return Objects.hash(greeting, secondPart);
        }
        public String toString() {
            return "HelloClass(greeting=" + getGreeting() + ", secondPart=" + getSecondPart() + ")";
        }
    }

    private static File createTempDirectory() throws IOException {
        Path directory = Files.createTempDirectory("jet-test-temp");
        File file = directory.toFile();
        file.deleteOnExit();
        return file;
    }

    private static String randomName() {
        return UuidUtil.newUnsecureUuidString();
    }

    private static void appendToFile(File file, String... lines) throws IOException {
        try (PrintWriter writer = new PrintWriter(new FileOutputStream(file, true))) {
            for (String payload : lines) {
                writer.write(payload + '\n');
            }
        }
    }
}
