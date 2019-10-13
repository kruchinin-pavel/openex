package org.openex.orderbook;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RunWith(Parameterized.class)
public class ConsoleProcessorTest {
    private final List<String> inputFiles;
    private final String[] expectedOutput;
    private final ConsoleProcessor processor;

    public ConsoleProcessorTest(List<String> inputFileNames, String expectedOutputFileName,
                                Supplier<ConsoleProcessor> processor) throws IOException {
        this.inputFiles = inputFileNames;
        this.expectedOutput = Files.readAllLines(Paths.get(expectedOutputFileName))
                .stream().map(String::trim).toArray(String[]::new);
        this.processor = processor.get();
    }

    @Parameterized.Parameters
    public static List<Object[]> data() {
        return Stream.of(
                (Supplier<ConsoleProcessor>) ConsoleProcessor::arrayListConsole,
                ConsoleProcessor::linkedListConsole,
                ConsoleProcessor::multiMapConsole)
                .flatMap(console -> Stream.of(
                        new Object[]{
                                Collections.singletonList("src/test/resources/test1.txt"),
                                "src/test/resources/test1_output_expected.txt",
                                console
                        },
                        new Object[]{
                                Collections.singletonList("src/test/resources/test2.txt"),
                                "src/test/resources/test2_output_expected.txt",
                                console
                        },
                        new Object[]{
                                Collections.singletonList("src/test/resources/test3.txt"),
                                "src/test/resources/test3_output_expected.txt",
                                console
                        },
                        new Object[]{
                                Collections.singletonList("src/test/resources/testLSE_4.2.3.1.txt"),
                                "src/test/resources/testLSE_4.2.3.1_exp.txt",
                                console
                        },
                        new Object[]{
                                Arrays.asList("src/test/resources/testLSE_4.2.3.1.txt",
                                        "src/test/resources/testLSE_4.2.3.2_incr.txt"),
                                "src/test/resources/testLSE_4.2.3.2_exp.txt",
                                console
                        },
                        new Object[]{
                                Arrays.asList("src/test/resources/testLSE_4.2.3.1.txt",
                                        "src/test/resources/testLSE_4.2.3.2_incr.txt",
                                        "src/test/resources/testLSE_4.2.3.2_2_incr.txt"),
                                "src/test/resources/testLSE_4.2.3.2_2_exp.txt",
                                console
                        },
                        new Object[]{
                                Arrays.asList("src/test/resources/testLSE_4.2.3.1.txt",
                                        "src/test/resources/testLSE_4.2.3.2_incr.txt",
                                        "src/test/resources/testLSE_4.2.3.2_2_incr.txt",
                                        "src/test/resources/testLSE_4.2.3.2_3_incr.txt"),
                                "src/test/resources/testLSE_4.2.3.2_3_exp.txt",
                                console
                        }))
                .collect(Collectors.toList());
    }

    @Test
    public void testCase() {
        inputFiles.forEach(inputFile -> {
            try {
                Files.readAllLines(Paths.get(inputFile))
                        .stream().map(String::trim)
                        .collect(Collectors.toList())
                        .forEach(processor::consume);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        List<String> output = new ArrayList<>();
        processor.print(v -> {
            output.add(v.trim());
            System.out.println(v);
        });
        Assert.assertArrayEquals(expectedOutput, output.toArray(new String[0]));
    }
}