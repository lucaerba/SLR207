import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

public class WordCountProcessor {
    private static final String FOLDER = "/cal/commoncrawl/";
    private static final String OUTPUT_FILE = "output_simple.txt";
    private static final String TIME_FILE = "time_simple.txt";

    public static void main(String[] args) {
        try {
            List<Path> files = Files.list(Paths.get(FOLDER))
                    .filter(path -> path.toString().endsWith(".warc.wet"))
                    .collect(Collectors.toList());

            int totalFiles = files.size();
            int increment = 1;  // Number of files to process in each iteration

            for (int nFiles = increment; nFiles <= totalFiles; nFiles += increment) {
                processFiles(files, nFiles);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void processFiles(List<Path> files, int nFiles) {
        long start = System.currentTimeMillis();
        Map<String, Integer> wordCountMap = new HashMap<>();

        System.out.println("Processing " + nFiles + " files out of " + files.size() + " total files");

        for (int i = 0; i < nFiles; i++) {
            Path file = files.get(i);
            try (BufferedReader reader = Files.newBufferedReader(file)) {
                System.out.println("Reading file: " + file.getFileName());

                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.replace("\n", " ").trim();
                    String[] words = line.split("\\s+");

                    for (String word : words) {
                        wordCountMap.put(word, wordCountMap.getOrDefault(word, 0) + 1);
                    }
                }
            } catch (IOException e) {
                System.out.println("Error processing file " + file + ": " + e.getMessage());
            }
        }

        List<Map.Entry<String, Integer>> sortedWordCountMap = wordCountMap.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .sorted(Map.Entry.comparingByValue())
                .collect(Collectors.toList());

        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(OUTPUT_FILE))) {
            for (Map.Entry<String, Integer> entry : sortedWordCountMap) {
                writer.write(entry.getKey() + " " + entry.getValue());
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        long end = System.currentTimeMillis();

        try (BufferedWriter timeWriter = Files.newBufferedWriter(Paths.get(TIME_FILE), StandardOpenOption.APPEND, StandardOpenOption.CREATE)) {
            timeWriter.write("n_files: " + nFiles + "\n");
            timeWriter.write("total time: " + (end - start) / 1000.0 + " seconds\n");
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Time taken for " + nFiles + " files: " + (end - start) + " seconds");
        System.out.println("Total number of unique words: " + sortedWordCountMap.size());
    }
}
