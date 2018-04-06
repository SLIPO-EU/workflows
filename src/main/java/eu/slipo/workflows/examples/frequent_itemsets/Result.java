package eu.slipo.workflows.examples.frequent_itemsets;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.mutable.MutableInt;
import org.springframework.data.util.Pair;
import org.springframework.util.Assert;

class Result
{
    private static final Pattern linePattern = 
        Pattern.compile("^(\\d+(\\s+\\d+)*)\\s+\\Q(\\E(\\d+)\\Q)\\E$");
    
    private static final Function<Object, MutableInt> counterFactory = x -> new MutableInt(0);
    
    private final int size;
    
    private final int cardinality;
    
    private final Map<NavigableSet<Integer>, Integer> freqMap;

    Result(int size, int cardinality, Map<NavigableSet<Integer>, Integer> freqMap)
    {
        Assert.isTrue(size > 0, "Expected size > 0");
        Assert.isTrue(cardinality > 0, "Expected cardinality > 0");
        Assert.notNull(freqMap, "Expected a non-null frequency map");
        this.size = size;
        this.cardinality = cardinality;
        this.freqMap = freqMap;
    }
    
    int size()
    {
        return size;
    }
    
    int cardinality()
    {
        return cardinality;
    }
    
    Map<NavigableSet<Integer>, Integer> freqMap()
    {
        return freqMap;
    }
    
    void writeToFile(Path outputPath) throws IOException
    {
        Assert.notNull(outputPath, "Expected an output path");
        
        final Comparator<Map.Entry<NavigableSet<Integer>, Integer>> frequencyComparator = 
            Comparator.comparing(Map.Entry::getValue);
        
        final Iterator<Pair<NavigableSet<Integer>, Integer>> recordIterator = freqMap.entrySet()
            .stream()
            .sorted(frequencyComparator.reversed())
            .map(e -> Pair.of(e.getKey(), e.getValue()))
            .iterator();
        
        // Write results to file
        
        try (BufferedWriter writer = Files.newBufferedWriter(outputPath)) {
            writer.write(String.format("%d %d%n%n", size, cardinality));
            while (recordIterator.hasNext()) {
                final Pair<NavigableSet<Integer>, Integer> record = recordIterator.next();
                final Set<Integer> itemset = record.getFirst();
                final String itemsetAsString = itemset.stream()
                    .collect(Collectors.mapping(Number::toString, Collectors.joining(" ")));
                final Integer f = record.getSecond();
                writer.write(String.format("%s (%d)%n", itemsetAsString, f));
            }
        }
    }
    
    static Result loadFromFile(Path resultPath) throws IOException
    {
        Assert.notNull(resultPath, "Expected an input path");
        
        int size = -1, cardinality = -1;
        Map<NavigableSet<Integer>, Integer> freqMap = null;
        
        try (BufferedReader reader = Files.newBufferedReader(resultPath)) {
            String headerLine = reader.readLine();
            String headerParts[] = headerLine.split("\\s+");
            size = Integer.parseInt(headerParts[0]);
            cardinality = Integer.parseInt(headerParts[1]);
            reader.readLine(); // expect an empty line
            
            freqMap = new HashMap<>();
            String line = null;
            while ((line = reader.readLine()) != null) {
                Matcher lineMatcher = linePattern.matcher(line);
                if (!lineMatcher.matches())
                    throw new IllegalStateException("Encountered a line of unexpected format");
                TreeSet<Integer> itemset = Arrays.stream(lineMatcher.group(1).split("\\s+"))
                    .collect(Collectors.mapping(Integer::parseInt, Collectors.toCollection(TreeSet::new)));
                Integer f = Integer.valueOf(lineMatcher.group(3));
                freqMap.put(itemset, f);
            }
        }
        
        return new Result(size, cardinality, freqMap);
    }
}
