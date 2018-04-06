package eu.slipo.workflows.examples.frequent_itemsets;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Maps;

class FrequencyCounter
{
    private static final Logger logger = LoggerFactory.getLogger(FrequencyCounter.class);
    
    private static final Function<Object, MutableInt> counterFactory = x -> new MutableInt(0); 
    
    /**
     * The input file containing transactions with items
     */
    private final Path inputPath;
    
    public FrequencyCounter(Path inputPath)
    {
        Assert.notNull(inputPath, "An input file is required");
        this.inputPath = inputPath;
    }

    /**
     * Count frequencies of items (singleton itemsets) 
     */
    Result countSingletonFrequencies() throws IOException
    {
        logger.info("Counting singleton frequencies: {}", inputPath);
        
        final Map<Integer, MutableInt> freq = new HashMap<>();
        int numberOfTransactions = 0;
        try (BufferedReader reader = Files.newBufferedReader(inputPath)) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                final Set<Integer> items = Arrays.stream(line.split("\\s+"))
                    .collect(Collectors.mapping(Integer::parseInt, Collectors.toSet()));
                for (final Integer i: items)
                    freq.computeIfAbsent(i, counterFactory).increment();
                numberOfTransactions++;
            }
        }
        
        final Map<NavigableSet<Integer>, Integer> freqMap = freq.entrySet()
            .stream()
            .collect(Collectors.toMap(
                e -> ImmutableSortedSet.of(e.getKey()), 
                e -> e.getValue().toInteger()));
        
        logger.info("Processed {} lines of {}", numberOfTransactions, inputPath);
        
        return new Result(numberOfTransactions, 1, freqMap);
    }
    
    /**
     * Count frequencies for itemsets adding 1 item to sets of existing result
     * 
     * @param result1 The result of a previous counting phase (frequencies of itemsets with
     *   a cardinality of k).
     */
    Result countFrequencies(Result result1) throws IOException
    {
        final int k = result1.cardinality();
        logger.info("Counting frequencies for k={}: {}", k + 1, inputPath);
        
        final TreeSet<Integer> candidates = result1.freqMap().keySet()
            .stream()
            .flatMap(Set::stream)
            .collect(Collectors.toCollection(TreeSet::new));
        
        final Map<NavigableSet<Integer>, MutableInt> freqMap = new HashMap<>();
        int numberOfTransactions = 0;
        try (BufferedReader reader = Files.newBufferedReader(inputPath)) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                final Set<Integer> transaction = Arrays.stream(line.split("\\s+"))
                    .collect(Collectors.mapping(Integer::parseInt, Collectors.toSet()));
                for (NavigableSet<Integer> itemset1: result1.freqMap().keySet()) {
                    if (!transaction.containsAll(itemset1))
                        continue;
                    for (Integer y: candidates.tailSet(itemset1.last(), false)) {
                        if (!transaction.contains(y))
                            continue;
                        NavigableSet<Integer> itemset = new TreeSet<>(itemset1);
                        itemset.add(y);
                        freqMap.computeIfAbsent(itemset, counterFactory).increment();
                    }
                }
                numberOfTransactions++;
            }
        }
        
        return new Result(
            numberOfTransactions, 
            k + 1, 
            Maps.transformValues(freqMap, MutableInt::toInteger));
    }
    
    static Result mergeResults(List<Result> partialResults, double thresholdFrequency)
    {
        Assert.isTrue(thresholdFrequency > 0 && thresholdFrequency < 1,
            "A threshold frequency is given as a relative frequency in (0,1)");
        
        final int numParts = partialResults.size();
        Assert.isTrue(numParts > 1, "Expected at least 2 partial results ");
        
        final int k = partialResults.get(0).cardinality();
        Assert.isTrue(partialResults.stream().allMatch(r -> r.cardinality() == k), 
            "Expected all partial results to contain itemsets of same cardinality");
        
        final Map<NavigableSet<Integer>, MutableInt> aggregateFreqMap = new HashMap<>();        
        for (int p = 0; p < numParts; ++p) {
            partialResults.get(p).freqMap().forEach(
                (set, f) -> aggregateFreqMap.computeIfAbsent(set, counterFactory).add(f));
        }
        
        final int size = partialResults.stream().mapToInt(Result::size).sum();
        final int threshold = Double.valueOf(thresholdFrequency * size).intValue();
        
        Map<NavigableSet<Integer>, Integer> freqMap = aggregateFreqMap.entrySet()
            .stream()
            .filter(e -> e.getValue().intValue() > threshold)
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toInteger()));
        
        return new Result(size, k, freqMap);
    }
}
