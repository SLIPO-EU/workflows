package eu.slipo.workflows.examples.frequent_itemsets;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Collectors.mapping;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;

import com.google.common.collect.Lists;

import eu.slipo.workflows.Workflow;
import eu.slipo.workflows.WorkflowBuilderFactory;


@Configuration("frequent_itemsets.workflows")
public class FrequentItemsetsWorkflows
{
    public static final String RESULT_FILENAME = "result.dat";
    
    private static final String counterNameFormat = "counter-p%d-%d";
    
    private static final String mergerNameFormat = "merger-p%d";
    
    private static final String partNameFormat = "part%d.dat";
        
    @Autowired
    private WorkflowBuilderFactory workflowBuilderFactory;
    
    @Autowired
    @Qualifier("frequent_itemsets.splitFile.flow")
    private Flow splitFileFlow;
    
    @Autowired
    @Qualifier("frequent_itemsets.mergeFiles.flow")
    private Flow mergeFilesFlow;
    
    @Autowired
    @Qualifier("frequent_itemsets.concatenateFiles.flow")
    private Flow concatenateFilesFlow;
    
    @Autowired
    @Qualifier("frequent_itemsets.countSingletonFrequencies.flow")
    private Flow countSingletonFrequenciesFlow;
    
    @Autowired
    @Qualifier("frequent_itemsets.countFrequencies.flow")
    private Flow countFrequenciesFlow;
    
    public class Builder
    {
        private final UUID workflowId;
        
        private final Path inputPath;
        
        private int numParts = 2;
        
        private double thresholdFrequency = 0.50;
        
        private int maxSize = 3;
        
        private Builder(UUID workflowId, Path inputPath)
        {
            this.workflowId = workflowId;
            this.inputPath = inputPath;
        }
        
        /**
         * Set the number of parts to split input to
         * @param n
         */
        public Builder numParts(int n)
        {
            Assert.isTrue(n > 1, "The number of parts must be > 1");
            this.numParts = n;
            return this;
        }
        
        public Builder thresholdFrequency(double f)
        {
            Assert.isTrue(f > 0 && f < 1, 
                "The threshold must be given as a relative frequency in range (0,1)");
            this.thresholdFrequency = f;
            return this;
        }
        
        /**
         * Set the maximum size (i.e cardinality) for itemsets we are interested into
         * @param k The size; must be <tt> 0 < k < 10 </tt>
         */
        public Builder maxSize(int k)
        {
            Assert.isTrue(k > 0 && k < 10, "The size k must be a positive integer (< 10)");
            this.maxSize = k;
            return this;
        }
        
        /** 
         * Build a workflow to find frequent itemsets in given input.
         */
        public Workflow build()
        {
            return buildWorkflow();
        }
        
        private Workflow buildWorkflow()
        {
            final Workflow.Builder workflowBuilder = workflowBuilderFactory.get(workflowId);
            
            // Split input
            
            List<String> splitterParts = IntStream.rangeClosed(1, numParts).boxed()
                .collect(mapping(i -> String.format(partNameFormat, i), toList()));
                
            workflowBuilder.job(b -> b
                .name("splitter")
                .flow(splitFileFlow)
                .input(inputPath)
                .parameters(p -> p.addString("outputPrefix", "part").addString("outputSuffix", ".dat")
                    .addLong("numParts", Long.valueOf(numParts)))
                .output(Lists.transform(splitterParts, Paths::get)));
            
            // Count singleton frequencies
            
            {
                List<String> counterNames = IntStream.rangeClosed(1, numParts).boxed()
                    .collect(mapping(i -> String.format(counterNameFormat, 1, i), toList()));
                
                String mergerName = String.format(mergerNameFormat, 1);
                
                IntStream.rangeClosed(1, numParts).forEach(i  -> {
                    workflowBuilder.job(b -> b
                        .name(String.format(counterNameFormat, 1, i))
                        .flow(countSingletonFrequenciesFlow)
                        .input("splitter", String.format(partNameFormat, i))
                        .parameters(p -> p.addString("outputName", "result.dat"))
                        .output("result.dat"));
                });

                workflowBuilder.job(b -> b
                    .name(mergerName)
                    .flow(mergeFilesFlow)
                    .input(counterNames, "result.dat")
                    .parameters(p -> p.addString("outputName", "result.dat")
                        .addDouble("thresholdFrequency", thresholdFrequency))
                    .output("result.dat"));
            }
            
            // Count frequencies of itemsets of size k = 2, 3, ...
            
            IntStream.rangeClosed(2, maxSize).forEach(k -> {
                
                List<String> counterNames = IntStream.rangeClosed(1, numParts).boxed()
                    .collect(mapping(i -> String.format(counterNameFormat, k, i), toList()));
                
                String mergerName = String.format(mergerNameFormat, k);
                
                IntStream.rangeClosed(1, numParts).forEach(i  -> {
                    workflowBuilder.job(b -> b
                        .name(String.format(counterNameFormat, k, i))
                        .flow(countFrequenciesFlow)
                        .input("splitter", String.format(partNameFormat, i))
                        .input(String.format(mergerNameFormat, k - 1), "result.dat", "result1")
                        .parameters(p -> p.addString("outputName", "result.dat"))
                        .output("result.dat"));
                });
                
                workflowBuilder.job(b -> b
                    .name(mergerName)
                    .flow(mergeFilesFlow)
                    .input(counterNames, "result.dat")
                    .parameters(p -> p.addString("outputName", "result.dat")
                        .addDouble("thresholdFrequency", thresholdFrequency))
                    .output("result.dat"));
            });
            
            List<String> mergerNames = IntStream.rangeClosed(1, maxSize).boxed()
                .collect(mapping(i -> String.format(mergerNameFormat, i), toList()));
            
            workflowBuilder.job(b -> b
                .name("cat")
                .flow(concatenateFilesFlow)
                .input(mergerNames, "result.dat")
                .parameters(p -> p.addString("outputName", "result.dat"))
                .output("result.dat"));
            
            workflowBuilder.output("cat", RESULT_FILENAME);
            
            // Done
            
            return  workflowBuilder.build();
        }
    }
    
    /**
     * Get a builder to assist building a workflow
     * 
     * @param id The workflow id
     * @param inputPath The input file. Each line represents a "transaction" i.e. a 
     *    set of items.
     * @return a builder for a workflow
     */
    public Builder getBuilder(UUID workflowId, Path inputPath)
    {
        Assert.notNull(workflowId, "A workflow id is required");
        Assert.isTrue(inputPath != null && inputPath.isAbsolute(), 
            "An input is expected (as an absolute path)");
        return this.new Builder(workflowId, inputPath);
    }


    
}
