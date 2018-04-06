package eu.slipo.workflows.examples.frequent_itemsets;

import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * A tasklet to merge partial results for itemsets of same cardinality
 */
public class MergeFilesTasklet implements Tasklet
{
    private static final Logger logger = LoggerFactory.getLogger(MergeFilesTasklet.class);
    
    private final List<Path> resultPaths;
    
    private final double thresholdFrequency;
    
    private final Path outputDir;
    
    private final String outputName;
    
    public MergeFilesTasklet(
        Path outputDir, String outputName, double thresholdFrequency, Path... resultPaths)
    {
        Assert.isTrue(outputDir != null && outputDir.isAbsolute(), 
            "Expected an absolute path for output directory");
        Assert.isTrue(!StringUtils.isEmpty(outputName), "An output name is required");
        Assert.isTrue(resultPaths.length > 1, 
            "Expected at least 2 partial results to merge");
        Assert.isTrue(thresholdFrequency > 0 && thresholdFrequency < 1,
            "A threshold frequency is expected as a relative frequency in (0,1)");
        this.resultPaths = new ArrayList<>(Arrays.asList(resultPaths));
        this.thresholdFrequency = thresholdFrequency;
        this.outputDir = outputDir;
        this.outputName = outputName;
    }
    
    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext)
        throws Exception
    {
        StepExecution stepExecution = chunkContext.getStepContext().getStepExecution();
        ExecutionContext executionContext = stepExecution.getExecutionContext();
        
        // Create output directory
        try {
            Files.createDirectory(outputDir);
        } catch (FileAlreadyExistsException ex) {
            // no-op
        }
        
        // Load partial results and merge
        
        List<Result> partialResults = new ArrayList<>();
        for (Path path: resultPaths)
            partialResults.add(Result.loadFromFile(path));
        
        final Result result = FrequencyCounter.mergeResults(partialResults, thresholdFrequency);
        
        final Path outputPath = outputDir.resolve(outputName);
        result.writeToFile(outputPath);
        
        logger.info("Merged {} partial results into {}", partialResults.size(), outputPath);
        
        // Update execution context
        
        executionContext.put("outputDir", outputDir.toString());
        executionContext.put("outputName", outputName);
        
        return null;
    }

}
