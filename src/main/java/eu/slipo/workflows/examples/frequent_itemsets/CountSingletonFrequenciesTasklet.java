package eu.slipo.workflows.examples.frequent_itemsets;

import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;

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
 * A tasklet that counts frequencies of singleton itemsets.
 */
class CountSingletonFrequenciesTasklet implements Tasklet
{
    private static final Logger logger = LoggerFactory.getLogger(CountSingletonFrequenciesTasklet.class);
    
    /**
     * The input file containing transactions with items
     */
    private final Path inputPath;
    
    private final Path outputDir;
    
    private final String outputName;
    
    public CountSingletonFrequenciesTasklet( Path outputDir, String outputName, Path inputPath)
    {
        Assert.isTrue(inputPath != null && inputPath.isAbsolute(), 
            "Expected input file (transactions file) as an absolute file path");
        Assert.isTrue(outputDir != null && outputDir.isAbsolute(), 
            "Expected an absolute path for output directory");
        Assert.isTrue(!StringUtils.isEmpty(outputName), "An output name is required");
        this.inputPath = inputPath;
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
        
        // Count frequencies and write result to file
        
        final Result result = (new FrequencyCounter(inputPath)).countSingletonFrequencies();
        
        final Path outputPath = outputDir.resolve(outputName);
        result.writeToFile(outputPath);
        
        // Update execution context
        
        executionContext.put("outputDir", outputDir.toString());
        executionContext.put("outputName", outputName);
        
        return null;
    } 

}
