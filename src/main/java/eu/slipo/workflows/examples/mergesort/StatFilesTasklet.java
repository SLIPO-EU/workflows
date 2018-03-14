package eu.slipo.workflows.examples.mergesort;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.util.Assert;

public class StatFilesTasklet implements Tasklet
{
    private static final Logger logger = LoggerFactory.getLogger(StatFilesTasklet.class);
    
    private final List<Path> inputList;
    
    public StatFilesTasklet(List<Path> input)
    {
        Assert.notEmpty(input, "Expected a non empty collection of inputs");
        this.inputList = input;
    }

    private long countLines(Path inputPath) throws IOException
    {
        long count = 0;
        try (BufferedReader reader = Files.newBufferedReader(inputPath)) {
            String line = null;
            while ((line = reader.readLine()) != null)
                count++;
        }
        return count;
    }
    
    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext)
        throws Exception
    {
        StepExecution stepExecution = chunkContext.getStepContext().getStepExecution();
        ExecutionContext executionContext = stepExecution.getExecutionContext();
        
        LongStream.Builder sizes = LongStream.builder();
        
        for (Path input: this.inputList) {
            long size = Files.size(input);
            long lineCount = countLines(input);
            logger.info("Got input {}: {} lines, {} bytes", input, lineCount, size);
            sizes.add(size);
        }
        
        LongSummaryStatistics sizeStatistics = sizes.build().summaryStatistics();
        
        executionContext.put("input", inputList);
        
        executionContext.put("size.max", sizeStatistics.getMax());
        executionContext.put("size.min", sizeStatistics.getMin());
        executionContext.put("size.average", sizeStatistics.getAverage());
        executionContext.put("size", sizeStatistics.getSum());
        executionContext.put("count", sizeStatistics.getCount());
        
        return null;
    }
}