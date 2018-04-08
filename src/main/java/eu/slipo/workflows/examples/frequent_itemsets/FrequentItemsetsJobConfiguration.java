package eu.slipo.workflows.examples.frequent_itemsets;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import eu.slipo.workflows.ExecutionContextPromotionListeners;
import eu.slipo.workflows.util.tasklet.ConcatenateFilesTasklet;
import eu.slipo.workflows.util.tasklet.SplitFileTasklet;

@Configuration("frequent_itemsets.jobConfiguration")
public class FrequentItemsetsJobConfiguration
{  
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
   
    @Autowired
    private Path jobDataDirectory;
        
    @PostConstruct
    private void setupDataDirectories() throws IOException
    {
        String dirNames[] = new String[] {
            "frequent_itemsets.splitFile",
            "frequent_itemsets.concatenateFiles",
            "frequent_itemsets.countSingletonFrequencies", 
            "frequent_itemsets.countFrequencies",
            "frequent_itemsets.mergeFiles"
        };
        
        for (String dirName: dirNames) 
        {
            try {
                Files.createDirectory(jobDataDirectory.resolve(dirName));
            } catch (FileAlreadyExistsException ex) {}
        }
    }
    
    @Bean("frequent_itemsets.contextPromotionListener")
    public StepExecutionListener contextListener()
    {
        return ExecutionContextPromotionListeners.fromKeys("outputDir");
    }
    
    private static final long RESULT_CACHE_MAX_SIZE = 1000L;
    
    @Bean("frequent_itemsets.resultCache")
    public LoadingCache<Path, Result> resultCache()
    {
        return CacheBuilder.newBuilder()
            .maximumSize(RESULT_CACHE_MAX_SIZE)
            .build(new CacheLoader<Path, Result> () {

                @Override
                public Result load(Path path) throws Exception
                {
                    return Result.loadFromFile(path);
                }
            });
    }
    
    @Bean("frequent_itemsets.splitFile.tasklet")
    @JobScope
    public Tasklet splitFileTasklet(
        @Value("#{jobParameters['input']}") String input,
        @Value("#{jobParameters['outputPrefix']?:'part'}") String outputPrefix,
        @Value("#{jobParameters['outputSuffix']?:'.dat'}") String outputSuffix,
        @Value("#{jobParameters['numParts']}") Number numParts,
        @Value("#{jobExecution.jobInstance.id}") Long jobId)
    {
        Path inputPath = Paths.get(input);
        Assert.isTrue(Files.exists(inputPath), "The input file does not exist");
        Path outputDir = jobDataDirectory.resolve(
            Paths.get("frequent_itemsets.splitFile", String.valueOf(jobId)));
        return new SplitFileTasklet(
            inputPath, outputDir, numParts.intValue(), outputPrefix, outputSuffix);
    }
    
    @Bean("frequent_itemsets.splitFile.step")
    public Step splitFileStep(
        @Qualifier("frequent_itemsets.splitFile.tasklet") Tasklet tasklet,
        @Qualifier("frequent_itemsets.contextPromotionListener") StepExecutionListener contextListener) 
        throws Exception
    {
        return stepBuilderFactory.get("frequent_itemsets.splitFile")
            .tasklet(tasklet).listener(contextListener)
            .build();
    }
    
    @Bean("frequent_itemsets.splitFile.flow")
    public Flow splitFileFlow(@Qualifier("frequent_itemsets.splitFile.step") Step step)
    {
        return new FlowBuilder<Flow>("frequent_itemsets.splitFile").start(step).end();
    }
    
    @Bean("frequent_itemsets.concatenateFiles.tasklet")
    @JobScope
    public Tasklet concatenateFilesTasklet(
        @Value("#{jobParameters['input']}") String input,
        @Value("#{jobParameters['outputName']}") String outputName,
        @Value("#{jobExecution.jobInstance.id}") Long jobId)
    {
        final byte[] separator = "\n".getBytes();
        
        List<Path> inputPaths = Arrays.stream(input.split(File.pathSeparator))
            .collect(Collectors.mapping(Paths::get, Collectors.toList()));

        Path outputDir = jobDataDirectory.resolve(
            Paths.get("frequent_itemsets.concatenateFiles", String.valueOf(jobId)));
        return new ConcatenateFilesTasklet(inputPaths, outputDir, outputName, separator);
    }

    @Bean("frequent_itemsets.concatenateFiles.step")
    public Step concatenateFilesStep(
        @Qualifier("frequent_itemsets.concatenateFiles.tasklet") Tasklet tasklet,
        @Qualifier("frequent_itemsets.contextPromotionListener") StepExecutionListener contextListener) 
        throws Exception
    {
        return stepBuilderFactory.get("frequent_itemsets.concatenateFiles")
            .tasklet(tasklet).listener(contextListener)
            .build();
    }
    
    @Bean("frequent_itemsets.concatenateFiles.flow")
    public Flow concatenateFilesFlow(@Qualifier("frequent_itemsets.concatenateFiles.step") Step step)
    {
        return new FlowBuilder<Flow>("frequent_itemsets.concatenateFiles").start(step).end();
    }
    
    @Bean("frequent_itemsets.mergeFiles.tasklet")
    @JobScope
    public Tasklet mergeFilesTasklet(
        @Value("#{jobParameters['input']}") String resultFiles,
        @Value("#{jobParameters['outputName']}") String outputName,
        @Value("#{jobParameters['thresholdFrequency']}") double thresholdFrequency,
        @Value("#{jobExecution.jobInstance.id}") Long jobId,
        @Qualifier("frequent_itemsets.resultCache") LoadingCache<Path, Result> resultCache)
    {
        Assert.isTrue(!StringUtils.isEmpty(outputName), "An output name is required");
        Assert.isTrue(thresholdFrequency > 0 && thresholdFrequency < 1,
            "A threshold frequency is expected as a relative frequency in (0,1)");
        List<Path> resultPaths = Arrays.stream(resultFiles.split(File.pathSeparator))
            .filter(p -> !p.isEmpty())
            .collect(Collectors.mapping(Paths::get, Collectors.toList()));
        Assert.isTrue(resultPaths.size() > 1, 
            "Expected at least 2 partial results to merge");
        
        Path outputDir = jobDataDirectory.resolve(
            Paths.get("frequent_itemsets.mergeFiles", String.valueOf(jobId)));
        
        return new Tasklet() 
        {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) 
                throws Exception
            {                
                StepExecution stepExecution = chunkContext.getStepContext().getStepExecution();
                ExecutionContext executionContext = stepExecution.getExecutionContext();
                
                // Create output directory
                try {
                    Files.createDirectory(outputDir);
                } catch (FileAlreadyExistsException ex) {}
                
                // Load partial results and merge
                List<Result> partialResults = new ArrayList<>();
                for (Path path: resultPaths)
                    partialResults.add(resultCache.get(path));
                
                final Result result = FrequencyCounter.mergeResults(partialResults, thresholdFrequency);
                
                final Path outputPath = outputDir.resolve(outputName);
                result.writeToFile(outputPath);
                
                // Update execution context
                executionContext.put("outputDir", outputDir.toString());
                
                return null;
            }
        };
    }
    
    @Bean("frequent_itemsets.mergeFiles.step")
    public Step mergeFilesStep(
        @Qualifier("frequent_itemsets.mergeFiles.tasklet") Tasklet tasklet,
        @Qualifier("frequent_itemsets.contextPromotionListener") StepExecutionListener contextListener)
        throws Exception
    {
        return stepBuilderFactory.get("frequent_itemsets.mergeFiles")
            .tasklet(tasklet).listener(contextListener)
            .build();
    }

    @Bean("frequent_itemsets.mergeFiles.flow")
    public Flow mergeFilesFlow(@Qualifier("frequent_itemsets.mergeFiles.step") Step step)
    {
        return new FlowBuilder<Flow>("frequent_itemsets.mergeFiles").start(step).end();
    }
    
    @Bean("frequent_itemsets.countSingletonFrequencies.tasklet")
    @JobScope
    public Tasklet countSingletonFrequenciesTasklet(
        @Value("#{jobParameters['input']}") String input,
        @Value("#{jobParameters['outputName']}") String outputName,
        @Value("#{jobExecution.jobInstance.id}") Long jobId)
    {
        Path inputPath = Paths.get(input);
        Assert.isTrue(inputPath != null && inputPath.isAbsolute(), 
            "Expected input file (transactions file) as an absolute file path");
        Assert.isTrue(!StringUtils.isEmpty(outputName), "An output name is required");
        
        Path outputDir = jobDataDirectory.resolve(
            Paths.get("frequent_itemsets.countSingletonFrequencies", String.valueOf(jobId)));
        
        return new Tasklet()
        {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext)
                throws Exception
            {
                StepExecution stepExecution = chunkContext.getStepContext().getStepExecution();
                ExecutionContext executionContext = stepExecution.getExecutionContext();
                
                // Create output directory
                try {
                    Files.createDirectory(outputDir);
                } catch (FileAlreadyExistsException ex) {}
                
                // Count frequencies and write result to file
                
                final Result result = (new FrequencyCounter(inputPath)).countSingletonFrequencies();
                
                final Path outputPath = outputDir.resolve(outputName);
                result.writeToFile(outputPath);
                
                // Update execution context
                executionContext.put("outputDir", outputDir.toString());
                
                return null;
            }
        };
    }
    
    @Bean("frequent_itemsets.countSingletonFrequencies.step")
    public Step countSingletonFrequenciesStep(
        @Qualifier("frequent_itemsets.countSingletonFrequencies.tasklet") Tasklet tasklet,
        @Qualifier("frequent_itemsets.contextPromotionListener") StepExecutionListener contextListener)
    {
        return stepBuilderFactory.get("frequent_itemsets.countSingletonFrequencies")
            .tasklet(tasklet).listener(contextListener)
            .build();    
    }
    
    @Bean("frequent_itemsets.countSingletonFrequencies.flow")
    public Flow countSingletonFrequenciesFlow(
        @Qualifier("frequent_itemsets.countSingletonFrequencies.step") Step step)
    {
        return new FlowBuilder<Flow>("frequent_itemsets.countSingletonFrequencies").start(step).end();
    }
     
    @Bean("frequent_itemsets.countFrequencies.tasklet")
    @JobScope
    public Tasklet countFrequenciesTasklet(
        @Value("#{jobParameters['input']}") String input,
        @Value("#{jobParameters['input.result1']}") String result1File,
        @Value("#{jobParameters['outputName']}") String outputName,
        @Value("#{jobExecution.jobInstance.id}") Long jobId,
        @Qualifier("frequent_itemsets.resultCache") LoadingCache<Path, Result> resultCache)
    {
        Path inputPath = Paths.get(input);
        Assert.isTrue(inputPath != null && inputPath.isAbsolute(), 
            "Expected input file (transactions file) as an absolute file path");
        Path result1Path = Paths.get(result1File);
        Assert.isTrue(result1Path != null && result1Path.isAbsolute(), 
            "Expected results file (of previous counting phase) as an absolute file path");
        Assert.isTrue(!StringUtils.isEmpty(outputName), "An output name is required");
        
        Path outputDir = jobDataDirectory.resolve(        
            Paths.get("frequent_itemsets.countFrequencies", String.valueOf(jobId)));
        
        return new Tasklet()
        {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext)
                throws Exception
            {
                StepExecution stepExecution = chunkContext.getStepContext().getStepExecution();
                ExecutionContext executionContext = stepExecution.getExecutionContext();
                
                // Create output directory
                try {
                    Files.createDirectory(outputDir);
                } catch (FileAlreadyExistsException ex) {}
                
                // Count frequencies and write result to file
                
                final Result result1 = resultCache.get(result1Path);
                final Result result = (new FrequencyCounter(inputPath)).countFrequencies(result1);
                
                final Path outputPath = outputDir.resolve(outputName);
                result.writeToFile(outputPath);
                
                // Update execution context
                executionContext.put("outputDir", outputDir.toString());
                
                return null;
            }
        };
    }
   
    @Bean("frequent_itemsets.countFrequencies.step")
    public Step countFrequenciesStep(
        @Qualifier("frequent_itemsets.countFrequencies.tasklet") Tasklet tasklet,
        @Qualifier("frequent_itemsets.contextPromotionListener") StepExecutionListener contextListener)
    {
        return stepBuilderFactory.get("frequent_itemsets.countFrequencies")
            .tasklet(tasklet).listener(contextListener)
            .build();    
    }
    
    @Bean("frequent_itemsets.countFrequencies.flow")
    public Flow countFrequenciesFlow(
        @Qualifier("frequent_itemsets.countFrequencies.step") Step step)
    {
        return new FlowBuilder<Flow>("frequent_itemsets.countFrequencies").start(step).end();
    }
}
