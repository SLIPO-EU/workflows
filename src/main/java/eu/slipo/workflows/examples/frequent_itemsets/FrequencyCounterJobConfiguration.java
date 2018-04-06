package eu.slipo.workflows.examples.frequent_itemsets;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import javax.annotation.PostConstruct;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;

import eu.slipo.workflows.ExecutionContextPromotionListeners;
import eu.slipo.workflows.util.tasklet.SplitFileTasklet;

@Configuration("frequent_itemsets.jobConfiguration")
public class FrequencyCounterJobConfiguration
{
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
   
    @Autowired
    private Path jobDataDirectory;
        
    @PostConstruct
    private void setupDataDirectories() throws IOException
    {
        String dirNames[] = new String[] {
            "frequent_itemsets.countSingletonFrequencies", 
            "frequent_itemsets.countFrequencies",
            "frequent_itemsets.mergeResults"
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
    
    @Bean("frequent_itemsets.splitFile.tasklet")
    @JobScope
    public Tasklet splitFileTasklet(
        @Value("#{jobParameters['input']}") String input,
        @Value("#{jobParameters['outputPrefix']?:'p'}") String outputPrefix,
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
    
    @Bean("frequent_itemsets.mergeFiles.tasklet")
    @JobScope
    public Tasklet mergeFilesTasklet(
        @Value("#{jobParameters['input']}") String input,
        @Value("#{jobParameters['outputName']}") String outputName,
        @Value("#{jobParameters['thresholdFrequency']}") double thresholdFrequency,
        @Value("#{jobExecution.jobInstance.id}") Long jobId)
    {
        Path[] inputPaths = Arrays.stream(input.split(File.pathSeparator))
            .filter(p -> !p.isEmpty())
            .map(Paths::get)
            .toArray(Path[]::new);
        Path outputDir = jobDataDirectory.resolve(
            Paths.get("frequent_itemsets.mergeFiles", String.valueOf(jobId)));
        return new MergeFilesTasklet(outputDir, outputName, thresholdFrequency, inputPaths);
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
        Path outputDir = jobDataDirectory.resolve(
            Paths.get("frequent_itemsets.countSingletonFrequencies", String.valueOf(jobId)));
        return new CountSingletonFrequenciesTasklet(outputDir, outputName, inputPath);
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
        @Value("#{jobExecution.jobInstance.id}") Long jobId)
    {
        Path inputPath = Paths.get(input);
        Path result1Path = Paths.get(result1File);
        Path outputDir = jobDataDirectory.resolve(
            Paths.get("frequent_itemsets.countFrequencies", String.valueOf(jobId)));
        return new CountFrequenciesTasklet(outputDir, outputName, inputPath, result1Path);
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
