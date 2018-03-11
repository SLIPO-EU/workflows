package eu.slipo.workflows.tests;

import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import eu.slipo.workflows.service.EventBasedWorkflowScheduler;
import eu.slipo.workflows.service.WorkflowScheduler;

@Configuration
public class WorkflowSchedulerConfiguration
{
    @Autowired
    JobRepository jobRepository;
    
    @Autowired
    @Qualifier("asyncJobLauncher")
    JobLauncher jobLauncher;
    
    @Autowired
    JobExplorer jobExplorer;
    
    @Autowired
    StepBuilderFactory stepBuilderFactory; 
    
    @Autowired
    JobBuilderFactory jobBuilderFactory;
    
    @Bean
    WorkflowScheduler workflowScheduler()
    {
        EventBasedWorkflowScheduler scheduler = new EventBasedWorkflowScheduler();
        scheduler.setJobRepository(jobRepository);
        scheduler.setJobLauncher(jobLauncher);
        scheduler.setJobExplorer(jobExplorer);
        scheduler.setJobBuilderFactory(jobBuilderFactory);
        scheduler.setStepBuilderFactory(stepBuilderFactory);
        scheduler.setMaxDurationAfterUpdate(3600L);
        return scheduler;
    }
}
