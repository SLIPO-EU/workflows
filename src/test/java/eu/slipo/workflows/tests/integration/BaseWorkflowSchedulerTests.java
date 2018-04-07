package eu.slipo.workflows.tests.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.springframework.batch.test.AssertFile.assertFileEquals;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.TaskExecutor;

import eu.slipo.workflows.Workflow;
import eu.slipo.workflows.WorkflowExecution;
import eu.slipo.workflows.WorkflowExecutionCompletionListener;
import eu.slipo.workflows.WorkflowExecutionCompletionListenerSupport;
import eu.slipo.workflows.WorkflowExecutionSnapshot;
import eu.slipo.workflows.WorkflowExecutionStatus;
import eu.slipo.workflows.exception.WorkflowExecutionStartException;
import eu.slipo.workflows.service.WorkflowScheduler;

public abstract class BaseWorkflowSchedulerTests
{   
    protected static class Result
    {
        protected final String name;
        
        protected final Path expectedResultPath;
        
        Result(String name, Path expectedResultPath)
        {
            this.name = name;
            this.expectedResultPath = expectedResultPath;
        }
        
        public static Result of(String name, Path expectedResultPath)
        {
            return new Result(name, expectedResultPath);
        }
    }
    
    @Autowired
    protected TaskExecutor taskExecutor;
    
    @Autowired
    protected JobRepository jobRepository;
    
    @Autowired
    protected WorkflowScheduler workflowScheduler;
    
    protected abstract void info(String msg, Object ...args);
    
    protected abstract void warn(String msg, Object ...args);
    
    /**
     * Get the URL to a classpath resource
     */
    protected static URL getResource(Path p)
    {
        if (p == null || p.isAbsolute()) 
            throw new IllegalArgumentException(
                "Expected a relative path to resolve as a classpath resource");
        return BaseWorkflowSchedulerTests.class.getResource("/" + p.toString());
    }
    
    /**
     * Start a bunch of workflows, wait for all to complete. After completion check all results
     * against expected results.
     * 
     * @param workflowToResult
     */
    protected void startAndWaitToComplete(Map<Workflow, Result> workflowToResult) 
        throws Exception
    {        
        final int n = workflowToResult.size();
        final CountDownLatch done = new CountDownLatch(n);
        
        WorkflowExecutionCompletionListener callback = new WorkflowExecutionCompletionListenerSupport()
        {
            @Override
            public void onSuccess(WorkflowExecutionSnapshot workflowExecutionSnapshot)
            {
                final Workflow workflow = workflowExecutionSnapshot.workflow();
                done.countDown();
            }
        };
        
        Random rng = new Random();
        
        for (Workflow workflow: workflowToResult.keySet()) {
            taskExecutor.execute(new Runnable() 
            {
                @Override
                public void run()
                {
                    // Simulate some random delay
                    try {
                        Thread.sleep(50L + rng.nextInt(100));
                    } catch (InterruptedException ex) {
                        throw new IllegalStateException(ex);
                    }
                    
                    // Submit workflow
                    try {
                        workflowScheduler.start(workflow, callback);
                    } catch (WorkflowExecutionStartException ex) {
                        throw new IllegalStateException(ex);
                    }
                }
            });
        }
        
        // Wait for everyone
        
        done.await();
      
        // Test status of completed workflows
        
        for (Workflow workflow: workflowToResult.keySet()) {
            WorkflowScheduler.ExecutionSnapshot snapshot = workflowScheduler.poll(workflow.id());
            WorkflowExecutionSnapshot workflowExecutionSnapshot = snapshot.workflowExecutionSnapshot();
            assertTrue(workflowExecutionSnapshot.isComplete());
            assertEquals(WorkflowExecutionStatus.COMPLETED, snapshot.status());
        }
        
        // Test results
        
        for (Workflow workflow: workflowToResult.keySet()) {
            final Result r = workflowToResult.get(workflow);
            final Path expectedResultPath = r.expectedResultPath;
            
            Map<String, Path> outputMap = workflow.output();
            Path resultPath = outputMap.get(r.name);
            info("The workflow {} is finished (result at {})", workflow.id(), resultPath);
            
            assertTrue("Expected output result to be a readable file", 
                Files.isRegularFile(resultPath) && Files.isReadable(resultPath));
            
            assertFileEquals(expectedResultPath.toFile(), resultPath.toFile());
            
            // Test node status
            
            WorkflowExecution workflowExecution = new WorkflowExecution(workflow);
            workflowExecution.load(jobRepository);
            assertTrue(workflowExecution.isComplete());
        };
    }
    
    protected void startAndWaitToComplete(Workflow workflow, Result r)
        throws Exception
    {
        startAndWaitToComplete(Collections.singletonMap(workflow, r));
    }
}
