package eu.slipo.workflows.tests.integration.mergesort;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.springframework.batch.test.AssertFile.assertFileEquals;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.task.TaskExecutor;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.StringUtils;

import eu.slipo.workflows.Workflow;
import eu.slipo.workflows.WorkflowBuilderFactory;
import eu.slipo.workflows.WorkflowExecution;
import eu.slipo.workflows.WorkflowExecutionCompletionListener;
import eu.slipo.workflows.WorkflowExecutionCompletionListenerSupport;
import eu.slipo.workflows.WorkflowExecutionEventListener;
import eu.slipo.workflows.WorkflowExecutionEventListenerSupport;
import eu.slipo.workflows.WorkflowExecutionSnapshot;
import eu.slipo.workflows.WorkflowExecutionStatus;
import eu.slipo.workflows.WorkflowExecutionStopListener;
import eu.slipo.workflows.examples.mergesort.MergesortJobConfiguration;
import eu.slipo.workflows.examples.mergesort.MergesortWorkflows;
import eu.slipo.workflows.exception.WorkflowExecutionStartException;
import eu.slipo.workflows.service.WorkflowScheduler;
import eu.slipo.workflows.tests.BatchConfiguration;
import eu.slipo.workflows.tests.JobDataConfiguration;
import eu.slipo.workflows.tests.TaskExecutorConfiguration;
import eu.slipo.workflows.tests.WorkflowBuilderFactoryConfiguration;
import eu.slipo.workflows.tests.WorkflowSchedulerConfiguration;
import eu.slipo.workflows.tests.integration.BaseWorkflowSchedulerTests;

@RunWith(SpringRunner.class)
@EnableAutoConfiguration
@SpringBootTest(
    classes = {    
        TaskExecutorConfiguration.class, 
        BatchConfiguration.class,     
        WorkflowBuilderFactoryConfiguration.class,
        WorkflowSchedulerConfiguration.class,
        JobDataConfiguration.class,
        // job-specific application context
        MergesortJobConfiguration.class,
        MergesortWorkflows.class
    })
public class WorkflowSchedulerTests extends BaseWorkflowSchedulerTests
{
    private static Logger logger = LoggerFactory.getLogger(WorkflowSchedulerTests.class);
    
    public static final String RESULT_FILENAME = MergesortWorkflows.RESULT_FILENAME;
    
    @SuppressWarnings("serial")
    private static class InvalidInputException extends RuntimeException
    {
        InvalidInputException(String message)
        {
            super(message);
        }
    }
    
    private static class Fixture
    {
        final Path inputPath;
        
        final Path expectedResultPath;

        Fixture(Path inputPath, Path expectedResultPath)
        {
            this.inputPath = inputPath;
            this.expectedResultPath = expectedResultPath;
        }
        
        Fixture(URL inputPath, URL expectedResultPath)
        {
            this.inputPath = Paths.get(inputPath.getPath());
            this.expectedResultPath = Paths.get(expectedResultPath.getPath());
        }
    }
    
    private static class LoggingExecutionEventListener extends WorkflowExecutionEventListenerSupport
    {
        @Override
        public void beforeNode(
            WorkflowExecutionSnapshot workflowExecutionSnapshot, String nodeName, JobExecution jobExecution)
        {
            Workflow workflow = workflowExecutionSnapshot.workflow();
            logger.info("The workflow node {}/{} reports *before* as {}", 
                workflow.id(), nodeName, jobExecution.getStatus());
        }
        
        @Override
        public void afterNode(
            WorkflowExecutionSnapshot workflowExecutionSnapshot, String nodeName, JobExecution jobExecution)
        {
            Workflow workflow = workflowExecutionSnapshot.workflow();
            logger.debug("workflow {}: {}", workflow.id(), workflowExecutionSnapshot.debugGraph());
            logger.info("The workflow node {}/{} reports *after* as {}", 
                workflow.id(), nodeName, jobExecution.getStatus());
        }
    }
    
    private static class LoggingCompletionListener extends WorkflowExecutionCompletionListenerSupport
    {
        @Override
        public void onSuccess(WorkflowExecutionSnapshot workflowExecutionSnapshot)
        {
            Workflow workflow = workflowExecutionSnapshot.workflow();
            logger.info("The workflow {} is complete; The output is at %s",
                workflow.id(), workflow.outputDirectory());
        }
    }
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        final Path fixturePath = Paths.get("testcases/integration/mergesort");
        
        // Add fixtures from src/test/resources
        
        fixtures.add(new Fixture(
            getResource(fixturePath.resolve("1.txt")),
            getResource(fixturePath.resolve("1-sorted.txt"))));
        
        fixtures.add(new Fixture(
            getResource(fixturePath.resolve("2.txt")),
            getResource(fixturePath.resolve("2-sorted.txt"))));
        
        // Add randomly generated fixtures
        
        for (long sampleSize: Arrays.asList(1000 * 1000L, 3000 * 1000L)) 
        {
            Path inputPath = 
                Files.createTempFile("workflowSchedulerTests", ".txt");
            Path expectedResultPath = inputPath.resolveSibling(
                StringUtils.stripFilenameExtension(inputPath.getFileName().toString()) + "-sorted.txt");
            generateInputAndExpectedResult(inputPath, expectedResultPath, sampleSize);
            fixtures.add(new Fixture(inputPath, expectedResultPath));
        }
    }

    private static void generateInputAndExpectedResult(Path inputPath, Path expectedResultPath, long sampleSize) 
        throws IOException
    {
        Random random = new Random();
        
        long[] sample = random.longs(sampleSize).toArray();
        
        try (BufferedWriter writer = Files.newBufferedWriter(inputPath)) {
            for (long r: sample) {
                writer.write(Long.toString(r));
                writer.newLine();
            }
        }
        
        Arrays.sort(sample);
        
        try (BufferedWriter writer = Files.newBufferedWriter(expectedResultPath)) {
            for (long r: sample) {
                writer.write(Long.toString(r));
                writer.newLine();
            }
        }
        
        logger.info("Generated a sample of {} longs into {}. The expected result is at {}",
            sampleSize, inputPath, expectedResultPath);
    }
    
    @AfterClass
    public static void tearDownAfterClass() throws Exception {}
    
    private static List<Fixture> fixtures = new ArrayList<>();
    
    @Autowired
    private WorkflowBuilderFactory workflowBuilderFactory;
    
    @Autowired
    @Qualifier("mergesort.workflows")
    private MergesortWorkflows mergesortWorkflows;
    
    @Autowired
    @Qualifier("mergesort.splitFile.flow")
    private Flow splitFileFlow;
    
    @Autowired
    @Qualifier("mergesort.statFiles.step")
    private Step statFilesStep;
    
    @Autowired
    @Qualifier("mergesort.mergeFiles.flow")
    private Flow mergeFilesFlow;
    
    @Autowired
    @Qualifier("mergesort.sortFile.flow")
    private Flow sortFileFlow;
    
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    
    @Before
    public void setUp() throws Exception {}

    @After
    public void tearDown() throws Exception {}
    
    @Override
    protected void info(String msg, Object... args)
    {
        logger.info(msg, args);
    }

    @Override
    protected void warn(String msg, Object... args)
    {
        logger.warn(msg, args);
    }
    
    //
    // Tests
    //
        
    @Test(timeout = 60 * 1000L)
    public void test1k5p80() throws Exception
    {
        Fixture f = fixtures.get(0);
        Workflow workflow = mergesortWorkflows.getBuilder(UUID.randomUUID(), f.inputPath)
            .numParts(80).mergeIn(5)
            .build();
        startAndWaitToComplete(workflow, Result.of(RESULT_FILENAME, f.expectedResultPath));
    }
    
    @Test(timeout = 60 * 1000L)
    public void test2k5p80() throws Exception
    {
        Fixture f = fixtures.get(1);
        Workflow workflow = mergesortWorkflows.getBuilder(UUID.randomUUID(), f.inputPath)
            .numParts(80).mergeIn(5)
            .build();
        startAndWaitToComplete(workflow, Result.of(RESULT_FILENAME, f.expectedResultPath));
    }
    
    @Test(timeout = 60 * 1000L)
    public void test1k5p30() throws Exception
    {
        Fixture f = fixtures.get(0);
        Workflow workflow = mergesortWorkflows.getBuilder(UUID.randomUUID(), f.inputPath)
            .numParts(30).mergeIn(5)
            .build();
        startAndWaitToComplete(workflow, Result.of(RESULT_FILENAME, f.expectedResultPath));
    }
    
    @Test(timeout = 60 * 1000L)
    public void test2k5p30() throws Exception
    {
        Fixture f = fixtures.get(1);
        Workflow workflow = mergesortWorkflows.getBuilder(UUID.randomUUID(), f.inputPath)
            .numParts(30).mergeIn(5)
            .build();
        startAndWaitToComplete(workflow, Result.of(RESULT_FILENAME, f.expectedResultPath));
    }
    
    @Test(timeout = 60 * 1000L)
    public void test1k2p25() throws Exception
    {
        Fixture f = fixtures.get(0);
        Workflow workflow = mergesortWorkflows.getBuilder(UUID.randomUUID(), f.inputPath)
            .numParts(25).mergeIn(2)
            .build();
        startAndWaitToComplete(workflow, Result.of(RESULT_FILENAME, f.expectedResultPath));
    }
    
    @Test(timeout = 60 * 1000L)
    public void test2k2p25() throws Exception
    {
        Fixture f = fixtures.get(1);
        Workflow workflow = mergesortWorkflows.getBuilder(UUID.randomUUID(), f.inputPath)
            .numParts(25).mergeIn(2)
            .build();
        startAndWaitToComplete(workflow, Result.of(RESULT_FILENAME, f.expectedResultPath));
    }
    
    @Test(timeout = 60 * 1000L)
    public void test1k2p10() throws Exception
    {
        Fixture f = fixtures.get(0);
        Workflow workflow = mergesortWorkflows.getBuilder(UUID.randomUUID(), f.inputPath)
            .numParts(10).mergeIn(2)
            .build();
        startAndWaitToComplete(workflow, Result.of(RESULT_FILENAME, f.expectedResultPath));
    }
    
    @Test(timeout = 60 * 1000L)
    public void test2k2p10() throws Exception
    {
        Fixture f = fixtures.get(1);
        Workflow workflow = mergesortWorkflows.getBuilder(UUID.randomUUID(), f.inputPath)
            .numParts(10).mergeIn(2)
            .build();
        startAndWaitToComplete(workflow, Result.of(RESULT_FILENAME, f.expectedResultPath));
    }
    
    @Test(timeout = 60 * 1000L)
    public void test1k10p30() throws Exception
    {
        Fixture f = fixtures.get(0);
        Workflow workflow = mergesortWorkflows.getBuilder(UUID.randomUUID(), f.inputPath)
            .numParts(30).mergeIn(10)
            .build();
        startAndWaitToComplete(workflow, Result.of(RESULT_FILENAME, f.expectedResultPath));
    }
    
    @Test(timeout = 60 * 1000L)
    public void test2k10p30() throws Exception
    {
        Fixture f = fixtures.get(1);
        Workflow workflow = mergesortWorkflows.getBuilder(UUID.randomUUID(), f.inputPath)
            .numParts(30).mergeIn(10)
            .build();
        startAndWaitToComplete(workflow, Result.of(RESULT_FILENAME, f.expectedResultPath));
    }
    
    @Test(timeout = 60 * 1000L)
    public void test3k3p80() throws Exception
    {        
        Fixture f = fixtures.get(2);
        Workflow workflow = mergesortWorkflows.getBuilder(UUID.randomUUID(), f.inputPath)
            .numParts(80).mergeIn(3)
            .build();
        startAndWaitToComplete(workflow, Result.of(RESULT_FILENAME, f.expectedResultPath));
    }
    
    @Test(timeout = 60 * 1000L)
    public void test3k10p80() throws Exception
    {
        Fixture f = fixtures.get(2);
        Workflow workflow = mergesortWorkflows.getBuilder(UUID.randomUUID(), f.inputPath)
            .numParts(80).mergeIn(10)
            .build();
        startAndWaitToComplete(workflow, Result.of(RESULT_FILENAME, f.expectedResultPath));
    }
    
    @Test(timeout = 60 * 1000L)
    public void test4k3p80() throws Exception
    {        
        Fixture f = fixtures.get(3);
        Workflow workflow = mergesortWorkflows.getBuilder(UUID.randomUUID(), f.inputPath)
            .numParts(80).mergeIn(3)
            .build();
        startAndWaitToComplete(workflow, Result.of(RESULT_FILENAME, f.expectedResultPath));
    }
    
    @Test(timeout = 60 * 1000L)
    public void test4k10p150() throws Exception
    {
        Fixture f = fixtures.get(3);
        Workflow workflow = mergesortWorkflows.getBuilder(UUID.randomUUID(), f.inputPath)
            .numParts(150).mergeIn(10)
            .build();
        startAndWaitToComplete(workflow, Result.of(RESULT_FILENAME, f.expectedResultPath));
    }
    
    @Test(timeout = 150 * 1000L)
    public void testPx12() throws Exception
    {
        Fixture f1 = fixtures.get(0);
        Fixture f2 = fixtures.get(1);
        Fixture f3 = fixtures.get(2);
        Fixture f4 = fixtures.get(3);
        
        Map<Workflow, Result> workflowToResult = new HashMap<>();
        
        // f1
        
        Workflow w1a = mergesortWorkflows.getBuilder(UUID.randomUUID(), f1.inputPath)
            .numParts(90).mergeIn(5)
            .build();
        workflowToResult.put(w1a, Result.of(RESULT_FILENAME, f1.expectedResultPath));
        
        Workflow w1b = mergesortWorkflows.getBuilder(UUID.randomUUID(), f1.inputPath)
            .numParts(90).mergeIn(8)
            .build();
        workflowToResult.put(w1b, Result.of(RESULT_FILENAME, f1.expectedResultPath));
        
        Workflow w1c = mergesortWorkflows.getBuilder(UUID.randomUUID(), f1.inputPath)
            .numParts(180).mergeIn(12)
            .build();
        workflowToResult.put(w1c, Result.of(RESULT_FILENAME, f1.expectedResultPath));

        // f2
        
        Workflow w2a = mergesortWorkflows.getBuilder(UUID.randomUUID(), f2.inputPath)
            .numParts(50).mergeIn(5)
            .build();
        workflowToResult.put(w2a, Result.of(RESULT_FILENAME, f2.expectedResultPath));
        
        Workflow w2b = mergesortWorkflows.getBuilder(UUID.randomUUID(), f2.inputPath)
            .numParts(50).mergeIn(12)
            .build();
        workflowToResult.put(w2b, Result.of(RESULT_FILENAME, f2.expectedResultPath));
        
        Workflow w2c = mergesortWorkflows.getBuilder(UUID.randomUUID(), f2.inputPath)
            .numParts(40).mergeIn(2)
            .build();
        workflowToResult.put(w2c, Result.of(RESULT_FILENAME, f2.expectedResultPath));
        
        // f3
        
        Workflow w3a = mergesortWorkflows.getBuilder(UUID.randomUUID(), f3.inputPath)
            .numParts(50).mergeIn(5)
            .build();
        workflowToResult.put(w3a, Result.of(RESULT_FILENAME, f3.expectedResultPath));
        
        Workflow w3b = mergesortWorkflows.getBuilder(UUID.randomUUID(), f3.inputPath)
            .numParts(50).mergeIn(12)
            .build();
        workflowToResult.put(w3b, Result.of(RESULT_FILENAME, f3.expectedResultPath));
        
        Workflow w3c = mergesortWorkflows.getBuilder(UUID.randomUUID(), f3.inputPath)
            .numParts(40).mergeIn(2)
            .build();
        workflowToResult.put(w3c, Result.of(RESULT_FILENAME, f3.expectedResultPath));
        
        // f4
        
        Workflow w4a = mergesortWorkflows.getBuilder(UUID.randomUUID(), f4.inputPath)
            .numParts(90).mergeIn(5)
            .build();
        workflowToResult.put(w4a, Result.of(RESULT_FILENAME, f4.expectedResultPath));
        
        Workflow w4b = mergesortWorkflows.getBuilder(UUID.randomUUID(), f4.inputPath)
            .numParts(150).mergeIn(5)
            .build();
        workflowToResult.put(w4b, Result.of(RESULT_FILENAME, f4.expectedResultPath));
        
        Workflow w4c = mergesortWorkflows.getBuilder(UUID.randomUUID(), f4.inputPath)
            .numParts(150).mergeIn(10)
            .build();
        workflowToResult.put(w4c, Result.of(RESULT_FILENAME, f4.expectedResultPath));
        
        // Submit workflows, expect results
        
        startAndWaitToComplete(workflowToResult);
    }
    
    @Test(timeout = 30 * 1000L)
    public void test1WithListeners() throws Exception
    {
        Fixture f = fixtures.get(0);
        UUID workflowId = UUID.randomUUID();
        
        // Setup listeners
        
        Set<String> completed = new ConcurrentSkipListSet<>();
        
        CountDownLatch success = new CountDownLatch(1);
        
        WorkflowExecutionEventListener globalListener = new WorkflowExecutionEventListenerSupport()
        {
            @Override
            public void afterNode(WorkflowExecutionSnapshot workflowExecutionSnapshot,
                String nodeName, JobExecution jobExecution)
            {
                if (jobExecution.getStatus() == BatchStatus.COMPLETED)
                    completed.add(nodeName);
            }
            
            @Override
            public void onSuccess(WorkflowExecutionSnapshot workflowExecutionSnapshot)
            {
                success.countDown();
            }
        };
        
        CountDownLatch split = new CountDownLatch(1);
        
        JobExecutionListener splitListener = new JobExecutionListenerSupport()
        {
            @Override
            public void afterJob(JobExecution jobExecution)
            {
               split.countDown();
            }
        };
        
        // Build a workflow
        
        Workflow workflow = workflowBuilderFactory.get(workflowId)
            .job(b -> b.name("splitter")
                .flow(splitFileFlow)
                .input(f.inputPath)
                .parameters(p -> p.addLong("numParts", 3L)
                    .addString("outputPrefix", "part").addString("outputSuffix", ".txt"))
                .output("part1.txt", "part2.txt", "part3.txt"))
            .job(b -> b.name("sorter-1")
                .flow(sortFileFlow)
                .input("splitter", "part1.txt")
                .parameters(p -> p.addString("outputName", "r.txt"))
                .output("r.txt"))
            .job(b -> b.name("sorter-2")
                .flow(sortFileFlow)
                .input("splitter", "part2.txt")
                .parameters(p -> p.addString("outputName", "r.txt"))
                .output("r.txt"))
            .job(b -> b.name("sorter-3")
                .flow(sortFileFlow)
                .input("splitter", "part3.txt")
                .parameters(p -> p.addString("outputName", "r.txt"))
                .output("r.txt"))
            .job(b -> b.name("merger")
                .flow(mergeFilesFlow)
                .input("sorter-1", "r.txt")
                .input("sorter-2", "r.txt")
                .input("sorter-3", "r.txt")
                .parameters(p -> p.addString("outputName", "r.txt"))
                .output("r.txt"))
            .output("merger", "r.txt")
            .listener(globalListener)
            .listener(new LoggingExecutionEventListener())
            .listener("splitter", splitListener)
            .build();
       
        startAndWaitToComplete(workflow, Result.of(RESULT_FILENAME, f.expectedResultPath));
        
        // Test listeners
        
        assertTrue("Expected split counter to be zero", split.getCount() == 0L);
        assertTrue("Expected success counter to be zero", success.getCount() == 0L);
        assertEquals(
            new HashSet<>(Arrays.asList("splitter", "sorter-1", "sorter-2", "sorter-3", "merger")), 
            completed);
    }
    
    @Test(timeout = 30 * 1000L)
    public void test1WithFailureAndRestart() throws Exception
    {
        Fixture fixture = fixtures.get(0);
        
        // Build a Batch step that simulates a failure
        
        final String FAILURE_MESSAGE = "something is invalid";    
        final AtomicBoolean shouldFail = new AtomicBoolean(true);
        
        Tasklet validatorTasklet = new Tasklet()
        {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext)
                throws Exception
            {
                if (shouldFail.compareAndSet(true, false))
                    throw new InvalidInputException(FAILURE_MESSAGE);
                return RepeatStatus.FINISHED;
            }
        };
                
        Step validatorStep = stepBuilderFactory.get("validator")
            .tasklet(validatorTasklet).build();
        
        // Setup a global success/failure listener
        
        final CountDownLatch failed = new CountDownLatch(1);
        final CountDownLatch succeeded = new CountDownLatch(1);              
        final AtomicReference<List<Throwable>> exceptionsRef = new AtomicReference<>();
        
        WorkflowExecutionCompletionListener globalListener = new WorkflowExecutionCompletionListenerSupport()
        {
            @Override
            public void onFailure(WorkflowExecutionSnapshot workflowExecutionSnapshot,
                Map<String, List<Throwable>> failureExceptions)
            {
                exceptionsRef.set(failureExceptions.get("validator"));
                failed.countDown();
            }
            
            @Override
            public void onSuccess(WorkflowExecutionSnapshot workflowExecutionSnapshot)
            {
                succeeded.countDown();
            }
        };
        
        // Build a workflow
        
        UUID workflowId = UUID.randomUUID();
        Workflow workflow = workflowBuilderFactory.get(workflowId)
            .job(b -> b.name("splitter")
                .flow(splitFileFlow)
                .input(fixture.inputPath)
                .parameters(p -> p.addLong("numParts", 2L)
                    .addString("outputPrefix", "part").addString("outputSuffix", ".txt"))
                .output("part1.txt", "part2.txt"))
            .job(b -> b.name("validator")
                .flow(validatorStep)
                .input("splitter", "part1.txt")
                .input("splitter", "part2.txt"))
            .job(b -> b.name("sorter-1")
                .flow(sortFileFlow)
                .after("validator")
                .input("splitter", "part1.txt")
                .parameters(p -> p.addString("outputName", "r.txt"))
                .output("r.txt"))
            .job(b -> b.name("sorter-2")
                .flow(sortFileFlow)
                .after("validator")
                .input("splitter", "part2.txt")
                .parameters(p -> p.addString("outputName", "r.txt"))
                .output("r.txt"))
            .job(b -> b.name("merger")
                .flow(mergeFilesFlow)
                .input("sorter-1", "r.txt")
                .input("sorter-2", "r.txt")
                .parameters(p -> p.addString("outputName", "r.txt"))
                .output("r.txt"))
            .output("merger", "r.txt")
            .build();
       
        // Start and wait until failure
        
        workflowScheduler.start(workflow, globalListener);
        failed.await();
        assertEquals(0L, failed.getCount());
        assertEquals(1L, succeeded.getCount());
        
        // Test after failure
        
        List<Throwable> exceptions = exceptionsRef.get();
        assertNotNull(exceptions);
        assertEquals(1, exceptions.size());
        
        Throwable exception1 = exceptions.get(0);
        assertTrue(exception1 instanceof InvalidInputException);
        assertEquals(FAILURE_MESSAGE, exception1.getMessage());
        
        WorkflowScheduler.ExecutionSnapshot snapshot1 = workflowScheduler.poll(workflowId);
        assertEquals(WorkflowExecutionStatus.FAILED, snapshot1.status());
        WorkflowExecutionSnapshot executionSnapshot1 = snapshot1.workflowExecutionSnapshot();
        assertTrue(executionSnapshot1.isFailed());
        assertTrue(executionSnapshot1.node("splitter").executionId() > 0);
        assertEquals(BatchStatus.COMPLETED, executionSnapshot1.node("splitter").status());
        assertTrue(executionSnapshot1.node("validator").executionId() > 0);
        assertEquals(BatchStatus.FAILED, executionSnapshot1.node("validator").status());
        assertEquals(-1L, executionSnapshot1.node("sorter-1").executionId());
        assertEquals(BatchStatus.UNKNOWN, executionSnapshot1.node("sorter-1").status());
        assertEquals(-1L, executionSnapshot1.node("sorter-2").executionId());
        assertEquals(BatchStatus.UNKNOWN, executionSnapshot1.node("sorter-2").status());
        assertEquals(-1L, executionSnapshot1.node("merger").executionId());
        assertEquals(BatchStatus.UNKNOWN, executionSnapshot1.node("merger").status());
        
        // Restart
        
        workflowScheduler.start(workflow, globalListener);
        succeeded.await();
        
        // Test after success
        
        WorkflowScheduler.ExecutionSnapshot snapshot2 = workflowScheduler.poll(workflowId);
        assertEquals(WorkflowExecutionStatus.COMPLETED, snapshot2.status());
        WorkflowExecutionSnapshot executionSnapshot2 = snapshot2.workflowExecutionSnapshot();
        assertTrue(executionSnapshot2.isComplete());
        
        Path resultPath = workflow.output("r.txt");
        assertTrue(Files.exists(resultPath) && Files.isReadable(resultPath));
        assertFileEquals(fixture.expectedResultPath.toFile(), resultPath.toFile());
    }
    
    @Test(timeout = 30 * 1000L)
    public void test1WithStopAndRestart()
        throws Exception
    {
        Fixture fixture = fixtures.get(0);
        
        // Build a flow that delays execution (just waiting to be stopped)
        
        final AtomicBoolean shouldDelay = new AtomicBoolean(true);
        final AtomicBoolean stopRequested = new AtomicBoolean(false);
        final CountDownLatch delayed = new CountDownLatch(1);
        final CountDownLatch stopped = new CountDownLatch(1);
        
        Tasklet prepareTasklet = new Tasklet()
        {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext)
                throws Exception
            {
                if (!shouldDelay.get())
                    throw new IllegalStateException(
                        "This task was expected to always run before delayTasklet!");
                logger.info("prepareTasklet: I do nothing");
                return null;
            }
        };
        
        Step prepareStep = stepBuilderFactory.get("prepare").tasklet(prepareTasklet).build();
        
        Tasklet delayTasklet = new Tasklet()
        {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext)
                throws Exception
            {
                if (shouldDelay.get()) {
                    delayed.countDown();
                    while (!stopRequested.get()) {
                        logger.info("delayTasklet: Sleeping for another 1s...");
                        Thread.sleep(1000L);
                    }
                } else {
                    logger.info("delayTasklet: This task is finally done!");
                }
                return null;
            }
        };
        
        Step delayStep = stepBuilderFactory.get("delay").tasklet(delayTasklet).build();
        
        Flow delayFlow = new FlowBuilder<Flow>("delayFlow")
            .start(prepareStep)
            .next(delayStep)
            .build();
        
        // Build a workflow
        
        UUID workflowId = UUID.randomUUID();
        Workflow workflow = workflowBuilderFactory.get(workflowId)
            .job(b -> b.name("splitter")
                .flow(splitFileFlow)
                .input(fixture.inputPath)
                .parameters(p -> p.addLong("numParts", 2L)
                    .addString("outputPrefix", "part").addString("outputSuffix", ".txt"))
                .output("part1.txt", "part2.txt"))
            .job(b -> b.name("delay")
                .flow(delayFlow)
                .input("splitter", "part1.txt")
                .input("splitter", "part2.txt"))
            .job(b -> b.name("sorter-1")
                .flow(sortFileFlow)
                .after("delay")
                .input("splitter", "part1.txt")
                .parameters(p -> p.addString("outputName", "r.txt"))
                .output("r.txt"))
            .job(b -> b.name("sorter-2")
                .flow(sortFileFlow)
                .after("delay")
                .input("splitter", "part2.txt")
                .parameters(p -> p.addString("outputName", "r.txt"))
                .output("r.txt"))
            .job(b -> b.name("merger")
                .flow(mergeFilesFlow)
                .input("sorter-1", "r.txt")
                .input("sorter-2", "r.txt")
                .parameters(p -> p.addString("outputName", "r.txt"))
                .output("r.txt"))
            .output("merger", "r.txt")
            .build();
       
        // Start and stop after a while
        
        workflowScheduler.start(workflow);
        delayed.await();
        
        WorkflowExecutionStopListener stopListener = new WorkflowExecutionStopListener()
        {
            @Override
            public void onStopped(WorkflowExecutionSnapshot workflowExecutionSnapshot)
            {
                stopped.countDown();
            }
        };
        
        workflowScheduler.stop(workflowId, stopListener);
        logger.info("Requested from workflow {} to stop", workflowId);
        stopRequested.set(true); // to break the loop inside delayTasklet
        stopped.await();
        
        // Test after stopped
        
        WorkflowScheduler.ExecutionSnapshot snapshot1 = workflowScheduler.poll(workflowId);
        assertEquals(WorkflowExecutionStatus.STOPPED, snapshot1.status());
        WorkflowExecutionSnapshot executionSnapshot1 = snapshot1.workflowExecutionSnapshot();
        assertTrue(!executionSnapshot1.isComplete() && !executionSnapshot1.isFailed());
        assertTrue(executionSnapshot1.node("splitter").executionId() > 0);
        assertEquals(BatchStatus.COMPLETED, executionSnapshot1.node("splitter").status());
        assertTrue(executionSnapshot1.node("delay").executionId() > 0);
        assertEquals(BatchStatus.STOPPED, executionSnapshot1.node("delay").status());
        
        // Restart
        
        shouldDelay.set(false);
        
        final CountDownLatch succeeded = new CountDownLatch(1);
        
        WorkflowExecutionCompletionListener successListener = new WorkflowExecutionCompletionListenerSupport()
        {
            @Override
            public void onSuccess(WorkflowExecutionSnapshot workflowExecutionSnapshot)
            {
                succeeded.countDown();
            }
        };
        
        workflowScheduler.start(workflow, successListener);
        succeeded.await();
        
        // Test after success
        
        WorkflowScheduler.ExecutionSnapshot snapshot2 = workflowScheduler.poll(workflowId);
        assertEquals(WorkflowExecutionStatus.COMPLETED, snapshot2.status());
        WorkflowExecutionSnapshot executionSnapshot2 = snapshot2.workflowExecutionSnapshot();
        assertTrue(executionSnapshot2.isComplete());
        
        Path resultPath = workflow.output("r.txt");
        assertTrue(Files.exists(resultPath) && Files.isReadable(resultPath));
        assertFileEquals(fixture.expectedResultPath.toFile(), resultPath.toFile());
    }
    
    @Test(timeout = 30 * 1000L)
    public void test1WithExecutionContext()
        throws Exception
    {
        Fixture fixture = fixtures.get(0);
        
        // Build a flow consuming entries from the execution context of a node
        
        final AtomicReference<Map<String, Object>> reportContextRef = 
            new AtomicReference<Map<String,Object>>(null);
        
        Tasklet reportTasklet = new Tasklet()
        {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext)
                throws Exception
            {
                StepContext stepExecution = chunkContext.getStepContext();
                Map<String, Object> jobExecutionContext = stepExecution.getJobExecutionContext();
                
                reportContextRef.set(jobExecutionContext);
                return null;
            }
        };
        
        Step reportStep = stepBuilderFactory.get("report").tasklet(reportTasklet).build();
        
        // Build a workflow
        
        UUID workflowId = UUID.randomUUID();
        Workflow workflow = workflowBuilderFactory.get(workflowId)
            .job(b -> b.name("splitter")
                .flow(splitFileFlow)
                .input(fixture.inputPath)
                .parameters(p -> p.addLong("numParts", 2L)
                    .addString("outputPrefix", "part").addString("outputSuffix", ".txt"))
                .output("part1.txt", "part2.txt"))
            .job(b -> b.name("stat")
                .flow(statFilesStep)
                .input("splitter", "*")
                .exportToContext("count", "size", "size.average", "size.min", "size.max"))
            .job(b -> b.name("sorter-1")
                .flow(sortFileFlow)
                .input("splitter", "part1.txt")
                .parameters(p -> p.addString("outputName", "r.txt"))
                .output("r.txt"))
            .job(b -> b.name("sorter-2")
                .flow(sortFileFlow)
                .input("splitter", "part2.txt")
                .parameters(p -> p.addString("outputName", "r.txt"))
                .output("r.txt"))
            .job(b -> b.name("merger")
                .flow(mergeFilesFlow)
                .input("sorter-1", "r.txt")
                .input("sorter-2", "r.txt")
                .parameters(p -> p.addString("outputName", "r.txt"))
                .output("r.txt"))
            .job(b -> b.name("report")
                .flow(reportStep)
                .contextFrom("stat", "size", "totalSize")
                .contextFrom("stat", "size.average", "averageSize")
                .contextFrom("stat", "count", "fileCount"))
            .output("merger", "r.txt")
            .build();
        
        System.err.println(workflow.debugGraph());
        
        // Start and wait to complete
        
        CountDownLatch done = new CountDownLatch(1);
        
        WorkflowExecutionCompletionListener successListener = new WorkflowExecutionCompletionListenerSupport()
        {
            @Override
            public void onSuccess(WorkflowExecutionSnapshot workflowExecutionSnapshot)
            {
                done.countDown();
            }  
        };
        
        workflowScheduler.start(workflow, successListener);
        done.await();
        
        // Test after success
        
        Map<String, Object> reportContext = reportContextRef.get();
        assertNotNull(reportContext);
        assertNotNull(reportContext.get("totalSize"));
        assertTrue(reportContext.get("totalSize") instanceof Number);
        assertEquals(Files.size(fixture.inputPath), ((Number) reportContext.get("totalSize")).longValue());
        assertNotNull(reportContext.get("averageSize"));
        assertTrue(reportContext.get("averageSize") instanceof Double);
        assertNotNull(reportContext.get("fileCount"));
        assertTrue(reportContext.get("fileCount") instanceof Number);
        assertEquals(2, ((Number) reportContext.get("fileCount")).intValue());
        
        WorkflowScheduler.ExecutionSnapshot snapshot = workflowScheduler.poll(workflowId);
        assertEquals(WorkflowExecutionStatus.COMPLETED, snapshot.status());
        WorkflowExecutionSnapshot executionSnapshot = snapshot.workflowExecutionSnapshot();
        assertTrue(executionSnapshot.isComplete());
        
        Path resultPath = workflow.output("r.txt");
        assertTrue(Files.exists(resultPath) && Files.isReadable(resultPath));
        assertFileEquals(fixture.expectedResultPath.toFile(), resultPath.toFile());
    }
}
