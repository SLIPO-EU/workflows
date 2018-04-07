package eu.slipo.workflows.tests.integration.frequent_itemsets;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.util.Pair;
import org.springframework.test.context.junit4.SpringRunner;

import eu.slipo.workflows.Workflow;
import eu.slipo.workflows.WorkflowBuilderFactory;
import eu.slipo.workflows.examples.frequent_itemsets.FrequentItemsetsJobConfiguration;
import eu.slipo.workflows.examples.frequent_itemsets.FrequentItemsetsWorkflows;
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
        FrequentItemsetsJobConfiguration.class,
        FrequentItemsetsWorkflows.class,
    })
public class WorkflowSchedulerTests extends BaseWorkflowSchedulerTests
{
    private static Logger logger = LoggerFactory.getLogger(WorkflowSchedulerTests.class);
    
    public static final String RESULT_FILENAME = FrequentItemsetsWorkflows.RESULT_FILENAME;
    
    /**
     * The maximum size (cardinality) of the itemsets we are interested into
     */
    public static final int MAX_CARDINALITY = 3;
    
    private static class Fixture
    {
        final Path inputPath;
        
        final Path expectedResultPath;

        final double thresholdFrequency;
        
        Fixture(Path inputPath, Path expectedResultPath, double thresholdFrequency)
        {
            this.inputPath = inputPath;
            this.expectedResultPath = expectedResultPath;
            this.thresholdFrequency = thresholdFrequency;
        }
        
        Fixture(URL inputPath, URL expectedResultPath, double thresholdFrequency)
        {
            this.inputPath = Paths.get(inputPath.getPath());
            this.expectedResultPath = Paths.get(expectedResultPath.getPath());
            this.thresholdFrequency = thresholdFrequency;
        }
    }
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        final Path fixturePath = Paths.get("testcases/integration/frequent_itemsets");
        
        // Add fixtures from src/test/resources (datasets from http://fimi.ua.ac.be/data/)
        
        for (double thresholdFrequency: Arrays.asList(0.50, 0.20, 0.10, 0.05)) {
            int percentage = Double.valueOf(100 * thresholdFrequency).intValue();
            for (String datasetName: Arrays.asList("retail", "kosarak")) {
                Fixture fixture = new Fixture(
                    getResource(
                        fixturePath.resolve(String.format("%s.dat.gz", datasetName))),
                    getResource(
                        fixturePath.resolve(String.format("out-%s-%02dp.dat", datasetName, percentage))),
                    thresholdFrequency);
                fixtures.put(Pair.of(datasetName, percentage), fixture);
            }
        }
    }
    
    @AfterClass
    public static void tearDownAfterClass() throws Exception {}
    
    /**
     * A map of test fixtures keyed to a pair of (datasetName, thresholdAsPercentage)
     */
    private static Map<Pair<String, Integer>, Fixture> fixtures = new HashMap<>();
    
    @Autowired
    @Qualifier("frequent_itemsets.workflows")
    private FrequentItemsetsWorkflows frequentItemsetsWorkflows;
    
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
        
    @Test // Fixme (timeout = 60 * 1000L)
    public void testD1n4t50() throws Exception
    {
        Fixture f = fixtures.get(Pair.of("retail", 50));
        Workflow workflow = frequentItemsetsWorkflows.getBuilder(UUID.randomUUID(), f.inputPath)
            .numParts(4)
            .thresholdFrequency(f.thresholdFrequency)
            .maxSize(MAX_CARDINALITY)
            .build();
        startAndWaitToComplete(workflow, Result.of(RESULT_FILENAME, f.expectedResultPath));
    } 
}
