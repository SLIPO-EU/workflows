package eu.slipo.workflows.tasklet;

import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.util.Assert;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A tasklet that exports part of job-level execution context to a target JSON file
 */
public class ExportExecutionContextTasklet implements Tasklet
{
    private static final Logger logger = LoggerFactory.getLogger(ExportExecutionContextTasklet.class);
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    private static final FileAttribute<?> directoryAttribute = PosixFilePermissions
        .asFileAttribute(PosixFilePermissions.fromString("rwxr-xr-x"));
    
    private final Set<String> keys;
    
    private final Path targetPath;
    
    public ExportExecutionContextTasklet(Path targetPath, Set<String> keys)
    {
        Assert.notNull(targetPath, "Expected a target path for JSON file");
        Assert.isTrue(targetPath.isAbsolute(), "The target path is expected as an absolute path");
        Assert.notNull(keys, "Expected a non-null set of keys to export");
        this.targetPath = targetPath;
        this.keys = keys;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext)
        throws Exception
    {
        StepContext stepContext = chunkContext.getStepContext();
        Map<String, Object> jobExecutionContext = stepContext.getJobExecutionContext();
        
        logger.info("Exporting context to {}", targetPath);
        
        // Create target directory, if needed
        
        try {
            Files.createDirectories(targetPath.getParent(), directoryAttribute);
            logger.info("Created target directory at {}", targetPath.getParent());
        } catch (FileAlreadyExistsException e) {
            // no-op: The directory already exists
        }
        
        // Export context as JSON
        
        Map<String, Object> exportedContext = new HashMap<>();
        for (String key: keys) {
            Object value = jobExecutionContext.get(key);
            if (value == null) {
                throw new IllegalStateException(
                    "The exported key [" + key +"] is not found into execution context");
            }
            exportedContext.put(key, value);
        }
        
        objectMapper.writeValue(targetPath.toFile(), exportedContext);
        
        return RepeatStatus.FINISHED;
    }

}
