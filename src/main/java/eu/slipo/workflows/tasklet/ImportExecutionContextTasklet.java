package eu.slipo.workflows.tasklet;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.commons.collections4.IterableUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.util.Assert;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * A tasklet that imports entries into step-level execution context
 */
public class ImportExecutionContextTasklet implements Tasklet
{
    private static final Logger logger = LoggerFactory.getLogger(ImportExecutionContextTasklet.class);
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    private static final long cacheMaxSize = 5 * 1000L;
    
    private static final CacheLoader<Path, Map<?, ?>> cacheLoader = new CacheLoader<Path, Map<?, ?>> () 
    {
        @Override
        public Map<?, ?> load(Path sourcePath) throws Exception
        {
            return objectMapper.readValue(sourcePath.toFile(), Map.class);
        } 
    };
    
    private static final LoadingCache<Path, Map<?,?>> cache = CacheBuilder.newBuilder()
        .maximumSize(cacheMaxSize)
        .build(cacheLoader);
    
    private final Map<String, URI> sources;

    /**
     * Load context entries from the given sources.
     * 
     * <p>A source is a URI of the form {@code file:///<path>#<context-key>}. The {@code path}
     * corresponds to a JSON file (exported by some other job node), while the {@code contextKey}
     * to the key of the target entry (as exported by the producer node).
     * 
     * @param sources The map of sources
     */
    public ImportExecutionContextTasklet(Map<String, URI> sources)
    {
        Assert.isTrue(sources.values().stream().allMatch(u -> u.getScheme().equals("file")), 
            "Expected all sources to be given as file:// URIs");
        this.sources = sources;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext)
        throws Exception
    {
        final StepContext stepContext = chunkContext.getStepContext();
        final ExecutionContext executionContext = stepContext.getStepExecution().getExecutionContext();
        
        logger.info("Importing into step execution context: {}", sources);
        
        // Load each value represented by source URI
        
        sources.forEach(
            (key, sourceUri) -> executionContext.put(key, extractValue(sourceUri)));
        
        return RepeatStatus.FINISHED;
    }
     
    private Object extractValue(URI sourceUri)
    {
        Assert.state(sourceUri.getScheme().equals("file"), "Expected a file:// source");
        
        final Path sourcePath = Paths.get(sourceUri.getPath());
        Assert.state(sourcePath.isAbsolute(), 
            "The path of a file:// URI is expected to be absolute");
        
        final String key = sourceUri.getFragment();
        Assert.state(key != null, 
            "Expected a non-empty fragment to locate the key of a context entry");
        
        Map<?, ?> map = null;
        try {
            map = cache.get(sourcePath);
        } catch (ExecutionException e) {
            throw new IllegalStateException(
                String.format("Failed to load context map from %s", sourcePath), 
                e.getCause());
        }
        
        return map.get(key);
    }
}
