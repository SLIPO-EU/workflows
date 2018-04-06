package eu.slipo.workflows.util.tasklet;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;

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
 * A tasklet that splits a line-oriented text file to a fixed number of parts.
 */
public class SplitFileTasklet implements Tasklet
{
    private static final Logger logger = LoggerFactory.getLogger(SplitFileTasklet.class);
    
    private static final byte[] NEWLINE = String.format("%n").getBytes();
    
    private static final byte[] EMPTY_DATA = "".getBytes();
    
    private static final InputStream EMPTY_INPUT = new ByteArrayInputStream(EMPTY_DATA);
    
    private static final List<String> GZIP_EXTENSIONS = Arrays.asList("gz", "z", "GZ", "Z");
    
    public static final String DEFAULT_OUTPUT_SUFFIX = ".dat";  
    
    private final Path inputPath;
    
    private final boolean decompress;
    
    private final String outputPrefix;
    
    private final String outputSuffix;
    
    private final Path outputDir;
    
    private final int numParts;
    
    public SplitFileTasklet(
        Path inputPath, Path outputDir, int numParts, String outputPrefix, String outputSuffix)
    {
        Assert.isTrue(numParts > 1, "The number of parts must be > 1");
        Assert.isTrue(!StringUtils.isEmpty(outputPrefix), 
            "A non-empty output prefix is required");
        Assert.isTrue(outputDir != null && outputDir.isAbsolute(), 
            "The output directory must be given as an absolute path");
        Assert.isTrue(!StringUtils.isEmpty(inputPath.toString()) && inputPath.isAbsolute(), 
            "The input is required (as an absolute path)");
        
        this.inputPath = inputPath;
        
        String fileName = inputPath.getFileName().toString();
        String fileExtension = StringUtils.getFilenameExtension(fileName);
        this.decompress = fileExtension != null && GZIP_EXTENSIONS.contains(fileExtension);
        
        this.outputDir = outputDir;
        this.numParts = numParts;
        this.outputPrefix = outputPrefix;
        this.outputSuffix = outputSuffix == null? DEFAULT_OUTPUT_SUFFIX : outputSuffix;
    }
    
    private Path outputPath(int partNumber)
    {
        return outputDir.resolve(
            outputPrefix + String.valueOf(partNumber) + outputSuffix);
    }

    private long inputSize() throws IOException
    {
        return decompress? 
            computeDecompressedInputSize() : Files.size(inputPath);
    }
    
    private long computeDecompressedInputSize() throws IOException
    {
        // See https://stackoverflow.com/questions/7317243/gets-the-uncompressed-size-of-this-gzipinputstream
        
        long size = -1L;
        try (RandomAccessFile fp = new RandomAccessFile(inputPath.toFile(), "r")) {        
            fp.seek(fp.length() - Integer.BYTES);
            int n = fp.readInt();
            size = Integer.toUnsignedLong(Integer.reverseBytes(n));
        }
        
        return size;
    }
    
    private InputStream openInputStream() 
        throws IOException
    {
        final InputStream in = Files.newInputStream(inputPath, StandardOpenOption.READ);
        
        if (decompress) {
            // The input is regarded as a gzip'ed stream of data
            InputStream decompressedIn = null;
            try {
                 decompressedIn = new GZIPInputStream(in);
            } catch (IOException ex) {
                // Prevent a leak on raw input (and rethrow)
                in.close(); 
                throw ex;
            }
            return decompressedIn;
        } else {
            // The input is returned without any transformation
            return in;
        }
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
            logger.debug("Created output directory at {}", outputDir);
        } catch (FileAlreadyExistsException ex) {
            // no-op
        }
        
        // Split input
        
        Assert.state(Files.isRegularFile(inputPath) && Files.isReadable(inputPath), 
            "The input path must point to a readable file");
        
        final long inputSize = inputSize();
        final long partSize = 1 + ((inputSize - 1) / numParts);
        
        InputStream in = openInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        OutputStream out = null;
        int partNumber = 0;
        int writeCount = 0;
        try {
            // Read line by line until part size is exceeded
            String line = null;
            while ((line = reader.readLine()) != null) {
                if (out == null) {
                    partNumber++;
                    Path outPath = outputPath(partNumber);
                    out = Files.newOutputStream(outPath, 
                        StandardOpenOption.TRUNCATE_EXISTING, 
                        StandardOpenOption.CREATE);
                    writeCount = 0;
                }
                // Append this line to output
                byte[] buf = line.getBytes();
                out.write(buf);
                out.write(NEWLINE);
                writeCount += buf.length + NEWLINE.length;
                // Check if part size was exceeded
                if (writeCount > partSize) {
                    out.close();
                    out = null;
                }
            }
        } finally {
            reader.close();
            if (out != null) 
                out.close();
        }
        
        logger.info("Split {} to {} parts inside {}", inputPath, partNumber, outputDir);
        
        // If parts less than requested (e.g because of huge lines), pad with empty files
        
        for (int i = partNumber + 1; i <= numParts; ++i) {
            Path outPath = outputPath(i);
            Files.copy(EMPTY_INPUT, outPath, StandardCopyOption.REPLACE_EXISTING);
        }
        
        // Update execution context
        
        executionContext.put("outputDir", outputDir.toString());
        executionContext.putInt("partNumber", partNumber);
        
        return RepeatStatus.FINISHED;
    }   
}
