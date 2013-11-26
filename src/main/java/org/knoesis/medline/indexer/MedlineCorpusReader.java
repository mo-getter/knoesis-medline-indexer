
package org.knoesis.medline.indexer;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.document.Document;
import org.knoesis.lucene.indexer.CorpusReader;
import org.knoesis.lucene.indexer.FieldDocFactory;
import org.knoesis.lucene.indexer.utils.PropUtils;
import org.knoesis.util.concurrent.parallel.Operation;
import org.knoesis.util.concurrent.parallel.Parallel;
import org.knoesis.util.concurrent.producerconsumer.Production;

/**
 *
 * @author alan
 */
public class MedlineCorpusReader implements CorpusReader {
    
    private static final String PROP_KEY_IO_THREADS = "luceneindexer.iothreads";
    private static final String PROP_COURPUS_DIR = "luceneindexer.corpusdir";
    
    private Properties properties;
    private File corpus;
    private FieldDocFactory fields;
    private int numIoThreads = 1;
    
    private static final FileFilter L1_FILTER = new FileFilter() {
        @Override
        public boolean accept(File dir) {
            return dir.isDirectory();
        }
    };
    
    private static final FilenameFilter CITS_FILTER = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return dir.isDirectory() && name.startsWith("cits_");
        }
    };
    
    @Override
    public void init(FieldDocFactory fields, Properties properties) {
        this.fields = fields;
        this.properties = properties;
        numIoThreads = PropUtils.getInt(properties, PROP_KEY_IO_THREADS, numIoThreads);
        String corpusDir = properties.getProperty(PROP_COURPUS_DIR);
        setCorpus(new File(corpusDir));
    }

    @Override
    public void produce(Production<Document> production) {
        Operation<File> parseFileOp = new ParseMedlineFileOperation(production, fields);
        try {
            File[] l1Files = corpus.listFiles(L1_FILTER);
            Arrays.sort(l1Files);
            for (File dir : l1Files) {
                File[] l2Files = dir.listFiles(CITS_FILTER);
                if (l2Files == null) {
                    continue;
                }
                Arrays.sort(l2Files);
                for (File cits : l2Files) {
                    File[] l3Files = cits.listFiles();
                    if (l3Files == null) {
                        continue;
                    }
                    Arrays.sort(l3Files);
                    Parallel.forEach(Arrays.asList(l3Files), parseFileOp, numIoThreads);
                }
            }
        } catch (InterruptedException ex) {
            Logger.getLogger(MedlineCorpusReader.class.getName()).log(Level.SEVERE, "Producer thread interrupted", ex);
        }
    }
    
    private void setCorpus(File corpus) {
        if (!(corpus.exists() && corpus.canExecute() && corpus.canRead())) {
            throw new IllegalArgumentException("Corpus directory " + corpus.getAbsolutePath() + " does not exist or you do not have the necessary permissions (r/x).");
        }
        this.corpus = corpus;
    }

}
