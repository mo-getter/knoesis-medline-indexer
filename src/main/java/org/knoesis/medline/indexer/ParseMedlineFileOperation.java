
package org.knoesis.medline.indexer;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.document.Document;
import org.knoesis.lucene.indexer.FieldDocFactory;
import org.knoesis.util.concurrent.parallel.Operation;
import org.knoesis.util.concurrent.producerconsumer.Production;

/**
 *
 * @author alan
 */
public class ParseMedlineFileOperation implements Operation<File> {

    private final Production<Document> production;
    private final FieldDocFactory fields;

    public ParseMedlineFileOperation(Production<Document> production, FieldDocFactory fields) {
        this.production = production;
        this.fields = fields;
    }

    @Override
    public void perform(File file) {
        System.out.println("Reading " + file.getAbsolutePath());
        MedlineDocumentFile docFile = null;
        try {
            docFile = new MedlineDocumentFile(file, fields);
            for (Document document : docFile) {
                production.put(document);
            }
        } catch (Exception ex) {
            Logger.getLogger(ParseMedlineFileOperation.class.getName()).log(Level.SEVERE, String.format("Exception occurred while parsing file %s", file, ex));
        } finally {
            if (docFile != null) {
                docFile.close();
            }
        }
    }

}
