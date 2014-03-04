/**
 * Copyright (C) 2014 Kno.e.sis
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.knoesis.medline.indexer;

import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.knoesis.lucene.indexer.FieldDocFactory;
import org.knoesis.lucene.indexer.Indexer;

/**
 *
 * @author alan
 */
public class MedlineIndexer implements Indexer {
    
    private static final String PMID = "PMID";

    private Properties properties;
    private IndexWriter writer;
    private FieldDocFactory fields;
    
    @Override
    public void init(IndexWriter writer, FieldDocFactory fields, Properties properties) {
        this.writer = writer;
        this.fields = fields;
        this.properties = properties;
    }
    
    @Override
    public void consume(Iterable<Document> consumables) {
        for (Document doc : consumables) {
            String pmid = doc.get(PMID);
            if (pmid == null) {
                continue;
            }
            Term pmidTerm = new Term(PMID, pmid);
            try {
                // Removes existing doc(s) with the same PMID.
                writer.updateDocument(pmidTerm, doc);
            } catch (IOException ex) {
                Logger.getLogger(MedlineIndexer.class.getName()).log(Level.SEVERE, String.format("Failed to add Document [PMID:%s] to index", pmid), ex);
            }
            fields.recycle(doc);
        }
    }

}
