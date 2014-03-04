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
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.knoesis.medline.indexer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.knoesis.lucene.indexer.FieldDocFactory;

/**
 *
 * @author mo-getter
 */
public class MedlineDocumentFile implements Iterable<Document> {
    
    private static final Set<String> IGNORED_FIELDS = new HashSet<String>(Arrays.asList("IRAD","BTI","CTI","CN","CRDT","DCOM","DA","LR","DEP","EN","ED","FED","EDAT","GS","GN","GR","IR","FIR","ISBN","IS","TA","LID","MID","JID","OCI","OID","OTO","OWN","PS","FPS","PL","PHST","PT","PUBM","PMCR","STAT","VI","VTI"));
    private static final Pattern PATTERN_NEW_ENTRY = Pattern.compile("^[a-zA-Z0-9]+(?:\\s+)?-\\s.+");
    
    private static final Pattern MQ_PATTERN = Pattern.compile("/([^*][^/]+)");
    private static final Pattern MC_PATTERN = Pattern.compile("\\*([^/]+)");
    
    private final File file;
    private final FieldDocFactory fieldFactory;
    private final BufferedReader reader;
    
    public MedlineDocumentFile(File file, FieldDocFactory fieldFactory) throws IOException {
        this.file = file;
        this.fieldFactory = fieldFactory;
        reader = new BufferedReader(new FileReader(file));
    }
    
    @Override
    public Iterator<Document> iterator() {
        return new DocumentFileIterator();
    }
    
    public void close() {
        if (reader != null) {
            try { reader.close(); } catch (IOException ex) {}
        }
    }
    
    private class DocumentFileIterator implements Iterator<Document> {
        
        private Document next;

        @Override
        public boolean hasNext() {
            if (next == null) {
                next = parseNext();
                if (next == null) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public Document next() {
            if (next == null) {
                next = parseNext();
            }
            Document ret = next;
            next = null;
            return ret;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Not supported.");
        }
        
        private Document parseNext() {
            Document document = null;
            try {
                String line = null;
                String currentFieldName = null;
                StringBuilder currentFieldValue = new StringBuilder();
                while ((line = reader.readLine()) != null && !line.trim().isEmpty()) {
                    if (document == null) {
                        document = fieldFactory.createDocument();
                    }
                    if (PATTERN_NEW_ENTRY.matcher(line).matches()) { // denotes start of a new field entry
                        if (currentFieldName != null && currentFieldValue.length() > 0) {
                            if (!IGNORED_FIELDS.contains(currentFieldName)) {
                                Fieldable field = fieldFactory.createField(currentFieldName, currentFieldValue.toString());
                                document.add(field);
                            }
                            currentFieldName = null;
                            currentFieldValue.setLength(0);
                        }
                        String[] parts = line.split("- ", 2);
                        currentFieldName = parts[0].trim();
                        currentFieldValue.append(parts[1].trim().replace("\\s+", " "));
                    }
                    else { // continuation of field value
                        currentFieldValue.append(" ").append(line.trim().replace("\\s+", " "));
                    }
                }
                if (document != null) {
                    finalizeFields(document);
                }
            } catch (Exception ex) {
                Logger.getLogger(DocumentFileIterator.class.getName()).log(Level.SEVERE, "Error parsing file", ex);
                throw new NoSuchElementException("Error parsing file: " + file.getAbsolutePath());
            }
            return document;
        }
        
        private void finalizeFields(Document document) {
            Fieldable[] meshHeadings = document.getFieldables("MH");
            for (Fieldable field : meshHeadings) {
                String value = field.stringValue();
                // extract mesh qualifiers
                Matcher matcher = MQ_PATTERN.matcher(value);
                while (matcher.find()) {
                    document.add(fieldFactory.createField("MQ", matcher.group(1)));
                }
                // extract mesh central terms
                matcher = MC_PATTERN.matcher(value);
                while (matcher.find()) {
                    document.add(fieldFactory.createField("MC", matcher.group(1)));
                }
            }
            if (document.getFieldable("AB") == null && document.getFieldable("OAB") != null) {
                document.add(fieldFactory.createField("AB", document.get("OAB")));
                document.removeField("OAB");
            }
            if (document.getFieldable("TI") == null && document.getFieldable("TT") != null) {
                document.add(fieldFactory.createField("TI", document.get("TT")));
                document.removeField("TT");
            }
            // parse "DP" (date of publication) into a NumericField "DT"
            Fieldable dateField = document.getFieldable("DP");
            if (dateField != null) {
                String dateStr = dateField.stringValue();
                long date = MedlineDateParser.parseAsLong(dateStr);
                date = DateTools.round(date, DateTools.Resolution.DAY);
                Fieldable intDateField = fieldFactory.createField("DT", String.valueOf(date));
                document.add(intDateField);
            }
        }
        
    }
    
}
