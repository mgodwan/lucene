package org.apache.lucene.index;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundDirectory;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;
import java.util.*;

public class TestSegmentGroups extends LuceneTestCase {

    static List<Integer> status = new ArrayList<>();
    static List<Integer> defaults = List.of(200, 201, 204, 300, 500, 503, 502, 504);
    static List<Integer> filtered = List.of(400, 404);

    static {
        status.addAll(defaults);
        status.addAll(filtered);
    }

    public void testSegregatedIndexRead() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(
                dir,
                newIndexWriterConfig(new MockAnalyzer(random()))
                        .setMaxBufferedDocs(250)
                        .setMergePolicy(new TieredMergePolicy())
                        .setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND));
        for (int i = 0; i < 50000; i++) {
            Document doc = new Document();
            doc.add(newStringField("pk", Integer.toString(i), Field.Store.YES));
            int docStatus = status.get(random().nextInt(status.size()));
            doc.add(new IntPoint("status", docStatus));
            doc.add(new SortedNumericDocValuesField("status", docStatus));
            writer.addDocument(doc);
        }
        writer.commit();
        SegmentInfos infos = writer.cloneSegmentInfos();

        infos.forEach(si -> {
            DocValuesProducer producer = null;
            Directory localDir = null;
            try {
                localDir = si.info.dir;
                if (si.info.getUseCompoundFile()) {
                    localDir = si.info.getCodec().compoundFormat().getCompoundReader(localDir, si.info, IOContext.READONCE);
                }
                final FieldInfos coreFieldInfos = si.info.getCodec().fieldInfosFormat().read(localDir, si.info, "", IOContext.READONCE);
                final SegmentReadState segmentReadState = new SegmentReadState(localDir, si.info, coreFieldInfos, IOContext.READONCE);
                producer = si.info.getCodec().docValuesFormat().fieldsProducer(segmentReadState);
                SortedNumericDocValues vals = producer.getSortedNumeric(coreFieldInfos.fieldInfo("status"));

                int cnt = vals.docValueCount();
                Set<Long> dvs = new TreeSet<>();
                System.out.println("Count: " + cnt);
                vals.nextDoc();
                while (vals.docID() != DocIdSetIterator.NO_MORE_DOCS) {
                    //System.out.println( "DOCID ::: " + vals.docID() + " ---> " + vals.nextValue());
                    dvs.add(vals.nextValue());
                    vals.nextDoc();
                }
                if (dvs.stream().anyMatch(s -> filtered.contains(s))) {
                    assert dvs.stream().noneMatch(s -> defaults.contains(s)) : String.format("%s : [bucket: %s] [merged: %s] -> %s", si.info.name, si.info.getAttribute("bucket"), si.info.getAttribute("merged"), dvs);
                } else {
                    assert dvs.stream().noneMatch(s -> filtered.contains(s)) : String.format("%s : [bucket: %s] [merged: %s] -> %s", si.info.name, si.info.getAttribute("bucket"), si.info.getAttribute("merged"), dvs);
                }
                System.out.println(String.format("%s : [bucket: %s] [merged: %s] -> %s", si.info.name, si.info.getAttribute("bucket"), si.info.getAttribute("merged"), dvs));
            } catch (Exception ex) {
                ex.printStackTrace();
                throw new RuntimeException(ex);
            } finally {
                if (localDir instanceof CompoundDirectory) {
                    IOUtils.closeWhileHandlingException(localDir);
                }
                IOUtils.closeWhileHandlingException(producer);
            }
        });
        writer.close();

        DirectoryReader reader = DirectoryReader.open(dir);
        System.out.println("Total docs: " + reader.numDocs());
        reader.close();
        dir.close();
    }
}
