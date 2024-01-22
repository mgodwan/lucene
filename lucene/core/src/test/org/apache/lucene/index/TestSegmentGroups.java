package org.apache.lucene.index;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;

import java.io.IOException;

public class TestSegmentGroups extends LuceneTestCase {

    static int[] status = {
            200, 300, 500, 400, 404, 503, 502, 201, 204, 504
    };

    public void testWrite() throws Exception {
        Directory dir = newDirectory();
        RandomIndexWriter rw = new RandomIndexWriter(random(), dir);
        for (int i = 0; i < 6000; i++) {
            Document doc = new Document();
            doc.add(newStringField("pk", Integer.toString(i), Field.Store.YES));
            doc.add(new IntPoint("status", status[random().nextInt(0, status.length)]));
            rw.addDocument(doc);
        }
        rw.close();

//        // If buffer size is small enough to cause a flush, errors ensue...
//        IndexWriter w =
//                new IndexWriter(
//                        dir,
//                        newIndexWriterConfig(new MockAnalyzer(random()))
//                                .setMaxBufferedDocs(2)
//                                .setOpenMode(IndexWriterConfig.OpenMode.APPEND));
//
//        for (int i = 501; i < 3000; i++) {
//            Document doc = new Document();
//            String value = Integer.toString(i);
//            doc.add(newStringField("pk", value, Field.Store.YES));
//            doc.add(new IntPoint("status", 200));
//            w.updateDocument(new Term("pk", value), doc);
//        }

        IndexReader r = DirectoryReader.open(dir);
        assertEquals("index should contain docs", 3000, r.numDocs());
        r.close();
        dir.close();
    }

    public void testSegregatedIndexRead() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(
                dir,
                newIndexWriterConfig(new MockAnalyzer(random()))
                        .setMaxBufferedDocs(50)
                        .setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND));
        for (int i = 0; i < 3000; i++) {
            Document doc = new Document();
            doc.add(newStringField("pk", Integer.toString(i), Field.Store.YES));
            doc.add(new IntPoint("status", status[random().nextInt(0, status.length)]));
            writer.addDocument(doc);
        }
        writer.flush();
        writer.cloneSegmentInfos().forEach(
                si -> {

                    try (PointsReader reader = getPointReader(si)){
                        int min = IntPoint.decodeDimension(reader.getValues("status").getMinPackedValue(), 0);
                        int max = IntPoint.decodeDimension(reader.getValues("status").getMaxPackedValue(), 0);
                        System.out.println(si.info.name + " -> " + si.info.getAttribute("bucket") + "[ " + min + ", " + max + "]");
                    } catch (Exception ex) {

                    }
                }
        );
        writer.close();

        DirectoryReader reader = DirectoryReader.open(dir);
        System.out.println("Total docs: " + reader.numDocs());
        reader.close();
        dir.close();
    }

    private PointsReader getPointReader(SegmentCommitInfo si) throws IOException {
        final Codec codec = si.info.getCodec();
        final Directory dir = si.info.dir;
        final Directory cfsDir;
        if (si.info.getUseCompoundFile()) {
            cfsDir = codec.compoundFormat().getCompoundReader(dir, si.info, IOContext.READONCE);
        } else {
            cfsDir = dir;
        }

        final FieldInfos coreFieldInfos = codec.fieldInfosFormat().read(cfsDir, si.info, "", IOContext.READONCE);
        final SegmentReadState segmentReadState =
                new SegmentReadState(cfsDir, si.info, coreFieldInfos, IOContext.READONCE);

        return codec.pointsFormat().fieldsReader(segmentReadState);
    }
}
