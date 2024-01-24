package org.apache.lucene.index;

import org.apache.lucene.codecs.CompoundDirectory;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.IOUtils;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

public class MainTest {

    public static void main(String[] args) throws Exception {
        Directory indexDirectory = new NIOFSDirectory(Path.of(args[0]));
        SegmentInfos.readLatestCommit(indexDirectory).forEach(si -> {
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
                Set<Long> dvs = new HashSet<>();
                System.out.println("Count: " + cnt);
                vals.nextDoc();
                while (vals.docID() != DocIdSetIterator.NO_MORE_DOCS) {
                    //System.out.println( "DOCID ::: " + vals.docID() + " ---> " + vals.nextValue());
                    dvs.add(vals.nextValue());
                    vals.nextDoc();
                }
                System.out.println(String.format("%s : [%s] -> %s", si.info.name, si.info.getAttribute("bucket"), dvs));
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
    }
}
