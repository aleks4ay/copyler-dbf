package kiyv.domain.javadbf;

import com.linuxense.javadbf.DBFException;
import com.linuxense.javadbf.DBFReader;
import com.linuxense.javadbf.DBFRow;
import com.linuxense.javadbf.DBFUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class WorkerReader {
    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());

    public Map<String, String> getAll(byte[] dataByteArray) {
        Map<String, String> mapWorker = new HashMap<>();
        if (dataByteArray.length == 0) {
            return mapWorker;
        }
        mapWorker.put("     0", "-");
        DBFReader reader = null;
        try {
            InputStream is = new ByteArrayInputStream(dataByteArray);
            reader = new DBFReader(is);
            DBFRow row;
            while ((row = reader.nextRow()) != null) {
                String id = row.getString("ID");
                String name = new String(row.getString("DESCR").getBytes("ISO-8859-15"), "Windows-1251");
                mapWorker.put(id, name);
            }
            log.debug("Was read {} Worker from 1S.", mapWorker.size());
            return mapWorker;
        } catch (DBFException | IOException e) {
            log.warn("Exception during writing all 'Worker'." , e);
        }
        finally {
            DBFUtils.close(reader);
        }
        log.debug("Worker not found.");
        return null;
    }
}
