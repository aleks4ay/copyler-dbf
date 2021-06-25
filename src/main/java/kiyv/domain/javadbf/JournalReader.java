package kiyv.domain.javadbf;

import java.io.*;
import com.linuxense.javadbf.*;
import kiyv.copiller.CopyTmc;
import kiyv.domain.tools.File1CReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import kiyv.domain.model.Journal;
import kiyv.domain.tools.TimeConverter;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class JournalReader {
    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());

/*    public static void main(String[] args) {
        String fileName = "c:/1C/Copy250106/1SJOURN.DBF";
        Map<String, Journal> allJournal = new JournalReader().getAllJournal(File1CReader.file2byteArray(fileName));
    }*/

    public Map<String, Journal> getAllJournal(byte[] dataByteArray) {
        Map<String, Journal> mapJournal = new HashMap<>();
        if (dataByteArray.length == 0) {
            return mapJournal;
        }
        DBFReader reader = null;
        try {
            InputStream is = new ByteArrayInputStream(dataByteArray);
            reader = new DBFReader(is);
            DBFRow row;
            while ((row = reader.nextRow()) != null) {
                Date keyOrderYear = row.getDate("DATE");
                int keyOrderIsEnable = row.getInt("CLOSED");
                if (keyOrderYear.getTime() < /*1580897872000L*/1560805200000L || keyOrderIsEnable ==4 ) {
                    continue;
                }
                String idDoc = row.getString("IDDOC");
                String docNumber = new String(row.getString("DOCNO").getBytes("ISO-8859-15"), "Windows-1251");
                long dateCreate = row.getDate("DATE").getTime();
                long timeCreate = TimeConverter.convertStrToTimeMillisecond(row.getString("TIME"));
                Journal journal = new Journal(0, idDoc, docNumber, new Timestamp(dateCreate + timeCreate));
                mapJournal.put(idDoc, journal);
            }
            log.debug("Was read {} Journal from 1C '1SJOURN'.", mapJournal.size());
            return mapJournal;
        } catch (DBFException | IOException e) {
            log.warn("Exception during writing all 'Journal'.", e);
        }
        finally {
            DBFUtils.close(reader);
        }
        log.debug("Journal not found.");
        return new HashMap<>();
    }
}
