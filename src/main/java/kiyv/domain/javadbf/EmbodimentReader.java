package kiyv.domain.javadbf;

import com.linuxense.javadbf.DBFException;
import com.linuxense.javadbf.DBFReader;
import com.linuxense.javadbf.DBFRow;
import com.linuxense.javadbf.DBFUtils;
import kiyv.domain.tools.File1CReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class EmbodimentReader  {
    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());

/*    public static void main(String[] args) {
        String fileName = "c:/1C/Copy250106/SC14716.DBF";
        for (String s : new EmbodimentReader().getAllEmbodiment(File1CReader.file2byteArray(fileName)).values()) {
            System.out.println(s);
        }
    }*/

    public Map<String, String> getAllEmbodiment(byte[] dataByteArray) {
        Map<String, String> mapEmbodiment = new HashMap<>();
        if (dataByteArray.length == 0) {
            return mapEmbodiment;
        }
        DBFReader embodimentReader = null;
        try {
            InputStream is = new ByteArrayInputStream(dataByteArray);
            embodimentReader = new DBFReader(is);
            DBFRow embodimentRow;
            while ((embodimentRow = embodimentReader.nextRow()) != null) {
                String id = embodimentRow.getString("ID");
                String descr = new String(embodimentRow.getString("DESCR").getBytes("ISO-8859-15"), "Windows-1251");
                mapEmbodiment.put(id, descr);
            }
            log.debug("Was read {} rows from 1C SC14716.", mapEmbodiment.size());
            return mapEmbodiment;
        } catch (DBFException | IOException e) {
            log.warn("Exception during reading all rows 'Embodiment'.", e);
        } finally {
            DBFUtils.close(embodimentReader);
        }
        log.debug("Embodiment not found.");
        return new HashMap<>();
    }
}
