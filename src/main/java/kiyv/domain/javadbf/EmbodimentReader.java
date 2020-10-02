package kiyv.domain.javadbf;

import com.linuxense.javadbf.DBFException;
import com.linuxense.javadbf.DBFReader;
import com.linuxense.javadbf.DBFRow;
import com.linuxense.javadbf.DBFUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class EmbodimentReader  {

    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());

    public Map<String, String> getAllEmbodiment() {
        Map<String, String> mapEmbodiment = new HashMap<>();

        DBFReader embodimentReader = null;
        try {
            embodimentReader = new DBFReader(new FileInputStream("D:\\KiyV management2\\DB_copy\\SC14716.DBF"));

            DBFRow embodimentRow;
            while ((embodimentRow = embodimentReader.nextRow()) != null) {
                String id = embodimentRow.getString("ID");
                String descr = new String(embodimentRow.getString("DESCR").getBytes("ISO-8859-15"), "Windows-1251");
                mapEmbodiment.put(id, descr);
            }
            log.debug("Was read {} rows from 1C SC14716.", mapEmbodiment.size());
            return mapEmbodiment;
        } catch (DBFException | IOException e) {
            log.warn("Exception during reading file 'SC14716.dbf'.", e);
        } catch (Exception e) {
            log.warn("Exception during reading all rows 'Embodiment'.", e);
        } finally {
            DBFUtils.close(embodimentReader);
        }
        log.debug("Embodiment not found.");
        return null;
    }
}
