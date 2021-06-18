package kiyv.domain.javadbf;

import com.linuxense.javadbf.*;
import kiyv.domain.model.Tmc;
import kiyv.domain.tools.File1CReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class TmcReader {
    private static final Logger log = LoggerFactory.getLogger(TmcReader.class);

    public static void main(String[] args) {
        String fileName = "c:/1C/Copy250106/SC302.DBF";
        new TmcReader()
                .getAll(File1CReader.file2byteArray(fileName))
                .forEach(System.out::println);
    }

    public List<Tmc> getAll(byte[] dataByteArray) {
        List<Tmc> listTmc = new ArrayList<>();
        if (dataByteArray.length == 0) {
            return listTmc;
        }
        DBFReader reader = null;
        try {
            InputStream is = new ByteArrayInputStream(dataByteArray);
            reader = new DBFReader(is);
            DBFRow row;
            while ((row = reader.nextRow()) != null) {
                String id = row.getString("ID");
                String parentId = row.getString("PARENTID");
                String code = row.getString("CODE");
                String descr = new String(row.getString("DESCR").getBytes("ISO-8859-1"), "Windows-1251");
                int isFolder = row.getInt("ISFOLDER");
                String descrAll = new String(row.getString("SP276").getBytes("ISO-8859-1"), "Windows-1251");
                String type = row.getString("SP277");
                listTmc.add(new Tmc(id, parentId, code, descr, isFolder, descrAll, type));
            }
            log.debug("Was read {} Tmc from 1S.", listTmc.size());
            return listTmc;
        } catch (DBFException | IOException e) {
            log.warn("Exception during writing all 'Tmc'.", e);
        }
        finally {
            DBFUtils.close(reader);
        }
        log.debug("Tmc not found.");
        return null;
    }
}
