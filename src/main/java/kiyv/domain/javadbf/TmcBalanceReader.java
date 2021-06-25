package kiyv.domain.javadbf;

import com.linuxense.javadbf.DBFException;
import com.linuxense.javadbf.DBFReader;
import com.linuxense.javadbf.DBFRow;
import com.linuxense.javadbf.DBFUtils;
import kiyv.domain.tools.File1CReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class TmcBalanceReader {
    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());

/*    public static void main(String[] args) {
        String fileName = "c:/1C/Copy250106/RG1253.DBF";
        for (Integer i : new TmcBalanceReader().getAll(File1CReader.file2byteArray(fileName)).values()) {
            System.out.println(i);
        }
    }*/

    public Map<String, Integer> getAll(byte[] dataByteArray) {
        Map<String, Integer> tmcBalanceMap = new HashMap<>();
        if (dataByteArray.length == 0) {
            return tmcBalanceMap;
        }
        DBFReader reader = null;
        try {
            InputStream is = new ByteArrayInputStream(dataByteArray);
            reader = new DBFReader(is);
            DBFRow row;
            while ((row = reader.nextRow()) != null) {
                if (! row.getString("SP4643").equals("     C")) {
                    continue;
                }
                String idTmc = row.getString("SP1249");
                Integer count = row.getInt("SP1251");
                tmcBalanceMap.put(idTmc, count);
            }
            log.debug("Was read {} TMC Balance from 1C 'RG1253'.", tmcBalanceMap.size());
            return tmcBalanceMap;
        } catch (DBFException e) {
            log.warn("Exception during writing all 'TMC Balance'.", e);
        }
        finally {
            DBFUtils.close(reader);
        }
        log.debug("TMC Balance not found.");
        return new HashMap<>();
    }
}
