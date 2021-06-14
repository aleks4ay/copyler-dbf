package kiyv.domain.javadbf;

import com.linuxense.javadbf.DBFException;
import com.linuxense.javadbf.DBFReader;
import com.linuxense.javadbf.DBFRow;
import com.linuxense.javadbf.DBFUtils;
import kiyv.domain.model.Tmc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class TmcBalanceReader {
    private String dbfPath = null;
    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());

    public TmcBalanceReader(String dbfPath) {
        this.dbfPath = dbfPath;
    }

    public Map<String, Integer> getAll() {
        Map<String, Integer> tmcBalanceMap = new HashMap<>();
        DBFReader reader = null;
        try {
            reader = new DBFReader(new FileInputStream(dbfPath + "\\RG1253.DBF"));

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

        } catch (DBFException | IOException e) {
            log.warn("Exception during reading file 'RG1253.dbf'.", e);
        } catch (Exception e) {
            log.warn("Exception during writing all 'TMC Balance'.", e);
        }
        finally {
            DBFUtils.close(reader);
        }

        log.debug("TMC Balance not found.");
        return null;
    }
}
