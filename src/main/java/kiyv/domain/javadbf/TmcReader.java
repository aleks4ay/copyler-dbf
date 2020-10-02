package kiyv.domain.javadbf;

import java.io.*;
import com.linuxense.javadbf.*;
import kiyv.domain.model.Tmc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class TmcReader {
    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());

    public List<Tmc> getAll() {

        List<Tmc> listTmc = new ArrayList<>();

        DBFReader reader = null;
        try {
            reader = new DBFReader(new FileInputStream("D:\\KiyV management2\\DB_copy\\SC302.DBF"));

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
            log.warn("Exception during reading file 'Tmc'.", e);
        } catch (Exception e) {
            log.warn("Exception during writing all 'Tmc'.", e);
        }
        finally {
            DBFUtils.close(reader);
        }

        log.debug("Tmc not found.");
        return null;
    }
}
