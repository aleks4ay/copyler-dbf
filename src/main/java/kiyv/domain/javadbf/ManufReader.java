package kiyv.domain.javadbf;

import com.linuxense.javadbf.DBFException;
import com.linuxense.javadbf.DBFReader;
import com.linuxense.javadbf.DBFRow;
import com.linuxense.javadbf.DBFUtils;
import kiyv.domain.model.Manufacture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class ManufReader {
    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());

    public static void main(String[] args) {
        Map<String, Manufacture> manufactureMap = new ManufReader().getAll();
        for (Manufacture c : manufactureMap.values()) {
            System.out.println(c.getIdDoc() + ", " + c.getIdOrder() + ", " + c.getDescrSecond());
        }
        System.out.println(manufactureMap.size());
    }

    public Map<String, Manufacture> getAll() {

        Map<String, Manufacture> mapManufacture = new HashMap<>();

        DBFReader reader = null;
        try {
            reader = new DBFReader(new FileInputStream("D:\\KiyV management2\\DB_copy\\DT2728.DBF"));

            DBFRow row;
            while ((row = reader.nextRow()) != null) {
                String key = row.getString("SP2722");
                if (key.equals("     0")) {
                    continue;
                }

                String idDoc = row.getString("IDDOC");
                String idOrder = row.getString("SP2722");
                int pos = row.getInt("LINENO");
                String id = idDoc + "-" + pos;
                int quantity = row.getInt("SP2725");

                String idTmc = row.getString("SP2721");
                String descrSecond = new String(row.getString("SP14726").getBytes("ISO-8859-15"), "Windows-1251");
                int sizeA = row.getInt("SP14722");
                int sizeB = row.getInt("SP14723");
                int sizeC = row.getInt("SP14724");
                String embodiment = row.getString("SP14725");

                Manufacture manufacture = new Manufacture(id, idDoc, pos, null, idOrder, null, 0L,
                        quantity, idTmc, descrSecond, sizeA, sizeB, sizeC, embodiment);

                mapManufacture.put(id, manufacture);
            }
            log.debug("Was read {} Manufacture from 1C 'DT2728'.", mapManufacture.size());

            return mapManufacture;

        } catch (DBFException | IOException e) {
            log.warn("Exception during reading file 'DT2728.dbf'.", e);
        } catch (Exception e) {
            log.warn("Exception during writing all 'Manufacture'.", e);
        }
        finally {
            DBFUtils.close(reader);
        }

        log.debug("Manufacture not found.");
        return null;
    }
}
