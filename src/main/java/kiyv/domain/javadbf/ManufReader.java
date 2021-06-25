package kiyv.domain.javadbf;

import com.linuxense.javadbf.*;
import kiyv.domain.model.Manufacture;
import kiyv.domain.tools.File1CReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class ManufReader {
    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());

    public Map<String, Manufacture> getWithOrder(byte[] dataByteArray) {
        return getAllTemplate("with order", dataByteArray);
    }

    public Map<String, Manufacture> getWithoutOrder(byte[] dataByteArray) {
        return getAllTemplate("without order", dataByteArray);
    }

    public Map<String, Manufacture> getAll(byte[] dataByteArray) {
        return getAllTemplate("all Manufacture", dataByteArray);
    }

    private Map<String, Manufacture> getAllTemplate(String typeOfProduct, byte[] dataByteArray) {
        Map<String, Manufacture> mapManufacture = new HashMap<>();
        if (dataByteArray.length == 0) {
            return mapManufacture;
        }

        DBFReader reader = null;
        try {
            InputStream is = new ByteArrayInputStream(dataByteArray);
            reader = new DBFReader(is);
            DBFRow row;
            while ((row = reader.nextRow()) != null) {
                String key = row.getString("SP2722");
                if (typeOfProduct.equalsIgnoreCase("without order")) {
                    if (! key.equals("     0")) {
                        continue;
                    }
                }
                if (typeOfProduct.equalsIgnoreCase("with order")) {
                    if (key.equals("     0")) {
                        continue;
                    }
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
            log.debug("Was read {} Manufacture from 1C.", mapManufacture.size());
            return mapManufacture;
        } catch (DBFException | IOException e) {
            log.warn("Exception during writing all 'Manufacture'.", e);
        }
        finally {
            DBFUtils.close(reader);
        }
        log.debug("Manufacture not found.");
        return new HashMap<>();
    }
}
