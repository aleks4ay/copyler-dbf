package kiyv.domain.javadbf;

import com.linuxense.javadbf.DBFException;
import com.linuxense.javadbf.DBFReader;
import com.linuxense.javadbf.DBFRow;
import com.linuxense.javadbf.DBFUtils;
import kiyv.domain.model.Description;
import kiyv.domain.tools.File1CReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class DescriptionReader  {
    private static final Logger log = LoggerFactory.getLogger(DescriptionReader.class);

    public static void main(String[] args) {
        String fileName = "c:/1C/Copy250106/DT1898.DBF";
        new DescriptionReader()
                .getAll(File1CReader.file2byteArray(fileName))
                .forEach(System.out::println);
    }

    public List<Description> getAll(byte[] dataByteArray) {
        List<Description> descriptionList = new ArrayList<>();
        if (dataByteArray.length == 0) {
            return descriptionList;
        }
        DBFReader reader = null;
        try {
            InputStream is = new ByteArrayInputStream(dataByteArray);
            reader = new DBFReader(is);
            DBFRow row;
            while ((row = reader.nextRow()) != null) {
                String idDoc = row.getString("IDDOC");
                String idTmc = row.getString("SP1902");//Must not be: 'Go designer to size measurement', 'Shipment', 'Fixing', 'DELETED'
                if (idTmc.equalsIgnoreCase("   CBN") || idTmc.equalsIgnoreCase("   7LH") ||
                        idTmc.equalsIgnoreCase("   9VQ") || idTmc.equalsIgnoreCase("     0")) {
                    continue;
                }
                int position = row.getInt("LINENO");
                int quantity = row.getInt("SP1905");
                String descrSecond = new String(row.getString("SP14676").getBytes("ISO-8859-15"), "Windows-1251");
                int sizeA = row.getInt("SP14686");
                int sizeB = row.getInt("SP14687");
                int sizeC = row.getInt("SP14688");
                String idEmbodiment = row.getString("SP14717");
                String kod = idDoc + "-" + position;
                descriptionList.add(new Description(kod, idDoc, position, idTmc, quantity, descrSecond, sizeA, sizeB, sizeC, idEmbodiment));
            }
            log.debug("Was read {} Description from 1C 'DT1898'.", descriptionList.size());
            return descriptionList;
        } catch (DBFException | IOException e) {
            log.warn("Exception during writing all 'Description'.", e);
        }
        finally {
            DBFUtils.close(reader);
        }
        log.debug("Description not found.");
        return null;
    }
}
