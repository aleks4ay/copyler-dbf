package kiyv.domain.javadbf;

import java.io.*;
import com.linuxense.javadbf.*;
import kiyv.domain.model.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class DescriptionReader  {

    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());

    public static void main(String[] args) {
        List<Description> descriptionList = new DescriptionReader().getAll();
        for (Description d : descriptionList) {
            System.out.println(d.getId() + ", " + d.getIdDoc() + ", " + d.getPosition() + ", " + d.getIdTmc() + ", " +
                    d.getQuantity() + ", " + d.getDescrSecond() + ", " + d.getEmbodiment() +
                    d.getSizeA() + ", " + d.getSizeB() + ", " + d.getSizeB());
        }
        System.out.println(descriptionList.size());
    }


    public List<Description> getAll() {

        List<Description> descriptionList = new ArrayList<>();
        DBFReader reader = null;
        try {
            reader = new DBFReader(new FileInputStream("D:\\KiyV management2\\DB_copy\\DT1898.DBF"));

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
//                double price = row.getDouble("SP14681");

                String kod = idDoc + "-" + position;

                descriptionList.add(new Description(kod, idDoc, position, idTmc, quantity, descrSecond, sizeA, sizeB, sizeC, idEmbodiment));
            }
            log.debug("Was read {} Description from 1C 'DT1898'.", descriptionList.size());

            return descriptionList;

        } catch (DBFException | IOException e) {
            log.warn("Exception during reading file 'DT1898.dbf'.", e);
        } catch (Exception e) {
            log.warn("Exception during writing all 'Description'.", e);
        }
        finally {
            DBFUtils.close(reader);
        }

        log.debug("Description not found.");
        return null;
    }
}
