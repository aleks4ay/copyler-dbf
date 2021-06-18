package kiyv.domain.javadbf;

import com.linuxense.javadbf.DBFException;
import com.linuxense.javadbf.DBFReader;
import com.linuxense.javadbf.DBFRow;
import com.linuxense.javadbf.DBFUtils;
import kiyv.domain.model.InvoiceDescription;
import kiyv.domain.tools.File1CReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class InvoiceDescriptionReader {
    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());

    public static void main(String[] args) {
        String fileName = "c:/1C/Copy250106/DT3592.DBF";
        new InvoiceDescriptionReader()
                .getAll(File1CReader.file2byteArray(fileName))
                .forEach(System.out::println);
    }

    public List<InvoiceDescription> getAll(byte[] dataByteArray) {
        List<InvoiceDescription> invoiceDescriptions = new ArrayList<>();
        if (dataByteArray.length == 0) {
            return invoiceDescriptions;
        }
        DBFReader reader = null;
        try {
            InputStream is = new ByteArrayInputStream(dataByteArray);
            reader = new DBFReader(is);
            DBFRow row;
            while ((row = reader.nextRow()) != null) {
                String idInvoice = row.getString("IDDOC");
                String idTmc = row.getString("SP3579");//Must not be: 'Go designer to size measurement', 'Shipment', 'Fixing', 'DELETED'
                if (idTmc.equalsIgnoreCase("   CBN") || idTmc.equalsIgnoreCase("   7LH") ||
                        idTmc.equalsIgnoreCase("   9VQ") || idTmc.equalsIgnoreCase("     0")) {
                    continue;
                }
                int position = row.getInt("LINENO");
                String id = idInvoice + "-" + position;
                int quantity = row.getInt("SP3581");
                double newPayment = row.getDouble("SP3589");
                InvoiceDescription invoice = new InvoiceDescription(id, idInvoice, idTmc, quantity, newPayment);
                invoiceDescriptions.add(invoice);
            }
            log.debug("Was read {} Invoice from 1C 'DH3592'.", invoiceDescriptions.size());
            return invoiceDescriptions;
        } catch (DBFException e) {
            log.warn("Exception during writing all 'Invoice'.", e);
        }
        finally {
            DBFUtils.close(reader);
        }
        log.debug("Invoice not found.");
        return null;
    }
}
