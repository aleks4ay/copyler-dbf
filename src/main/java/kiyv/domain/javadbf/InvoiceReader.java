package kiyv.domain.javadbf;

import com.linuxense.javadbf.DBFException;
import com.linuxense.javadbf.DBFReader;
import com.linuxense.javadbf.DBFRow;
import com.linuxense.javadbf.DBFUtils;
import kiyv.domain.model.Invoice;
import kiyv.domain.tools.File1CReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class InvoiceReader {
    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());

    public static void main(String[] args) {
        String fileName = "c:/1C/Copy250106/DH3592.DBF";
        Map<String, Invoice> invoices = new InvoiceReader().getAll(File1CReader.file2byteArray(fileName));
        if (invoices.size() > 0) {
            for (Invoice i : invoices.values()) {
                System.out.println(i);
            }
        }
    }

    public Map<String, Invoice> getAll(byte[] dataByteArray) {
        Map<String, Invoice> mapInvoice = new HashMap<>();
        if (dataByteArray.length == 0) {
            return mapInvoice;
        }
        DBFReader reader = null;
        try {
            InputStream is = new ByteArrayInputStream(dataByteArray);
            reader = new DBFReader(is);
            DBFRow row;
            while ((row = reader.nextRow()) != null) {
                String key = row.getString("SP3561");
                if (key.equals("   0     0")) {
                    continue;
                }
                String idDoc = row.getString("IDDOC");
                String idOrder = row.getString("SP3561").substring(4);
                double newPayment = row.getDouble("SP3589");
                Invoice invoice = new Invoice(idDoc, null, idOrder, null, 0L, newPayment);
                mapInvoice.put(idDoc, invoice);
            }
            log.debug("Was read {} Invoice from 1C 'DH3592'.", mapInvoice.size());
            return mapInvoice;
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
