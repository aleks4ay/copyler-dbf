package kiyv.domain.javadbf;

import com.linuxense.javadbf.DBFException;
import com.linuxense.javadbf.DBFReader;
import com.linuxense.javadbf.DBFRow;
import com.linuxense.javadbf.DBFUtils;
import kiyv.domain.model.Invoice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class InvoiceReader {
    private String dbfPath = null;
    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());

    public InvoiceReader(String dbfPath) {
        this.dbfPath = dbfPath;
    }

    public Map<String, Invoice> getAll() {

        Map<String, Invoice> mapInvoice = new HashMap<>();

        DBFReader reader = null;
        try {
            reader = new DBFReader(new FileInputStream(dbfPath + "\\DH3592.DBF"));

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

        } catch (DBFException | IOException e) {
            log.warn("Exception during reading file 'DH3592.dbf'.", e);
        } catch (Exception e) {
            log.warn("Exception during writing all 'Invoice'.", e);
        }
        finally {
            DBFUtils.close(reader);
        }

        log.debug("Invoice not found.");
        return null;
    }
}
