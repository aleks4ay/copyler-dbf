package kiyv.copiller;

import kiyv.domain.dao.*;
import kiyv.domain.javadbf.InvoiceDescriptionReader;
import kiyv.domain.model.Invoice;
import kiyv.domain.model.InvoiceDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class CopyInvoiceDescription {

    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());

    public static void main(String[] args) {
        new CopyInvoiceDescription().doCopyNewRecord();
    }

    public void doCopyNewRecord() {
        long start = System.currentTimeMillis();
        log.info("Start writing 'I N V O I C E   D E S C R I P T I O N'.");

        UtilDao utilDao = new UtilDao();
        Connection connPostgres = utilDao.getConnPostgres();

        InvoiceDao invoiceDao = new InvoiceDaoJdbc(connPostgres);
        InvoiceDescriptionDaoJdbc invoiceDescriptionDaoJdbc = new InvoiceDescriptionDaoJdbc(connPostgres);

        InvoiceDescriptionReader invoiceDescriptionReader = new InvoiceDescriptionReader();

        Map<String, Invoice> mapInvoice = invoiceDao.getAll()
                .stream()
                .collect(Collectors.toMap(Invoice::getIdDoc, Invoice::getInvoice));


        List<InvoiceDescription> listInvoiceDescription = invoiceDescriptionReader.getAll();

        List<InvoiceDescription> listNewDescription = new ArrayList<>();
        List<InvoiceDescription> listUpdatingDescription = new ArrayList<>();

        Map<String, InvoiceDescription> mapOldInvoiceDescription = invoiceDescriptionDaoJdbc.getAll()
                .stream()
                .collect(Collectors.toMap(InvoiceDescription::getId, InvoiceDescription::getInvoiceDescription));

        for (InvoiceDescription newInvoiseDescription : listInvoiceDescription) {
            String newId = newInvoiseDescription.getId();
            String idInvoice = newInvoiseDescription.getIdInvoice();

            if (!mapInvoice.containsKey(idInvoice)) {
                continue;
            }
            if (!mapOldInvoiceDescription.containsKey(newId)) {
                listNewDescription.add(newInvoiseDescription);
            } else if (!mapOldInvoiceDescription.get(newId).equals(newInvoiseDescription)) {
                log.info("UPDATE InvoiseDescription with code '{}'. Different fields: {}.",
                        newId,
                        newInvoiseDescription.getDifferences(mapOldInvoiceDescription.get(newId))
                );
                listUpdatingDescription.add(newInvoiseDescription);
                mapOldInvoiceDescription.remove(newId);
            }
            else {
                mapOldInvoiceDescription.remove(newId);
            }
        }

        if (listNewDescription.size() > 0) {
            log.info("Save to DataBase. Must be added {} new InvoiseDescription.", listNewDescription.size());
            invoiceDescriptionDaoJdbc.saveAll(listNewDescription);
        }
        if (listUpdatingDescription.size() > 0) {
            log.info("Write change to DataBase. Must be updated {} InvoiseDescription.", listUpdatingDescription.size());
            invoiceDescriptionDaoJdbc.updateAll(listUpdatingDescription);
        }
        if (mapOldInvoiceDescription.size() > 0) {
            log.info("Delete old InvoiseDescription from DataBase. Must be deleted {} InvoiseDescription.", mapOldInvoiceDescription.size());
            for (InvoiceDescription description : mapOldInvoiceDescription.values()) {
                log.info("DELETE InvoiseDescription with Code '{}'.", description.getId());
            }
            invoiceDescriptionDaoJdbc.deleteAll(mapOldInvoiceDescription.keySet());
        }

        long end = System.currentTimeMillis();
        log.info("End writing 'I N V O I C E   D E S C R I P T I O N'. Time = {} c.", (double)(end-start)/1000);

        utilDao.closeConnection(connPostgres);
    }
}
