package kiyv.copiller;

import kiyv.domain.dao.*;
import kiyv.domain.javadbf.JournalReader;
import kiyv.domain.javadbf.ManufReader;
import kiyv.domain.model.Journal;
import kiyv.domain.model.Manufacture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class CopyManuf {

    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());


    public static void main(String[] args) {
        new CopyManuf().doCopyNewRecord();
    }


    public void doCopyNewRecord() {
        long start = System.currentTimeMillis();
        log.info("Start writing 'M A N U F A C T U R E'.");

        UtilDao utilDao = new UtilDao();
        Connection connPostgres = utilDao.getConnPostgres();

        StatusDao statusDao = new StatusDaoJdbc(connPostgres);
        ManufDao manufDao = new ManufDaoJdbc(connPostgres);
        OrderDao orderDao = new OrderDaoJdbc(connPostgres);

        JournalReader journalReader = new JournalReader();
        ManufReader manufReader = new ManufReader();


        List<String> listIdOrder = orderDao.getAllId();
        Map<String, Journal> mapJournal = journalReader.getAllJournal();
        Map<String, Manufacture> mapManuf = manufReader.getAll();

        List<Manufacture> listNewManuf = new ArrayList<>();
        List<Manufacture> listUpdatingManuf = new ArrayList<>();

        Map<String, Manufacture> oldManuf = manufDao.getAll()
                .stream()
                .collect(Collectors.toMap(Manufacture::getId, Manufacture::getManufacture));

        for (Manufacture manufacture : mapManuf.values()) {
            String id = manufacture.getId();
            String idDoc = manufacture.getIdDoc();
            String idOrder = manufacture.getIdOrder();

            if (mapJournal.containsKey(idDoc) && listIdOrder.contains(idOrder)) {
                Journal journal = mapJournal.get(idDoc);

                Timestamp dateManuf = journal.getDateCreate();

                manufacture.setDocNumber(journal.getDocNumber());
                manufacture.setTimeManufacture(dateManuf);
                manufacture.setTime21(dateManuf.getTime());


                if (!oldManuf.containsKey(id)) {
                    listNewManuf.add(manufacture);
                } else if (!oldManuf.get(id).equals(manufacture)) {
                    log.info("UPDATE Manufacture with Id = '{}', '{}'. Different fields: {}.",
                            manufacture.getId(),
                            manufacture.getDocNumber(),
                            manufacture.getDifferences(oldManuf.get(id))
                    );
                    listUpdatingManuf.add(manufacture);
                    oldManuf.remove(id);
                }
                else {
                    oldManuf.remove(id);
                }
            }
        }

        if (listNewManuf.size() > 0) {
            log.info("Save to DataBase. Must be added {} new Manufactures.", listNewManuf.size());
            manufDao.saveAll(listNewManuf);
            orderDao.saveFromManuf(listNewManuf);
            statusDao.saveFromManuf(listNewManuf);
        }
        if (listUpdatingManuf.size() > 0) {
            log.info("Write change to DataBase. Must be updated {} Manufactures.", listUpdatingManuf.size());
            manufDao.updateAll(listUpdatingManuf);
        }
        if (oldManuf.size() > 0) {
            log.info("Delete old Manufactures from DataBase. Must be deleted {} Manufactures.", oldManuf.size());
            for (Manufacture manufacture : oldManuf.values()) {
                log.info("DELETE Manufacture with id '{}', '{}'.", manufacture.getId(), manufacture.getDocNumber());
            }
            manufDao.deleteAll(oldManuf.keySet());
        }

        long end = System.currentTimeMillis();
        log.info("End writing 'M A N U F A C T U R E'. Time = {} c.", (double)(end-start)/1000);

        utilDao.closeConnection(connPostgres);
    }
}
