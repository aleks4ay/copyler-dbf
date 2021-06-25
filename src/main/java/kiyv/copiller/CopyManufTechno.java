package kiyv.copiller;

import kiyv.domain.dao.ManufDao;
import kiyv.domain.dao.ManufTechnoDaoJdbc;
import kiyv.domain.dao.TmcDaoTechnoJdbc;
import kiyv.domain.dao.UtilDao;
import kiyv.domain.exception.FileNotReadedException;
import kiyv.domain.javadbf.ManufReader;
import kiyv.domain.model.Journal;
import kiyv.domain.model.Manufacture;
import kiyv.domain.model.Tmc;
import kiyv.domain.tools.File1CReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class CopyManufTechno {
    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());

    public void run(String manufactureFileNames, Map<String, Journal> mapJournal) {
        log.info("   writing 'M A N U F A C T U R E for Techno'.");
        try {
            byte[] manufactureBytes = new File1CReader().file2byteArray(manufactureFileNames);
            UtilDao utilDao = new UtilDao();
            Connection connPostgres = utilDao.getConnPostgres();
            ManufDao manufTechnoDao = new ManufTechnoDaoJdbc(connPostgres);
            List<Tmc> tmcTechnoList = new TmcDaoTechnoJdbc(connPostgres).getAll();
            Map<String, Manufacture> mapManuf = new ManufReader().getAll(manufactureBytes);
            List<Manufacture> listNewManuf = new ArrayList<>();
            List<Manufacture> listUpdatingManuf = new ArrayList<>();
            Map<String, Manufacture> oldManuf = manufTechnoDao.getAll()
                    .stream()
                    .collect(Collectors.toMap(Manufacture::getId, Manufacture::getManufacture));
            for (Manufacture manufacture : mapManuf.values()) {
                String id = manufacture.getId();
                String idManuf = manufacture.getIdDoc();
                String idTmc = manufacture.getIdTmc();
                boolean idTmsIsTechno = tmcTechnoList
                        .stream()
                        .anyMatch(t -> t.getId().equals(idTmc));
                if (! idTmsIsTechno) {
                    continue;
                }
                if (mapJournal.containsKey(idManuf)) {
                    Journal journal = mapJournal.get(idManuf);
                    Timestamp dateManuf = journal.getDateCreate();
                    manufacture.setDocNumber(journal.getDocNumber());
                    manufacture.setTimeManufacture(dateManuf);
                    if (!oldManuf.containsKey(id)) {
                        listNewManuf.add(manufacture);
                    } else if (!oldManuf.get(id).equals(manufacture)) {
                        log.info("   UPDATE Manufacture for Techno with Id = '{}', '{}'. Different fields: {}.",
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
                manufTechnoDao.saveAll(listNewManuf);
            }
            if (listUpdatingManuf.size() > 0) {
                manufTechnoDao.updateAll(listUpdatingManuf);
            }
            if (oldManuf.size() > 0) {
                for (Manufacture manufacture : oldManuf.values()) {
                    log.info("   DELETE Manufacture with id '{}', '{}'.", manufacture.getId(), manufacture.getDocNumber());
                }
                manufTechnoDao.deleteAll(oldManuf.keySet());
            }
            utilDao.closeConnection(connPostgres);
        } catch (FileNotReadedException e) {
            e.printStackTrace();
        }
    }
}
