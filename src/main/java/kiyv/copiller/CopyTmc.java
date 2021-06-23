package kiyv.copiller;

import kiyv.domain.javadbf.TmcReader;
import kiyv.domain.tools.File1CReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import kiyv.domain.dao.*;
import kiyv.domain.model.Tmc;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class CopyTmc {
    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());

    public static void main(String[] args) {
        String fileName = "C:/KiyV_management2/DB_copy/SC302.DBF";
        new CopyTmc().run(fileName);
    }

    public void run(String fileName) {
        log.info("   writing 'T M C'.");
        byte[] bytesTmc = File1CReader.file2byteArray(fileName);
        UtilDao utilDao = new UtilDao();
        Connection connPostgres = utilDao.getConnPostgresWithoutException();
        TmcDao tmcDao = new TmcDaoJdbc(connPostgres);
        TmcReader tmcReader = new TmcReader();
        List<Tmc> listNewTmc = new ArrayList<>();
        List<Tmc> listUpdatingTmc = new ArrayList<>();
        Map<String, Tmc> oldTmc = tmcDao.getAll()
                .stream()
                .collect(Collectors.toMap(Tmc::getId, Tmc::getTmc));
        List<Tmc> listTmcFrom1C = tmcReader.getAll(bytesTmc);
        for (Tmc tmc : listTmcFrom1C) {
            String idComparedTmc = tmc.getId();
            if (!oldTmc.containsKey(idComparedTmc)) {
                listNewTmc.add(tmc);
            } else if (!oldTmc.get(idComparedTmc).equals(tmc)) {
                listUpdatingTmc.add(tmc);
                oldTmc.remove(idComparedTmc);
            }
            else {
                oldTmc.remove(idComparedTmc);
            }
        }
        if (listNewTmc.size() > 0) {
            tmcDao.saveAll(listNewTmc);
        }
        if (listUpdatingTmc.size() > 0) {
            tmcDao.updateAll(listUpdatingTmc);
        }
        if (oldTmc.size() > 0) {
            tmcDao.deleteAll(oldTmc.keySet());
        }
        utilDao.closeConnection(connPostgres);
    }
}

