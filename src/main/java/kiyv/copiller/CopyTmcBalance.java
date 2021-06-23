package kiyv.copiller;

import kiyv.domain.dao.*;
import kiyv.domain.javadbf.TmcBalanceReader;
import kiyv.domain.tools.File1CReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.*;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class CopyTmcBalance {
    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());

    public void run(String tmcBalanceFileName) {
        log.info("   writing 'T M C-techno'.");
        byte[] bytesTmcBalance = File1CReader.file2byteArray(tmcBalanceFileName);
        UtilDao utilDao = new UtilDao();
        Connection connPostgres = utilDao.getConnPostgresWithoutException();
        TmcDaoTechnoBalanceJdbc tmcDao = new TmcDaoTechnoBalanceJdbc(connPostgres);
        TmcBalanceReader balanceReader = new TmcBalanceReader();
        Map<String, Integer> oldBalance = tmcDao.getAllBalance();
        Map<String, Integer> newBalance = balanceReader.getAll(bytesTmcBalance);
        Map<String, Integer> mapToUpdatingBalance = new HashMap<>();
        for (String idTmc : oldBalance.keySet()) {
            if (!oldBalance.get(idTmc).equals(newBalance.get(idTmc))) {
                mapToUpdatingBalance.put(idTmc, newBalance.get(idTmc));
            }
        }
        if (mapToUpdatingBalance.size() > 0) {
            tmcDao.updateAllBalance(mapToUpdatingBalance);
        }
        utilDao.closeConnection(connPostgres);
    }
}

