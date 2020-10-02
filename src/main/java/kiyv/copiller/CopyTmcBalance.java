package kiyv.copiller;

import kiyv.domain.dao.*;
import kiyv.domain.javadbf.TmcBalanceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.*;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class CopyTmcBalance {

    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());


    public static void main(String[] args) {

        new CopyTmcBalance().doCopyNewRecord();

    }

    public void doCopyNewRecord() {
        long start = System.currentTimeMillis();
        log.info("Start writing 'T M C-techno'.");

        UtilDao utilDao = new UtilDao();
        Connection connPostgres = utilDao.getConnPostgres();

        TmcDaoTechnoBalanceJdbc tmcDao = new TmcDaoTechnoBalanceJdbc(connPostgres);
        TmcBalanceReader balanceReader = new TmcBalanceReader();

        Map<String, Integer> oldBalance = tmcDao.getAllBalance();
        Map<String, Integer> newBalance = balanceReader.getAll();
        Map<String, Integer> mapToUpdatingBalance = balanceReader.getAll();

        for (String idTmc : oldBalance.keySet()) {
            if (oldBalance.get(idTmc) != newBalance.get(idTmc)) {
                mapToUpdatingBalance.put(idTmc, newBalance.get(idTmc));
            }
        }

        if (mapToUpdatingBalance.size() > 0) {
            log.info("Write change to DataBase. Must be updated {} Balance of TMC-techno.", mapToUpdatingBalance.size());
            tmcDao.updateAllBalance(mapToUpdatingBalance);
        }


        long end = System.currentTimeMillis();
        log.info("End writing 'Balance of TMC-techno'. Time = {} c.", (double)(end-start)/1000);

        utilDao.closeConnection(connPostgres);
    }

}

