package kiyv.run;

import kiyv.domain.dao.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.List;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class CopyOldState {

    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());

    public static void main(String[] args) {

        new CopyOldState().copyStatusState();

    }

    public void copyStatusState() {
        long start = System.currentTimeMillis();
        log.info("Start copy state 'S T A T U S' from database KiyV.");

        UtilDao utilDao = new UtilDao();
        Connection connPostgresFrom = utilDao.getConnPostgresFrom();
        Connection connPostgres = utilDao.getConnPostgres();

        StatusCopy statusCopy = new StatusCopy(connPostgresFrom, connPostgres);

        List<String> listStatusState = statusCopy.getAll();

        if (listStatusState.size() > 0) {
            log.info("Save to database KiyV-3. Must be copied {} state Status.", listStatusState.size());
            statusCopy.updateState(listStatusState);
        }
        long end = System.currentTimeMillis();
        log.info("End copy state 'S T A T U S'. Time = {} c.", (double)(end-start)/1000);

        utilDao.closeConnection(connPostgres);
        utilDao.closeConnection(connPostgresFrom);
    }
}
