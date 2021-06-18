package kiyv.domain.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.stream.IntStream;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class DefaultDaoJdbc {
    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());

    public boolean deleteAll(Collection<String> listId, Connection connPostgres, String sqlDeleteAll) {
        try {
            int result = 0;
            for (String idDoc : listId) {
                PreparedStatement ps = connPostgres.prepareStatement(sqlDeleteAll);
                ps.setString(1, idDoc);
                ps.addBatch();
                int[] numberOfUpdates = ps.executeBatch();
                result += IntStream.of(numberOfUpdates).sum();
            }
            if (result == listId.size()) {
                log.debug("Try commit delete all");
                connPostgres.commit();
                log.debug("Commit - OK. {} rows deleted.", result);
                return true;
            }
            else {
                connPostgres.rollback();
                log.debug("Deleted {}, but need to delete {} rows. Not equals!!!", result, listId.size());
            }
        } catch (SQLException e) {
            log.warn("Exception during delete {} old rows. SQL = {}.", listId.size() , sqlDeleteAll, e);
        }
        return false;
    }
}
