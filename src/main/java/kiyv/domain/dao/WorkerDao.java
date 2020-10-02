package kiyv.domain.dao;

import kiyv.domain.model.Worker;

import java.util.Collection;
import java.util.List;

public interface WorkerDao {

    Worker getById(String kod);

    List<Worker> getAll();

    boolean saveAll(List<Worker> workerList);

    boolean updateAll(List<Worker> workerList);

    boolean deleteAll(Collection<String> listId);
}
