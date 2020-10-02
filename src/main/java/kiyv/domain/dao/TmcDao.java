package kiyv.domain.dao;

import kiyv.domain.model.Tmc;

import java.util.Collection;
import java.util.List;

public interface TmcDao {

    Tmc getById(String id);

    List<Tmc> getAll();

    boolean saveAll(List<Tmc> tmcList);

    boolean updateAll(List<Tmc> tmcList);

    boolean deleteAll(Collection<String> listId);
}
