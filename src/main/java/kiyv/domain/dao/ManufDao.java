package kiyv.domain.dao;

import kiyv.domain.model.Manufacture;

import java.util.Collection;
import java.util.List;

public interface ManufDao {

    Manufacture getById(String id);

    List<Manufacture> getAll();

    boolean saveAll(List<Manufacture> manufactureList);

    boolean updateAll(List<Manufacture> manufactureList);

    boolean deleteAll(Collection<String> listIdDoc);
}
