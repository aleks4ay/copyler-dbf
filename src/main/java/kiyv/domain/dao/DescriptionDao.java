package kiyv.domain.dao;

import kiyv.domain.model.Description;

import java.util.Collection;
import java.util.List;

public interface DescriptionDao {

    Description getById(String id);

    List<Description> getAll();

    boolean saveAll(List<Description> descriptionList);

    boolean updateAll(List<Description> descriptionList);

    boolean deleteAll(Collection<String> listId);
}
