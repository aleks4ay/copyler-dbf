package kiyv.domain.dao;

import kiyv.domain.model.Client;

import java.util.Collection;
import java.util.List;

public interface ClientDao {

    Client getById(String kod);

    List<Client> getAll();

    boolean saveAll(List<Client> clientList);

    boolean updateAll(List<Client> clientList);

    boolean deleteAll(Collection<String> clientList);
}
