package kiyv.domain.model;

public class Client {

    private String id;
    private String name;

    public Client() {
    }

    public Client(String id, String name) {
        this.id = id;
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }


    @Override
    public String toString() {
        return "Client{" +
                ", id='" + id + '\'' +
                ", name='" + name + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (obj.getClass().equals(Client.class)) {
            Client client = (Client) obj;
            return this.id.equals(client.getId()) && this.name.equals(client.getName());
        }
        return false;
    }

    public Client getClient() {
        return this;
    }
}
