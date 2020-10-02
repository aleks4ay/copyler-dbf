package kiyv.domain.model;

public class Description {

    private String id;
    private String idDoc;
    private int position;
    private String idTmc;
    private int quantity;
    private String descrSecond;
    private int sizeA;
    private int sizeB;
    private int sizeC;
    private String embodiment;
    private Status status;


    public Description(String id, String idDoc, int position, String idTmc, int quantity,
                       String descrSecond, int sizeA, int sizeB, int sizeC, String embodiment) {
        this.id = id;
        this.idDoc = idDoc;
        this.position = position;
        this.idTmc = idTmc;
        this.quantity = quantity;
        this.descrSecond = descrSecond;
        this.sizeA = sizeA;
        this.sizeB = sizeB;
        this.sizeC = sizeC;
        this.embodiment = embodiment;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getIdDoc() {
        return idDoc;
    }

    public void setIdDoc(String idDoc) {
        this.idDoc = idDoc;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public String getIdTmc() {
        return idTmc;
    }

    public void setIdTmc(String idTmc) {
        this.idTmc = idTmc;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public String getDescrSecond() {
        return descrSecond;
    }

    public void setDescrSecond(String descrSecond) {
        this.descrSecond = descrSecond;
    }

    public int getSizeA() {
        return sizeA;
    }

    public void setSizeA(int sizeA) {
        this.sizeA = sizeA;
    }

    public int getSizeB() {
        return sizeB;
    }

    public void setSizeB(int sizeB) {
        this.sizeB = sizeB;
    }

    public int getSizeC() {
        return sizeC;
    }

    public void setSizeC(int sizeC) {
        this.sizeC = sizeC;
    }

    public String getEmbodiment() {
        return embodiment;
    }

    public void setEmbodiment(String embodiment) {
        this.embodiment = embodiment;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public Description getDescription() {
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj.getClass().equals(Description.class)) {
            Description description = (Description) obj;

            boolean equalsId = this.id.equals(description.id);
            boolean equalsOrder = this.idDoc.equals(description.idDoc);
            boolean equalsPosition = (this.position == description.getPosition());
            boolean equalsTmc = this.idTmc.equals(description.idTmc);
//            boolean equalsPrice = (this.price == description.getPrice());
            boolean equalsQuantity = (this.quantity == description.getQuantity());
            boolean equalsDescrSecond = this.descrSecond.equals(description.getDescrSecond());
            boolean equalsSizeA = (this.sizeA == description.getSizeA());
            boolean equalsSizeB = (this.sizeB == description.getSizeB());
            boolean equalsSizeC = (this.sizeC == description.getSizeC());

            boolean equalsEmbodiment = this.embodiment.equals(description.getEmbodiment());

            return equalsId & equalsOrder & equalsPosition & equalsTmc & equalsDescrSecond & equalsSizeA
                    & equalsSizeB & equalsSizeC & equalsQuantity & equalsEmbodiment;
        }
        return false;
    }

    public String getDifferences(Description description) {
        String result = "";
        if (! this.id.equals(description.id)) {
            result += "id ['" + description.id + "' --> '" + this.id + "'] ";
        }
        if (! this.idDoc.equals(description.idDoc) ) {
            result += "idDoc ['" + description.idDoc + "' --> '" + this.idDoc + "'] ";
        }
        if (this.position != description.position) {
            result += "position ['" + description.position + "' --> '" + this.position + "'] ";
        }
        if (! this.idTmc.equals(description.idTmc) ) {
            result += "idTmc ['" + description.idTmc + "' --> '" + this.idTmc + "'] ";
        }
        if (this.quantity != description.quantity) {
            result += "quantity ['" + description.quantity + "' --> '" + this.quantity + "'] ";
        }
        if (! this.descrSecond.equals(description.descrSecond) ) {
            result += "descrSecond ['" + description.descrSecond + "' --> '" + this.descrSecond + "'] ";
        }
        if (this.sizeA != description.sizeA) {
            result += "sizeA ['" + description.sizeA + "' --> '" + this.sizeA + "'] ";
        }
        if (this.sizeB != description.sizeB) {
            result += "sizeB ['" + description.sizeB + "' --> '" + this.sizeB + "'] ";
        }
        if (this.sizeC != description.sizeC) {
            result += "sizeC ['" + description.sizeC + "' --> '" + this.sizeC + "'] ";
        }
        if (! this.embodiment.equals(description.embodiment) ) {
            result += "embodiment ['" + description.embodiment + "' --> '" + this.embodiment + "'] ";
        }
//        if (! this.status.getDescrFirst().equals(description.status.getDescrFirst()) ) {
//            result += "descrFirst ['" + description.status.getDescrFirst() + "' --> '" + this.status.getDescrFirst() + "'] ";
//        }

        return result;
    }

    @Override
    public String toString() {
        return "Description{" +
                "id='" + id + '\'' +
                ", idDoc='" + idDoc + '\'' +
                ", position=" + position +
                ", idTmc='" + idTmc + '\'' +
                ", quantity=" + quantity +
                ", descrFirst='" + status.getDescrFirst() + '\'' +
                ", descrSecond='" + descrSecond + '\'' +
                ", sizeA=" + sizeA +
                ", sizeB=" + sizeB +
                ", sizeC=" + sizeC +
                ", embodiment='" + embodiment + '\'' +
                '}';
    }
}
