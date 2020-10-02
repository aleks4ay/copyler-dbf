package kiyv.domain.model;

public class InvoiceDescription {
    private String id;
    private String idInvoice;
    private String idTmc;
    private int quantity = 0;
    private double payment;

    public InvoiceDescription(String id, String idInvoice, String idTmc, int quantity, double payment) {
        this.id = id;
        this.idInvoice = idInvoice;
        this.idTmc = idTmc;
        this.quantity = quantity;
        this.payment = payment;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getIdInvoice() {
        return idInvoice;
    }

    public void setIdInvoice(String idInvoice) {
        this.idInvoice = idInvoice;
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

    public double getPayment() {
        return payment;
    }

    public void setPayment(double payment) {
        this.payment = payment;
    }

    public InvoiceDescription getInvoiceDescription() {
        return this;
    }


    @Override
    public boolean equals(Object obj) {
        if (obj.getClass().equals(InvoiceDescription.class)) {
            InvoiceDescription invDescr = (InvoiceDescription) obj;

            boolean equalsId = this.id.equals(invDescr.id);
            boolean equalsIdInvoice = this.idInvoice.equals(invDescr.idInvoice);
            boolean equalsTmc = this.idTmc.equals(invDescr.idTmc);
            boolean equalsQuantity = (this.quantity == invDescr.getQuantity());
            boolean equalsPrice = (this.payment == invDescr.getPayment());

            return equalsId & equalsIdInvoice & equalsTmc & equalsQuantity & equalsPrice;
        }
        return false;
    }

    public String getDifferences(InvoiceDescription invDescr) {
        String result = "";
        if (! this.id.equals(invDescr.id)) {
            result += "id ['" + invDescr.id + "' --> '" + this.id + "'] ";
        }
        if (! this.idInvoice.equals(invDescr.idInvoice) ) {
            result += "idInvoice ['" + invDescr.idInvoice + "' --> '" + this.idInvoice + "'] ";
        }
        if (! this.idTmc.equals(invDescr.idTmc) ) {
            result += "idTmc ['" + invDescr.idTmc + "' --> '" + this.idTmc + "'] ";
        }
        if (this.quantity != invDescr.quantity) {
            result += "quantity ['" + invDescr.quantity + "' --> '" + this.quantity + "'] ";
        }
        if (this.payment != invDescr.payment) {
            result += "payment ['" + invDescr.payment + "' --> '" + this.payment + "'] ";
        }
        return result;
    }

    @Override
    public String toString() {
        return "InvoiceDescription{" +
                "id='" + id + '\'' +
                ", idInvoice='" + idInvoice + '\'' +
                ", idTmc='" + idTmc + '\'' +
                ", quantity=" + quantity +
                ", payment=" + payment +
                '}';
    }
}
