package kiyv.domain.model;

public class Tmc {

    private String id;
    private String idParent;
    private String code;
    private String descr;
    private int isFolder;
    private String descrAll;
    private String type;

    public Tmc() {
    }

    public Tmc(String id) {
        this.id = id;
    }

    public Tmc(String id, String idParent, String code, String descr, int isFolder, String descrAll, String type) {
        this.id = id;
        this.idParent = idParent;
        this.code = code;
        this.descr = descr;
        this.isFolder = isFolder;
        this.descrAll = descrAll;
        this.type = type;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getIdParent() {
        return idParent;
    }

    public void setIdParent(String idParent) {
        this.idParent = idParent;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getDescr() {
        return descr;
    }

    public void setDescr(String descr) {
        this.descr = descr;
    }

    public int getIsFolder() {
        return isFolder;
    }

    public void setIsFolder(int isFolder) {
        this.isFolder = isFolder;
    }

    public String getDescrAll() {
        return descrAll;
    }

    public void setDescrAll(String descrAll) {
        this.descrAll = descrAll;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Tmc getTmc() {
        return this;
    }

    @Override
    public String toString() {
        return "Tmc{" +
                "id='" + id + '\'' +
                ", idParent='" + idParent + '\'' +
                ", code='" + code + '\'' +
                ", descr='" + descr + '\'' +
                ", isFolder=" + isFolder +
                ", descrAll='" + descrAll + '\'' +
                ", type='" + type + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (obj.getClass().equals(Tmc.class)) {
            Tmc tmc = (Tmc) obj;
            boolean equalsId = this.id.equals(tmc.getId());
            boolean equalsIdParent = this.idParent.equals(tmc.getIdParent());
            boolean equalsCode = this.code.equals(tmc.getCode());
            boolean equalsDescr = this.descr.equals(tmc.getDescr());
            boolean equalsIsFolder = (this.isFolder == tmc.getIsFolder());
            boolean equalsDescrAll = this.descrAll.equals(tmc.getDescrAll());
            boolean equalsType = this.type.equals(tmc.getType());

            return equalsId && equalsIdParent && equalsCode && equalsDescr && equalsIsFolder && equalsDescrAll && equalsType;
        }
        return false;
    }

    public boolean equalsTechno(Object obj) {
        if (obj.getClass().equals(Tmc.class)) {
            Tmc tmc = (Tmc) obj;
            boolean equalsId = this.id.equals(tmc.getId());
            boolean equalsIdParent = this.idParent.equals(tmc.getIdParent());
            boolean equalsDescr = this.descr.equals(tmc.getDescr());

            return equalsId && equalsIdParent && equalsDescr;
        }
        return false;
    }
}
