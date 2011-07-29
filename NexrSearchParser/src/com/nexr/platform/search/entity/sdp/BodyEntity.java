package com.nexr.platform.search.entity.sdp;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class BodyEntity extends BaseClass {

    public enum BODY {
        LTP,
        RC,
        RD,
        EC,
        ED,
        PL
    }

    protected final String BodyName = "BD";

    protected Element BD;

    public Element getBD() {
        return BD;
    }

    public BodyEntity(Document document) {
        this.document = document;
        BD = this.document.createElement(BodyName);
        for(BODY body : BODY.values()) {
            BD.appendChild(this.document.createElement(body.name()));
        }
    }

    public void setLTP(String value) {
        NodeList nodeList  = BD.getElementsByTagName(BODY.LTP.name());
        this.setValue(nodeList, value);
    }

    public void setRC(String value) {
        NodeList nodeList = BD.getElementsByTagName(BODY.RC.name());
        this.setValue(nodeList, value);
    }
    public void setRD(String value) {
        NodeList nodeList = BD.getElementsByTagName(BODY.RD.name());
        this.setValue(nodeList, value);
    }
    public void setEC(String value) {
        NodeList nodeList = BD.getElementsByTagName(BODY.EC.name());
        this.setValue(nodeList, value);
    }
    public void setED(String value) {
        NodeList nodeList = BD.getElementsByTagName(BODY.ED.name());
        this.setValue(nodeList, value);
    }
    public void setPL(String value) {
        NodeList nodeList = BD.getElementsByTagName(BODY.PL.name());
        this.setCDATAValue(nodeList, value);
    }

    public String toString() {
        return this.toString(BD);
    }
}
