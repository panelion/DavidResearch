package com.nexr.platform.search.entity.sdp;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class DataHeaderEntity extends BaseClass {

    public enum DATA_HEADER {
        TXID,
        SEQ,
        TS,
        MT,
        SN,
        SIP,
        ON,
        MN,
        PLS
    }

    protected final String DataHeaderName = "DHD";

    protected Element DHD;

    public Element getDHD() {
        return DHD;
    }

    public DataHeaderEntity(Document document) {
        this.document = document;
        DHD = this.document.createElement(DataHeaderName);

        for(DATA_HEADER DH : DATA_HEADER.values()) {
            DHD.appendChild(this.document.createElement(DH.name()));
        }
    }

    public void setTxId(String value) {
        NodeList nodeList  = DHD.getElementsByTagName(DATA_HEADER.TXID.name());
        this.setValue(nodeList, value);
    }

    public void setSeq(String value) {
        NodeList nodeList  = DHD.getElementsByTagName(DATA_HEADER.SEQ.name());
        this.setValue(nodeList, value);
    }

    public void setTS(String value) {
        NodeList nodeList  = DHD.getElementsByTagName(DATA_HEADER.TS.name());
        this.setValue(nodeList, value);
    }

    public void setMT(String value) {
        NodeList nodeList  = DHD.getElementsByTagName(DATA_HEADER.MT.name());
        this.setValue(nodeList, value);
    }

    public void setSN(String value) {
        NodeList nodeList  = DHD.getElementsByTagName(DATA_HEADER.SN.name());
        this.setValue(nodeList, value);
    }

    public void setSIP(String value) {
        NodeList nodeList  = DHD.getElementsByTagName(DATA_HEADER.SIP.name());
        this.setValue(nodeList, value);
    }

    public void setON(String value) {
        NodeList nodeList  = DHD.getElementsByTagName(DATA_HEADER.ON.name());
        this.setValue(nodeList, value);
    }

    public void setMN(String value) {
        NodeList nodeList  = DHD.getElementsByTagName(DATA_HEADER.MN.name());
        this.setValue(nodeList, value);
    }

    public void setPLS(String value) {
        NodeList nodeList  = DHD.getElementsByTagName(DATA_HEADER.PLS.name());
        this.setValue(nodeList, value);
    }

    public String toString() {
        return this.toString(DHD);
    }
}
