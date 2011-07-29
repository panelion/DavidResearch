package com.nexr.platform.search.entity.sdp;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

public class SystemHeaderEntity extends BaseClass {
    public enum SYSTEM_HEADER {
        CID,
        SYSID,
        LT,
        UID,
        SCID
    }

    protected final String SystemHeaderName = "SHD";

    protected Element SHD;

    public Element getSHD() {
        return SHD;
    }

    public SystemHeaderEntity(Document document) {
        this.document = document;

        SHD = this.document.createElement(SystemHeaderName);

        for(SYSTEM_HEADER SH : SYSTEM_HEADER.values()) {
            SHD.appendChild(this.document.createElement(SH.name()));
        }
    }

    public void setCID(String value) {
        NodeList nodeList  = SHD.getElementsByTagName(SYSTEM_HEADER.CID.name());
        this.setValue(nodeList, value);
    }

    public void setSysId(String value) {
        NodeList nodeList  = SHD.getElementsByTagName(SYSTEM_HEADER.SYSID.name());
        this.setValue(nodeList, value);
    }

    public void setLT(String value) {
        NodeList nodeList  = SHD.getElementsByTagName(SYSTEM_HEADER.LT.name());
        this.setValue(nodeList, value);
    }

    public void setUID(String value) {
        NodeList nodeList  = SHD.getElementsByTagName(SYSTEM_HEADER.UID.name());
        this.setValue(nodeList, value);
    }

    public void setScId(String value) {
        NodeList nodeList  = SHD.getElementsByTagName(SYSTEM_HEADER.SCID.name());
        this.setValue(nodeList, value);
    }

    public String toString(){
        return this.toString(SHD);
    }

    public static void main(String[] args) throws ParserConfigurationException {
        DocumentBuilderFactory _documentBuilderFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder documentBuilder = _documentBuilderFactory.newDocumentBuilder();
        Document document = documentBuilder.newDocument();
        SystemHeaderEntity entity = new SystemHeaderEntity(document);

        entity.setCID("11111");
        entity.setLT("22222");
        entity.setScId("33333");
        entity.setSysId("44444");
        entity.setUID("555555");

        System.out.println(entity.toString());

    }
}
