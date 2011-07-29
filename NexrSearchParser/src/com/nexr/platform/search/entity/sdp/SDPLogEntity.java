package com.nexr.platform.search.entity.sdp;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class SDPLogEntity extends BaseClass {

    protected final String LogName = "TXLG";
    protected Element TXLG;

    private BodyEntity bodyEntity;
    private DataHeaderEntity dataHeaderEntity;
    private SystemHeaderEntity systemHeaderEntity;

    public SDPLogEntity(Document document) {
        this.document = document;

        TXLG = this.document.createElement(LogName);

        bodyEntity = new BodyEntity(document);
        dataHeaderEntity = new DataHeaderEntity(document);
        systemHeaderEntity = new SystemHeaderEntity(document);

        TXLG.appendChild(systemHeaderEntity.getSHD());
        TXLG.appendChild(dataHeaderEntity.getDHD());
        TXLG.appendChild(bodyEntity.getBD());
    }

    public BodyEntity getBodyEntity() {
        return bodyEntity;
    }

    public DataHeaderEntity getDataHeaderEntity() {
        return dataHeaderEntity;
    }

    public SystemHeaderEntity getSystemHeaderEntity() {
        return systemHeaderEntity;
    }

    public String toString() {
        return this.toString(this.TXLG);
    }


}
