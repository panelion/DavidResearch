package com.nexr.platform.search.generator;

import com.nexr.platform.search.utils.io.AppendRootInputStream;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

public abstract class AbstractSdpLogGenerator implements LogGenerator {

    private DocumentBuilderFactory _documentBuilderFactory;

    public AbstractSdpLogGenerator() {
        _documentBuilderFactory = DocumentBuilderFactory.newInstance();
    }

    /**
     * Document 를 생성 한다.
     * @return  Document
     */
    protected Document createDocument() {

        Document document = null;
        try {
            DocumentBuilder documentBuilder;
            documentBuilder = _documentBuilderFactory.newDocumentBuilder();
            document  = documentBuilder.newDocument();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        }

        return document;
    }

    /**
     * String Xml Data 를 Element 형태로 변환 한다.
     * @param xmlRow    String Xml Data.
     * @return Element Xml Data Element
     */
    protected Element getElementByStrXml(String xmlRow) {

        try {
            DocumentBuilder documentBuilder = _documentBuilderFactory.newDocumentBuilder();
            Document readDocument = documentBuilder.parse(AppendRootInputStream.createInputStream(xmlRow));

            return readDocument.getDocumentElement();

        } catch (Exception e) {
            System.err.println(xmlRow);
        }

        return null;
    }

}
