package com.panelion.utils;

import com.panelion.utils.io.AppendRootInputStream;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringWriter;

/**
 * Created by IntelliJ IDEA.
 * User: david
 * Date: 7/29/11
 * Time: 2:16 PM
 */
public class XmlUtils {

    private DocumentBuilderFactory _documentBuilderFactory;
    DocumentBuilder _documentBuilder;
    private Document _document;

    public Document getDocument() {
        return _document;
    }

    public XmlUtils() throws ParserConfigurationException {
        this.createDocument();
    }

    /**
     * Document 를 생성 한다.
     * @return  Document
     */
    public void createDocument() throws ParserConfigurationException {
        DocumentBuilderFactory _documentBuilderFactory = DocumentBuilderFactory.newInstance();
        _documentBuilder = _documentBuilderFactory.newDocumentBuilder();
        _document = _documentBuilder.newDocument();
    }

    /**
     * Element 를 생성 한다.
     * @param elementName   Element Name
     * @return              Element
     */
    public Element createElement(String elementName) {
        return _document.createElement(elementName);
    }

    public Element setTextValue(Element element, String value) {
        element.setTextContent(value);
        return element;
    }

    public Element setCDataValue(Element element, String cDataValue) {
        element.appendChild(_document.createCDATASection(cDataValue));
        return element;
    }

    /**
     * XmlData 를 파싱 하여 Element 형태로 생성 한다.
     * @param xmlRow
     * @return
     */
    public Element getElementByStrXml(String xmlRow) {

        Element element = null;

        try {
            Document readDocument = _documentBuilder.parse(AppendRootInputStream.createInputStream(xmlRow));
            element = readDocument.getDocumentElement();
        } catch (Exception e) {
            System.err.println(xmlRow);
        }

        return element;
    }

    /**
     * Child Node List 를 얻어 온다.
     * @param element   parent Element
     * @return          child Node List;
     */
    public NodeList getChildNodes(Element element) {
        NodeList nodeList = null;
        if(element.hasChildNodes()) {
            nodeList = element.getChildNodes();
        }
        return nodeList;
    }

    public Element setChildNodes(Element parentElement, Element childElement) {
        NodeList childNodeList = this.getChildNodes(parentElement);
        boolean hasChild = false;

        for(int i = 0 ; i < childNodeList.getLength(); i++) {
            if(childNodeList.item(i).equals(childElement)){
                hasChild = true;
            }
        }

        if(!hasChild) parentElement.appendChild(childElement);

        return parentElement;
    }

    /**
     * Element 의 Xml Data 를 String 형태로 리턴 한다.
     * @param element                변환 할 Element 개체
     * @return                       String Xml Data.
     * @throws javax.xml.transform.TransformerException  변환 도중 에러 발생.
     */
    public String getElementToString(Element element) throws TransformerException {

        StringWriter buffer = new StringWriter();

        TransformerFactory transFactory = TransformerFactory.newInstance();
        Transformer trans = transFactory.newTransformer();
        trans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        trans.transform(new DOMSource(element), new StreamResult(buffer));

        return buffer.toString();

    }

    public void close() {
        if(this._document != null) this._document = null;
        if(this._documentBuilder != null) this._documentBuilder = null;
    }

    public static void main(String[] args) {
        try {
            XmlUtils xmlUtils = new XmlUtils();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        }
    }


}
