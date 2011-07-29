package com.nexr.platform.search.entity.sdp;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringWriter;

public class BaseClass {

    protected Document document;
    private Transformer trans;

    public BaseClass() {

        try {
            TransformerFactory transFactory = TransformerFactory.newInstance();
            trans = transFactory.newTransformer();
            trans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");

        } catch (TransformerConfigurationException e) {
            e.printStackTrace();
        }

    }

    protected void setValue(NodeList nodeList, String value) {
        if(nodeList.getLength() == 1){
            nodeList.item(0).setTextContent(value);
        }
    }

    protected void setCDATAValue(NodeList nodeList, String value) {
        if(nodeList.getLength() == 1)
            if(document != null) {
                Node node = nodeList.item(0);

                if(node.hasChildNodes()){
                    NodeList childNodeList = node.getChildNodes();
                    for(int i = childNodeList.getLength() - 1; i >= 0; i--) {
                        node.removeChild(childNodeList.item(i));
                    }
                }

                node.appendChild(document.createCDATASection(value));

            }
    }

    protected String toString(Element element) {

        StringWriter buffer = new StringWriter();

        try {
            trans.transform(new DOMSource(element), new StreamResult(buffer));
        } catch (TransformerException e) { }

        return buffer.toString();
    }


    public String generateCData(String methodName, String objectName, String TXID) {
        StringBuffer sb = new StringBuffer();
        sb.append("End of Flow ");
        sb.append(methodName);
        sb.append(". Input Message: =");
        sb.append("<sdp:sdpmessage xmlns:sdp=\"http://xml.accenture.com/sdp/sdpmessage\">");
        sb.append("<sdp:header>");
        sb.append("<ns2:servicelabel xmlns:ns2=\"http://xml.accenture.com/sdp/header\">");
        sb.append(objectName);
        sb.append("</ns2:servicelabel>");
        sb.append("</sdp:header>");
        sb.append("<sdp:body>");
        sb.append("<body:credential xmlns:body=\"http://xml.accenture.com/sdp/body\">");
        sb.append("<cred:TXID xmlns:cred=\"http://xml.accenture.com/sdp/core/uum/credential\">");
        sb.append(TXID);
        sb.append("</cred:TXID>");
        sb.append("</body:credential>");
        sb.append("<body:party xmlns:body=\"http://xml.accenture.com/sdp/body\">");
        sb.append("<par:partyidentificationnumber xmlns:par=\"http://xml.accenture.com/sdp/core/uum/party\"/>");
        sb.append("</body:party></sdp:body></sdp:sdpmessage>");

        return sb.toString();
    }

}
