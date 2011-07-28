package com.nexr.platform.search.utils.io;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: david
 * Date: 7/5/11
 * Time: 5:02 PM
 * ROOT 가 없는 XML DATA 에 ROOT 를 씌워 주는 역할을 한다.
 */
public class AppendRootInputStream extends SequenceInputStream {

    private AppendRootInputStream(Enumeration<InputStream> inputStreamEnumeration) throws FileNotFoundException {
        super(inputStreamEnumeration);
    }

    public static InputStream createInputStream(String path, String rootElement) throws FileNotFoundException {
    return createInputStream(new File(path), rootElement);
    }

    public static InputStream createInputStream(File file, String rootElement) throws FileNotFoundException {

        String startElement = "<"+rootElement+">\n";
        String endElement = "</"+rootElement+">\n";

        ByteArrayInputStream startInputStream = new ByteArrayInputStream(startElement.getBytes());
        FileInputStream bodyInputStream = new FileInputStream(file);
        ByteArrayInputStream endInputStream = new ByteArrayInputStream(endElement.getBytes());

        List<InputStream> inputStreamList = new ArrayList<InputStream>();

        inputStreamList.add(startInputStream);
        inputStreamList.add(bodyInputStream);
        inputStreamList.add(endInputStream);

        return new AppendRootInputStream(Collections.enumeration(inputStreamList));
    }

    public static InputStream createInputStream(String xmlData) throws FileNotFoundException {
        List<InputStream> inputStreamList = new ArrayList<InputStream>();

        ByteArrayInputStream bodyInputStream = new ByteArrayInputStream(xmlData.getBytes());

        inputStreamList.add(bodyInputStream);

        return new AppendRootInputStream(Collections.enumeration(inputStreamList));
    }

}
