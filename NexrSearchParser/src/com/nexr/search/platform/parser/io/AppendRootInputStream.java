package com.nexr.search.platform.parser.io;

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
 * To change this template use File | Settings | File Templates.
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
}
