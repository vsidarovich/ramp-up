package com.epam.bigdata.hive.second;

import eu.bitwalker.useragentutils.OperatingSystem;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class UserAgentFunction extends GenericUDTF {

    private static final String UNKNOWN = "unknown";
    private PrimitiveObjectInspector stringOI = null;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        System.out.println("Initialize");
        stringOI = (PrimitiveObjectInspector) args[0];

        List<String> fieldNames = new ArrayList<>(3);
        List<ObjectInspector> fieldOIs = new ArrayList<>(3);
        fieldNames.add(args[1].toString());
        fieldNames.add(args[2].toString());
        fieldNames.add(args[3].toString());

        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] record) throws HiveException {
        String userAgentInfo = ((LazyString) (record[0])).toString();

        UserAgent userAgent = UserAgent.parseUserAgentString(userAgentInfo);

        String browser = userAgent.getBrowser() == null ? UNKNOWN : userAgent.getBrowser().getName();

        String os = UNKNOWN;
        String device = UNKNOWN;

        OperatingSystem osSystem = userAgent.getOperatingSystem();

        if (userAgent.getOperatingSystem() != null) {
            os = osSystem.getName();
            if (osSystem.getDeviceType() != null) {
                device = osSystem.getDeviceType().getName();
            }
        }

        Object[] result = Arrays.asList(browser, os, device).toArray();
        forward(result);
    }

    @Override
    public void close() throws HiveException {
        // do nothing
    }
}
