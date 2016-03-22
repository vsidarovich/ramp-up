package com.epam.bigdata.second;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.RawComparator;

/**
 * Created by vsfmqueen on 10/4/15.
 */
public class LineComparator implements RawComparator<ByteWritable> {
        @Override
        public int compare(ByteWritable left, ByteWritable right) {
            return 0;
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return 0;
        }
}
