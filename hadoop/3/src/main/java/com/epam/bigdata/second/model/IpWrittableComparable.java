package com.epam.bigdata.second.model;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IpWrittableComparable implements WritableComparable {
    private double average;
    private int count;

    public IpWrittableComparable(double average, int count) {
        this.average = average;
        this.count = count;
    }

    public IpWrittableComparable() {
    }

    public double getAverage() {
        return average;
    }

    public void setAverage(double average) {
        this.average = average;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public int compareTo(Object o) {
        IpWrittableComparable comparable = (IpWrittableComparable) o;
        int averageResult = Double.compare(average, comparable.getAverage());
        int countResult = Integer.compare(count, comparable.getCount());
        return Integer.compare(averageResult, countResult);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(average);
        dataOutput.writeInt(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.average = dataInput.readDouble();
        this.count = dataInput.readInt();
    }

    @Override
    public boolean equals(Object o) {
        return EqualsBuilder.reflectionEquals(this, o);
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public String toString() {
        return average + "," + count;
    }
}
