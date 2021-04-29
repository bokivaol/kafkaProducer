
package com.mapr.examples.models;

import java.util.ArrayList;
import java.util.List;
import com.google.gson.annotations.SerializedName;

@SuppressWarnings("unused")
public class Test {

    @SerializedName("values")
    private ArrayList<Object> Values;

    public Object getValues() {
        return Values;
    }

    public void setValues(ArrayList<Object> values) {
        Values = values;
    }

}
