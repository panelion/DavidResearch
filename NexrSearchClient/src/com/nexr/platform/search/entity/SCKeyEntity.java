package com.nexr.platform.search.entity;

/**
 * Created by IntelliJ IDEA.
 * User: david
 * Date: 11. 8. 12.
 * Time: 오후 10:05
 */
public class SCKeyEntity {
    final String iSwitch;
    final String iBsc;
    final String iCell;
    final String iEndTime;
    final String all;
    final String separator = "@";

    public String getAll() {
        return all;
    }

    public SCKeyEntity(String iSwitch, String iBsc, String iCell, String iEndTime) {
        this.iSwitch = getValidValue(iSwitch) ;
        this.iBsc = getValidValue(iBsc);
        this.iCell = getValidValue(iCell);
        this.iEndTime = getValidValue(iEndTime);
        this.all = iSwitch + separator + iBsc + separator + iCell + separator + iEndTime;
    }

    @Override
    public int hashCode() {
        return all.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof SCKeyEntity && all.equals(((SCKeyEntity) o).all);
    }
    
    private String getValidValue(String value) {
        return isNull(value) ? "null" : value;
    }

    private boolean isNull(String value) {
        return value == null || value.trim().length() == 0 || value.equalsIgnoreCase("null");
    }
}

