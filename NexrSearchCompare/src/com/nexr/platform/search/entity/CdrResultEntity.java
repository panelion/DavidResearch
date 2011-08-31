package com.nexr.platform.search.entity;

/**
 * Created by IntelliJ IDEA.
 * User: david
 * Date: 11. 8. 23.
 * Time: 오전 10:35
 * To change this template use File | Settings | File Templates.
 */
public class CdrResultEntity {

    private final String i_out_ctn;
    private final String i_in_ctn;
    private final String i_release_time;
    private final String all;

    public CdrResultEntity(String i_out_ctn, String i_in_ctn, String i_release_time) {

        this.i_out_ctn = i_out_ctn;
        this.i_in_ctn = i_in_ctn;
        this.i_release_time = i_release_time;

        final String SEPARATOR = "@";

        this.all = this.i_out_ctn + SEPARATOR + this.i_in_ctn + SEPARATOR + this.i_release_time;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CdrResultEntity that = (CdrResultEntity) o;

        if (all != null ? !all.equals(that.all) : that.all != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return all != null ? all.hashCode() : 0;
    }

    @Override
    public String toString() {
        return i_out_ctn + "\t" + i_in_ctn + "\t" + i_release_time;
    }
}
