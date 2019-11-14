package com.cecs.model;

public class Artist {
    private float terms_freq;
    private String terms;
    private String name;
    private String familiarity;
    private String longitude;
    private String id;
    private String location;
    private String latitude;
    private String similar;
    private float hotness;

    public Artist(float terms_freq, String terms, String name, String familiarity, String longitude, String id,
            String location, String latitude, String similar, float hotness) {
        this.terms_freq = terms_freq;
        this.terms = terms;
        this.name = name;
        this.familiarity = familiarity;
        this.longitude = longitude;
        this.id = id;
        this.location = location;
        this.latitude = latitude;
        this.similar = similar;
        this.hotness = hotness;
    }

    public float getTerms_freq() {
        return terms_freq;
    }

    public void setTerms_freq(float terms_freq) {
        this.terms_freq = terms_freq;
    }

    public String getTerms() {
        return terms;
    }

    public void setTerms(String terms) {
        this.terms = terms;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFamiliarity() {
        return familiarity;
    }

    public void setFamiliarity(String familiarity) {
        this.familiarity = familiarity;
    }

    public String getLongitude() {
        return longitude;
    }

    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getLatitude() {
        return latitude;
    }

    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }

    public String getSimilar() {
        return similar;
    }

    public void setSimilar(String similar) {
        this.similar = similar;
    }

    public float getHotness() {
        return hotness;
    }

    public void setHotness(float hotness) {
        this.hotness = hotness;
    }

    @Override
    public String toString() {
        return name;
    }
}
