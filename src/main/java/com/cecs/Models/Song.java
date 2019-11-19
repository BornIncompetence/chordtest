package com.cecs.Models;

public class Song {
    private String key;
    private String mode_confidence;
    private String artist_mbtags_count;
    private String key_confidence;
    private String tatums_start;
    private String year;
    private String duration;
    private String hotttnesss;
    private String beats_start;
    private String time_signature_confidence;
    private String title;
    private String bars_confidence;
    private String id;
    private String bars_start;
    private String artist_mbtags;
    private String start_of_fade_out;
    private String tempo;
    private String end_of_fade_in;
    private String beats_confidence;
    private String tatums_confidence;
    private String mode;
    private String time_signature;
    private String loudness;
    private String artist;

    public Song(String key, String mode_confidence, String artist_mbtags_count, String key_confidence,
                String tatums_start, String year, String duration, String hotttnesss, String beats_start,
                String time_signature_confidence, String title, String bars_confidence, String id, String bars_start,
                String artist_mbtags, String start_of_fade_out, String tempo, String end_of_fade_in,
                String beats_confidence, String tatums_confidence, String mode, String time_signature, String loudness,
                String artist) {
        this.key = key;
        this.mode_confidence = mode_confidence;
        this.artist_mbtags_count = artist_mbtags_count;
        this.key_confidence = key_confidence;
        this.tatums_start = tatums_start;
        this.year = year;
        this.duration = duration;
        this.hotttnesss = hotttnesss;
        this.beats_start = beats_start;
        this.time_signature_confidence = time_signature_confidence;
        this.title = title;
        this.bars_confidence = bars_confidence;
        this.id = id;
        this.bars_start = bars_start;
        this.artist_mbtags = artist_mbtags;
        this.start_of_fade_out = start_of_fade_out;
        this.tempo = tempo;
        this.end_of_fade_in = end_of_fade_in;
        this.beats_confidence = beats_confidence;
        this.tatums_confidence = tatums_confidence;
        this.mode = mode;
        this.time_signature = time_signature;
        this.loudness = loudness;
        this.artist = artist;
    }

    public String getArtist() {
        return artist;
    }

    public void setArtist(String artist) {
        this.artist = artist;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getMode_confidence() {
        return mode_confidence;
    }

    public void setMode_confidence(String mode_confidence) {
        this.mode_confidence = mode_confidence;
    }

    public String getArtist_mbtags_count() {
        return artist_mbtags_count;
    }

    public void setArtist_mbtags_count(String artist_mbtags_count) {
        this.artist_mbtags_count = artist_mbtags_count;
    }

    public String getKey_confidence() {
        return key_confidence;
    }

    public void setKey_confidence(String key_confidence) {
        this.key_confidence = key_confidence;
    }

    public String getTatums_start() {
        return tatums_start;
    }

    public void setTatums_start(String tatums_start) {
        this.tatums_start = tatums_start;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }

    public String getHotttnesss() {
        return hotttnesss;
    }

    public void setHotttnesss(String hotttnesss) {
        this.hotttnesss = hotttnesss;
    }

    public String getBeats_start() {
        return beats_start;
    }

    public void setBeats_start(String beats_start) {
        this.beats_start = beats_start;
    }

    public String getTime_signature_confidence() {
        return time_signature_confidence;
    }

    public void setTime_signature_confidence(String time_signature_confidence) {
        this.time_signature_confidence = time_signature_confidence;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getBars_confidence() {
        return bars_confidence;
    }

    public void setBars_confidence(String bars_confidence) {
        this.bars_confidence = bars_confidence;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getBars_start() {
        return bars_start;
    }

    public void setBars_start(String bars_start) {
        this.bars_start = bars_start;
    }

    public String getArtist_mbtags() {
        return artist_mbtags;
    }

    public void setArtist_mbtags(String artist_mbtags) {
        this.artist_mbtags = artist_mbtags;
    }

    public String getStart_of_fade_out() {
        return start_of_fade_out;
    }

    public void setStart_of_fade_out(String start_of_fade_out) {
        this.start_of_fade_out = start_of_fade_out;
    }

    public String getTempo() {
        return tempo;
    }

    public void setTempo(String tempo) {
        this.tempo = tempo;
    }

    public String getEnd_of_fade_in() {
        return end_of_fade_in;
    }

    public void setEnd_of_fade_in(String end_of_fade_in) {
        this.end_of_fade_in = end_of_fade_in;
    }

    public String getBeats_confidence() {
        return beats_confidence;
    }

    public void setBeats_confidence(String beats_confidence) {
        this.beats_confidence = beats_confidence;
    }

    public String getTatums_confidence() {
        return tatums_confidence;
    }

    public void setTatums_confidence(String tatums_confidence) {
        this.tatums_confidence = tatums_confidence;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getTime_signature() {
        return time_signature;
    }

    public void setTime_signature(String time_signature) {
        this.time_signature = time_signature;
    }

    public String getLoudness() {
        return loudness;
    }

    public void setLoudness(String loudness) {
        this.loudness = loudness;
    }

    @Override
    public String toString() {
        return title;
    }
}
