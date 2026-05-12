package br.ufes.inf;

public class Location {
    private Integer frame;
    private Double time;
    private Double x;
    private Double y;

    public Location(Integer frame, Double time, Double x, Double y) {
        this.frame = frame;
        this.time = time;
        this.x = x;
        this.y = y;
    }

    public Integer getFrame() {
        return frame;
    }

    public void setFrame(Integer frame) {
        this.frame = frame;
    }

    public Double getTime() {
        return time;
    }

    public void setTime(Double time) {
        this.time = time;
    }

    public Double getX() {
        return x;
    }

    public void setX(Double x) {
        this.x = x;
    }

    public Double getY() {
        return y;
    }

    public void setY(Double y) {
        this.y = y;
    }
}
