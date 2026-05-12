package br.ufes.inf;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class EventoFutebol {
    private int index;
    private Item team;
    private Item type;
    private List<Item> subtypes;
    private Location start;
    private Location end;
    private int period;
    private Item from;
    private Item to;

    public EventoFutebol(int index, Item team, Item type, List<Item> subtypes, Location start, Location end, int period, Item from, Item to) {
        this.index = index;
        this.team = team;
        this.type = type;
        this.subtypes = subtypes;
        this.start = start;
        this.end = end;
        this.period = period;
        this.from = from;
        this.to = to;
    }

    public EventoFutebol() {}

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public Item getTeam() {
        return team;
    }

    public void setTeam(Item team) {
        this.team = team;
    }

    public Item getType() {
        return type;
    }

    public void setType(Item type) {
        this.type = type;
    }

    public List<Item> getSubtypes() {
        return subtypes;
    }

    public void setSubtypes(List<Item> subtypes) {
        this.subtypes = subtypes;
    }

    public Location getStart() {
        return start;
    }

    public void setStart(Location start) {
        this.start = start;
    }

    public Location getEnd() {
        return end;
    }

    public void setEnd(Location end) {
        this.end = end;
    }

    public int getPeriod() {
        return period;
    }

    public void setPeriod(int period) {
        this.period = period;
    }

    public Item getFrom() {
        return from;
    }

    public void setFrom(Item from) {
        this.from = from;
    }

    public Item getTo() {
        return to;
    }

    public void setTo(Item to) {
        this.to = to;
    }

    public void imprimeEvento() {
        switch (this.getType().getName()) {
            case "PASS":
                System.out.printf("[%.2f] Jogador %s tocou a bola para %s\n", this.getEnd().getTime() + (this.getPeriod()*45), this.getFrom().getName(), this.getTo().getName());
        }
    }

    @Override
    public String toString() {
        return "EventoFutebol{" +
                "index=" + index +
                ", team=" + team +
                ", subtypes=" + subtypes +
                ", start=" + start +
                ", end=" + end +
                ", period=" + period +
                ", from=" + from +
                ", to=" + to +
                '}';
    }
}
