package br.ufes.inf;

public class EventoTatico {
    private String matchId;
    private String team;
    private String insight;
    private String tempoRegulamentar;

    public EventoTatico() {}

    public EventoTatico(String matchId, String team, String insight, String tempoRegulamentar) {
        this.matchId = matchId;
        this.team = team;
        this.insight = insight;
        this.tempoRegulamentar = tempoRegulamentar;
    }

    public String getMatchId() { return matchId; }

    public void setMatchId(String matchId) { this.matchId = matchId; }

    public String getTeam() { return team; }

    public void setTeam(String team) { this.team = team; }

    public String getInsight() { return insight; }

    public void setInsight(String insight) { this.insight = insight; }

    public String getTempoRegulamentar() { return tempoRegulamentar; }

    public void setTempoRegulamentar(String tempoRegulamentar) { this.tempoRegulamentar = tempoRegulamentar; }
}