package br.ufes.inf;

public class EventoTatico {
    private String matchId;
    private String team;
    private String insight;

    public EventoTatico() {} // Necessário para o Jackson

    public EventoTatico(String matchId, String team, String insight) {
        this.matchId = matchId;
        this.team = team;
        this.insight = insight;
    }

    public String getMatchId() { return matchId; }

    public void setMatchId(String matchId) { this.matchId = matchId; }

    public String getTeam() { return team; }

    public void setTeam(String team) { this.team = team; }

    public String getInsight() { return insight; }

    public void setInsight(String insight) { this.insight = insight; }
}