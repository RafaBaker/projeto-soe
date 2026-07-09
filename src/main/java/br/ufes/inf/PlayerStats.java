package br.ufes.inf;

import java.util.HashMap;
import java.util.Map;

public class PlayerStats {
    private String playerId;
    private String playerName;
    private String teamId;
    private String teamName;
    private Map<String, Integer> heatmapGrid = new HashMap<>();

    public PlayerStats() {}

    public PlayerStats(String playerId, String playerName, String teamId, String teamName) {
        this.playerId = playerId;
        this.playerName = playerName;
        this.teamId = teamId;
        this.teamName = teamName;
    }

    public void addAction(double x, double y) {
        // O campo varia de 0.0 a 1.0.
        int gridX = (int) (x * 10);
        int gridY = (int) (y * 10);
        
        // Evita que o valor 1.0 exeda o array (índice 10)
        if (gridX >= 10) gridX = 9;
        if (gridY >= 10) gridY = 9;
        if (gridX < 0) gridX = 0;
        if (gridY < 0) gridY = 0;
        
        String key = gridX + "," + gridY;
        heatmapGrid.put(key, heatmapGrid.getOrDefault(key, 0) + 1);
    }

    public String getPlayerId() {
        return playerId;
    }

    public void setPlayerId(String playerId) {
        this.playerId = playerId;
    }

    public String getPlayerName() {
        return playerName;
    }

    public void setPlayerName(String playerName) {
        this.playerName = playerName;
    }

    public String getTeamId() {
        return teamId;
    }

    public void setTeamId(String teamId) {
        this.teamId = teamId;
    }

    public String getTeamName() {
        return teamName;
    }

    public void setTeamName(String teamName) {
        this.teamName = teamName;
    }

    public Map<String, Integer> getHeatmapGrid() {
        return heatmapGrid;
    }

    public void setHeatmapGrid(Map<String, Integer> heatmapGrid) {
        this.heatmapGrid = heatmapGrid;
    }
}
