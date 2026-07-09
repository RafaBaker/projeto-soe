document.addEventListener('DOMContentLoaded', () => {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    
    // UI Elements
    const gameSelect = document.getElementById('game-select');
    const insightsContainer = document.getElementById('insights-container');
    const insightCountEl = document.getElementById('insight-count');
    const statusEl = document.getElementById('connection-status');
    const scoreTeamA = document.getElementById('score-teamA');
    const scoreTeamB = document.getElementById('score-teamB');
    const teamAPlayersList = document.getElementById('teamA-players');
    const teamBPlayersList = document.getElementById('teamB-players');
    const pitchContainer = document.getElementById('pitch-container');
    const selectedPlayerName = document.getElementById('selected-player-name');

    // Init Grid
    if(pitchContainer) {
        for(let i=0; i<100; i++) {
            const cell = document.createElement('div');
            cell.className = 'heatmap-cell';
            cell.id = `cell-${i}`;
            pitchContainer.appendChild(cell);
        }
    }

    // State
    const matchesData = {};
    let currentMatchId = null;
    let activePlayerId = null;

    gameSelect.addEventListener('change', (e) => {
        currentMatchId = e.target.value;
        activePlayerId = null;
        teamAPlayersList.innerHTML = '';
        teamBPlayersList.innerHTML = '';
        renderFullUI();
    });

    function getOrCreateMatch(matchId) {
        if (!matchesData[matchId]) {
            matchesData[matchId] = {
                scoreA: 0, scoreB: 0, 
                insights: [],
                playersData: {},
                insightCount: 0
            };
            const option = document.createElement('option');
            option.value = matchId;
            option.textContent = `🏆 Partida: ${matchId}`;
            gameSelect.appendChild(option);
            
            // Remove the disabled 'Aguardando' option if present
            const disabledOpt = gameSelect.querySelector('option[disabled]');
            if(disabledOpt) disabledOpt.remove();

            if (!currentMatchId) {
                gameSelect.value = matchId;
                currentMatchId = matchId;
            }
        }
        return matchesData[matchId];
    }

    function renderFullUI() {
        if(!currentMatchId) return;
        const data = matchesData[currentMatchId];
        
        scoreTeamA.textContent = data.scoreA;
        scoreTeamB.textContent = data.scoreB;
        insightCountEl.textContent = data.insightCount;
        
        insightsContainer.innerHTML = '';
        data.insights.forEach(html => insightsContainer.insertAdjacentHTML('beforeend', html));
        
        if (data.insights.length === 0) {
            insightsContainer.innerHTML = `<div class="insight-card empty-state"><p>Aguardando eventos...</p></div>`;
        }

        // Render players that aren't rendered yet
        for(let pId in data.playersData) {
            const stats = data.playersData[pId];
            if (!document.getElementById(`li-player-${pId}`)) {
                const li = document.createElement('li');
                li.textContent = stats.playerName || stats.playerId;
                li.onclick = () => renderHeatmap(pId);
                li.id = `li-player-${pId}`;
                
                if(stats.teamName && stats.teamName.includes('Team A')) {
                    teamAPlayersList.appendChild(li);
                } else {
                    teamBPlayersList.appendChild(li);
                }
            }
        }
        
        renderHeatmap(activePlayerId);
    }

    function renderHeatmap(playerId, isLiveUpdate = false) {
        if (!isLiveUpdate) {
            activePlayerId = playerId;
            document.querySelectorAll('.team-list li').forEach(li => li.classList.remove('active'));
            const activeLi = document.getElementById(`li-player-${playerId}`);
            if(activeLi) activeLi.classList.add('active');
        }
        
        if(!playerId || !currentMatchId) {
            selectedPlayerName.textContent = "Selecione um Jogador";
            document.querySelectorAll('.heatmap-cell').forEach(cell => cell.style.opacity = 0);
            return;
        }

        const data = matchesData[currentMatchId];
        const stats = data.playersData[playerId];
        if(!stats) return;

        if (!isLiveUpdate) {
            selectedPlayerName.textContent = stats.playerName;
            document.querySelectorAll('.heatmap-cell').forEach(cell => cell.style.opacity = 0);
        }
        
        if(!stats.heatmapGrid) return;
        
        let maxVal = 0;
        for(let key in stats.heatmapGrid) {
            if(stats.heatmapGrid[key] > maxVal) maxVal = stats.heatmapGrid[key];
        }

        for(let key in stats.heatmapGrid) {
            const parts = key.split(",");
            const x = parseInt(parts[0]);
            const y = parseInt(parts[1]); 
            const cellIndex = y * 10 + x; 
            const cell = document.getElementById(`cell-${cellIndex}`);
            if(cell && maxVal > 0) {
                const intensity = stats.heatmapGrid[key] / maxVal;
                cell.style.opacity = intensity * 0.9; 
            }
        }
    }

    // INSIGHTS WS
    function connectInsights() {
        const ws = new WebSocket(`${protocol}//${window.location.host}/insights`);
        ws.onopen = () => { statusEl.textContent = 'ONLINE'; statusEl.className = 'stat-value text-green'; };
        ws.onmessage = (event) => {
            if (event.data === "pong") return; 
            try {
                const insight = JSON.parse(event.data);
                if(!insight.matchId) return;
                const data = getOrCreateMatch(insight.matchId);
                
                if (insight.insight.includes("Contra-Ataque")) {
                    const now = Date.now();
                    if (data.lastContraAtaque && (now - data.lastContraAtaque) < 10000) return;
                    data.lastContraAtaque = now;
                }

                if (insight.insight.includes("GOL")) {
                    if (insight.team.includes("Team A")) data.scoreA++;
                    else if (insight.team.includes("Team B")) data.scoreB++;
                }

                const titleUpper = insight.insight.toUpperCase();
                let colorClass = '';
                if (titleUpper.includes("TUMULTO")) colorClass = 'insight-tumulto';
                else if (titleUpper.includes("CONTRA-ATAQUE")) colorClass = 'insight-contraataque';
                else if (titleUpper.includes("PRESSÃO")) colorClass = 'insight-pressao';
                else if (titleUpper.includes("TIKI")) colorClass = 'insight-tiki';
                else if (titleUpper.includes("DOMÍNIO")) colorClass = 'insight-dominio';
                else if (titleUpper.includes("GOL")) colorClass = 'insight-gol';
                else if (titleUpper.includes("CARTÃO")) colorClass = 'insight-cartao';
                else if (titleUpper.includes("FINALIZAÇÃO")) colorClass = 'insight-finalizacao';

                const cardHtml = `<div class="insight-card ${colorClass}">
                    <div class="insight-header"><span>🏆 ${insight.matchId}</span><span>⏱️ ${insight.time}</span></div>
                    <div class="insight-title">${insight.insight}</div>
                    <div class="insight-team">⚽ ${insight.team}</div>
                </div>`;
                
                data.insights.unshift(cardHtml);
                if(data.insights.length > 50) data.insights.pop();
                data.insightCount++;
                
                if(currentMatchId === insight.matchId) {
                    scoreTeamA.textContent = data.scoreA;
                    scoreTeamB.textContent = data.scoreB;
                    insightCountEl.textContent = data.insightCount;
                    
                    const emptyState = document.querySelector('.empty-state');
                    if (emptyState) emptyState.remove();
                    
                    insightsContainer.insertAdjacentHTML('afterbegin', cardHtml);
                    if (insightsContainer.children.length > 50) {
                        insightsContainer.lastElementChild.remove();
                    }
                }
            } catch(e) {}
        };
        ws.onclose = () => { statusEl.textContent = 'RECONECTANDO'; statusEl.className = 'stat-value text-yellow'; setTimeout(connectInsights, 3000); };
    }

    // HEATMAP WS
    function connectHeatmap() {
        const hmWs = new WebSocket(`${protocol}//${window.location.host}/heatmap-ws`);
        hmWs.onmessage = (event) => {
            try {
                const stats = JSON.parse(event.data);
                if(!stats.matchId) return;
                const data = getOrCreateMatch(stats.matchId);
                
                const isNewPlayer = !data.playersData[stats.playerId];
                data.playersData[stats.playerId] = stats;
                
                if(currentMatchId === stats.matchId) {
                    if (isNewPlayer) {
                        const li = document.createElement('li');
                        li.textContent = stats.playerName || stats.playerId;
                        li.onclick = () => renderHeatmap(stats.playerId);
                        li.id = `li-player-${stats.playerId}`;
                        if(stats.teamName && stats.teamName.includes('Team A')) {
                            teamAPlayersList.appendChild(li);
                        } else {
                            teamBPlayersList.appendChild(li);
                        }
                    }
                    if (activePlayerId === stats.playerId) {
                        renderHeatmap(activePlayerId, true);
                    }
                }
            } catch(e) {}
        };
        hmWs.onclose = () => setTimeout(connectHeatmap, 3000);
    }

    connectInsights();
    connectHeatmap();
});
