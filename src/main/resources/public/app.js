document.addEventListener('DOMContentLoaded', () => {
    const insightsContainer = document.getElementById('insights-container');
    const insightCountEl = document.getElementById('insight-count');
    const statusEl = document.getElementById('connection-status');
    
    let insightCount = 0;

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}/insights`;
    
    let ws;
    let pingInterval;

    function connect() {
        ws = new WebSocket(wsUrl);

        ws.onopen = () => {
            statusEl.textContent = 'ONLINE';
            statusEl.className = 'stat-value text-green';
            
            // Enviar PING a cada 30 segundos para evitar timeout de inatividade
            pingInterval = setInterval(() => {
                if(ws.readyState === WebSocket.OPEN) {
                    ws.send("ping");
                }
            }, 30000);
        };

        ws.onmessage = (event) => {
            if (event.data === "pong") return; // Ignora o pong de keep-alive
            
            try {
                const insight = JSON.parse(event.data);
                console.log("Recebido do WebSocket:", insight);
                addInsightToUI(insight);
            } catch (e) {
                console.error("Recebido dado não JSON:", event.data);
            }
        };

        ws.onclose = () => {
            clearInterval(pingInterval);
            statusEl.textContent = 'RECONECTANDO';
            statusEl.className = 'stat-value text-yellow';
            setTimeout(connect, 3000);
        };
        
        ws.onerror = (err) => {
            console.error('WebSocket Error:', err);
            ws.close();
        }
    }

    function addInsightToUI(insight) {
        const emptyState = document.querySelector('.empty-state');
        if (emptyState) emptyState.remove();

        const card = document.createElement('div');
        card.className = 'insight-card';

        // Deduplicação de Contra-Ataque no frontend (evita repetições do Kafka)
        if (insight.insight.includes("Contra-Ataque")) {
            const now = Date.now();
            if (window.lastContraAtaque && (now - window.lastContraAtaque) < 10000) {
                return; // Ignora se teve outro contra ataque nos últimos 10 segundos
            }
            window.lastContraAtaque = now;
        }

        // Lógica do Placar
        if (insight.insight.includes("GOL")) {
            if (insight.team.includes("Team A")) {
                let sA = document.getElementById('score-teamA');
                sA.textContent = parseInt(sA.textContent) + 1;
            } else if (insight.team.includes("Team B")) {
                let sB = document.getElementById('score-teamB');
                sB.textContent = parseInt(sB.textContent) + 1;
            }
        }

        // Definir a cor temática baseada na string
        const titleUpper = insight.insight.toUpperCase();
        if (titleUpper.includes("TUMULTO")) {
            card.classList.add('insight-tumulto');
        } else if (titleUpper.includes("CONTRA-ATAQUE")) {
            card.classList.add('insight-contraataque');
        } else if (titleUpper.includes("PRESSÃO")) {
            card.classList.add('insight-pressao');
        } else if (titleUpper.includes("TIKI")) {
            card.classList.add('insight-tiki');
        } else if (titleUpper.includes("DOMÍNIO")) {
            card.classList.add('insight-dominio');
        } else if (titleUpper.includes("GOL")) {
            card.classList.add('insight-gol');
        } else if (titleUpper.includes("CARTÃO")) {
            card.classList.add('insight-cartao');
        } else if (titleUpper.includes("FINALIZAÇÃO")) {
            card.classList.add('insight-finalizacao');
        }

        card.innerHTML = `
            <div class="insight-header">
                <span>🏆 ${insight.matchId}</span>
                <span>⏱️ ${insight.time}</span>
            </div>
            <div class="insight-title">${insight.insight}</div>
            <div class="insight-team">⚽ ${insight.team}</div>
        `;

        insightsContainer.prepend(card);

        insightCount++;
        insightCountEl.textContent = insightCount;

        if (insightsContainer.children.length > 50) {
            insightsContainer.lastChild.remove();
        }
    }

    connect();

    // HEATMAP LOGIC
    const hmWsUrl = `${protocol}//${window.location.host}/heatmap-ws`;
    const playersData = {}; // playerId -> stats

    const teamAPlayersList = document.getElementById('teamA-players');
    const teamBPlayersList = document.getElementById('teamB-players');
    const pitchContainer = document.getElementById('pitch-container');
    const selectedPlayerName = document.getElementById('selected-player-name');

    if(pitchContainer) {
        for(let i=0; i<100; i++) {
            const cell = document.createElement('div');
            cell.className = 'heatmap-cell';
            cell.id = `cell-${i}`;
            pitchContainer.appendChild(cell);
        }
    }

    let activePlayerId = null;

    let hmWs;
    function connectHeatmap() {
        hmWs = new WebSocket(hmWsUrl);
        hmWs.onmessage = (event) => {
            try {
                const stats = JSON.parse(event.data);
                if(!playersData[stats.playerId]) {
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
                playersData[stats.playerId] = stats;
                
                if (activePlayerId === stats.playerId) {
                    renderHeatmap(activePlayerId);
                }
            } catch(e) {}
        };
        hmWs.onclose = () => setTimeout(connectHeatmap, 3000);
    }

    function renderHeatmap(playerId) {
        activePlayerId = playerId;
        const stats = playersData[playerId];
        if(!stats) return;

        document.querySelectorAll('.team-list li').forEach(li => li.classList.remove('active'));
        const activeLi = document.getElementById(`li-player-${playerId}`);
        if(activeLi) activeLi.classList.add('active');
        
        selectedPlayerName.textContent = stats.playerName;

        document.querySelectorAll('.heatmap-cell').forEach(cell => cell.style.opacity = 0);
        
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

    connectHeatmap();
});
