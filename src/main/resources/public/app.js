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
});
