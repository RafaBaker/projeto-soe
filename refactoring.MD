Evolução do Projeto SOE: Kafka Streams, CEP e Arquitetura Web 

Para atender aos novos requisitos, o projeto evoluirá de consumidores Kafka básicos para uma topologia inteligente usando Kafka Streams, e ganhará uma
          interface Web em tempo real.

## 1. Stack Tecnológica Proposta

     • Processamento de Eventos Complexos (CEP):  Kafka Streams API . Substituiremos a lógica manual com estado ( HashMap  em memória) pela poderosa DSL do Kafka
          Streams (operações stateless e stateful).
     • Backend Web (API & WebSockets): Para não reescrever todo o projeto em Spring Boot, propomos usar o Javalin (um micro-framework Java ultra-rápido e leve).
          Ele servirá a interface web e abrirá um túnel WebSocket para enviar os alertas táticos em tempo real para o navegador.
     • Frontend Web: HTML5, CSS3 (com um design moderno inspirado em painéis táticos, "Dark Mode" de alta performance) e Vanilla JavaScript (conectado via
          WebSocket).

## 2. Evento Composto Base (Refatoração)

     "Pressão Ofensiva Alta"

     • Lógica Kafka Streams: Faremos um  filter  para eventos no terço final do campo, um  groupByKey()  (pelo ID da partida), e um  windowedBy  com uma JanelaTemporal Fixa (Tumbling Window) ou Deslizante (Sliding Window). Faremos um  aggregate / count  e, se exceder 10 ações, disparamos o evento.

## 3. Novas Situações de Interesse (Com Álgebra de Allen)

     Para enriquecer as regras de negócio táticas, propomos implementar 3 novas topologias CEP:

### Situação 1: Contra-Ataque Rápido (Fast Counter-Attack)

     • Conceito: Uma ação defensiva (ex: roubada de bola/interceptação) seguida quase imediatamente por uma finalização do mesmo time.
     • Relação da Álgebra de Allen: BEFORE / MEETS (O Intervalo A ocorre antes de / encontra o Intervalo B num limite curto de tempo).
     • Implementação (Kafka Streams): Um  join  (stateful) entre o fluxo de eventos de "Defesa" e o fluxo de "Finalização", utilizando um  JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(15)) . Se as chaves (partida e time) derem match dentro de 15 segundos, emite o alerta de Contra-Ataque.

### Situação 2: Domínio Territorial (Territorial Dominance)

    • Conceito: O time troca muitos passes seguidos sem perder a bola durante uma janela de tempo. 
    • Relação da Álgebra de Allen: DURING (Os passes ocorrem durante uma janela ininterrupta de posse).
    • Implementação (Kafka Streams): Um  filter  para eventos de "PASS", um agrupamento por  (Match, Team) , aplicando uma Janela de Sessão ( SessionWindows ) cominactivity gap. Se a sessão acumular mais de 15 passes seguidos, o time está exercendo amplo domínio.
### Situação 3: Tumulto / Agressividade Crescente
    • Conceito: Muitas faltas ou cartões de ambos os times acontecendo em um período muito curto, indicando que o jogo está fugindo do controle do árbitro.
    • Relação da Álgebra de Allen: OVERLAPS (Os eventos de infração se sobrepõem em uma janela temporal restrita).
    • Implementação (Kafka Streams): Uso de  SlidingWindows  (Janelas Deslizantes) aglomerando todos os eventos de infração da partida. Um  count()  avalia ajanela. Se ultrapassar um limiar (ex: 4 faltas em 2 minutos), emite o alerta vermelho "Clima Tenso/Tumulto" para a interface web.

### Próximos Passos (Ação do Agente)

     1. Modificar o  pom.xml  para incluir dependências do Kafka Streams, Javalin e SLF4J.
     2. Criar as novas topologias do Kafka Streams no backend (substituindo o antigo  MonitoramentoPartida.java ).
     3. Criar o servidor  WebDashboardServer.java  para servir os alertas via WebSockets.
     4. Desenvolver o Frontend (Painel Tático).