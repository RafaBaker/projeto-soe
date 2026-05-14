package br.ufes.inf;

//
public class PlacarStore {
    private String teamA;
    private String teamB;
    private int goalsA = 0;
    private int goalsB = 0;

    public PlacarStore(String teamA, String teamB) {
        this.teamA = teamA;
        this.teamB = teamB;
    }

    // Método principal de atualização do estado
    public void registrarGol(String timeIdAvaliando, String idTimeA) {
        if (timeIdAvaliando.equals(idTimeA)) {
            goalsA++;
        } else {
            goalsB++;
        }
        imprimirPlacar();
    }

    public void imprimirPlacar() {
//        System.out.println("=====================================");
        System.out.print("\rPLACAR: " + teamA + " " + goalsA + " x " + goalsB + " " + teamB);
//        System.out.println("=====================================");
    }

    // Getters para caso você queira gerar um evento derivado do placar...
    public int getGolsTimeA() { return goalsA; }
    public int getGolsTimeB() { return goalsB; }
}
