import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Classe principal pública que contém o método main.
 */

/*
 * Aqui realizamos a criacao das threads Produtor e Consumidor.
 * Permitindo a implementacao do conceito de concorrencia, criacao de threads e sincronizacao.
 * E veremos o conceito de recurso compartilhado (o Buffer).
 */

public class Principal {
    public static void main(String[] args) {

        try (FileWriter escritorLog = new FileWriter("log.txt")) {

            Buffer buffer = new Buffer(escritorLog);

            Produtor produtor = new Produtor(buffer, "Produtor-1");
            Consumidor consumidor = new Consumidor(buffer, "Consumidor-1");

            System.out.println("Iniciando simulação...");

            produtor.start();
            consumidor.start();

            produtor.join(); /// espera a thread produtor terminar
            consumidor.join(); //// espera a thread consumidor terminar

            System.out.println("Simulação finalizada. Verifique o arquivo log.txt.");

        } catch (IOException | InterruptedException e) {
            System.err.println("Erro durante a execução: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

/**
 * Classe que representa o buffer compartilhado.
 * 
 * Esta classe representa diretamente o "recurso compartilhado" entre threads.Os threads que a gente criou tentarao
 * acessar esse recurso compartilhado(buffer) de forma concorrente e ,no final do trabalho,iremos evitar o problema disso
 * utilizando semaforos e mutex.
 */
class Buffer {

    private final LinkedList<Integer> bufferInterno;
    private final int tamanhoMaximo = 7;
    private final FileWriter escritorLog;


    // Semaforos para controle de espaço
    private final Semaphore vazio;  // Esse semaforo controla quantos espaços vazios há
    private final Semaphore cheio; // Ja esse controla quantos itens existem no buffer

    private final ReentrantLock mutex; //implementa exclusao mutua, garantindo que apenas 1 thread entre na regiao critica

    public Buffer(FileWriter escritorLog) {
        this.bufferInterno = new LinkedList<>();
        this.escritorLog = escritorLog;
        this.mutex = new ReentrantLock(); // O mutex serviria para proteger a regiao critica,
        //                                       utilizando a premissa de exclusão mútua

        this.vazio = new Semaphore(tamanhoMaximo); // 7 espaços
        this.cheio = new Semaphore(0); // nenhum item no começo
    }

    public void produzir(int item, String nomeThread) throws InterruptedException, IOException {

        vazio.acquire(); // a thread produtora espera espaço disponivel

        mutex.lock(); // entra na regiao critica,que é a regiao que as threads tentarao acessar o recurso compartilhado
        try {
            bufferInterno.addLast(item);
            int espacosDisponiveis = tamanhoMaximo - bufferInterno.size();

            String msgLog = String.format(
                "%s - Inserido um item no buffer – espaços disponíveis: %d%n",
                nomeThread, espacosDisponiveis);

            escritorLog.write(msgLog);
            escritorLog.flush();

        } finally {
            mutex.unlock();
        }

        cheio.release(); // libera que ha novo item, avisando para a thread consumidora que um novo item foi adicionado
    }

    public void consumir(String nomeThread) throws InterruptedException, IOException {

        cheio.acquire(); // espera item , ou seja,a thread consumidor espera , caso buffer esteja vazio.

        mutex.lock(); // entra região crítica , e o mutex ,como "porteiro" do buffer,
        //                 impede que o produtor mexa no buffer durante o consumo.


        try {
            bufferInterno.removeFirst();
            int espacosDisponiveis = tamanhoMaximo - bufferInterno.size();

            String msgLog = String.format(
                "%s - Consumido um item no buffer - espaços disponíveis: %d%n",
                nomeThread, espacosDisponiveis);

            escritorLog.write(msgLog);
            escritorLog.flush();

        } finally {
            mutex.unlock();
        }

        vazio.release(); // libera espaço do buffer, avisando para a thread produtora que um espaço foi liberado
    }
}

/**
 * Thread/Classe Produtora.
 * 
 * - Extende Thread → conceito estudado de criação e execução paralela.
 * - Produz quantidade variável de itens (entre 1 até 15).
 * - Demonstra comportamento concorrente onde múltiplas instancias podem existir.
 * 
 */
class Produtor extends Thread {

    private final Buffer buffer;
    private final int itensParaProduzir;

    public Produtor(Buffer buffer, String nome) {
        super(nome);
        this.buffer = buffer;

        // Produz entre 1 e 15 itens para cada execucao da classe Produtor
        this.itensParaProduzir = 1 + (int)(Math.random() * 15);
    }

    @Override
    public void run() {

        System.out.println(getName() + " irá produzir " + itensParaProduzir + " itens.");

        for (int i = 1; i <= itensParaProduzir; i++) {
            try {
                buffer.produzir(i, getName());
                Thread.sleep((long) (Math.random() * 200)); // Aqui a gente vai simular um tempo de producao dos itens
                //                                                   de forma aleatoria

            } catch (InterruptedException | IOException e) {
                System.err.println(getName() + " foi interrompido: " + e.getMessage());
                Thread.currentThread().interrupt();
                break;
            }
        }

        System.out.println(getName() + " finalizou a produção.");
    }
}

/**
 * Thread/Classe Consumidora.
 * 
 * -Consome até 12 itens,seguindo o pdf do trabalho.
 *  Mostra o comportamento de sincronização:
 *      -Espera buffer ter itens (semáforo cheio)
 *      -Trabalha dentro da região crítica (mutex)
 * - Demonstra a lógica clássica do problema Produtor/Consumidor.
 */
class Consumidor extends Thread {

    private final Buffer buffer;
    private final int itensParaConsumir = 12;

    public Consumidor(Buffer buffer, String nome) {
        super(nome);
        this.buffer = buffer;
    }

    @Override
    public void run() {

        System.out.println(getName() + " irá consumir até " + itensParaConsumir + " itens.");

        for (int i = 1; i <= itensParaConsumir; i++) {
            try {
                buffer.consumir(getName());
                Thread.sleep((long) (Math.random() * 250)); // Aqui a mesma coisa da thread produtora,só que 
                //                                             simulando um tempo de consumo
                //                                             de forma aleatoria

            } catch (InterruptedException | IOException e) {
                System.err.println(getName() + " foi interrompido: " + e.getMessage());
                Thread.currentThread().interrupt();
                break;
            }
        }

        System.out.println(getName() + " finalizou o consumo.");
    }
} 
