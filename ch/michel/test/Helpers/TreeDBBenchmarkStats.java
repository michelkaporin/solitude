package ch.michel.test.Helpers;

public class TreeDBBenchmarkStats {
    public String design;
    public long retrievalTime;
    public long decodeDecryptTime;
    public long computationTime;

    public TreeDBBenchmarkStats(String design, long retrievalTime, long decodeDecryptTime, long computationTime) {
        this.design = design;
        this.retrievalTime = retrievalTime;
        this.decodeDecryptTime = decodeDecryptTime;
        this.computationTime = computationTime;
    }
}