package ch.michel.test.helpers;

public class TreeDBBenchmarkStats {
    public String design;
    public float retrievalTime;
    public float decodeDecryptTime;
    public float computationTime;

    public TreeDBBenchmarkStats(String design, float retrievalTime, float decodeDecryptTime, float computationTime) {
        this.design = design;
        this.retrievalTime = retrievalTime;
        this.decodeDecryptTime = decodeDecryptTime;
        this.computationTime = computationTime;
    }
}