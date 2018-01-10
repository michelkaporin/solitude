package ch.michel;

public class Benchmark {
	private long getTime = 0;
	private long putTime = 0;
	
	private int getCount = 0;
	private int putCount = 0;

	public void addGetRequestTime(long elapsed) {
		this.getTime += elapsed;
		this.getCount += 1;
	}

	public void addGetRequestTime(long elapsed, int count) {
		this.getTime += elapsed;
		this.getCount += count;
	}
	
	public void addPutRequestTime(long elapsed) {
		this.putTime += elapsed;
		this.putCount += 1;
	}
	
	public double avgGet() {
		if (this.getCount == 0) {
			System.out.println("No GET requests happened so far.");
			return 0;
		}
		return (this.getTime / this.getCount) / 1000000.0; // in ms
	}
	
	public double avgPut() {
		if (this.putCount == 0) {
			System.out.println("No PUT requests happened so far.");
			return 0;
		}
		return (this.putTime / this.putCount) / 1000000.0; // in ms
	}
}
