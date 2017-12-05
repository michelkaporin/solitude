
public class Benchmark {
	private long getTime = 0;
	private long putTime = 0;
	
	private int getCount = 0;
	private int putCount = 0;

	public void addGetRequestTime(long elapsed) {
		this.getTime += elapsed;
		this.getCount += 1;
	}
	
	public void addPutRequestTime(long elapsed) {
		this.putTime += elapsed;
		this.putCount += 1;
	}
	
	public double avgGet() {
		if (this.getCount == 0) {
			System.out.println("ds");
		}
		return (this.getTime / this.getCount) / 1000000.0; // in ms
	}
	
	public double avgPut() {
		return (this.putTime / this.putCount) / 1000000.0; // in ms
	}
}
