package ch.michel;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;

import ch.lubu.Chunk;

public class S3 implements Storage {
	private AmazonS3 client;
	private Benchmark benchmark;

	public S3(String aws_access_key_id, String aws_secret_access_key) {
		BasicAWSCredentials awsCreds = new BasicAWSCredentials(aws_access_key_id, aws_secret_access_key);

		try {
			client = AmazonS3ClientBuilder.standard()
		                        .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
		                        .withRegion("eu-west-2")
		                        .build();
		} catch (Exception e) {
			System.out.println("Failed to setup credentials for Amazon S3: " + e.toString());
			System.exit(1);
		}
		
		this.benchmark = new Benchmark();
	}
	
	public boolean put(Chunk chunk, String bucket, byte[] data) {
		byte[] bytes = null;
		try {
			bytes = IOUtils.toByteArray(new ByteArrayInputStream(data));
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		ObjectMetadata metaData = new ObjectMetadata();
		metaData.setContentLength(bytes.length);
		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
		
		long start = System.nanoTime();
		try {
			client.putObject(new PutObjectRequest(bucket, chunk.getPrimaryAttribute(), byteArrayInputStream, metaData));
		} catch (Exception e) {
			return false;
		}
		benchmark.addPutRequestTime(System.nanoTime() - start);
		
		return true;
	}
	
	public byte[] get(Chunk chunk, String bucket) {
		long start = System.nanoTime();
        S3Object object = client.getObject(new GetObjectRequest(bucket, chunk.getPrimaryAttribute()));
        InputStream objectData = object.getObjectContent();
        byte[] result = processInputStream(objectData);
		benchmark.addGetRequestTime(System.nanoTime() - start);
        
        return result;
	}
	
	public boolean del(Chunk chunk, String bucket) {
		try {
			client.deleteObject(bucket, chunk.getPrimaryAttribute());
		} catch (Exception e) {
			return false;
		}
		return true;
	}
	
	public Benchmark getBenchmark() {
		return this.benchmark;
	}
	
	public void resetBenchmark() {
		this.benchmark = new Benchmark();
	}
    
    public void createBuckets(List<Label> labels) {
    		for (Label l : labels) {
			if (!client.doesBucketExistV2(l.name)) {
				client.createBucket(l.name);
			}
    		}
    }
	
    private byte[] processInputStream(InputStream input) {
    		ByteArrayOutputStream buffer = new ByteArrayOutputStream();
	    	int nRead;
	    	byte[] data = new byte[16384];
	    	
	    	try {
			while ((nRead = input.read(data, 0, data.length)) != -1) {
			  buffer.write(data, 0, nRead);
			}
			buffer.flush();
		} catch (IOException e) {
			System.out.println("Failed to convert S3 object return input stream to byte[].");
			e.printStackTrace();
		}

	    	return buffer.toByteArray();
    }
}
