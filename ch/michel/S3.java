package ch.michel;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

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

public class S3 {
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
	
	public void put(Chunk chunk, String bucket, byte[] data) {
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
		client.putObject(new PutObjectRequest(bucket, chunk.getPrimaryAttribute(), byteArrayInputStream, metaData)); 
		benchmark.addPutRequestTime(System.nanoTime() - start);
	}
	
	public S3Object get(Chunk chunk, String bucket) {
		long start = System.nanoTime();
        S3Object object = client.getObject(new GetObjectRequest(bucket, chunk.getPrimaryAttribute()));
        InputStream objectData = object.getObjectContent();
        try {
			processInputStream(objectData);
		} catch (IOException e) {
			e.printStackTrace();
		}
		benchmark.addGetRequestTime(System.nanoTime() - start);
        
        return object;
	}
	
	public void del(Chunk chunk, String bucket) {
        client.deleteObject(bucket, chunk.getPrimaryAttribute());
	}
	
	public Benchmark getBenchmark() {
		return this.benchmark;
	}
	
	public void resetBenchmark() {
		this.benchmark = new Benchmark();
	}
	
    private  void processInputStream(InputStream input) throws IOException {
    	// Read one text line at a time and display.
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        while (true) {
            String line = reader.readLine();
            if (line == null) break;
        }
    }

}
