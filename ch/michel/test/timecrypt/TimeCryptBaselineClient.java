package ch.michel.test.timecrypt;

import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.List;

public class TimeCryptBaselineClient {
    private String ip;
    private int port;
    private SocketChannel channel;

    private JsonParser jsonParser;

    public TimeCryptBaselineClient(String ip, int port) {
        this.ip = ip;
        this.port = port;

        jsonParser = new JsonParser();
    }
    
    public boolean insert(long from, long to, String metadata) throws IOException {
        String json = String.format("{ 'operationID': 'insert', 'from': %s, 'to': %s, 'data': '%s' }", from, to, metadata);
        return Boolean.valueOf(getResult(json));
    }

    public List<byte[]> getRange(long from, long to) throws IOException {
        String json = String.format("{ 'operationID': 'getRange', 'from': %s, 'to': %s }", from, to);
        String res = getResult(json);

        List<byte[]> result = new ArrayList<byte[]>();
        if (res.equals("[]")) return result;
        
        Decoder base64Decoder = Base64.getDecoder();
        String[] byteArrays = res.substring(1, res.length() - 1).split(","); // remove square braces and split by ','
        for (String encodedData : byteArrays) {
            result.add(base64Decoder.decode(encodedData.substring(1, encodedData.length() - 1))); // remove quotes
        }

        return result;
    }

    public boolean clean() throws IOException {
        String json = "{ 'operationID': 'cleanList' }";
        return Boolean.valueOf(getResult(json));
    }

    public void openConnection() throws IOException {
        InetSocketAddress hostAddress = new InetSocketAddress(this.ip, this.port);
        try {
			this.channel = SocketChannel.open(hostAddress);
		} catch (IOException e) {
            System.out.println("Failed to open a socket to the server");
			throw e;
		}
    }

    public void closeConnection() throws IOException {
        try {
			this.channel.close();
		} catch (IOException e) {
            System.out.println("Failed to close the connection to the server.");
            throw e;
		}
    }

    private String getResult(String requestJson) throws IOException {
        String apiResult = new String(writeAndRead(requestJson), Charset.forName("UTF-8"));

        try {
            JsonObject jobject = jsonParser.parse(apiResult).getAsJsonObject();
            String exception = jobject.get("msg").getAsString();
            System.out.println("Failed to perform operation: " + exception);
            throw new IOException(exception);
        } catch (NullPointerException | JsonParseException | IllegalStateException e) {
        return apiResult;
        }
    }

	private byte[] writeAndRead(String json) throws IOException {
		ByteBuffer buffer = ByteBuffer.wrap(json.getBytes());
        int bytesWritten = 0;
        try {
            while (bytesWritten != buffer.capacity()) {
                bytesWritten += this.channel.write(buffer);
            }
		} catch (IOException e) {
            System.out.println("Failed to send the command to the server.");
            throw e;
        }

        buffer = ByteBuffer.allocate(262144000);  // 250 MB buffer max for boundaries experiment
        int numRead = 0;
        try {
            numRead = channel.read(buffer);
        } catch (IOException e) {
            System.out.println("Failed to read the result of the command.");
            throw e;
        }
        if (numRead == -1) {
            throw new IOException("Failed to read the result of the command.");
        }

        byte[] trimmedBytes = new byte[numRead];
        System.arraycopy(buffer.array(), 0, trimmedBytes, 0, numRead);
        
        byte[] response = trimmedBytes;
        boolean retryParsing = true;
        while (retryParsing) {
            try {
                jsonParser.parse(new String(response));
                retryParsing = false;
            } catch (JsonSyntaxException e) {
                buffer = ByteBuffer.allocate(5000000);
                numRead = channel.read(buffer);
                if (numRead != 0) {
                    byte[] newBytes = new byte[numRead];
                    System.arraycopy(buffer.array(), 0, newBytes, 0, numRead);

                    byte[] newResponse = new byte[response.length + newBytes.length];
                    System.arraycopy(response, 0, newResponse, 0, response.length);
                    System.arraycopy(newBytes, 0, newResponse, response.length, newBytes.length);
                    response = newResponse;
                } else if (numRead == -1) {
                    throw new IOException("Failed to read the result of the command.");
                }
            }
        }

        return response;
    }
}