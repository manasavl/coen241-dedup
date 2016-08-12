import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DedupClient {
	
	/**
	 * Segmentation and finger-printing of files should return a list of 
	 * segment names (e.g. finger prints). A new file should be created
	 * that contains a list of segments to reconstruct the file.
	 */
	
	/**
	 * The file data contains information on the number of segments a file
	 * is split into and the segments that make up the file.
	 */
	private static HashSet<String> prepareSegments(String fileData) {
		HashSet<String> segments = new HashSet<>();
		File file = new File(fileData);
		try (
			BufferedReader br = new BufferedReader(new FileReader(file))
			) {
			int numSegments = Integer.parseInt(br.readLine());
			for (int i = 0; i < numSegments; i++) {
				String segmentName = br.readLine();
				segments.add(segmentName);
			}
			return segments;
		}
		catch (IOException e) {
			System.out.println(e);
		}
		return null;
	}
	
	public static HashSet<String> getFilesToUpload(HashSet<String> segments, String address, int port) {
		HashSet<String> toUpload = new HashSet<String>();
		try (
			Socket socket = new Socket(address, port);
			PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
			BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
			) {
			out.println("duplicate check");
			String input;
			for (String segment : segments) {
				out.println(segment);
				if ((input = in.readLine()) != null && input.equals("upload")) {
					toUpload.add(segment);
				}
			}
		}
		catch (IOException e) {
			System.out.println(e);
		}
		return toUpload;
	}
	
	// This sends the file to EC2 server
	public static void sendFile(File file, String address, int port) {
		try (
			Socket socket = new Socket(address, port);
			PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
			FileInputStream fis = new FileInputStream(file);
			BufferedInputStream bis = new BufferedInputStream(fis);
			BufferedOutputStream toServer = new BufferedOutputStream(socket.getOutputStream());
			) {
			out.println("upload " + file.getName()); // upload filename
			out.flush();
			byte[] fileByteArray = new byte[(int) file.length()];
			bis.read(fileByteArray, 0, fileByteArray.length);
			toServer.write(fileByteArray, 0, fileByteArray.length);
			pause(1); // Note: need to sleep for byte array to populate
			toServer.flush();
		}
		catch (IOException e) {
			System.out.println(e);
		}
	}
	
	private static void pause(int ms) {
		try {
			TimeUnit.MILLISECONDS.sleep(ms);
		}
		catch (InterruptedException e) {
			System.out.println(e);
		}
	}

	public static void main(String[] args) {
		args = new String[4];
		args[0] = "put";
		args[1] = "/Users/Johnny/Documents/workspace/coen241-dedup/data/file1";
		args[2] = "0.0.0.0";
		args[3] = "53878";
		if (args.length != 4) {
			System.err.println("Usage: java DedupClient [put | get] [file] [address] [port]");
			System.exit(1);
		}
		// Collection information
		String command = args[0]; // put, get, or delete
		String filePath = args[1]; // file path
		String address = args[2]; // server's address
		int port = Integer.parseInt(args[3]); // server's port
		
		// TODO: Step 1: Segment and fingerprint the files
		
		// Step 2: Compile set of finger prints/segments; check for existence of files
		File file = new File(filePath);
		String fileDataPath = filePath + ".data";
		File fileData = new File(fileDataPath);
		if (!fileData.exists() || !file.exists()) {
			System.err.println("File does not exist!");
			System.exit(1);
		}
		HashSet<String> segments = prepareSegments(fileDataPath);
		// Step 3: Communicate with server to execute command
		if (command.equalsIgnoreCase("put")) {
			// 1. Determine which segments to upload
			HashSet<String> toUpload = getFilesToUpload(segments, address, port); // Build list of unique segments
			// 2. Send .data file to the server
			sendFile(fileData, address, port);
			for (String segment : segments) {
				sendFile(new File("./data/" + segment), address, port);
			}
		}
		if (command.equalsIgnoreCase("get")) {
			
		}
		if (command.equalsIgnoreCase("delete")) {
			
		}
	}
	
}
