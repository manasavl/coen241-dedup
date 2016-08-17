import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import com.amazonaws.services.s3.AmazonS3;

public class DedupClient {

	/**
	 * Segmentation and finger-printing of files should return a list of segment
	 * names (e.g. finger prints). A new file should be created that contains a
	 * list of segments to reconstruct the file.
	 */

	/**
	 * The metadata file contains information on the number of segments a file
	 * is split into and the segments that make up the file.
	 */
	private static HashSet<String> prepareSegments(File metadata) {
		HashSet<String> segments = new HashSet<>();
		try (BufferedReader br = new BufferedReader(new FileReader(metadata))) {
			int numSegments = Integer.parseInt(br.readLine());
			for (int i = 0; i < numSegments; i++) {
				String line = br.readLine();
				String[] split = line.split(",");
				String segmentName = split[0];
				segments.add(segmentName);
			}
		} catch (IOException e) {
			System.out.println(e);
		}
		return segments;
	}

	/**
	 * Input is a set of segments/finger prints. It communicates with EC2 to
	 * determine which need to be uploaded. Returns a set of segments that are
	 * unique and thus need to be uploaded.
	 */
	public static HashSet<String> getFilesToUpload(HashSet<String> segments, String address, int port) {
		HashSet<String> toUpload = new HashSet<String>();
		try (Socket socket = new Socket(address, port);
				PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
				BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
			out.println("duplicate check");
			String input;
			for (String segment : segments) {
				out.println(segment);
				if ((input = in.readLine()) != null && input.equals("upload")) {
					toUpload.add(segment);
				}
			}
		} catch (IOException e) {
			System.out.println(e);
		}
		return toUpload;
	}

	// This sends the file to EC2 server
	public static void sendFile(File file, String address, int port) {
		try (Socket socket = new Socket(address, port);
				PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
				FileInputStream fis = new FileInputStream(file);
				BufferedInputStream bis = new BufferedInputStream(fis);
				BufferedOutputStream toServer = new BufferedOutputStream(socket.getOutputStream());) {
			out.println("upload " + file.getName() + " " + file.length()); // upload
																			// filename
			byte[] fileByteArray = new byte[(int) file.length()];
			bis.read(fileByteArray, 0, fileByteArray.length);
			toServer.write(fileByteArray, 0, fileByteArray.length);
			pause(1); // Note: need to sleep for byte array to populate
			toServer.flush();
		} catch (IOException e) {
			System.out.println(e);
		}
	}

	public static String[][] getFileSegments(String fileName, String address, int port) {
		try (Socket socket = new Socket(address, port);
				PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
				BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
			out.println("get " + fileName); // Tell server which file we want
			String input;
			input = in.readLine(); // Get number of segments we need
			if (input.equals("File does not exist!")) {
				System.err.println(fileName + " does not exist!");
				System.exit(1);
			} else {
				// Build our array of segments
				int numSegments = Integer.parseInt(input); // Line 1 is number
															// of segments
				String[][] segments = new String[numSegments][2];
				for (int i = 0; i < numSegments; i++) {
					String line = in.readLine(); // [fingerprint/filename,len]
					String[] split = line.split(",");
					segments[i][0] = split[0]; // file name
					segments[i][1] = split[1]; // length
				}
				return segments;
			}
		} catch (IOException e) {
			System.out.println(e);
		}
		return null;
	}
	
	public static void deleteFile(String fileName, String address, int port) {
		try (
			Socket socket = new Socket(address, port);
			PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
			BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
			) {
			out.println("delete " + fileName);
			String input = in.readLine();
			if (input.equals("File does not exist!")) {
				System.out.println(fileName + " does not exist!");
			}
		}
		catch (IOException e) {
			System.out.println(e);
		}
	}

	private static void pause(int ms) {
		try {
			TimeUnit.MILLISECONDS.sleep(ms);
		} catch (InterruptedException e) {
			System.out.println(e);
		}
	}
	
	public static void main(String[] args) {
		args = new String[4];
		args[0] = "delete";
//		args[1] =
//		 "/Users/Johnny/Documents/workspace/coen241-dedup/data/file1";
		args[1] = "file1";
		args[2] = "0.0.0.0";
		args[3] = "58762";
		if (args.length != 4) {
			System.err.println("Usage: java DedupClient [put | get] [file] [address] [port]");
			System.exit(1);
		}
		// Collection information
		String command = args[0]; // put, get, or delete
		String myFile = args[1]; // file path or file name
		String address = args[2]; // server's address
		int port = Integer.parseInt(args[3]); // server's port

		if (command.equalsIgnoreCase("put")) {
			// TODO: Step 1: Segment and finger print the files (output is
			// metadata file)
			File file = new File(myFile); // File we want to upload
			File metadata = new File(myFile + ".data"); // Metadata of file to
														// be sent to server
			String fileDir = file.getParent(); // Directory containing our file,
												// its metadata, and its
												// segments
			// Step 2: Compile set of fingerprints/segments
			// Step 3: Determine segments of our file
			HashSet<String> segments = prepareSegments(metadata);
			// Step 4:Determine segments that need to be uploaded
			HashSet<String> toUpload = getFilesToUpload(segments, address, port);
			// Step 5: Send metadata file to the server
			sendFile(metadata, address, port);
			// Step 6: Initialize S3 client
			MyS3Client client = new MyS3Client();
			// Step 7: Send segments to buckets
			for (String upload : toUpload) {
				File seg = new File(fileDir + "/" + upload);
				client.uploadFile(seg);
			}
		}
		if (command.equalsIgnoreCase("get")) {
			// Step 1: Get the segments needed to reconstruct our file
			String[][] fileSegments = getFileSegments(myFile, address, port);
			// Step 2: Initialize S3 client
			MyS3Client client = new MyS3Client();
			// Step 3: Download objects from S3
			client.downloadFile("testingFile", fileSegments);
		}
		if (command.equalsIgnoreCase("delete")) {
			deleteFile(myFile, address, port);
		}
	}

}
