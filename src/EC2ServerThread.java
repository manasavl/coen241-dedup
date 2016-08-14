import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class EC2ServerThread extends Thread {
	
	private EC2Server server;
	private Socket socket;
	
	static final String saveDest = "./SERVER/";
	
	public EC2ServerThread(EC2Server server, Socket socket) {
		this.server = server;
		this.socket = socket;
	}
	
	public static void receiveFile(String savePath, InputStream inputStream) {
		try (
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			FileOutputStream fos = new FileOutputStream(savePath);
			BufferedOutputStream bos = new BufferedOutputStream(fos);
			) {
			byte[] aByte = new byte[1];
			int bytesRead = inputStream.read(aByte, 0, aByte.length);
			do {
				baos.write(aByte);
				bytesRead = inputStream.read(aByte);
			}
			while (bytesRead != -1);
			bos.write(baos.toByteArray());
			bos.flush();
		}
		catch (IOException e) {
			System.out.println(e);
		}
	}
	
	public void run() {
		// Runs when a connection is made
		try (
			PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
			BufferedReader in = new BufferedReader(
					new InputStreamReader(
						socket.getInputStream()));
			) {
			String input, output;
			input = in.readLine();
			// If client needs to determine which segments to upload
			if (input.equals("duplicate check")) {
				// We are expecting a series of "check segment" lines
				while ((input = in.readLine()) != null) {
					if (!server.hasSegment(input)) {
						out.println("upload");
						server.putSegment(input);
					}
					else out.println("skip");
				}
			}
			// If client is uploading a file
			if (input.startsWith("upload")) {
				String[] split = input.split(" ");
				String fileName = split[1];
				System.out.println("FILE NAME: " + fileName);
				receiveFile(saveDest + fileName, socket.getInputStream());
			}
			// If client wants to get a file
			if (input.startsWith("get")) {
				String[] split = input.split(" ");
				String fileName = split[1];
				File file = new File(saveDest + fileName + ".data");
				if (!file.exists()) {
					out.println("File does not exist!");
				}
				else {
					try (
						BufferedReader br = new BufferedReader(new FileReader(file))
						) {
						String line;
						while ((line = br.readLine()) != null) {
							out.println(line);
						}
					}
					catch (IOException e) {
						System.out.println(e);
					}
				}
			}
			// If clients wants to delete a file
			if (input.startsWith("delete")) {
				String[] split = input.split(" ");
				String fileName = split[1];
				File file = new File(saveDest + fileName + ".data");
				if (!file.exists()) {
					out.println("file does not exist!");
				}
				else {
					out.println("Deleting file");
					try (
						BufferedReader br = new BufferedReader(new FileReader(file))
						) {
						String line = br.readLine(); // skip the first line, numSegments
						while ((line = br.readLine()) != null) {
							server.removeSegment(line);
						}
					}
					catch (IOException e) {
						System.out.println(e);
					}
				}
			}
		}
		catch (IOException e) {
			System.out.println(e);
		}
		EC2Server.saveMap();
	}

}
