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
		
	public EC2ServerThread(EC2Server server, Socket socket) {
		this.server = server;
		this.socket = socket;
	}
	
	/**
	 * The method that downloads the file the client is uploading.
	 */
	public static void receiveFile(String savePath, int fileLen, InputStream inputStream) {
		try (
			ByteArrayOutputStream baos = new ByteArrayOutputStream(fileLen);
			FileOutputStream fos = new FileOutputStream(savePath);
			BufferedOutputStream bos = new BufferedOutputStream(fos)
			) {
			byte[] aByte = new byte[1];
			int bytesRead = inputStream.read(aByte, 0, aByte.length);
			do {
				baos.write(aByte);
				bytesRead = inputStream.read(aByte);
			} while (bytesRead != -1);
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
				System.out.println("FILE NAME: " + fileName + ", length: " + split[2]);
				receiveFile(EC2Server.saveDest + fileName, Integer.parseInt(split[2]), socket.getInputStream());
				// ./SERVER/fileName
			}
			// If client wants to get a file
			if (input.startsWith("get")) {
				String[] split = input.split(" ");
				String fileName = split[1];
				File file = new File(EC2Server.saveDest + fileName + ".data");
				if (!file.exists()) {
					out.println("file: " + file.getName() + " does not exist!");
				}
				else {
					out.println("File exists");
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
						e.printStackTrace();
					}
				}
			}
			// If clients wants to delete a file
			if (input.startsWith("delete")) {
				String[] split = input.split(" ");
				String fileName = split[1];
				File file = new File(EC2Server.saveDest + fileName + ".data");
				if (!file.exists()) {
					out.println("file does not exist!");
				}
				else {
					System.out.println("Starting to delete: " + fileName);
					out.println("Deleting file");
					try (
						BufferedReader br = new BufferedReader(new FileReader(file));
						) {
						String line; 
						while ((line = br.readLine()) != null) {
							String segName = line.split(",")[0];
							server.removeSegment(segName);
						}
						if (file.delete()) {
							System.out.println("Deleted metadata file: " + file.getName());
						}
					}
					catch (IOException e) {
						System.out.println(e);
						e.printStackTrace();
					}
				}
			}
		}
		catch (IOException e) {
			System.out.println(e);
			e.printStackTrace();
		}
		EC2Server.saveMap();
	}

}
