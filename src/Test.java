import java.io.File;
import java.io.IOException;
import java.util.HashSet;

public class Test {
	
	
	public static void main(String[] args) {
		String dir = "/Users/Johnny/Downloads/test/";
		String name = "file1.txt";
		ContentBasedChunking cbc = new ContentBasedChunking();
		try {
			cbc.digest(dir + name);
		}
		catch (IOException e) {
			System.out.println(e);
		}
		File metadata = new File(dir + name + ".data"); // metadata
		DedupClient.compressFile(dir + name); // compress the original file
		HashSet<String> segments = DedupClient.prepareSegments(metadata);
		for (String seg : segments) {
			DedupClient.compressFile(dir + seg); // compress the segments
		}
	}
}
	
