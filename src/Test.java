import java.io.File;
import java.io.IOException;
import java.util.HashSet;

public class Test {

	
	static final String dir = "/Users/Johnny/Downloads/test/";
	
	public static void main(String[] args) {
		doChunking("testing1");
	}
	
	static void doChunking(String filename) {
		ContentBasedChunking cbc = new ContentBasedChunking();
		try {
			cbc.digest(dir + filename);
		}
		catch (IOException e) {
			System.out.println(e);
		}
	}
	
	static void compressSegments(String filename) {
		File metadata = new File(dir + filename + ".data");
		HashSet<String> segments = DedupClient.prepareSegments(metadata);
		for (String seg : segments) {
			DedupClient.compressFile(dir + seg);
		}
	}
	
	static void stats(String file1, String file2) {
		File metadata1 = new File(dir + file1 + ".data");
		File metadata2 = new File(dir + file2 + ".data");
		HashSet<String> seg1 = DedupClient.prepareSegments(metadata1);
		HashSet<String> seg2 = DedupClient.prepareSegments(metadata2);
		HashSet<String> seg = new HashSet<>();
		seg.addAll(seg1);
		seg.addAll(seg2);
		long size = 0;
		for (String segment : seg1) {
			File file = new File(dir + segment + ".zip");
			size += file.length();
		}
		System.out.println(file1 + " has " + seg1.size() + " unique segments that compress to " + size);
		
		size = 0;
		for (String segment : seg2) {
			File file = new File(dir + segment + ".zip");
			size += file.length();
		}
		System.out.println(file2 + " has " + seg2.size() + " unique segments that compress to " + size);
		
		size = 0;
		for (String segment : seg) {
			File file = new File(dir + segment + ".zip");
			File f = new File(dir + segment);
			size += file.length();
			file.deleteOnExit();
			f.deleteOnExit();
		}
		System.out.println(file1 + " and " + file2 + " have " + seg.size() + " unique segments that compress to " + size);
	}
	
}
