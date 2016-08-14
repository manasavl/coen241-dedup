package dedup;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.rabinfingerprint.fingerprint.RabinFingerprintLong;
import org.rabinfingerprint.fingerprint.RabinFingerprintLongWindowed;
import org.rabinfingerprint.handprint.BoundaryDetectors;
import org.rabinfingerprint.handprint.FingerFactory.ChunkBoundaryDetector;
import org.rabinfingerprint.polynomial.Polynomial;

public class ContentBasedChunking {

	static long bytesPerWindow = 48;
	static Polynomial p = Polynomial.createFromLong(10923124345206883L);
	private RabinFingerprintLong fingerHash = new RabinFingerprintLong(p);
	private RabinFingerprintLongWindowed fingerWindow = new RabinFingerprintLongWindowed(p, bytesPerWindow);
	private ChunkBoundaryDetector boundaryDetector = BoundaryDetectors.DEFAULT_BOUNDARY_DETECTOR;
	private final static int MIN_CHUNK_SIZE = 460;
	private final static int MAX_CHUNK_SIZE = 2800;
	private RabinFingerprintLong window = newWindowedFingerprint();
	private List<Finger> breakpoints = new ArrayList<Finger>();
	private Map<Finger, Long> fingerprints = new LinkedHashMap<Finger, Long>();
	
	public void digest (byte[] barray) {
		long chunkStart = 0;
		long chunkEnd = 0;
		int chunkLength = 0;
		
		ByteBuffer buf = ByteBuffer.allocateDirect(MAX_CHUNK_SIZE);
		buf.clear();
		/*
		 * fingerprint one byte at a time. we have to use this granularity to
		 * ensure that, for example, a one byte offset at the beginning of the
		 * file won't effect the chunk boundaries
		 */
		for (byte b : barray) {
			// push byte into fingerprints
			window.pushByte(b);
			fingerHash.pushByte(b);
			chunkEnd++;
			chunkLength++;
			buf.put(b);
			
			/*
			 * if we've reached a boundary (which we will at some probability
			 * based on the boundary pattern and the size of the fingerprint
			 * window), we store the current chunk fingerprint and reset the
			 * chunk fingerprinter.
			 */
			if (boundaryDetector.isBoundary(window)	&& chunkLength > MIN_CHUNK_SIZE) {
				byte[] c = new byte[chunkLength];
				buf.position(0);
				buf.get(c);
				
				// store last chunk offset
				Finger finger = new Finger(fingerHash.getFingerprintLong(), chunkLength, chunkStart);
				breakpoints.add(finger);
				addFingerprint(finger); 
				chunkStart = chunkEnd;
				chunkLength = 0;
				fingerHash.reset();
				buf.clear();
			} else if (chunkLength >= MAX_CHUNK_SIZE) {
				byte[] c = new byte[chunkLength];
				buf.position(0);
				buf.get(c);
				Finger finger = new Finger(fingerHash.getFingerprintLong(), chunkLength, chunkStart);
				breakpoints.add(finger);
				addFingerprint(finger);
				fingerHash.reset();
				buf.clear();
				
				// store last chunk offset
				chunkStart = chunkEnd;
				chunkLength = 0;
			}
		}
		byte[] c = new byte[chunkLength];
		buf.position(0);
		buf.get(c);
		Finger finger = new Finger(fingerHash.getFingerprintLong(), chunkLength, chunkStart);
		breakpoints.add(finger);
		addFingerprint(finger);
		fingerHash.reset();
		buf.clear();
	}
	
	public double calculateDeduplicationRatio(){
		long deduplicatedData = 0, allData = 0;
		for (Finger fingerprint: fingerprints.keySet()){
			long count = fingerprints.get(fingerprint);
			if (count > 1){
				deduplicatedData+=(count-1)*fingerprint.getLength();
			}
			allData+=count*fingerprint.getLength();
		}
		return ((double) deduplicatedData)/allData;
	}

	private RabinFingerprintLongWindowed newWindowedFingerprint() {
		return new RabinFingerprintLongWindowed(fingerWindow);
	}
	
	private boolean addFingerprint(Finger finger){
		boolean alreadyExists = true;
		if (!fingerprints.containsKey(finger)){
			fingerprints.put(finger, 0L);
			alreadyExists = false;
			System.out.println("new one: " + finger.getHash() + ", length: " + finger.getLength() + ", start: " + finger.getPosition());
		} else {
			System.out.println("old one:" + finger.getHash() + ", length: " + finger.getLength() + ", start: " + finger.getPosition());
		}
		fingerprints.put(finger, fingerprints.get(finger)+1);
		return alreadyExists;
	}
	
	public Map<Finger, Long> getFingerprints() {
		return fingerprints;
	}
	
	public List<Finger> getBreakpoints() {
		return breakpoints;
	}

	public void reset() {
		breakpoints.clear();
		fingerprints.clear();
	}
}
