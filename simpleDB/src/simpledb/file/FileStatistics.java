package simpledb.file;

import java.util.HashMap;
import java.util.Map;

public class FileStatistics {

	private Map<Block, Integer> readInBlocks=new HashMap<Block, Integer>();
	
	/**
	 * Register a read to a Block, saving fileName and block-number for statistical purposes 
	 * 
	 */
	public void registerRead(Block blk) {
		Integer times=readInBlocks.get(blk);
		if(times==null){
			times=1;
		}else{
			times++;
		}
		readInBlocks.put(blk, times);
	}

	public void reset() {
		readInBlocks.clear();
	}

	public String getAggregatedReadStatistics() {
		StringBuilder res=new StringBuilder();
		String newline=System.getProperty("line.separator");
		res.append("Aggregated Reads Statistics: [FileName] [Reads]");
		res.append(newline);
		
		//construct aggregated data [filename] [num reads]
		Map<String, Integer>aggregate = new HashMap<String, Integer>();
		for(Map.Entry<Block, Integer> e: readInBlocks.entrySet()){
			Integer val=aggregate.get(e.getKey().fileName());
			if(val==null){
				val=e.getValue();
			}else{
				val+=e.getValue();
			}
			aggregate.put(e.getKey().fileName(), val);
		}
		
		//construct response
		for(Map.Entry<String, Integer> e: aggregate.entrySet()){
			res.append(e.getKey());
			res.append("\t");
			res.append(e.getValue());
			res.append(newline);
		}
		
		return res.toString();
	}

	public void clearReadStatistics() {
		readInBlocks.clear();
	}

}
