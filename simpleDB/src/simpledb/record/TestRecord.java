package simpledb.record;

import java.util.Random;

import simpledb.file.FileMgr;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class TestRecord {
	
	private static String ID="id";
	private static String POINTS="points";
	private static String NAME="name";
	private static int NAME_LENGTH=5;
	private static String FILE= "prova";
	private static String FILENAME = FILE + ".tbl";
	
	private FileMgr fileMgr;
	private RecordFile file;
	private int lastId=0;
	private Random rand;
	private Transaction tx;

	TestRecord(){
		Schema sch = new Schema();
		sch.addIntField(ID);
		sch.addIntField(POINTS);
		sch.addStringField(NAME, NAME_LENGTH);
		
		TableInfo ti = new TableInfo(FILE,sch);

		tx = new Transaction();
		file=new RecordFile(ti, tx);
		rand=new Random();
		fileMgr=SimpleDB.fileMgr();
	}

	private void insertNewRecords(int quantity){
		System.out.println("inserimento di " + quantity + " record");
		for(int i=0; i<quantity; i++){
			file.insert();
			file.setInt(ID, ++lastId);
			file.setInt(POINTS, rand.nextInt(101));
			file.setString(NAME, "abba:");
		}
		System.out.println(quantity + " record scritti");
		fileStats();
	}

	private void scanAllRecords(){
		System.out.println("lettura di tutti i record");
		file.beforeFirst();
		int count=0;
		while(file.next())
			count++;
		System.out.println(count + " record letti");
		fileStats();
	}

	private void deleteHalfRecords(){
		System.out.println("eliminazione di circa metà dei record");
		file.beforeFirst();
		int readCount=0;
		int deletedCount=0;
		while(file.next()){
			readCount++;
			if(file.getInt(POINTS)<50){
				deletedCount++;
				file.delete();
			}
		}
		System.out.println(readCount + " record letti, " + deletedCount + " record eliminati");
		fileStats();
	}
	
	private void scanAndAnalizeRecords(){
		System.out.println("lettura e calcolo della media del campo Punteggio");
		file.beforeFirst();
		int readCount=0;
		double sum=0;
		while(file.next()){
			readCount++;
			sum+=file.getInt(POINTS);
		}
		System.out.println(readCount + " record letti. Media calcolata per il campo " + POINTS + ": " + sum/readCount);
		fileStats();
	}
	
	private void fileStats(){
		System.out.println(tx.size(FILENAME) + " blocchi in " + FILENAME);
	    System.out.println(fileMgr.getAggregatedReadStatistics());
	    System.out.println(fileMgr.getAggregatedWriteStatistics());
	    fileMgr.resetStatistics();
	}
	
	public static void main(String[] args) {
		SimpleDB.init("studentdb");
		
		TestRecord test= new TestRecord();
		
		test.insertNewRecords(10000);

		test.scanAllRecords();

		test.deleteHalfRecords();

		test.scanAndAnalizeRecords();

		test.insertNewRecords(7000);

		test.scanAllRecords();
		
	}

}
