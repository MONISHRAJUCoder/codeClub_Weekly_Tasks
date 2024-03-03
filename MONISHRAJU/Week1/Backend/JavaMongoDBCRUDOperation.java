package mongojava;



import java.util.Iterator;
import java.util.Scanner;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;


class CRUDOperation{
	
	public void create_database(String dataBaseName, String collectionName) {
		
		try(MongoClient mongoClient = new MongoClient("localhost", 27017)){
			MongoCredential mongoDatabaseCredential = MongoCredential.createCredential("monishraju", dataBaseName, "root123".toCharArray());
			System.out.println("Connected to database successfully");
		
			//Accessing database order
			MongoDatabase  mongoDatabase = mongoClient.getDatabase(dataBaseName);
			System.out.println("mongoCredential = " + mongoDatabaseCredential);
			System.out.println("Database name = " + mongoDatabase.getName());
			
			//creaing collection
			mongoDatabase.createCollection(collectionName);
			System.out.println("Collection " + collectionName + " created successfully ");	
		}catch(Exception exe) {
			exe.printStackTrace();
		}
		
	}
	public void read_data(String dataBaseName, String collectionName, String columnName) {
		
		
		try(MongoClient mongoClient = new MongoClient("localhost", 27017)){
		  			
		  			MongoDatabase database = mongoClient.getDatabase(dataBaseName);
		  			
		  			//Retrieving a product collection
		  			MongoCollection<Document> productCollection = database.getCollection(collectionName);
		  			System.out.println("product Collection selected successfully");
		  			
		  			//Getting the iterable object
		  			FindIterable<Document> findIterable = productCollection.find();
		  			
		  			//Getting the iterator;
		  			Iterator<Document> iterator = findIterable.iterator();
		  			
		  			while(iterator.hasNext()) {
		  				Document document = iterator.next();
		  				System.out.println(document);
		  				System.out.println(columnName + " = " + document.get(columnName));
		  			}
		  		}
		  		catch(Exception exe) {
		  			exe.printStackTrace();
		  		}
	}
	public void update_data(String dataBaseName, String collectionName, String columnName, String rowNameinThatColumn, String updatingDataColumnName, String updatedData) {
			
			try(MongoClient mongoClient = new MongoClient("localhost", 27017)){
	  			
	  			MongoDatabase database = mongoClient.getDatabase(dataBaseName);
	  			
	  			//Retrieving a product collection
	  			MongoCollection<Document> collection = database.getCollection(collectionName);
	  			System.out.println(collectionName + " = " + "Collection selected successfully");
	  			
	  			
	  			collection.updateOne(Filters.eq(columnName, rowNameinThatColumn), Updates.set(updatingDataColumnName, updatedData));
	  			System.out.println("Document updated successfully");
	  			
	  		}catch(Exception exe) {
	  			exe.printStackTrace();
	  		}
			
}
	public void delete_data(String dataBaseName, String collectionName, String deletingDataColumnName, String deletingData) {
		
		try(MongoClient mongoClient = new MongoClient("localhost", 27017)){
  			
  			MongoDatabase database = mongoClient.getDatabase(dataBaseName);
  			
  			//Retrieving a product collection
  			MongoCollection<Document> collection = database.getCollection(collectionName);
  			System.out.println(collectionName + " = " + "Collection selected successfully");
  			
  			DeleteResult deleteResult = collection.deleteMany(Filters.eq(deletingDataColumnName, deletingData));
  			System.out.println("Deleted Documents count = " + deleteResult.getDeletedCount());
  			
  		}catch(Exception exe) {
  			exe.printStackTrace();
  		}	
}	
}

public class JavaMongoDBCRUDOperation {

	public static void main(String[] args) {
		
		CRUDOperation co = new CRUDOperation();
		
		
		String input, dataBaseName, collectionName, columnName, rowNameinThatColumn, updatingDataColumnName, updatedData, 
		deletingDataColumnName, deletingData;
		Scanner scanner = new Scanner(System.in);
		
		System.out.println("Welcome to CRUD Operation");
		String instruction[] = new String[] {"Create","Read", "Update", "Delete"};
		System.out.println("Instructions...");
		for(int i = 0; i < instruction.length; i++) {
			if (i == 0) {
				System.out.println(i + 1 + " Type " + instruction[i] + " to create a database in MongoDB ");
			}else {
				System.out.println(i + 1 + " Type " + instruction[i] + " to " + instruction[i] + " data in the DataBase ");
			}
		}
		
		System.out.print("Enter the Operation Name      		 : ");
		input = scanner.next();
		
		if (input.equals("create") || input.equals("Create")) {

			System.out.print("Enter the database name    		 : ");
			dataBaseName = scanner.next();	
			
			System.out.print("Enter the collection name    		 : ");
			collectionName = scanner.next();
			
			co.create_database(dataBaseName, collectionName);
			
		}else if(input.equals("Read") || input.equals("read")) {
			
			System.out.print("Enter the database name   		 : ");
			dataBaseName = scanner.next();
			System.out.print("Enter the collection name 		 : ");
			collectionName = scanner.next();
			System.out.print("Enter the column name 			 : ");
			columnName = scanner.next();
			co.read_data(dataBaseName, collectionName, columnName);
		
		}else if(input.equals("update") || input.equals("Update")) {

			System.out.print("Enter the database name			  : ");
			dataBaseName = scanner.next();
			System.out.print("Enter the collection name		 	  : ");
			collectionName = scanner.next();
			System.out.print("Enter the column name				  : ");
			columnName = scanner.next();
			System.out.print("Enter the row name that column	  : ");
			rowNameinThatColumn = scanner.next();
			System.out.print("Enter the updating column name	  : ");
			updatingDataColumnName = scanner.next();
			System.out.print("Enter the updating data			  : ");
			updatedData = scanner.next();
		
			co.update_data(dataBaseName, collectionName, columnName, rowNameinThatColumn, updatingDataColumnName, updatedData);	
		
		}else if(input.equals("delete") || input.equals("Delete")) {
			
			System.out.print("Enter the database name   		  : ");
			dataBaseName = scanner.next();
			System.out.print("Enter the collection name 		  : ");
			collectionName = scanner.next();
			System.out.print("Enter the deleting data column name : ");
			deletingDataColumnName = scanner.next();
			System.out.print("Enter the deleting data  			  : ");
			deletingData = scanner.next();  
			
			
			co.delete_data(dataBaseName, collectionName, deletingDataColumnName, deletingData);
			
		}else {
		
			System.out.println("Try again...");
		
		}
	}

}
