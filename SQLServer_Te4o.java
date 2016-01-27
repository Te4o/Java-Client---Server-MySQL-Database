import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SQLServer_Te4o implements Runnable {
    
    private BufferedReader reader;
    private PrintWriter writer;
    private Socket socket;
    private Connection link;
    
    public SQLServer_Te4o(Socket mSocket) throws IOException{
        this.socket = mSocket;
        writer = new PrintWriter(new OutputStreamWriter(this.socket.getOutputStream()));
        reader = new BufferedReader(new InputStreamReader(this.socket.getInputStream()));
    }
    
    private static Connection connect(String url, String user, String pass) throws SQLException{
        Connection result = null;
        result = DriverManager.getConnection(url,user,pass);
        
        return result;
        
    }
    @Override
    public void run() {
        String url = "jdbc:mysql://localhost:3306/dbtest";
        String user = "root";
        String pass = "940813";
        String state = null;
        
        try {
            this.link = SQLServer_Te4o.connect(url, user, pass);
            while(true){
                try {
                    String function = reader.readLine();
                    System.out.println("Funcion is "+function);
                    switch(function){
                        case "add_person":      addPerson(); break;
                        case "update_building": updateBuilding(); break;
                        case "update_apartment":updateApartment(); break;
                        case "update_person":   updatePerson(); break;
                        case "update_payment":  updatePayment(); break;
                        case "buildings_Info":  buildingsInfo(); break;
                        case "payments_Info":   paymentsInfo(); break;
                        case "apartments_Info": apatmentsInfo(); break;
                        case "people_Info":     peopleInfo(); break;
                        case "payments_perMonth": payments_byDate(); break;
                        case "payments_byPrice": payments_byPrice(); break;
                        case "delete_building": deleteBuilding(); break;
                        case "delete_apartment":deleteApartment(); break;
                        case "delete_person":   deletePerson(); break;
                        case "delete_payment":  deletePayment(); break;
              //          case "payments_perName": payments_byName(); break;
                            
                        // Za drugite funkcii si dobavq6 case-ove + metodi dolu
                        // za da vleze v metod clienta purvo puska writer.write("imeto na case-a\n")
                        // posle write-va abs su6tite ne6ta koito readva v metoda
                        // v slu4aq v metoda ako ima String name = reader.readLine(); i primerno
                        // String asd = reader.readLine();
                        // togava v clienta tr da ima writer.write("ime\n");
                        // sled tva writer.write("asd\n");
                            
                        
                        default : break;
                    }
                } catch (IOException ex) {
                    Logger.getLogger(SQLServer_Te4o.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        catch (SQLException ex) {
            Logger.getLogger(SQLServer_Te4o.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    private void addPerson(){
        System.out.println("In addPerson function..");
        try {
            String name = reader.readLine();
            String egn = reader.readLine();
            String ap_id = reader.readLine();
            System.out.println("Got a name: " + name );
            System.out.println("Got a EGN: " + egn );
            System.out.println("Got a Apartment ID: " + ap_id );
            
            PreparedStatement sth = this.link.prepareStatement("INSERT INTO persons(name,EGN,apartment_id) VALUES('"+name+"','"+egn+"','"+ap_id+"')");
            sth.execute();
            
        } catch (Exception ex) {
            Logger.getLogger(SQLServer_Te4o.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    // UPDATE
    private void updateBuilding(){
        System.out.println("In updateBuilding function..");
        try {
            String Name = reader.readLine();
            String Address = reader.readLine();
           
            System.out.println("Got a Name: " + Name );
            System.out.println("Got a Address: " + Address );
            
            
            PreparedStatement sth = this.link.prepareStatement("INSERT INTO buildings(name,address) VALUES('"+Name+"','"+Address+"')");
            sth.execute();
            
        } catch (Exception ex) {
            Logger.getLogger(SQLServer_Te4o.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    private void updateApartment(){
        System.out.println("In updateApartment function..");
        try {
            String Number = reader.readLine();
            String Area = reader.readLine();
            String Building_id = reader.readLine();
           
            System.out.println("Got a Number: " + Number );
            System.out.println("Got a Area: " + Area );
            System.out.println("Got a Building_id: " + Building_id );
            
            
            PreparedStatement sth = this.link.prepareStatement("INSERT INTO apartments(number,area,building_id) VALUES('"+Number+"','"+Area+"','"+Building_id+"')");
            sth.execute();
            
        } catch (Exception ex) {
            Logger.getLogger(SQLServer_Te4o.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    private void updatePerson(){
        System.out.println("In updatePerson function..");
        try {
            String Name = reader.readLine();
            String EGN = reader.readLine();
            String apartment_id = reader.readLine();
           
            System.out.println("Got a Name: " + Name );
            System.out.println("Got a EGN: " + EGN );
            System.out.println("Got a apartment_id: " + apartment_id );
            
            
            PreparedStatement sth = this.link.prepareStatement("INSERT INTO persons(name,EGN,apartment_id ) VALUES('"+Name+"','"+EGN+"','"+apartment_id+"')");
            sth.execute();
            
        } catch (Exception ex) {
            Logger.getLogger(SQLServer_Te4o.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    private void updatePayment(){
        System.out.println("In updatePayment function..");
        try{
        String ID = reader.readLine();
        String Price = reader.readLine();
        String Apartment_ID = reader.readLine();
        String Date = reader.readLine();
        
        System.out.println("Got a ID: " + ID );
        System.out.println("Got a Price: " + Price );
        System.out.println("Got a Apartment_ID: " + Apartment_ID );
        System.out.println("Got a date: " + Date );
        
        PreparedStatement sth = this.link.prepareStatement("INSERT INTO payments(id,price,apartment_id,date) VALUES('"+ID+"','"+Price+"','"+Apartment_ID+"','"+Date+"')");
        sth.execute();
        
        }catch (Exception ex){
        Logger.getLogger(SQLServer_Te4o.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    // VIEW INFO
    private void buildingsInfo(){
        System.out.println("In buildingsInfo function..");
        try{
            
            ResultSet rs = null;
            
            PreparedStatement sth = this.link.prepareStatement("SELECT * FROM buildings");
            rs = sth.executeQuery();
            
            ResultSetMetaData metadata = rs.getMetaData();
            
            writer.println(metadata.getColumnName(1)); // id   
            writer.println(metadata.getColumnName(2)); // name
            writer.println(metadata.getColumnName(3)); // address
            
            writer.println("--END_COLS");
            
            writer.flush();
            
            while(rs.next()){                
                writer.println(rs.getInt(1));       // id
                writer.println(rs.getString(2));    // name
                writer.println(rs.getString(3));    // address
            }
            
            writer.println("--END_ROWS");
            writer.flush();
            
        } catch(Exception ex){
             Logger.getLogger(SQLServer_Te4o.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    private void paymentsInfo(){
        System.out.println("In paymentsInfo function..");
        try{
            ResultSet rs = null;
            
            PreparedStatement sth = this.link.prepareStatement("SELECT * FROM payments");
            rs = sth.executeQuery();
            
           ResultSetMetaData metadata = rs.getMetaData();
            
            writer.println(metadata.getColumnName(1)); // id   
            writer.println(metadata.getColumnName(2)); // price
            writer.println(metadata.getColumnName(3)); // apartment_id
            writer.println(metadata.getColumnName(4)); // date
            
            writer.println("--END_COLS");
            
            writer.flush();
            
            while(rs.next()){                
                writer.println(rs.getInt(1));       // id
                writer.println(rs.getInt(2));       // price
                writer.println(rs.getInt(3));       // apartment_id
                writer.println(rs.getString(4));    // date
            }
            
            writer.println("--END_ROWS");
            writer.flush();
            
        } catch(Exception ex){
             Logger.getLogger(SQLServer_Te4o.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    private void payments_byDate(){
        System.out.println("In payments_byDate function..");
        try{
            ResultSet rs = null;
            
            PreparedStatement sth = this.link.prepareStatement("SELECT * FROM payments GROUP BY date");
            rs = sth.executeQuery();
            
           ResultSetMetaData metadata = rs.getMetaData();
            
            writer.println(metadata.getColumnName(1)); // date
            writer.println(metadata.getColumnName(2)); // id   
            writer.println(metadata.getColumnName(3)); // price
            writer.println(metadata.getColumnName(4)); // apartment_id
            
            
            
            writer.println("--END_COLS");
            
            writer.flush();
            
            while(rs.next()){                
                writer.println(rs.getString(1));    // date
                writer.println(rs.getInt(2));       // id
                writer.println(rs.getInt(3));       // price
                writer.println(rs.getInt(4));       // apartment_id
                
                
            }
            
            writer.println("--END_ROWS");
            writer.flush();
            
        } catch(Exception ex){
             Logger.getLogger(SQLServer_Te4o.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    private void payments_byPrice(){
        System.out.println("In payments_byPrice function..");
        try{
            ResultSet rs = null;
            
            PreparedStatement sth = this.link.prepareStatement("SELECT * FROM payments GROUP BY price");
            rs = sth.executeQuery();
            
           ResultSetMetaData metadata = rs.getMetaData();
            
            
            writer.println(metadata.getColumnName(1)); // id   
            writer.println(metadata.getColumnName(2)); // price
            writer.println(metadata.getColumnName(3)); // apartment_id
            writer.println(metadata.getColumnName(4)); // date
            
            
            writer.println("--END_COLS");
            
            writer.flush();
            
            while(rs.next()){                
               
                writer.println(rs.getInt(1));       // id
                writer.println(rs.getInt(2));       // price
                writer.println(rs.getInt(3));       // apartment_id
                writer.println(rs.getString(4));    // date
                
            }
            
            writer.println("--END_ROWS");
            writer.flush();
            
        } catch(Exception ex){
             Logger.getLogger(SQLServer_Te4o.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    private void apatmentsInfo(){
        System.out.println("In apartmentsInfo function..");
        try{
            ResultSet rs = null;
            
            PreparedStatement sth = this.link.prepareStatement("SELECT * FROM apartments");
            rs = sth.executeQuery();
            
            ResultSetMetaData metadata = rs.getMetaData();
            
            writer.println(metadata.getColumnName(1)); // id   
            writer.println(metadata.getColumnName(2)); // number
            writer.println(metadata.getColumnName(3)); // area
            writer.println(metadata.getColumnName(4)); // building_id
            
            writer.println("--END_COLS");
            writer.flush();
            
            while(rs.next()){                
                writer.println(rs.getInt(1));    // id
                writer.println(rs.getInt(2));    // number
                writer.println(rs.getInt(3));    // area
                writer.println(rs.getInt(4));    // building_id
            }
            
            writer.println("--END_ROWS");
            writer.flush();
            
        } catch(Exception ex){
             Logger.getLogger(SQLServer_Te4o.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    private void peopleInfo(){
        System.out.println("In peopleInfo function..");
        try{
            ResultSet rs = null;
            
            PreparedStatement sth = this.link.prepareStatement("SELECT * FROM persons");
            rs = sth.executeQuery();
            
           ResultSetMetaData metadata = rs.getMetaData();
            
            writer.println(metadata.getColumnName(1)); // name  
            writer.println(metadata.getColumnName(2)); // EGN
            writer.println(metadata.getColumnName(3)); // apartment_id
            
            writer.println("--END_COLS");
            writer.flush();
            
            while(rs.next()){                
                writer.println(rs.getString(1));    // name
                writer.println(rs.getString(2));    // EGN
                writer.println(rs.getInt(3));       // apartment_id
            }
            
            writer.println("--END_ROWS");
            writer.flush();
            
        } catch(Exception ex){
             Logger.getLogger(SQLServer_Te4o.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    // DELETE
    private void deleteBuilding(){
        System.out.println("In deleteBuilding function..");
        try{     
        String building_id = reader.readLine();
        
        System.out.println("Got a building ID: " + building_id );
        
        String deleteBuilding = "DELETE FROM buildings WHERE id = ?";
        PreparedStatement sth = this.link.prepareStatement(deleteBuilding);
        sth.setString(1,building_id);
        sth.executeUpdate();
        
        } catch(Exception ex){
             Logger.getLogger(SQLServer_Te4o.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    private void deleteApartment(){
        System.out.println("In deleteApartment function..");
        try{     
        String id = reader.readLine();
        
        System.out.println("Got a apartment ID: " + id );
        
        String deleteApartment = "DELETE FROM apartments WHERE id = ?";
        PreparedStatement sth = this.link.prepareStatement(deleteApartment);
        sth.setString(1,id);
        sth.executeUpdate();
        
        } catch(Exception ex){
             Logger.getLogger(SQLServer_Te4o.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    private void deletePerson(){
        System.out.println("In deletePerson function..");
        try{     
        String aID = reader.readLine();
        
        System.out.println("Got a apartment ID: " + aID );
        
        String deletePerson = "DELETE FROM persons WHERE apartment_id = ?";
        PreparedStatement sth = this.link.prepareStatement(deletePerson);
        sth.setString(1,aID);
        sth.executeUpdate();
        
        } catch(Exception ex){
             Logger.getLogger(SQLServer_Te4o.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    private void deletePayment(){
        System.out.println("In deletePayment function..");
        try{     
        String aID = reader.readLine();
        
        System.out.println("Got a apartment ID: " + aID );
        
        String deletePayment = "DELETE FROM payments WHERE apartment_id = ?";
        PreparedStatement sth = this.link.prepareStatement(deletePayment);
        sth.setString(1,aID);
        sth.executeUpdate();
        
        } catch(Exception ex){
             Logger.getLogger(SQLServer_Te4o.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}