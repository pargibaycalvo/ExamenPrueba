/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package enoloxia;

/**
 *
 * @author oracle
 */
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JOptionPane;
import javax.xml.stream.XMLStreamException;

public class Enoloxia {

    static File archivo = new File("/home/oracle/NetBeansProjects/enoloxia/analisis.txt");
    static ResultSet result;
    static Connection conn;
    static String[][]datos=new String[4][7];

    public static void main(String[] args) throws FileNotFoundException, 
    IOException, SQLException, ClassNotFoundException, XMLStreamException {
        
        conexion();
        
        leerfichero(archivo);
        actualizartablas();
        
//        recorremos el Array para poder introducir los datos en la tabla indicando sus posiciones
        for(int i=0; i<datos.length; i++){
            escribirdatos(datos[i][0],
                    nombreuva(datos[i][4]),
                    acidezminmax(Integer.parseInt(datos[i][1]),datos[i][4]),
                    Integer.parseInt(datos[i][5]));
            }
        
    }
    
    //leer fichero analisis.txt y introducir sus datos en un Array
    static void leerfichero(File archivo) throws IOException{
      BufferedReader br=null;
      int d =0;  
      try {
         br =new BufferedReader(new FileReader(archivo));
         String line=br.readLine();
         while (null!=line) {
            datos[d]=line.split(",");
            d++;
            line=br.readLine();
         }
         for(int i=0; i<datos.length; i++){
             for (String dato : datos[i]) {
                 
                 System.out.println(dato);
             }
             
            }
      } catch (Exception e) {
          System.out.println("Error en la lectura del fichero "+e.getMessage());
      } finally {
         if (null!=br) {
            br.close();
         }
      }
    }
    
    //conexion a la base de datos
    static void conexion(){
        try {
            String driver = "jdbc:oracle:thin:";
            String host = "localhost";
            String porto = "1521";
            String sid = "orcl";
            String usuario = "hr";
            String password = "hr";
            String url = driver+usuario
                    +"/"+password+"@"+host
                    +":"+porto+":"+sid;
            conn = DriverManager.getConnection(url);
            System.out.println("Base de datos operativa. Conectado");
        } catch (SQLException ex) {
            Logger.getLogger(Enoloxia.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
    //consultar acidez min y max en la tabla sql 
    static String acidezminmax(int acidez, String cod){
        int min=0;
        int max=0;
        String trata = null;

        try {
            PreparedStatement ver = conn.prepareStatement("Select ACIDEZMIN,ACIDEZMAX from uvas where TIPO=?");
            ver.setString(1, cod);
            result=ver.executeQuery();
            result.next();
                min = Integer.parseInt(result.getString("ACIDEZMIN"));
                max = Integer.parseInt(result.getString("ACIDEZMAX"));
            if (acidez < min) {
                return "subir acidez";
            } else if (acidez > max) {
                return "baixar acidez";
            } else {
                return "equilibrqada";
            }
            
        } catch (SQLException ex) {
            System.out.println("Error, verificar que la base este conectada "+ex.getMessage());
        }
        return null;
    }
    
    //actualizar numero de analisis tabla clientes
    static void actualizartablas(){

        try {   
            PreparedStatement actualiza = conn.prepareStatement("update clientes set NUMERODEANALISIS=NUMERODEANALISIS+1");
            actualiza.execute();
            System.out.println("Análisis actualizado");
        } catch (SQLException ex) {
            System.out.println("Error, introduzca el DNI correcto "+ex.getMessage());
        }
    }
    
    //escribir datos en la tabla xerado
    static void escribirdatos(String num, String nomeuva, String tratacidez, int total) throws SQLException{
         try {
            PreparedStatement ps = conn.prepareStatement("Insert into xerado(NUM, NOMEUVA, TRATACIDEZ, TOTAL) values(?,?,?,?)");
            ps.setString(1, num);
            ps.setString(2, nomeuva);
            ps.setString(3, tratacidez);
            ps.setInt(4, total * 15);//recogemos el valor y ya calculamos el total
            ps.execute();
        
    }catch (SQLException ex) {
            System.out.println("Error"+ex.getMessage());
        }
    }
    
    //nombre de la uva para la tabla xerado
    static String nombreuva(String nomeuva){
        try {
            PreparedStatement ver = conn.prepareStatement("Select nomeu from uvas where TIPO=?");
            ver.setString(1, nomeuva);
            result=ver.executeQuery();
            result.next();
                return result.getNString("nomeu");
        } catch (SQLException ex) {
            System.out.println("Error"+ex.getMessage());
        }
        return null;
    }
   
    
    
    
    
}
