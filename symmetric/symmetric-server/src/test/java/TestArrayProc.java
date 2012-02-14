import oracle.jdbc.*;
import oracle.sql.ArrayDescriptor;
import oracle.sql.ARRAY;
import oracle.jdbc.OracleTypes;
import java.sql.*;

public class TestArrayProc {

    public static void main(String[] args)
    throws ClassNotFoundException, SQLException
    {
        long ts = System.currentTimeMillis();
        DriverManager.registerDriver (new oracle.jdbc.OracleDriver());

        String url = "jdbc:oracle:thin:@//localhost:1521/XE";

        Connection conn =
        DriverManager.getConnection(url,"SampleRoot","admin");

        conn.setAutoCommit(false);

        // Create descriptors for each Oracle collection type required
        ArrayDescriptor oracleVarchar2Collection =
            ArrayDescriptor.createDescriptor("VARCHAR2_T",conn);

        ArrayDescriptor oracleIntegerCollection =
            ArrayDescriptor.createDescriptor("INTEGER_T",conn);

        ArrayDescriptor oracleNumberCollection =
            ArrayDescriptor.createDescriptor("NUMBER_T",conn);

        CallableStatement stmt = 
            conn.prepareCall("{ call insert_payments_a(?, ?, ?, ?, ?, ? ) }"); 

        int INSERT_COUNT = 100000;
        // JAVA arrays to hold the data.
        double[] payment_amount_array = new double[INSERT_COUNT];
        String[] card_number_array    = new String[INSERT_COUNT];
        String[] expire_month_array   = new String[INSERT_COUNT];
        String[] expire_year_array    = new String[INSERT_COUNT];
        String[] name_on_card_array   = new String[INSERT_COUNT];

        // Fill the Java arrays.
        for (int i=0; i< INSERT_COUNT; i++) {
            payment_amount_array[i]    = 99.99;
            card_number_array[i]       = "1234567890123456";
            expire_month_array[i]      = "12";
            expire_year_array[i]       = "15";
            name_on_card_array[i]     = "Mr S ODONNELL";
        }

        payment_amount_array[50] = 1001.00;

        // Cast the Java arrays into Oracle arrays
        ARRAY ora_payment_amount = new ARRAY (oracleNumberCollection,   conn, payment_amount_array);
        ARRAY ora_card_number    = new ARRAY (oracleVarchar2Collection, conn, card_number_array);
        ARRAY ora_expire_month   = new ARRAY (oracleVarchar2Collection, conn, expire_month_array);
        ARRAY ora_expire_year    = new ARRAY (oracleVarchar2Collection, conn, expire_year_array);
        ARRAY ora_name_on_card   = new ARRAY (oracleVarchar2Collection, conn, name_on_card_array);

        // Bind the input arrays.
        stmt.setObject(1, ora_payment_amount);
        stmt.setObject(2, ora_card_number);
        stmt.setObject(3, ora_expire_month);
        stmt.setObject(4, ora_expire_year);
        stmt.setObject(5, ora_name_on_card);

        // Bind the output array, this will contain any exception indexes.
        stmt.registerOutParameter(6, OracleTypes.ARRAY, "INTEGER_T");

        stmt.execute();

        // Get any exceptions. Remember Oracle arrays index from 1,
        // so all indexes are +1 off.
        int[] errors = new int[100];
        ARRAY ora_errors = ((OracleCallableStatement)stmt).getARRAY(6);
        // cast the oracle array back to a Java array
        // Remember, Oracle arrays are indexed from 1, while Java is indexed from zero
        // Any array indexes returned as failures from Oracle need to have 1 subtracted from them
        // to reference the correct Java array element!
        errors = ora_errors.getIntArray();
        System.out.println(errors.length);
        for (int i : errors) {
            System.out.println("error:"+i);
        }
        conn.commit();
        
        ts = System.currentTimeMillis() -ts;
        System.err.println("It took " + ts + "ms to insert " + INSERT_COUNT + " rows at " + ((double)ts/(double)INSERT_COUNT) + "ms/row");
    }
}