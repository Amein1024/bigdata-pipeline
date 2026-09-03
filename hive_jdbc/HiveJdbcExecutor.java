import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class HiveJdbcExecutor {

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            System.err.println(
                "Brug: java HiveJdbcExecutor \"SQL-KOMMANDO\""
            );
            System.exit(1);
        }

        String sql = args[0];

        String jdbcUrl =
            "jdbc:hive2://localhost:10000/default";

        Class.forName(
            "org.apache.hive.jdbc.HiveDriver"
        );

        try (
            Connection connection =
                DriverManager.getConnection(
                    jdbcUrl,
                    "",
                    ""
                );

            Statement statement =
                connection.createStatement()
        ) {
            statement.execute(sql);
        }
    }
}