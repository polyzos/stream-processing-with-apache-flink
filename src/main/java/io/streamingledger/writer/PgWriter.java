package io.streamingledger.writer;

import io.streamingledger.models.Account;
import io.streamingledger.utils.DataSourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.stream.Stream;

public class PgWriter {
    private static final Logger logger
            = LoggerFactory.getLogger(PgWriter.class);
    public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {
        Stream<Account> accounts = DataSourceUtils
                .loadDataFile("/data/accounts.csv")
                .map(DataSourceUtils::toAccount);

        String query = "INSERT INTO accounts(" +
                "accountId," +
                "districtId," +
                "frequency," +
                "creationDate," +
                "updateTime" +
                ") VALUES (?, ?, ?, ?, ?)";

        var count = 0;

        Class.forName("org.postgresql.Driver");
        try (final Connection connection =
                     DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "postgres");
        ) {
            for (Iterator<Account> it = accounts.iterator(); it.hasNext(); ) {
                try (PreparedStatement pst = connection.prepareStatement(query)) {

                    Account account = it.next();
                    logger.info("Sending account: {}", account);
                    pst.setString(1, account.getAccountId());
                    pst.setInt(2, account.getDistrictId());
                    pst.setString(3, account.getFrequency());
                    pst.setString(4, account.getCreationDate());
                    pst.setLong(5, account.getUpdateTime());
                    pst.execute();
                    count += 1;
                    if (count % 1000 == 0) {
                        logger.info("Total so far {}.", count);
                    }
                }
            }


        } catch (SQLException ex) {
            logger.error(ex.getMessage(), ex);
        }
    }
}
