package com.bertvanlangen.statementcachetest;


import com.beust.jcommander.JCommander;
import com.ibm.db2.jcc.DB2Connection;
import com.ibm.db2.jcc.DB2SystemMonitor;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

import static com.bertvanlangen.statementcachetest.StatementCacheTest.TestExecutionMetric.*;

public class StatementCacheTest {

    // JDBC driver name and database URL
    private static final String JDBC_DRIVER = "com.ibm.db2.jcc.DB2Driver";
    private static final String DB_URL = "jdbc:db2://localhost:50000/TEST "; // URL: IBM DB2 Universal Driver Type 4

    //  Database credentials
    private static final String USER = "db2user";
    private static final String PASS = "db2password";

    // Test config params
    private static final int NUM_QUERY_EXECUTIONS = 25;
    private static final int SLEEP_TIME_BETWEEN_QUERIES_MS = 10;

    public static void main(String[] args) throws SQLException, InterruptedException {

        CommandLineOptions options = new CommandLineOptions();
        new JCommander(options, args);

        boolean showDetails = options.details;

        warmUp(0); // warm up db2 server..


        executeTest(0, showDetails);
        executeTest(25, showDetails);
    }

    private static void warmUp(int maxStatementsCache) throws SQLException, InterruptedException {

        System.out.println();
        System.out.println("Warming up...");
        Connection connection = getConnection(maxStatementsCache);
        DB2SystemMonitor systemMonitor = ((DB2Connection) connection).getDB2SystemMonitor();
        systemMonitor.enable(true);

        for (int i = 0; i < NUM_QUERY_EXECUTIONS; i++) {
            executeQuery(systemMonitor, null, connection, "SELECT * FROM TEST.Person where personID = ?");
            Thread.sleep(SLEEP_TIME_BETWEEN_QUERIES_MS);
        }
        connection.close();
        System.out.println("Done warming up.");
    }

    private static void executeTest(int maxStatementsCache, boolean printIndividualExecutionTimes) throws SQLException, InterruptedException {

        System.out.println();
        System.out.println("# Statements cache " + (maxStatementsCache == 0 ? "disabled" : maxStatementsCache));

        Connection connection = getConnection(maxStatementsCache);
        DB2SystemMonitor systemMonitor = ((DB2Connection) connection).getDB2SystemMonitor();
        systemMonitor.enable(true);

        List<TestExecution> testExecutions = new ArrayList<TestExecution>(NUM_QUERY_EXECUTIONS);
        for (int i = 0; i < NUM_QUERY_EXECUTIONS; i++) {
            TestExecution execution = new TestExecution(String.format("%-25s", "Fetch Person record nr " + (i + 1)));
            testExecutions.add(execution);
            executeQuery(systemMonitor, execution, connection, "SELECT * FROM BANK.Person where personID = ?");
            Thread.sleep(SLEEP_TIME_BETWEEN_QUERIES_MS);
        }
        connection.close();

        if (printIndividualExecutionTimes) {
            printIndividualTestExecutions(testExecutions);
        }
        printTotalTestExecutions(testExecutions);
    }

    private static void executeQuery(DB2SystemMonitor systemMonitor, TestExecution execution, Connection connection, String query) throws SQLException {
        PreparedStatement preparedStatement = null;
        try {
            connection.setAutoCommit(true);
            BigDecimal random = new BigDecimal(Math.random()).multiply(new BigDecimal(100000000)).setScale(0, BigDecimal.ROUND_DOWN);

            systemMonitor.start(DB2SystemMonitor.RESET_TIMES);
            preparedStatement = connection.prepareStatement(query);
            preparedStatement.setBigDecimal(1, random);
            preparedStatement.execute();
            systemMonitor.stop();

            if (execution != null) {
                execution.addMetric(Metric.CORE_DRIVER_TIME, systemMonitor.getCoreDriverTimeMicros());
                execution.addMetric(Metric.NETWORK_TIME, systemMonitor.getNetworkIOTimeMicros());
                execution.addMetric(Metric.SERVER_TIME, systemMonitor.getServerTimeMicros());
                execution.addMetric(Metric.APPLICATION_TIME, systemMonitor.getApplicationTimeMillis());
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    private static Connection getConnection(int maxStatementsCache) throws SQLException {
        Connection con = null;
        try {
            // Register JDBC driver
            Class.forName(JDBC_DRIVER);

            Properties connProperties = new Properties();
            connProperties.put("user", USER);
            connProperties.put("password", PASS);

            if (maxStatementsCache > 0) {
                connProperties.put("maxStatements", String.valueOf(maxStatementsCache));
            }

            // Open a connection
            con =  DriverManager.getConnection(DB_URL, connProperties);

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return con;
    }

    private static void printIndividualTestExecutions(List<TestExecution> testExecutions) {
        for (TestExecution testExecution : testExecutions) {
            String executionDescription = testExecution.getExecutionDescription();
            System.out.print(executionDescription + " | ");
            for (TestExecutionMetric executionMetric : testExecution.getExecutionMetrics()) {
                System.out.print(executionMetric.metric.toString());
                System.out.print(" " + microToMillis(executionMetric.time)  + " ms | ");
            }
            System.out.println();
        }
    }

    private static void printTotalTestExecutions(List<TestExecution> testExecutions) {
        long totalServerTime = 0;
        long totalNetworkTime = 0;
        long totalCoreDriverTime = 0;
        long totalApplicationTime = 0;
        long totalTime = 0;
        for (TestExecution testExecution : testExecutions) {
            for (TestExecutionMetric executionMetric : testExecution.getExecutionMetrics()) {
                switch (executionMetric.metric) {
                    case SERVER_TIME:
                        totalServerTime += executionMetric.time;
                        totalTime += executionMetric.time;
                        break;
                    case NETWORK_TIME:
                        totalNetworkTime += executionMetric.time;
                        totalTime += executionMetric.time;
                        break;
                    case CORE_DRIVER_TIME:
                        totalCoreDriverTime += executionMetric.time;
                        totalTime += executionMetric.time;
                        break;
                    case APPLICATION_TIME:
                        totalApplicationTime += executionMetric.time;
                        totalTime += executionMetric.time;
                        break;
                }
            }
        }
        System.out.println("------------------------------------------------------------------------------------------------------");
        System.out.printf("%25s | Core driver %10s ms | Network IO %7s ms | Server %10s ms | Application %10s ms | >> TOTAL %s ms%n"
                , "Total execution times"
                , microToMillis(totalCoreDriverTime)
                , microToMillis(totalNetworkTime)
                , microToMillis(totalServerTime)
                , microToMillis(totalApplicationTime)
                , microToMillis(totalTime));
        System.out.println("------------------------------------------------------------------------------------------------------");
    }

    private static String microToMillis(long timeInMicros) {
        return String.format("%.3f", ((double) timeInMicros / 1000d));
    }

    static class TestExecution{
        private String executionDescription;
        private List<TestExecutionMetric> executionMetrics = new ArrayList<TestExecutionMetric>(4);

        TestExecution(String executionDescription) {
            this.executionDescription = executionDescription;
        }

        String getExecutionDescription() {
            return executionDescription;
        }

        void addMetric(Metric metricDescription, long metricTimeMs) {
            executionMetrics.add(new TestExecutionMetric(metricDescription, metricTimeMs));
        }

        List<TestExecutionMetric> getExecutionMetrics() {
            return executionMetrics;
        }
    }

    static class TestExecutionMetric {
        private Metric metric;
        private long time;

        enum Metric {
           SERVER_TIME,
           NETWORK_TIME,
           CORE_DRIVER_TIME,
           APPLICATION_TIME
       }

        TestExecutionMetric(Metric metric, long time) {
            this.metric = metric;
            this.time = time;
        }
    }

}
