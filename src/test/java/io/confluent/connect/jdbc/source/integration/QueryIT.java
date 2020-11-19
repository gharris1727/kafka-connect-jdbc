package io.confluent.connect.jdbc.source.integration;

import static junit.framework.TestCase.assertTrue;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.source.JdbcSourceTask;
import io.confluent.connect.jdbc.source.JdbcSourceTaskConfig;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.SingleInstancePostgresRule;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for Postgres OOM conditions.
 */
@Category(IntegrationTest.class)
public class QueryIT {

  private static Logger log = LoggerFactory.getLogger(QueryIT.class);

  @Rule
  public SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance();

  public Map<String, String> props;
  public JdbcSourceTask task;

  @Before
  public void before() {
    props = new HashMap<>();
    String jdbcURL = String
        .format("jdbc:postgresql://localhost:%s/postgres", pg.getEmbeddedPostgres().getPort());
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, jdbcURL);
    props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, "postgres");
    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
    props.put(JdbcSourceTaskConfig.TOPIC_PREFIX_CONFIG, "topic_");
  }

  public void startTask() {
    task = new JdbcSourceTask();
    task.start(props);
  }

  @After
  public void stopTask() {
    if (task != null) {
      task.stop();
    }
  }

  @Test
  public void testQuery() throws InterruptedException, SQLException {
    createTestTable();
    props.put(JdbcSourceConnectorConfig.QUERY_CONFIG, "SELECT * FROM (SELECT * FROM test_table WHERE c2 > 1) AS test_table");
    props.put(JdbcSourceTaskConfig.TABLES_CONFIG, "");
    startTask();
    assertTrue(task.poll().size() > 0);
  }

  private void createTestTable() throws SQLException {
    log.info("Creating test table");
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
      try (Statement s = c.createStatement()) {
        s.execute("CREATE TABLE test_table ( c1 text, c2 integer )");
        s.execute("INSERT INTO test_table VALUES ( 'Hello World', 1 )");
        s.execute("INSERT INTO test_table VALUES ( 'Goodbye World', 2 )");
      }
    }
    log.info("Created table");
  }
}
