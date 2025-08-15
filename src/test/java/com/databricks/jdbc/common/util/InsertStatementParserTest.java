package com.databricks.jdbc.common.util;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.common.util.InsertStatementParser.InsertInfo;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class InsertStatementParserTest {

  @Test
  void testParseBasicInsert() {
    String sql = "INSERT INTO users (id, name, email) VALUES (?, ?, ?)";
    InsertInfo info = InsertStatementParser.parseInsert(sql);

    assertNotNull(info);
    assertEquals("users", info.getTableName());
    assertEquals(Arrays.asList("id", "name", "email"), info.getColumns());
    assertEquals(sql, info.getOriginalSql());
  }

  @Test
  void testParseInsertWithWhitespace() {
    String sql = "   INSERT   INTO   users   (  id  ,  name  ,  email  )   VALUES   ( ?, ?, ? )";
    InsertInfo info = InsertStatementParser.parseInsert(sql);

    assertNotNull(info);
    assertEquals("users", info.getTableName());
    assertEquals(Arrays.asList("id", "name", "email"), info.getColumns());
  }

  @Test
  void testParseInsertWithBackticks() {
    String sql = "INSERT INTO `my_table` (`id`, `user_name`, `email_address`) VALUES (?, ?, ?)";
    InsertInfo info = InsertStatementParser.parseInsert(sql);

    assertNotNull(info);
    assertEquals("`my_table`", info.getTableName());
    assertEquals(Arrays.asList("id", "user_name", "email_address"), info.getColumns());
  }

  @Test
  void testParseInsertWithSchemaPrefix() {
    String sql = "INSERT INTO schema.users (id, name) VALUES (?, ?)";
    InsertInfo info = InsertStatementParser.parseInsert(sql);

    assertNotNull(info);
    assertEquals("schema.users", info.getTableName());
    assertEquals(Arrays.asList("id", "name"), info.getColumns());
  }

  @Test
  void testParseInsertCaseInsensitive() {
    String sql = "insert into Users (ID, Name) values (?, ?)";
    InsertInfo info = InsertStatementParser.parseInsert(sql);

    assertNotNull(info);
    assertEquals("Users", info.getTableName());
    assertEquals(Arrays.asList("ID", "Name"), info.getColumns());
  }

  @Test
  void testParseInvalidSql() {
    assertNull(InsertStatementParser.parseInsert("SELECT * FROM users"));
    assertNull(InsertStatementParser.parseInsert("UPDATE users SET name = ?"));
    assertNull(InsertStatementParser.parseInsert("DELETE FROM users"));
    assertNull(InsertStatementParser.parseInsert(null));
    assertNull(InsertStatementParser.parseInsert(""));
    assertNull(InsertStatementParser.parseInsert("   "));
  }

  @Test
  void testParseInsertWithoutValues() {
    String sql = "INSERT INTO users (id, name)";
    InsertInfo info = InsertStatementParser.parseInsert(sql);
    assertNull(info);
  }

  @Test
  void testParseInsertWithoutColumns() {
    String sql = "INSERT INTO users VALUES (?, ?)";
    InsertInfo info = InsertStatementParser.parseInsert(sql);
    assertNull(info);
  }

  @Test
  void testIsParametrizedInsert() {
    assertTrue(
        InsertStatementParser.isParametrizedInsert("INSERT INTO users (id, name) VALUES (?, ?)"));
    assertFalse(
        InsertStatementParser.isParametrizedInsert(
            "INSERT INTO users (id, name) VALUES (1, 'John')"));
    assertFalse(InsertStatementParser.isParametrizedInsert("SELECT * FROM users"));
    assertFalse(InsertStatementParser.isParametrizedInsert(null));
  }

  @Test
  void testInsertInfoCompatibility() {
    InsertInfo info1 =
        InsertStatementParser.parseInsert("INSERT INTO users (id, name) VALUES (?, ?)");
    InsertInfo info2 =
        InsertStatementParser.parseInsert("INSERT INTO users (id, name) VALUES (?, ?)");
    InsertInfo info3 =
        InsertStatementParser.parseInsert("INSERT INTO users (id, email) VALUES (?, ?)");
    InsertInfo info4 =
        InsertStatementParser.parseInsert("INSERT INTO orders (id, name) VALUES (?, ?)");

    assertNotNull(info1);
    assertNotNull(info2);
    assertNotNull(info3);
    assertNotNull(info4);

    assertTrue(info1.isCompatibleWith(info2));
    assertFalse(info1.isCompatibleWith(info3)); // Different columns
    assertFalse(info1.isCompatibleWith(info4)); // Different table
  }

  @Test
  void testGenerateMultiRowInsert() {
    InsertInfo info =
        InsertStatementParser.parseInsert("INSERT INTO users (id, name, email) VALUES (?, ?, ?)");
    assertNotNull(info);

    String multiRowSql = InsertStatementParser.generateMultiRowInsert(info, 3);
    String expected = "INSERT INTO users (id, name, email) VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)";
    assertEquals(expected, multiRowSql);
  }

  @Test
  void testGenerateMultiRowInsertSingleRow() {
    InsertInfo info =
        InsertStatementParser.parseInsert("INSERT INTO users (id, name) VALUES (?, ?)");
    assertNotNull(info);

    String multiRowSql = InsertStatementParser.generateMultiRowInsert(info, 1);
    String expected = "INSERT INTO users (id, name) VALUES (?, ?)";
    assertEquals(expected, multiRowSql);
  }

  @Test
  void testGenerateMultiRowInsertInvalidInput() {
    InsertInfo info =
        InsertStatementParser.parseInsert("INSERT INTO users (id, name) VALUES (?, ?)");
    assertNotNull(info);

    assertNull(InsertStatementParser.generateMultiRowInsert(null, 3));
    assertNull(InsertStatementParser.generateMultiRowInsert(info, 0));
    assertNull(InsertStatementParser.generateMultiRowInsert(info, -1));
  }

  @Test
  void testInsertInfoEqualsAndHashCode() {
    InsertInfo info1 =
        new InsertInfo(
            "users", List.of("id", "name"), "INSERT INTO users (id, name) VALUES (?, ?)");
    InsertInfo info2 =
        new InsertInfo(
            "users", List.of("id", "name"), "INSERT INTO users (id, name) VALUES (?, ?)");
    InsertInfo info3 =
        new InsertInfo(
            "users", List.of("id", "email"), "INSERT INTO users (id, email) VALUES (?, ?)");

    assertEquals(info1, info2);
    assertNotEquals(info1, info3);
    assertEquals(info1.hashCode(), info2.hashCode());
    assertNotEquals(info1.hashCode(), info3.hashCode());
  }
}
