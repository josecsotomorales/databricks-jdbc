package com.databricks.jdbc.common.util;

import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for parsing INSERT statements to extract table and column information. Supports
 * detecting compatible INSERT statements that can be combined into multi-row batches.
 */
public class InsertStatementParser {

  // Pattern to match INSERT INTO table (col1, col2, ...) VALUES format
  private static final Pattern INSERT_PATTERN =
      Pattern.compile(
          "^\\s*INSERT\\s+INTO\\s+([\\w`\\.]+)\\s*\\(([^)]+)\\)\\s+VALUES\\s*\\(",
          Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  /** Represents the parsed components of an INSERT statement. */
  public static class InsertInfo {
    private final String tableName;
    private final List<String> columns;
    private final String originalSql;

    public InsertInfo(String tableName, List<String> columns, String originalSql) {
      this.tableName = tableName;
      this.columns = columns;
      this.originalSql = originalSql;
    }

    public String getTableName() {
      return tableName;
    }

    public List<String> getColumns() {
      return columns;
    }

    public String getOriginalSql() {
      return originalSql;
    }

    public int getColumnCount() {
      return columns.size();
    }

    /**
     * Checks if this INSERT is compatible with another INSERT for batching. Two INSERTs are
     * compatible if they target the same table with the same columns.
     */
    public boolean isCompatibleWith(InsertInfo other) {
      return Objects.equals(this.tableName, other.tableName)
          && Objects.equals(this.columns, other.columns);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      InsertInfo that = (InsertInfo) o;
      return Objects.equals(tableName, that.tableName) && Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode() {
      return Objects.hash(tableName, columns);
    }
  }

  /**
   * Parses an INSERT statement to extract table and column information.
   *
   * @param sql the INSERT SQL statement to parse
   * @return InsertInfo object containing parsed information, or null if not a valid INSERT
   */
  public static InsertInfo parseInsert(String sql) {
    if (sql == null || sql.trim().isEmpty()) {
      return null;
    }

    String trimmedSql = sql.trim();
    Matcher matcher = INSERT_PATTERN.matcher(trimmedSql);

    if (!matcher.find()) {
      return null;
    }

    String tableName = matcher.group(1).trim();
    String columnsStr = matcher.group(2).trim();

    // Parse column names, handling quoted identifiers and whitespace
    List<String> columns = parseColumns(columnsStr);

    if (columns.isEmpty()) {
      return null;
    }

    return new InsertInfo(tableName, columns, trimmedSql);
  }

  /** Parses a comma-separated list of column names, handling quoted identifiers. */
  private static List<String> parseColumns(String columnsStr) {
    return List.of(columnsStr.split(",")).stream()
        .map(String::trim)
        .map(col -> col.replaceAll("^`|`$", "")) // Remove backticks if present
        .filter(col -> !col.isEmpty())
        .toList();
  }

  /**
   * Checks if the given SQL statement is a parametrized INSERT statement suitable for batching.
   *
   * @param sql the SQL statement to check
   * @return true if it's a parametrized INSERT that can be batched
   */
  public static boolean isParametrizedInsert(String sql) {
    InsertInfo info = parseInsert(sql);
    return info != null && sql.contains("?");
  }

  /**
   * Generates a multi-row INSERT statement from the template and number of rows.
   *
   * @param insertInfo the parsed INSERT information
   * @param numberOfRows the number of rows to include in the batch
   * @return the multi-row INSERT SQL statement
   */
  public static String generateMultiRowInsert(InsertInfo insertInfo, int numberOfRows) {
    if (insertInfo == null || numberOfRows <= 0) {
      return null;
    }

    StringBuilder sql = new StringBuilder();
    sql.append("INSERT INTO ")
        .append(insertInfo.getTableName())
        .append(" (")
        .append(String.join(", ", insertInfo.getColumns()))
        .append(") VALUES ");

    // Generate placeholders for each row
    String valueClause = "(" + "?, ".repeat(insertInfo.getColumns().size() - 1) + "?)";

    for (int i = 0; i < numberOfRows; i++) {
      if (i > 0) {
        sql.append(", ");
      }
      sql.append(valueClause);
    }

    return sql.toString();
  }
}
