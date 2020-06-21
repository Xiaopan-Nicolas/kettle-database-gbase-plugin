package org.pentaho.di.core.database;

import java.sql.ResultSet;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.plugins.DatabaseMetaPlugin;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.StringUtil;

/**
 * @Author: cunxiaopan
 * @Date: 2020/5/26 10:46 上午
 * @Description: GBase kettle 插件
 */
@DatabaseMetaPlugin(type = "GBase", typeDescription = "GBase")
public class GbaseDatabaseMeta extends BaseDatabaseMeta implements DatabaseInterface {

  /**
   * <p>支持的连接方式</p>
   * <ul>
   *   <li>native</li>
   *   <li>odbc</li>
   *   <li>jndi</li>
   * </ul>
   */
  @Override
  public int[] getAccessTypeList() {
    return new int[] { DatabaseMeta.TYPE_ACCESS_NATIVE, DatabaseMeta.TYPE_ACCESS_ODBC, DatabaseMeta.TYPE_ACCESS_JNDI };
  }

  /**
   * <p>获取默认数据库端口</p>
   */
  @Override
  public int getDefaultDatabasePort() {
    if (getAccessType() == DatabaseMeta.TYPE_ACCESS_NATIVE) {
      return 5258;
    }
    return -1;
  }

  /**
   * <p>当前数据库是否支持自增类型的字段,暂时实现为不支持，返回false</p>
   * <ul>
   *   <li>GBase 8t自增类型有serial、serial8、bigserial</li>
   *   <li>serial类型支知持的最大值为道2,147,483,647</li>
   *   <li>serial8和bigserial类型支持的最大值为9,223,372,036,854,775,807</li>
   *   <li>自增类型可以版支持serial(n)类型定义，权即从n值开始自增</li>
   * </ul>
   */
  @Override
  public boolean supportsAutoInc() {
    return false;
  }

  /**
   * <p>获取限制读取条数的数据，追加再select语句后实现限制返回的结果数</p>
   *
   * @see org.pentaho.di.core.database.DatabaseInterface#getLimitClause(int)
   */
  @Override
  public String getLimitClause(int nrRows) {
    return " LIMIT " + nrRows;
  }

  /**
   * 返回获取表所有字段信息的语句
   *
   * @param tableName
   * @return The SQL to launch.
   */
  @Override
  public String getSQLQueryFields(String tableName) {
    return "SELECT * FROM " + tableName + " WHERE 1=0";
  }

  @Override
  public String getSQLTableExists(String tablename) {
    return getSQLQueryFields(tablename);
  }

  @Override
  public String getSQLColumnExists(String columnname, String tablename) {
    return getSQLQueryColumnFields(columnname, tablename);
  }

  public String getSQLQueryColumnFields(String columnname, String tableName) {
    return "SELECT " + columnname + " FROM " + tableName + " WHERE 1=0";
  }

  /**
   * @see org.pentaho.di.core.database.DatabaseInterface#getNotFoundTK(boolean)
   */
  @Override
  public int getNotFoundTK( boolean use_autoinc ) {
    if ( supportsAutoInc() && use_autoinc ) {
      return 1;
    }
    return super.getNotFoundTK( use_autoinc );
  }

  @Override
  public String getDriverClass() {
    if (getAccessType() == DatabaseMeta.TYPE_ACCESS_ODBC) {
      return "sun.jdbc.odbc.JdbcOdbcDriver";
    } else {
      return "com.gbase.jdbc.Driver";
    }
  }

  /**
   *  获取连接数据库的url
   * @param hostname
   * @param port
   * @param databaseName
   * @return
   * @throws KettleDatabaseException
   */
  @Override
  public String getURL(String hostname, String port, String databaseName) throws KettleDatabaseException {
    if (getAccessType() == DatabaseMeta.TYPE_ACCESS_ODBC) {
      return "jdbc:odbc:" + databaseName;
    } else if (getAccessType() == DatabaseMeta.TYPE_ACCESS_NATIVE) {
      if ( Const.isEmpty( port ) ) {
        return "jdbc:gbase://" + hostname + "/" + databaseName;
      } else {
        return "jdbc:gbase://" + hostname + ":" + port + "/" + databaseName;
      }
    } else {
      throw new KettleDatabaseException("不支持的数据库连接方式[" + getAccessType() + "]");
    }
  }

  /**
   * @return The extra option separator in database URL for this platform (usually this is semicolon ; )
   */
  @Override
  public String getExtraOptionSeparator() {
    return "&";
  }

  /**
   * @return This indicator separates the normal URL from the options
   */
  @Override
  public String getExtraOptionIndicator() {
    return "?";
  }

  /**
   * @return true if the database supports transactions.
   */
  @Override
  public boolean supportsTransactions() {
    return false;
  }

  /**
   * @return true if the database supports bitmap indexes
   */
  @Override
  public boolean supportsBitmapIndex() {
    return false;
  }

  /**
   * @return true if the database supports views
   */
  @Override
  public boolean supportsViews() {
    return true;
  }

  /**
   * @return true if the database supports synonyms
   */
  @Override
  public boolean supportsSynonyms() {
    return false;
  }

  /**
   * Generates the SQL statement to add a column to the specified table
   *
   * @param tablename
   *          The table to add
   * @param v
   *          The column defined as a value
   * @param tk
   *          the name of the technical key field
   * @param use_autoinc
   *          whether or not this field uses auto increment
   * @param pk
   *          the name of the primary key field
   * @param semicolon
   *          whether or not to add a semi-colon behind the statement.
   * @return the SQL statement to add a column to the specified table
   */
  @Override
  public String getAddColumnStatement( String tablename, ValueMetaInterface v, String tk, boolean use_autoinc,
      String pk, boolean semicolon ) {
    return "ALTER TABLE " + tablename + " ADD " + getFieldDefinition( v, tk, pk, use_autoinc, true, false );
  }

  /**
   * Generates the SQL statement to modify a column in the specified table
   *
   * @param tablename
   *          The table to add
   * @param v
   *          The column defined as a value
   * @param tk
   *          the name of the technical key field
   * @param use_autoinc
   *          whether or not this field uses auto increment
   * @param pk
   *          the name of the primary key field
   * @param semicolon
   *          whether or not to add a semi-colon behind the statement.
   * @return the SQL statement to modify a column in the specified table
   */
  @Override
  public String getModifyColumnStatement( String tablename, ValueMetaInterface v, String tk, boolean use_autoinc,
      String pk, boolean semicolon ) {
    return "ALTER TABLE " + tablename + " MODIFY " + getFieldDefinition( v, tk, pk, use_autoinc, true, false );
  }

  /**
   * 新增、修改、删除字段 sql 后半句
   * @param v
   * @param tk
   * @param pk
   * @param use_autoinc
   * @param add_fieldname
   * @param add_cr
   * @return
   */
  @Override
  public String getFieldDefinition( ValueMetaInterface v, String tk, String pk, boolean use_autoinc,
      boolean add_fieldname, boolean add_cr ) {
    String retval = "";

    String fieldname = v.getName();
    int length = v.getLength();
    int precision = v.getPrecision();

    if ( add_fieldname ) {
      retval += fieldname + " ";
    }

    int type = v.getType();
    switch ( type ) {
      case ValueMetaInterface.TYPE_DATE:
        retval += "DATETIME";
        break;
      case ValueMetaInterface.TYPE_TIMESTAMP:
        retval += "TIMESTAMP";
        break;
      case ValueMetaInterface.TYPE_BOOLEAN:
        if ( supportsBooleanDataType() ) {
          retval += "BOOLEAN";
        } else {
          retval += "CHAR(1)";
        }
        break;

      case ValueMetaInterface.TYPE_NUMBER:
      case ValueMetaInterface.TYPE_INTEGER:
      case ValueMetaInterface.TYPE_BIGNUMBER:
        // Technical key  || Primary key
        if ( fieldname.equalsIgnoreCase( tk ) ||
            fieldname.equalsIgnoreCase( pk )
        ) {
          if ( use_autoinc ) {
            retval += "BIGINT AUTO_INCREMENT NOT NULL PRIMARY KEY";
          } else {
            retval += "BIGINT NOT NULL PRIMARY KEY";
          }
        } else {
          // Integer values...
          if ( precision == 0 ) {
            if ( length > 9 ) {
              if ( length < 19 ) {
                // can hold signed values between -9223372036854775808 and 9223372036854775807
                // 18 significant digits
                retval += "BIGINT";
              } else {
                retval += "DECIMAL(" + length + ")";
              }
            } else {
              retval += "INT";
            }
          } else {
            // Floating point values...
            if ( length > 15 ) {
              retval += "DECIMAL(" + length;
              if ( precision > 0 ) {
                retval += ", " + precision;
              }
              retval += ")";
            } else {
              // A double-precision floating-point number is accurate to approximately 15 decimal places.
              // http://mysql.mirrors-r-us.net/doc/refman/5.1/en/numeric-type-overview.html
              retval += "DOUBLE";
            }
          }
        }
        break;
      case ValueMetaInterface.TYPE_STRING:
        if ( length > 0 ) {
          if ( length == 1 ) {
            retval += "CHAR(1)";
          } else if ( length < 256 ) {
            retval += "VARCHAR(" + length + ")";
          } else if ( length < 65536 ) {
            retval += "TEXT";
          } else if ( length < 16777216 ) {
            retval += "MEDIUMTEXT";
          } else {
            retval += "LONGTEXT";
          }
        } else {
          retval += "TINYTEXT";
        }
        break;
      case ValueMetaInterface.TYPE_BINARY:
        retval += "LONGBLOB";
        break;
      default:
        retval += " UNKNOWN";
        break;
    }

    if ( add_cr ) {
      retval += Const.CR;
    }

    return retval;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.pentaho.di.core.database.DatabaseInterface#getReservedWords()
   */
  @Override
  public String[] getReservedWords() {
    return new String[] { "ADD", "ALL", "ALTER", "ANALYZE", "AND", "AS", "ASC", "ASENSITIVE", "BEFORE", "BETWEEN",
        "BIGINT", "BINARY", "BLOB", "BOTH", "BY", "CALL", "CASCADE", "CASE", "CHANGE", "CHAR", "CHARACTER", "CHECK",
        "COLLATE", "COLUMN", "CONDITION", "CONNECTION", "CONSTRAINT", "CONTINUE", "CONVERT", "CREATE", "CROSS",
        "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR", "DATABASE", "DATABASES",
        "DAY_HOUR", "DAY_MICROSECOND", "DAY_MINUTE", "DAY_SECOND", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DELAYED",
        "DELETE", "DESC", "DESCRIBE", "DETERMINISTIC", "DISTINCT", "DISTINCTROW", "DIV", "DOUBLE", "DROP", "DUAL",
        "EACH", "ELSE", "ELSEIF", "ENCLOSED", "ESCAPED", "EXISTS", "EXIT", "EXPLAIN", "FALSE", "FETCH", "FLOAT", "FOR",
        "FORCE", "FOREIGN", "FROM", "FULLTEXT", "GOTO", "GRANT", "GROUP", "HAVING", "HIGH_PRIORITY", "HOUR_MICROSECOND",
        "HOUR_MINUTE", "HOUR_SECOND", "IF", "IGNORE", "IN", "INDEX", "INFILE", "INNER", "INOUT", "INSENSITIVE", "INSERT",
        "INT", "INTEGER", "INTERVAL", "INTO", "IS", "ITERATE", "JOIN", "KEY", "KEYS", "KILL", "LEADING", "LEAVE", "LEFT",
        "LIKE", "LIMIT", "LINES", "LOAD", "LOCALTIME", "LOCALTIMESTAMP", "LOCATE", "LOCK", "LONG", "LONGBLOB",
        "LONGTEXT", "LOOP", "LOW_PRIORITY", "MATCH", "MEDIUMBLOB", "MEDIUMINT", "MEDIUMTEXT", "MIDDLEINT",
        "MINUTE_MICROSECOND", "MINUTE_SECOND", "MOD", "MODIFIES", "NATURAL", "NOT", "NO_WRITE_TO_BINLOG", "NULL",
        "NUMERIC", "ON", "OPTIMIZE", "OPTION", "OPTIONALLY", "OR", "ORDER", "OUT", "OUTER", "OUTFILE", "POSITION",
        "PRECISION", "PRIMARY", "PROCEDURE", "PURGE", "READ", "READS", "REAL", "REFERENCES", "REGEXP", "RENAME",
        "REPEAT", "REPLACE", "REQUIRE", "RESTRICT", "RETURN", "REVOKE", "RIGHT", "RLIKE", "SCHEMA", "SCHEMAS",
        "SECOND_MICROSECOND", "SELECT", "SENSITIVE", "SEPARATOR", "SET", "SHOW", "SMALLINT", "SONAME", "SPATIAL",
        "SPECIFIC", "SQL", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "SQL_BIG_RESULT", "SQL_CALC_FOUND_ROWS",
        "SQL_SMALL_RESULT", "SSL", "STARTING", "STRAIGHT_JOIN", "TABLE", "TERMINATED", "THEN", "TINYBLOB", "TINYINT",
        "TINYTEXT", "TO", "TRAILING", "TRIGGER", "TRUE", "UNDO", "UNION", "UNIQUE", "UNLOCK", "UNSIGNED", "UPDATE",
        "USAGE", "USE", "USING", "UTC_DATE", "UTC_TIME", "UTC_TIMESTAMP", "VALUES", "VARBINARY", "VARCHAR",
        "VARCHARACTER", "VARYING", "WHEN", "WHERE", "WHILE", "WITH", "WRITE", "XOR", "YEAR_MONTH", "ZEROFILL" };
  }

  /*
   * (non-Javadoc)
   *
   * @see org.pentaho.di.core.database.DatabaseInterface#getStartQuote()
   */
  @Override
  public String getStartQuote() {
    return "`";
  }

  /**
   * Simply add an underscore in the case of MySQL!
   *
   * @see org.pentaho.di.core.database.DatabaseInterface#getEndQuote()
   */
  @Override
  public String getEndQuote() {
    return "`";
  }

  /**
   * @param tableNames
   *          The names of the tables to lock
   * @return The SQL command to lock database tables for write purposes.
   */
  @Override
  public String getSQLLockTables( String[] tableNames ) {
    String sql = "LOCK TABLES ";
    for ( int i = 0; i < tableNames.length; i++ ) {
      if ( i > 0 ) {
        sql += ", ";
      }
      sql += tableNames[i] + " WRITE";
    }
    sql += ";" + Const.CR;

    return sql;
  }

  /**
   * @param tableName
   *          The name of the table to unlock
   * @return The SQL command to unlock a database table.
   */
  @Override
  public String getSQLUnlockTables( String[] tableName ) {
    return "UNLOCK TABLES"; // This unlocks all tables
  }

  @Override
  public boolean needsToLockAllTables() {
    return true;
  }

  /**
   * @return extra help text on the supported options on the selected database platform.
   */
  @Override
  public String getExtraOptionsHelpText() {
    return "http://www.gbase8a.com/";
  }

  @Override
  public String[] getUsedLibraries() {
    return new String[] { "gbase-connector-java-8.3.81.53.jar" };
  }

  /**
   * @param tableName
   * @return true if the specified table is a system table
   */
  @Override
  public boolean isSystemTable( String tableName ) {
    if ( tableName.startsWith( "sys" ) ) {
      return true;
    }
    if ( tableName.equals( "dtproperties" ) ) {
      return true;
    }
    return false;
  }

  /**
   * Get the SQL to insert a new empty unknown record in a dimension.
   *
   * @param schemaTable
   *          the schema-table name to insert into
   * @param keyField
   *          The key field
   * @param versionField
   *          the version field
   * @return the SQL to insert the unknown record into the SCD.
   */
  @Override
  public String getSQLInsertAutoIncUnknownDimensionRow( String schemaTable, String keyField, String versionField ) {
    return "insert into " + schemaTable + "(" + keyField + ", " + versionField + ") values (1, 1)";
  }

  /**
   * @param string
   * @return A string that is properly quoted for use in a SQL statement (insert, update, delete, etc)
   */
  @Override
  public String quoteSQLString( String string ) {
    string = string.replaceAll( "'", "\\\\'" );
    string = string.replaceAll( "\\n", "\\\\n" );
    string = string.replaceAll( "\\r", "\\\\r" );
    return "'" + string + "'";
  }

  /**
   * @return true if the database is a MySQL variant, like MySQL 5.1, InfiniDB, InfoBright, and so on.
   */
  @Override
  public boolean isMySQLVariant() {
    return true;
  }

  /**
   * Returns a false as Oracle does not allow for the releasing of savepoints.
   */
  @Override
  public boolean releaseSavepoint() {
    return false;
  }

  @Override
  public boolean supportsErrorHandlingOnBatchUpdates() {
    return true;
  }

  @Override
  public boolean isRequiringTransactionsOnQueries() {
    return false;
  }

  /**
   * @return true if Kettle can create a repository on this type of database.
   */
  @Override
  public boolean supportsRepository() {
    return true;
  }

}
