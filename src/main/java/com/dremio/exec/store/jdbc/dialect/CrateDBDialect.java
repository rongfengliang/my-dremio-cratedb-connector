package com.dremio.exec.store.jdbc.dialect;

import com.dremio.exec.store.jdbc.ColumnPropertiesProcessors;
import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.dremio.exec.store.jdbc.dialect.arp.ArpTypeMapper;
import com.dremio.exec.store.jdbc.dialect.arp.ArpYaml;
import com.dremio.exec.store.jdbc.rel2sql.PostgresRelToSqlConverter;
import com.google.common.base.Joiner;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.SqlCollation.Coercibility;
import org.apache.calcite.sql.SqlWriter.Frame;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;

public final class CrateDBDialect extends ArpDialect {
   private static final String DOUBLE_PRECISION = "DOUBLE PRECISION";
   private static final Integer MAX_IDENTIFIER_LENGTH = 63;
   private static final boolean DISABLE_PUSH_COLLATION = Boolean.getBoolean("dremio.jdbc.postgres.push-collation.disable");
   private static final SqlCollation POSTGRES_BINARY_COLLATION;
   private final ArpTypeMapper typeMapper;

   public CrateDBDialect(ArpYaml yaml) {
      super(yaml);
      this.typeMapper = new CrateDBTypeMapper(yaml);
   }

   protected boolean requiresAliasForFromItems() {
      return true;
   }

   public boolean supportsRegexString(String regex) {
      int index = 0;

      while(-1 != (index = regex.indexOf(92, index))) {
         if (index >= regex.length() - 1) {
            return true;
         }

         char escaped = regex.charAt(index + 1);
         if (Character.isLetter(escaped)) {
            switch(escaped) {
            case 'B':
            case 'U':
            case 'a':
            case 'b':
            case 'c':
            case 'e':
            case 'f':
            case 'n':
            case 'r':
            case 't':
            case 'u':
            case 'v':
            case 'x':
               ++index;
               break;
            case 'C':
            case 'D':
            case 'E':
            case 'F':
            case 'G':
            case 'H':
            case 'I':
            case 'J':
            case 'K':
            case 'L':
            case 'M':
            case 'N':
            case 'O':
            case 'P':
            case 'Q':
            case 'R':
            case 'S':
            case 'T':
            case 'V':
            case 'W':
            case 'X':
            case 'Y':
            case 'Z':
            case '[':
            case '\\':
            case ']':
            case '^':
            case '_':
            case '`':
            case 'd':
            case 'g':
            case 'h':
            case 'i':
            case 'j':
            case 'k':
            case 'l':
            case 'm':
            case 'o':
            case 'p':
            case 'q':
            case 's':
            case 'w':
            default:
               return false;
            }
         }
      }

      return true;
   }

   public TypeMapper getDataTypeMapper(JdbcPluginConfig config) {
      return this.typeMapper;
   }

   public Integer getIdentifierLengthLimit() {
      return MAX_IDENTIFIER_LENGTH;
   }

   public SqlNode getCastSpec(RelDataType type) {
      return (SqlNode)(type.getSqlTypeName() == SqlTypeName.DOUBLE ? new SqlDataTypeSpec(new SqlBasicTypeNameSpec(type.getSqlTypeName(), -1, -1, (String)null, SqlParserPos.ZERO), (TimeZone)null, SqlParserPos.ZERO) {
         public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
            writer.keyword(DOUBLE_PRECISION);
         }
      } : super.getCastSpec(type));
   }

   public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
      SqlOperator operator = call.getOperator();
      boolean hasAlreadyAppliedParentheses = leftPrec == 0 && rightPrec == 0;
      if ((operator == SqlStdOperatorTable.EQUALS || operator == SqlStdOperatorTable.NOT_EQUALS || operator == SqlStdOperatorTable.LESS_THAN_OR_EQUAL || operator == SqlStdOperatorTable.LESS_THAN || operator == SqlStdOperatorTable.GREATER_THAN_OR_EQUAL || operator == SqlStdOperatorTable.GREATER_THAN) && !hasAlreadyAppliedParentheses) {
         Frame frame = writer.startList("(", ")");
         super.unparseCall(writer, call, leftPrec, rightPrec);
         writer.endList(frame);
      } else {
         super.unparseCall(writer, call, leftPrec, rightPrec);
      }

   }

   public SqlNode emulateNullDirection(SqlNode node, boolean nullsFirst, boolean desc) {
      return null;
   }

   public PostgresRelToSqlConverter getConverter() {
      return new PostgresRelToSqlConverter(this);
   }

   public ArpSchemaFetcher newSchemaFetcher(JdbcPluginConfig config) {
      String query = String.format("SELECT NULL, SCH, NME FROM (SELECT TABLEOWNER CAT, SCHEMANAME SCH, TABLENAME NME from pg_catalog.pg_tables UNION ALL SELECT VIEWOWNER CAT, SCHEMANAME SCH, VIEWNAME NME FROM pg_catalog.pg_views) t WHERE UPPER(SCH) NOT IN ('PG_CATALOG', '%s')", Joiner.on("','").join(config.getHiddenSchemas()));
      return new PGSchemaFetcher(query, config);
   }

   public boolean supportsFetchOffsetInSetOperand() {
      return false;
   }

   public SqlCollation getDefaultCollation(SqlKind kind) {
      if (DISABLE_PUSH_COLLATION) {
         return null;
      } else {
         switch(kind) {
         case LITERAL:
         case IDENTIFIER:
            return POSTGRES_BINARY_COLLATION;
         default:
            return null;
         }
      }
   }

   static {
      POSTGRES_BINARY_COLLATION = new SqlCollation(Coercibility.NONE) {
         private static final long serialVersionUID = 1L;

         public void unparse(SqlWriter writer) {
         }
      };
   }

   private static class CrateDBTypeMapper extends ArpTypeMapper {
      public CrateDBTypeMapper(ArpYaml yaml) {
         super(yaml);
      }

      protected SourceTypeDescriptor createTypeDescriptor(AddPropertyCallback addColumnPropertyCallback, InvalidMetaDataCallback invalidMetaDataCallback, Connection connection, TableIdentifier table, ResultSetMetaData metaData, String columnLabel, int colIndex) throws SQLException {
         int colType = metaData.getColumnType(colIndex);
         int precision = metaData.getPrecision(colIndex);
         int scale = metaData.getScale(colIndex);
         if (colType == 2 && 0 == precision) {
            scale = 6;
            precision = 38;
            if (null != addColumnPropertyCallback) {
               addColumnPropertyCallback.addProperty(columnLabel, ColumnPropertiesProcessors.ENABLE_EXPLICIT_CAST);
            }
         }

         return new SourceTypeDescriptor(columnLabel, colType, metaData.getColumnTypeName(colIndex), colIndex, precision, scale);
      }

      protected SourceTypeDescriptor createTableTypeDescriptor(AddPropertyCallback addColumnPropertyCallback, Connection connection, String columnName, int sourceJdbcType, String typeString, TableIdentifier table, int colIndex, int precision, int scale) {
         if (sourceJdbcType == 2 && precision == 0) {
            scale = 6;
            precision = 38;
            if (null != addColumnPropertyCallback) {
               addColumnPropertyCallback.addProperty(columnName, ColumnPropertiesProcessors.ENABLE_EXPLICIT_CAST);
            }
         }

         return new TableSourceTypeDescriptor(columnName, sourceJdbcType, typeString, table.getCatalog(), table.getSchema(), table.getCatalog(), colIndex, precision, scale);
      }
   }

   static class PGSchemaFetcher extends ArpSchemaFetcher {
      private static final Logger logger = LoggerFactory.getLogger(PGSchemaFetcher.class);

      public PGSchemaFetcher(String query, JdbcPluginConfig config) {
         super(query, config);
      }

      protected long getRowCount(List<String> tablePath) {
         String sql = MessageFormat.format("SELECT reltuples::bigint AS EstimatedCount\nFROM pg_class\nWHERE  oid = {0}::regclass", this.getConfig().getDialect().quoteStringLiteral(PERIOD_JOINER.join(tablePath)));
         Optional<Long> estimate = this.executeQueryAndGetFirstLong(sql);
         if (estimate.isPresent() && (Long)estimate.get() != 0L) {
            return (Long)estimate.get();
         } else {
            logger.debug("Row count estimate not detected for table {}. Retrying with count query.", this.getQuotedPath(tablePath));
            return super.getRowCount(tablePath);
         }
      }
   }
}
