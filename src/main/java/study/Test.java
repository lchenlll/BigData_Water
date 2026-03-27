//package study;
//
//import java.sql.*;
//
//public class Test {
//    public static void main(String[] args) {
//        // 请根据实际情况修改
//        String url = "jdbc:postgresql://localhost:5432/tes";
//        String user = "postgres";
//        String password = "2003.3.15zdt";
//        String tableName = "ts_kv"; // 你的表名
//
//        try (Connection conn = DriverManager.getConnection(url, user, password)) {
//            System.out.println("✅ 数据库连接成功！");
//
//            String sql = "SELECT * FROM " + tableName + " LIMIT 10";
//            try (Statement stmt = conn.createStatement();
//                 ResultSet rs = stmt.executeQuery(sql)) {
//
//                ResultSetMetaData metaData = rs.getMetaData();
//                int columnCount = metaData.getColumnCount();
//
//                System.out.println("📊 查询结果（前10条）：");
//                int rowCount = 0;
//                while (rs.next()) {
//                    rowCount++;
//                    StringBuilder row = new StringBuilder("行 " + rowCount + ": ");
//                    for (int i = 1; i <= columnCount; i++) {
//                        row.append(metaData.getColumnName(i))
//                                .append("=")
//                                .append(rs.getString(i))
//                                .append(" ");
//                    }
//                    System.out.println(row);
//                }
//
//                if (rowCount == 0) {
//                    System.out.println("⚠️ 表中没有数据，请先插入一些测试数据。");
//                } else {
//                    System.out.println("✅ 共查询到 " + rowCount + " 条数据。");
//                }
//            }
//        } catch (SQLException e) {
//            System.err.println("❌ 数据库连接或查询失败！请检查：");
//            System.err.println("   - 数据库服务是否启动");
//            System.err.println("   - 连接地址、用户名、密码是否正确");
//            System.err.println("   - 表名是否正确");
//            System.err.println("   - 是否已添加 PostgreSQL JDBC 驱动");
//            e.printStackTrace();
//        }
//    }
//}