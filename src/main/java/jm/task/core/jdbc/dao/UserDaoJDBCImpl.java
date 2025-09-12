package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {
    public UserDaoJDBCImpl() {

    }

    public void createUsersTable() {
        String createTable = "CREATE TABLE IF NOT EXISTS USER (USER_ID INT NOT NULL AUTO_INCREMENT, NAME VARCHAR(40) NOT NULL, LAST_NAME VARCHAR(60) NOT NULL, AGE TINYINT NOT NULL, PRIMARY KEY(USER_ID))";
        try (Connection conn = Util.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(createTable);
        } catch (SQLException  | ClassNotFoundException er) {
            er.printStackTrace();
        }
    }

    public void dropUsersTable() {
        String  dropTable = "DROP TABLE IF EXISTS USER";
        try(Connection conn = Util.getConnection(); Statement statement = conn.createStatement()) {
            statement.executeUpdate(dropTable);
        } catch (SQLException  | ClassNotFoundException er) {
            er.printStackTrace();
        }
    }

    public void saveUser(String name, String lastName, byte age) {
            String  sql = "INSERT INTO USER (NAME, LAST_NAME, AGE) VALUES (?, ?, ?)";
            try (Connection conn = Util.getConnection(); PreparedStatement pst = conn.prepareStatement(sql)) {
                conn.setAutoCommit(false);
                pst.setString(1, name);
                pst.setString(2, lastName);
                pst.setByte(3, age);
                pst.executeUpdate();
                conn.commit();
            } catch (SQLException  | ClassNotFoundException er) {
                er.printStackTrace();
            }
    }

    public void removeUserById(long id) {
        String statement = "DELETE FROM USER WHERE USER_ID = ?";
        try(Connection connection = Util.getConnection(); PreparedStatement prst = connection.prepareStatement(statement)) {
            connection.setAutoCommit(false);
            prst.setLong(1, id);
            prst.executeUpdate();
            connection.commit();
        } catch (SQLException  | ClassNotFoundException er) {
            er.printStackTrace();
        }
    }

    public List<User> getAllUsers() {
        List<User> users = new ArrayList<>();
        String sql = "SELECT NAME, LAST_NAME, AGE FROM USER";
        try (Connection conn = Util.getConnection(); PreparedStatement prst = conn.prepareStatement(sql); ResultSet rst = prst.executeQuery()) {
            while (rst.next()) {
                users.add(new User(rst.getString("name"), rst.getString("last_name"), rst.getByte("age")));
            }
        } catch (SQLException  | ClassNotFoundException er) {
            er.printStackTrace();
        }
        return users;
    }

    public void cleanUsersTable() {
        String sql = "DELETE FROM USER";
        try (Connection conn = Util.getConnection(); Statement stm = conn.createStatement()) {
            stm.execute(sql);
        } catch (SQLException  | ClassNotFoundException er) {
            er.printStackTrace();
        }
    }
}
