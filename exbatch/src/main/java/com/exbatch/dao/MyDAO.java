package com.exbatch.dao;

import com.exbatch.domain.User;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.stereotype.Repository;
import org.apache.ibatis.session.SqlSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Repository
public class MyDAO {

	@Autowired
	SqlSessionTemplate sqlSessionTemplate;

	public String getSuperAdmin(String userType) throws SQLException {
		return sqlSessionTemplate.selectOne("database.getSuperAdmin", userType);
	}

	public long getBatchOffset(String batchName) throws Exception {
		return sqlSessionTemplate.selectOne("database.getBatchOffset", batchName);
	}

	public void setBatchOffset(String batchName, long offset) {
		Map<String, Object> param = new HashMap<>();
		param.put("batch_name", batchName);
		param.put("batch_offset", new Long(offset));

		sqlSessionTemplate.update("database.updateBatchOffset", param);
	}

	public List<User> getAllUser() throws SQLException {
		return sqlSessionTemplate.selectList("database.getAllUser");
	}

	public List<User> getPagingUserList(long offset, long noOfRecords) throws SQLException {
		Map<String, Long> param = new HashMap<String, Long>();

		param.put("offset", new Long(offset));
		param.put("noOfRecords", new Long(noOfRecords));

		return sqlSessionTemplate.selectList("database.getPagingUserList", param);
	}

	public List<String> getAdminUserNameList() throws SQLException {

		Map<String, String> param = new HashMap<String, String>();
		param.put("one", "ADMIN");
		param.put("two", "SA");
		return sqlSessionTemplate.selectList("database.getAdminUserNameList", param);
	}

	public List<String> getAllUserNameList() throws SQLException {
		return sqlSessionTemplate.selectList("database.getAllUserNameList");
	}

	public void updateAdminName() throws SQLException {

		Map<String, String> param = new HashMap<String, String>();
		param.put("findAdminName", "루트 관리자");
		param.put("newAdminName", "root 관리자");

		sqlSessionTemplate.update("database.updateAdminName", param);
	}

	public List<String> findAdminEMail() throws SQLException {
		User admin = new User();
		admin.setUserId("root");
		admin.setUserType("ADMIN");
		List<String> adminEmails = sqlSessionTemplate.selectList("database.findAdminEMail", admin);
		return adminEmails;
	}

	public void saveAdmin(String userId, String userName, String userType) throws SQLException {
		Map<String, String> param = new HashMap<String, String>();
		param.put("user_id", userId);
		param.put("user_name", userName);
		param.put("user_type", userType);
		sqlSessionTemplate.insert("database.saveAdmin", param);
	}

	public void insertBatch2() {

		List<User> list = new ArrayList<>();

		list.add(new User("mbc", "mbc", "mbc@google.co.kr", "GUEST"));
		list.add(new User("ebs", "ebs", "ebs@google.co.kr", "GUEST"));
		list.add(new User("kbs", "kbs", "kbs@google.co.kr", "GUEST"));
		list.add(new User("sbs", "sbs", "sbs@google.co.kr", "GUEST"));

		Map<String, Object> param = new HashMap<>();
		param.put("list", list);

		sqlSessionTemplate.insert("database.insertBatch2", param);
	}

	public void updateBatch2() {
		List<String> list = new ArrayList<>();

		list.add("mbc");
		list.add("sbs");
		list.add("kbs");
		sqlSessionTemplate.update("database.updateBatch2", list);
	}
}
