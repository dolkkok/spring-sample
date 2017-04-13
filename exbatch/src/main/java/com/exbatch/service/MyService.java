package com.exbatch.service;

import com.exbatch.dao.MyDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;

@Service
public class MyService {

	@Autowired
	private MyDAO dao;

	/// Transaction 이 올바르게 동작하는지 확인하기위해서
	/// 고의적으로 중복된 키 값을 두 번 INSERT 함.
	public void saveAdmins() {
		try {
			dao.saveAdmin("test", "Test 관리자", "GUEST");
			dao.saveAdmin("test", "Test 관리자", "GUEST");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
