
# 예제를 실행하기 위해서 필요한 쿼리
```sql
CREATE TABLE `batch_offset` (
	`batch_name` VARCHAR(20) NOT NULL,
	`batch_offset` BIGINT(20) NULL DEFAULT 0,
	PRIMARY KEY (`batch_name`)
)
COLLATE='utf8_general_ci'
ENGINE=InnoDB;

INSERT INTO batch_offset values ('mybatch', 0);


CREATE TABLE `users` (
	`user_no` INT(11) NOT NULL AUTO_INCREMENT,
	`user_id` VARCHAR(50) NOT NULL,
	`user_name` VARCHAR(50) NULL DEFAULT NULL,
	`user_email` VARCHAR(50) NULL DEFAULT NULL,
	`user_type` ENUM('SA','ADMIN','MANAGER','DBA','GUEST') NOT NULL DEFAULT 'GUEST',
	PRIMARY KEY (`user_no`),
	INDEX `idx_user_id` (`user_id`)
)
COLLATE='utf8_general_ci'
ENGINE=InnoDB;

CREATE TABLE `users2` (
	`user_no` INT(11) NOT NULL AUTO_INCREMENT,
	`user_id` VARCHAR(50) NOT NULL,
	`user_name` VARCHAR(50) NULL DEFAULT NULL,
	`user_email` VARCHAR(50) NULL DEFAULT NULL,
	`user_type` ENUM('SA','ADMIN','MANAGER','DBA','GUEST') NOT NULL DEFAULT 'GUEST',
	PRIMARY KEY (`user_no`),
	INDEX `idx_user_id` (`user_id`)
)
COLLATE='utf8_general_ci'
ENGINE=InnoDB;

INSERT INTO `users` (`user_id`, `user_name`, `user_email`, `user_type`) VALUES ('dba', 'DB 관리자', 'dba@gmail.com', 'ADMIN');
INSERT INTO `users` (`user_id`, `user_name`, `user_email`, `user_type`) VALUES ('ebs', 'ebs', 'ebs@google.co.kr', 'GUEST');
INSERT INTO `users` (`user_id`, `user_name`, `user_email`, `user_type`) VALUES ('james', '제임스', 'james@gmail.com', 'GUEST');
INSERT INTO `users` (`user_id`, `user_name`, `user_email`, `user_type`) VALUES ('KBS', 'kbs', 'kbs@google.co.kr', 'GUEST');
INSERT INTO `users` (`user_id`, `user_name`, `user_email`, `user_type`) VALUES ('MBC', 'mbc', 'mbc@google.co.kr', 'GUEST');
INSERT INTO `users` (`user_id`, `user_name`, `user_email`, `user_type`) VALUES ('root', 'root 루트 관리자', 'root@gmail.com', 'ADMIN');
INSERT INTO `users` (`user_id`, `user_name`, `user_email`, `user_type`) VALUES ('sa', '수퍼 관리자', 'sa@gmail.com', 'SA');
INSERT INTO `users` (`user_id`, `user_name`, `user_email`, `user_type`) VALUES ('SBS', 'sbs', 'sbs@google.co.kr', 'GUEST');
INSERT INTO `users` (`user_id`, `user_name`, `user_email`, `user_type`) VALUES ('sys', '시스템 관리자', 'sys@gmail.com', 'ADMIN');

SELECT * FROM users;
SELECT * FROM users2;
SELECT * FROM batch_offset;
```
