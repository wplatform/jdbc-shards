CREATE TABLE IF NOT EXISTS `t_student_course` (
  `f_id` INT NOT NULL,
  `f_student_id` INT NULL,
  `t_course_name` VARCHAR(200) NULL,
  `f_course_no` VARCHAR(45) NULL,
  `t_score` DECIMAL(3,2) NULL,
  `t_learn_year` YEAR NULL,
  `f_gmt` TIMESTAMP NULL,
  PRIMARY KEY (`f_id`))
ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `t_student` (
  `f_student_id` INT NOT NULL,
  `f_student_no` VARCHAR(50) NULL,
  `f_name` VARCHAR(50) NULL,
  `t_birthday` DATE NULL,
  `f_phone` VARCHAR(20) NULL,
  `f_sex` INT NULL,
  `f_school_id` INT NULL,
  `f_address` VARCHAR(500) NULL,
  `f_gmt` TIMESTAMP NULL,
  PRIMARY KEY (`f_student_id`),
  UNIQUE INDEX `index_stu_no` (`f_student_no` ASC))
ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `t_school` (
  `f_id` INT NOT NULL,
  `f_name` VARCHAR(500) NULL,
  `f_found_date` DATE NULL,
  `f_address` VARCHAR(45) NULL,
  `f_gmt` TIMESTAMP NULL,
  PRIMARY KEY (`f_id`))
ENGINE = InnoDB;