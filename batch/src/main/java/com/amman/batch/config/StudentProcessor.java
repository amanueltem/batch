package com.amman.batch.config;

import org.springframework.batch.item.ItemProcessor;

import com.amman.batch.student.Student;

public class StudentProcessor implements ItemProcessor<Student, Student>{

	@Override
	public Student process(Student student) throws Exception {
		
		return student;
	}

}
