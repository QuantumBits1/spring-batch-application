package com.spring.batch.writer;

import com.spring.batch.model.Employee;
import com.spring.batch.repository.EmployeeRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.List;

@Component
public class EmployeeDBWriter implements ItemWriter<Employee> {

    private static final Logger logger = LoggerFactory.getLogger(EmployeeDBWriter.class);
    private EmployeeRepository employeeRepository;

    public EmployeeDBWriter(EmployeeRepository employeeRepository) {
        this.employeeRepository = employeeRepository;
    }

    @Override
    public void write(List<? extends Employee> employees) throws Exception {
        employeeRepository.saveAll(employees);
        logger.info("{} employees saved in database", employees.size());
    }
}
