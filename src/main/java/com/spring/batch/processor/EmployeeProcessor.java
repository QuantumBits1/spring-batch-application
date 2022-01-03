package com.spring.batch.processor;

import com.spring.batch.dto.EmployeeDTO;
import com.spring.batch.model.Employee;
import com.spring.batch.writer.EmployeeDBWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.util.Random;

@Component
public class EmployeeProcessor implements ItemProcessor<EmployeeDTO, Employee> {

    private static final Logger logger = LoggerFactory.getLogger(EmployeeDBWriter.class);
    private ExecutionContext executionContext;

    public EmployeeProcessor(ExecutionContext executionContext) {
        this.executionContext = executionContext;
    }

    @Override
    public Employee process(EmployeeDTO employeeDTO) throws Exception {
        logger.info("Inside process method. Employee = {}", employeeDTO.toString());

        if(!isValid(employeeDTO)) { return null; }

        Employee employee = new Employee();
        employee.setEmployeeId(employeeDTO.getEmployeeId() + executionContext.getString("customFileName"));
        employee.setFirstName(employeeDTO.getFirstName());
        employee.setLastName(employeeDTO.getLastName());
        employee.setEmail(employeeDTO.getEmail());
        employee.setAge(employeeDTO.getAge());
        System.out.println("inside processor " + employee);
        return employee;
    }

    public boolean isValid(EmployeeDTO employeeDTO) {
        return (employeeDTO.getFirstName().startsWith("AAA")) ? false : true;
    }
}
