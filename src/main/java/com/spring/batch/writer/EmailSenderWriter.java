package com.spring.batch.writer;

import com.spring.batch.dto.EmployeeDTO;
import org.springframework.batch.item.ItemWriter;

import java.util.List;

public class EmailSenderWriter implements ItemWriter<EmployeeDTO> {

    @Override
    public void write(List<? extends EmployeeDTO> list) throws Exception {
        System.out.println("Email send successfully to all the employees.");
    }
}
