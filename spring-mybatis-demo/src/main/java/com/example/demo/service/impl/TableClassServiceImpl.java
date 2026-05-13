package com.example.demo.service.impl;

import com.example.demo.mapper.TableClassMapper;
import com.example.demo.model.TableClass;
import com.example.demo.service.TableClassService;
import org.springframework.stereotype.Service;

@Service
public class TableClassServiceImpl implements TableClassService {

    private final TableClassMapper tableClassMapper;

    public TableClassServiceImpl(TableClassMapper tableClassMapper) {
        this.tableClassMapper = tableClassMapper;
    }

    @Override
    public TableClass queryByIdAndName(Long id, String name) {
        if (id > 1) {
            return tableClassMapper.selectByIdAndName(id, name);
        }
        return null;
    }
}
