package com.example.demo.service;

import com.example.demo.model.TableClass;

public interface TableClassService {

    TableClass queryByIdAndName(Long id, String name);
}
