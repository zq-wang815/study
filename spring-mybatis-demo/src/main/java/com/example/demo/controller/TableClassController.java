package com.example.demo.controller;

import com.example.demo.model.TableClass;
import com.example.demo.service.TableClassService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class TableClassController {

    private final TableClassService tableClassService;

    public TableClassController(TableClassService tableClassService) {
        this.tableClassService = tableClassService;
    }

    @PostMapping("/api/query")
    public TableClass query(@RequestBody Map<String, Object> params) {
        Long id = params.get("id") != null
                ? Long.valueOf(params.get("id").toString()) : null;
        String name = (String) params.get("name");

        return tableClassService.queryByIdAndName(id, name);
    }
}
