package com.example.demo.mapper;

import com.example.demo.model.TableClass;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface TableClassMapper {

    TableClass selectByIdAndName(@Param("id") Long id, @Param("name") String name);
}
