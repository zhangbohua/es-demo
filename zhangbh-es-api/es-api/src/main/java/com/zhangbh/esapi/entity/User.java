package com.zhangbh.esapi.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * @Author zhangbh-b
 * @create 2020-06-04 20:38
 */
@Data
@Component
@AllArgsConstructor
@NoArgsConstructor
public class User {

    private String name;

    private int age;
}
