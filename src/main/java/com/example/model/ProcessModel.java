package com.example.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
//public class ProcessModel {
//    private String serviceName;
//    private List<SpanModel.Tags> tags = new ArrayList<SpanModel.Tags>();
//}
public class ProcessModel {
    private String serviceName;
    private List<Map<String, String>> tags;
}
