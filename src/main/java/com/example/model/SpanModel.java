package com.example.model;


import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
//public class SpanModel {
//    private String traceId;
//    private String spanId;
//    private String operationName;
//    private List<SpanParent> references = new ArrayList<SpanParent>();
//    private List<Tags> tags = new ArrayList<Tags>();
//    private ProcessModel process;
//
//    @Getter
//    @Setter
//    @NoArgsConstructor
//    public class SpanParent {
//        private String traceId;
//        private String spanId;
//    }
//
//    @Getter
//    @Setter
//    @NoArgsConstructor
//    public class Tags {
//        private String key;
//        private String vStr;
//    }
//}
public class SpanModel {
    private String traceId;
    private String spanId;
    private String operationName;
    private List<Map<String, String>> references;
    private List<Map<String, String>> tags;
    private ProcessModel process;

}
