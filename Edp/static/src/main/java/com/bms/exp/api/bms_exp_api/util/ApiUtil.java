package com.bms.exp.api.bms_exp_api.util;

import org.springframework.stereotype.Component;

import java.util.HashMap;

@Component
public interface ApiUtil {

    public Object makePostCall(Object requestBody, String endPoint, String apiUrl, HashMap<String, String > queryParams);

    public Object makePutCall(Object requestBody, String endPoint, String apiUrl, HashMap<String, String > queryParams);

    public Object makeGetCall(String endPoint, String apiUrl, HashMap<String, String > queryParams);


}
