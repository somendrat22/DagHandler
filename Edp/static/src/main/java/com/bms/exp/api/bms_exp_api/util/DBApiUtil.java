package com.bms.exp.api.bms_exp_api.util;

import com.bms.exp.api.bms_exp_api.models.Theatre;
import com.bms.exp.api.bms_exp_api.requestbody.CreateUserRequestBody;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpMethod;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.util.HashMap;

@Service
public class DBApiUtil extends ApiUtilImpl {

    @Value("${bms.db.api.url}")
    String dbApiUrl;

    @Autowired
    RestTemplate restTemplate;

    public void createUser(CreateUserRequestBody createUserRequestBody){
        String endpoint = "/user/create";
        makePostCall(createUserRequestBody, endpoint, dbApiUrl, new HashMap<>());
    }

    public CreateUserRequestBody getUserByEmail(String email){
        String endPoint = "/user/" + email;
        CreateUserRequestBody user = (CreateUserRequestBody) makeGetCall(endPoint, dbApiUrl, new HashMap<>());
        return user;
    }

    public void createTheater(Theatre theatre){
        makePostCall(theatre, "/theatre/create", dbApiUrl, new HashMap<>());
    }

}
