package com.bms.exp.api.bms_exp_api.controller;

import com.bms.exp.api.bms_exp_api.requestbody.CreateUserRequestBody;
import com.bms.exp.api.bms_exp_api.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/exp/user")
public class CommonUserController {
    UserService userService;
    @Autowired
    CommonUserController(UserService userService){
        this.userService = userService;
    }

    @PostMapping("/registration")
    public ResponseEntity registerUser(@RequestBody CreateUserRequestBody createUserRequestBody){
        // service
        try{
            userService.createUser(createUserRequestBody);
            return new ResponseEntity("User created successfully", HttpStatus.CREATED);
        }catch (Exception e){
            return new ResponseEntity(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
