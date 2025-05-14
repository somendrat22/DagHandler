package com.bms.exp.api.bms_exp_api.exceptions;

import com.bms.exp.api.bms_exp_api.enums.UserType;

public class UserNotFound extends RuntimeException{
    public UserNotFound(String message){
        super(message);
    }
}
