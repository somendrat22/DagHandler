package com.bms.exp.api.bms_exp_api.exceptions;

public class UnAuthorized extends RuntimeException{
    public UnAuthorized(String message){
        super(message);
    }
}
