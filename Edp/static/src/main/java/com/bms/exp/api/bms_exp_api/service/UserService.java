package com.bms.exp.api.bms_exp_api.service;

import com.bms.exp.api.bms_exp_api.requestbody.CreateUserRequestBody;
import com.bms.exp.api.bms_exp_api.util.DBApiUtil;
import com.bms.exp.api.bms_exp_api.util.MailApiUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class UserService {
    DBApiUtil dbApiUtil;
    MailApiUtil mailApiUtil;
    @Autowired
    UserService(DBApiUtil dbApiUtil, MailApiUtil mailApiUtil){
        this.dbApiUtil =dbApiUtil;
        this.mailApiUtil = mailApiUtil;
    }

    public void createUser(CreateUserRequestBody createUserRequestBody){
        // DB API CALL
        try{
            dbApiUtil.createUser(createUserRequestBody);
        }catch (Exception e){
            throw e;
        }
        // Call Mail api to send mail

        try{
            mailApiUtil.sendUserRegistrationMail(createUserRequestBody.getEmail(), createUserRequestBody.getFirstName());
        }catch (Exception e){
            throw e;
        }

    }
}
