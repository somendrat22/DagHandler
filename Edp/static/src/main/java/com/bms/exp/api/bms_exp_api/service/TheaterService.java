package com.bms.exp.api.bms_exp_api.service;

import com.bms.exp.api.bms_exp_api.enums.UserType;
import com.bms.exp.api.bms_exp_api.exceptions.UnAuthorized;
import com.bms.exp.api.bms_exp_api.exceptions.UserNotFound;
import com.bms.exp.api.bms_exp_api.models.Theatre;
import com.bms.exp.api.bms_exp_api.requestbody.CreateTheaterRequestBody;
import com.bms.exp.api.bms_exp_api.requestbody.CreateUserRequestBody;
import com.bms.exp.api.bms_exp_api.util.DBApiUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class TheaterService {

    @Autowired
    DBApiUtil dbApiUtil;


    public void createTheater(CreateTheaterRequestBody createTheaterRequestBody, String email){
        // Check user exist with this email or not
        // Check user is a theater owner or  not

        // You need to hit database api to get user by email

        CreateUserRequestBody user  = dbApiUtil.getUserByEmail(email);
        if(user == null){
            throw new UserNotFound(String.format("User with email %s does not exist.", user.getEmail()));
        }

        if(!user.getUserType().equals(UserType.THEATEROWNERS.toString())){
            throw new UnAuthorized(String.format("User with email %s does not have access to create threater", user.getEmail()));
        }

        Theatre theatre = new Theatre();

        theatre.setTheaterLoc(createTheaterRequestBody.getLocation());
        theatre.setTheaterName(createTheaterRequestBody.getTheaterName());
        theatre.setOwner(user);

        dbApiUtil.createTheater(theatre);


    }
}
