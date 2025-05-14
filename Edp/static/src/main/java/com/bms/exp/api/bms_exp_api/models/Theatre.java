package com.bms.exp.api.bms_exp_api.models;



import com.bms.exp.api.bms_exp_api.requestbody.CreateUserRequestBody;

import java.util.UUID;


public class Theatre {
    UUID id;
    String theaterName;
    String theaterLoc;
    CreateUserRequestBody owner;

    public Theatre(UUID id, String theaterName, String theaterLoc, CreateUserRequestBody owner) {
        this.id = id;
        this.theaterName = theaterName;
        this.theaterLoc = theaterLoc;
        this.owner = owner;
    }

    public Theatre() {
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getTheaterName() {
        return theaterName;
    }

    public void setTheaterName(String theaterName) {
        this.theaterName = theaterName;
    }

    public String getTheaterLoc() {
        return theaterLoc;
    }

    public void setTheaterLoc(String theaterLoc) {
        this.theaterLoc = theaterLoc;
    }

    public CreateUserRequestBody getOwner() {
        return owner;
    }

    public void setOwner(CreateUserRequestBody owner) {
        this.owner = owner;
    }
}

