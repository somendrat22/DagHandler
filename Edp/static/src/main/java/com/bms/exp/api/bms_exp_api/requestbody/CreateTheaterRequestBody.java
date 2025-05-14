package com.bms.exp.api.bms_exp_api.requestbody;

public class CreateTheaterRequestBody {
    String location;
    String theaterName;

    public CreateTheaterRequestBody(String location, String theaterName) {
        this.location = location;
        this.theaterName = theaterName;
    }

    public CreateTheaterRequestBody() {
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getTheaterName() {
        return theaterName;
    }

    public void setTheaterName(String theaterName) {
        this.theaterName = theaterName;
    }
}
