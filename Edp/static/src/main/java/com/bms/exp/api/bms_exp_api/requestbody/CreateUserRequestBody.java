package com.bms.exp.api.bms_exp_api.requestbody;

import com.bms.exp.api.bms_exp_api.enums.UserType;
import lombok.Data;

import java.util.UUID;

public class CreateUserRequestBody {
    UUID id;
    String firstName;
    String lastName;
    String email;
    String password;
    Long contactNumber;
    UserType userType;

    public CreateUserRequestBody(UUID id, String firstName, String lastName, String email, String password, Long contactNumber, UserType userType) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
        this.email = email;
        this.password = password;
        this.contactNumber = contactNumber;
        this.userType = userType;
    }

    public CreateUserRequestBody() {
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Long getContactNumber() {
        return contactNumber;
    }

    public void setContactNumber(Long contactNumber) {
        this.contactNumber = contactNumber;
    }

    public UserType getUserType() {
        return userType;
    }

    public void setUserType(UserType userType) {
        this.userType = userType;
    }
}
