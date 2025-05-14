package com.bms.exp.api.bms_exp_api.controller;

import com.bms.exp.api.bms_exp_api.requestbody.CreateTheaterRequestBody;
import com.bms.exp.api.bms_exp_api.service.TheaterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/theater")
public class ThearterController {

    @Autowired
    TheaterService theaterService;

    @PostMapping("/create")
    public ResponseEntity createTheater(@RequestBody CreateTheaterRequestBody createTheaterRequestBody,
                                        @RequestParam String userEmail){

        // Theater Service
        theaterService.createTheater(createTheaterRequestBody, userEmail);
    }
}
