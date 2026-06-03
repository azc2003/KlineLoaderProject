package com.example.demo.controller;

import com.example.demo.service.LoadService;
import com.example.demo.service.ValidationService;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

@Validated
@RestController
public class KlineLoadController {
    @Autowired private LoadService loadService;
    @Autowired private ValidationService validationService;

    @PostMapping("/klineloader")
    public ResponseEntity KlineLoad(
            @RequestParam @NotBlank String symbol,
            @RequestParam(defaultValue = "1m") String interval,
            @RequestParam @NotNull Long startTime,
            @RequestParam @NotNull Long endTime,
            @RequestParam(required = false) Integer limit,
            @RequestParam(defaultValue = "pool") String mode) {
        validationService.validationWithLimit(symbol, interval, startTime, endTime, limit);
        loadService.loadURL(symbol, interval, startTime, endTime, limit, mode);
        return new ResponseEntity(HttpStatus.CREATED);
    }
}
