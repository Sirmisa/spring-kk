package com.example.demo.controller;

import com.example.demo.model.User;
import com.example.demo.service.UserProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/users")
@RequiredArgsConstructor
public class UserController {

    private final UserProducerService producerService;

    @PostMapping
    public void sendUser(@RequestBody User user) {
        producerService.sendUser(user);
    }
} 