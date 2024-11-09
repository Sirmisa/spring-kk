package com.example.demo.controller;

import com.example.demo.model.User;
import com.example.demo.service.KafkaProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/users")
@RequiredArgsConstructor
public class UserController {

    private final KafkaProducerService producerService;

    @PostMapping
    public void sendUser(@RequestBody User user) {
        producerService.sendMessage("users", String.valueOf(user.getId()), user);
    }
} 