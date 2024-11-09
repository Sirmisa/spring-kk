package com.example.demo.controller;

import com.example.demo.proto.User;
import com.example.demo.service.UserKafkaService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/users")
@RequiredArgsConstructor
public class UserController {

    private final UserKafkaService userKafkaService;

    @PostMapping
    public void sendUser(@RequestBody UserDto userDto) {
        User user = User.newBuilder()
            .setId(userDto.id())
            .setName(userDto.name())
            .setEmail(userDto.email())
            .build();
        userKafkaService.sendUser(user);
    }
}

record UserDto(int id, String name, String email) {} 