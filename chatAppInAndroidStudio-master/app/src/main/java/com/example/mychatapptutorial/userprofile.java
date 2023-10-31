package com.example.mychatapptutorial;

public class userprofile {


    public String username,userUID;

    public userprofile() {
    }

    public userprofile(String username, String userUID) {
        this.username = username;
        this.userUID = userUID;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getUserUID() {
        return userUID;
    }

    public void setUserUID(String userUID) {
        this.userUID = userUID;
    }
}
