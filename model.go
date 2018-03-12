package main

var fakeDB string

func saveMessage(text string) { fakeDB += text + "\n" }

func getMessage() string { return fakeDB }
