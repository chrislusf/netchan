# netchan
As you can guess, netchan makes Golang channel work over network. 
Both ends of the channel can talk synchronously or asychronously.

Example:
```go
  // a sending process
  ch := sender.NewChannel("my_log")
  for line := range lines {
    ch <- []byte(line)
  }
  
  // a receiving process, on a different machine
  ch := receiver.NewChannel("my_log")
  for line := range ch {
    println(string(line))
  }
```

Both channels are chan []byte. Of course, sender channel is send-only, and receiver channel is receive-only.

Simple, right? Read on. It's more sophisticated underneath.

# Design
netchan has 2 or 3 components: optional Leader, Sender, Receiver

## Leader
Leader is solely for service discovery. Basically, mapping a name to a service location.

This can be skipped if everything runs intranet, where we just need a simple network broadcast.

Current Leader implementation is a simple http server. Later, service discovery can use Etcd, Zookeeper, etc.

## Sender and Receiver
It's actually not Sender sends messages to Receiver, but Receivers pull messages out of Sender.

Sender stores all messages on disk locally for a configurable time, depending on avaiable disk space.  This makes asynchronous
message passing possible. When a Receiver requests the data by a timestamp, Sender will respond with corresponding data. 

If a Receiver catches up, Sender will pass the most recent messages to the Receiver.

One Sender can have multiple Receivers.

Receiver can give Sender a filter, so that Receiver only receives filtered results.

# Comparison
netchan usage is similar to common messaging systems. However, netchan does not need a separate messaging system such as Kafka, NATS, etc. The data is stored on the sender directly.

# Work in progress
Welcome any sorts of help, discussion, pull requests, tests, etc.
