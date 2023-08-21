package main

type limiter struct{}

// TODO: Implement a proper rate limiter.
func (*limiter) Limit() bool { return false }
