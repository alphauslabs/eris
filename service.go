package main

import (
	v1 "github.com/alphauslabs/jupiter/proto/v1"
)

type service struct {
	v1.UnimplementedJupiterServer
}
