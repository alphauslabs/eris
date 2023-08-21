package main

type Member string

func (m Member) String() string {
	return string(m)
}
