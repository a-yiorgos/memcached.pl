# Simple Makefile for memcached.pl

.PHONY: test

test:
	@echo "Running test suite..."
	@perl -I. t/memcached.t
