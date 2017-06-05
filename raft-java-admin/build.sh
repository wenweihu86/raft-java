#!/usr/bin/env bash

mvn clean package
mvn dependency:copy-dependencies
