#!/bin/bash

docker build -t starwitorg/sae-geo-merger:$(poetry version --short) .