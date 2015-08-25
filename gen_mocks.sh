#!/bin/bash
mkdir -p mock
mockgen -destination mock/sqs_interface_mock.go -package mock github.com/Wattpad/sqsconsumer SQSAPI
