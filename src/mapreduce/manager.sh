#!/usr/bin/env bash
clear() {
    rm -r ./mrtmp.test*
    rm -r 824-mrinput-*
}

if [[ "$1"=='clear' ]]; then
    clear
fi