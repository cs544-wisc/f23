# DRAFT!  Don't start yet.

# P1 (6% of grade): Docker/Linux Warmup

## Overview

TODO

Learning objectives:
* TODO

Before starting, please review the [general project directions](../projects.md).

## Corrections/Clarifications

* none yet

## Part 1: Virtual Machine Setup

We'll use Google's Cloud (GCP) for our virtual machines.  They've
generously given each 544 student $100 in credits, which should last
the whole semseter if you stick to the [budget
plan](https://github.com/cs544-wisc/f23/blob/main/projects.md#compute-setup).

Setup a virtual machine that you'll use for the first few projects
(we'll eventually delete it and create a more powerful one for some of
the later projects).

You should have some experience in creating VMs in a prior course (320
or 400), so these directions will be brief, and you can decide things
like what geographic region to create it in (I picked Iowa since it's
nearby), but here are some highlights:

* you can launch and see VMs from here: https://console.cloud.google.com/compute/instances
* be sure to create an e2-small instance with a 25 GB boot drive.  The monthly estimate should be about $14.73 for the VM (if its not, you probably selected something wrong, and might run out of free credits before the end of the semester).
* the boot disk must be Ubuntu 22.04 LTS -- select the x86/64 version (**not** Arm64)
* you may have modified firewall settings for other courses, but that's not necessary for 544
* you'll need to setup an SSH key so you can connect from your laptop: https://console.cloud.google.com/compute/metadata?tab=sshkeys (the browser-based SSH client won't work for what we need to do in this class)

## Part 2: Docker Install

Carefully follow the directions here to install Docker 24.0.5 on your virtual machine: https://docs.docker.com/engine/install/ubuntu/

Notes:
* there are several different approaches described under "Installation methods".  Use the directions under "Install using the apt repository".  Make sure you don't keep going after you reach "Install from a package"
* the first step under "Install Docker Engine" has two options: "Latest" or "Specific version".  Choose **"Specific version"**
* we'll use version "5:24.0.5-1~ubuntu.22.04~jammy" -- be sure to modify the `VERSION_STRING` accordingly!

## Part 3: Shell Script

## Part 4: Docker Image



## Submission

TODO

## Tester
