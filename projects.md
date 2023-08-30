# General Project Directions

## Collaboration/Conduct Policy

Students may work alone or optionally work in groups of two.

**Partners**
* a comment at top should specify the names of both partners by their @wisc.edu email
* there should only be one submission in one GitHub classroom repo for the team
* the repo history should show at least one commit from each partner
* it's NOT OK to submit code from your partner that you didn't help write or understand

**Other Collaboration**
* a comment at the top should list everybody you talked to or collaborated with (besides course staff, of course) by their @wisc.edu email
* you can talk about logic and help each other debug
* **no code copying**; for example, no emailing code (or similar), no photos of code, no looking at code and typing it line by line
* copying code then changing it is still copying and thus not allowed

**ChatGPT, Large Language Models, Interactive Tools**
* use of the tools is permitted, with proper citation
* a "chats" directory should contain screenshots or PDFs of any chats, in their entirety.  Name them as chat1.png, chat2.png, etc.  PDF and JPG formats are also permitted.

**Other Online Resources**
* you **may not** use any online resource intended to provide solutions specific to CS 544
* you **may** use other online resources, with proper citation
* add a comment in your code before anything you copied from such sources

## Compute Setup

We'll be using Google cloud (GCP) for our work this semester.  We'll be
sharing $100 of credit per student.  Some notes about how to manage this credit:

* some people have multiple GCP accounts, for example, one with @gmail.com and one with @wisc.edu.  You must redeem the credits with your @wisc.edu, but it's fine to then use them under any account
* Google accounts have multiple "billing accounts" -- these might typically correspond to credit cards.  In your case, you'll have a billing account corresponding to $100 credit we'll provide
* Virtual machines and other resources are created under "projects", which are assigned to a billing account (and can be reassigned).  When your first billing count is nearly exhausted, you'll need to move your VM over (or create a new one)

Here is a plan/budget for what VM you should have at each point during the semester:

https://docs.google.com/spreadsheets/d/1wSURq5fH5CkUKFLk1MijVMiIbCdUSJbvt0OPx_JfCCc/edit?usp=sharing

Please monitor your credits carefully.  If you're burning through credits faster than the schedule, it is your responsibility to conserve credits (for example, by shutting down your VM overnight).

Your VMs should always run the Ubuntu 22.04 LTS (be sure it is the "x86/64" option).  Here are versions for the software we'll use this semester:

TODO

Be sure to backup your work regularly to a private GitHub repo, or with an `scp` to your personal computer.

For commands like `scp` and others you will use this semester, the the
cloud console's in-browser SSH client won't work.  You'll need to
setup SSH keys with `ssh-keygen` and configure it on
https://console.cloud.google.com/compute/metadata to get from your
personal computer.

## Submission

You'll be doing all your work on GitHub classroom.  Watch for Canvas
announcements providing project invite links.

* if you worked with a partner, make one submission.  Requirement: commit history must show at least one commit from each partner.
* projects have four parts; for notebooks, use big headers to divide your work into the four parts ("# Part 1: ...")
* for question based project work, (Q1, Q2, etc), include comments like ("# Q1: ...") before the answers
* each project will specify some specific files you need to commit (like a p1.ipynb or server.py); in addition to those, include whatever is needed (except data) for somebody to run your code

## Late Policy

The general rule is no submissions >3 days late.  Each day late suffers a 10% penalty (so a 90% submission that is 2 days late gets 70%).

Common exceptions, and who to ask:
- Illness: you can **ask your TA** to override the late penalty, but they cannot give more than 3 days.  To qualify, you share a link to your repo; your commit history should show you made a substantial start on the project at least 3 days prior to the deadline
- Busyness: same policy as illness (you might be unusually busy due to travel, extracurriculars, other courses, etc).  This shouldn't happen more than a couple times a semester
- McBurney accomodations: **email your instructor** a proposed policy for the semester (we can discuss again if your situation changes during the semester)

If you're working with a project partner who has the late penalty waived by a TA, that means it is waived for you as well, even if you don't have special circumstances yourself.

For other cases (falling very far behind, family emergencies, etc), **reach out to your instructor** to discuss possible accomodations.

## Resubmission Policy

Resubmissions generally won't be allowed once projects have been
graded, except in unusual situations, or when we made a mistake on our
end (like a misleading specification).

## "Pre-grading" Policy

We won't pre-grade work, so don't ask questions like "does this all
look good?" during office hours.  When grading many assignments, it's
natural we'll notice issues someone might miss during office hours.
We also don't have the staff time to effectively grade submissions
twice.

You can always ask more specific questions if you're looking for some reassurance, such as:

* "does X in the spec mean Y, or does it mean Z"
* "what would be a good way to test this function?"
* "would this plot be clearer with ????, or without it"
* "is this result supposed to be deterministic, or are there reasons it might be different for other deployments?"
