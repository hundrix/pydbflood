# Description

This is a very simple tool to flood SQL statements against a database server.


# Notes for using git service

I'm new to programming and this is my first project, so I put some usage hints here.

## Push to Bitbucket

Set up Git on local machine

	mkdir /path/to/your/project
	cd /path/to/your/project
	git init
	git remote add bitbucket https://username@bitbucket.org/username/dbtester.git

Create first file, commit, and push

	git add .
	git commit -m 'Initial commit with contributors'
	git push -u bitbucket master

## Push to Github

	git remote add github https://github.com/username/dbtester
	git push -u github master

## Setup a remote repository

	git remote add bitbucket https://username@bitbucket.org/username/dbtester.git
	git fetch bitbucket
	git pull bitbucket master

	git clone https://username@bitbucket.org/username/dbtester.git
	git remote rename origin bitbucket
	git remote add github https://github.com/username/dbtester
	git remote show
	git remote show bitbucket
