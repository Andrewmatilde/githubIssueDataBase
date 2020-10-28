package main

import (
	"database/sql"
	"fmt"
	"github.com/PingCAP-QE/libs/crawler"
	"github.com/google/go-github/v32/github"
)

func insertRepositoryData(db *sql.DB, repo *github.Repository) {
	_, err := db.Exec(`INSERT INTO REPOSITORY (ID,OWNER, REPO_NAME) VALUES (?,?,?)`, *repo.ID, *repo.Owner.Login, *repo.Name)
	if err != nil {
		fmt.Println("Insert fail while INSERT INTO REPOSITORY (ID,OWNER, REPO_NAME) VALUES", err)
	}
}

func insertIssueData(db *sql.Tx, repo *github.Repository, issueWithComment *crawler.IssueWithComments) {
	closeAt := sql.NullTime{}
	if issueWithComment.Closed {
		closeAt = sql.NullTime{
			Time:  issueWithComment.ClosedAt.Time,
			Valid: true,
		}
	}

	_, err := db.Exec(
		`INSERT INTO ISSUE 
    	(NUMBER, REPOSITORY_ID, CLOSED, CLOSED_AT, CREATED_AT, TITLE) 
    	VALUES (?,?,?,?,?,?);`,
		issueWithComment.Number, *repo.ID, issueWithComment.Closed,
		closeAt, issueWithComment.CreatedAt.Time, issueWithComment.Title)
	if err != nil {
		fmt.Println("Insert fail while INSERT INTO ISSUE ", err)
	}
}

func insertLabelDataAndRelationshipWithIssue(db *sql.Tx, repo *github.Repository, issueWithComments crawler.IssueWithComments) {
	for _, node := range issueWithComments.Labels.Nodes {
		_, err := db.Exec(
			`INSERT INTO LABEL (NAME) VALUES (?);`,
			node.Name)
		if err != nil {
			fmt.Println("INSERT INTO LABEL", err)
		}

		_, err = db.Exec(
			`INSERT INTO LABEL_ISSUE_RELATIONSHIP (LABEL_ID, ISSUE_ID)
				SELECT LABEL.ID,ISSUE.ID 
				FROM LABEL,ISSUE where LABEL.NAME = ? 
				                   and ISSUE.REPOSITORY_ID = ? 
				                   and ISSUE.NUMBER = ?;`,
			node.Name, *repo.ID, issueWithComments.Number)
		if err != nil {
			fmt.Println("INSERT INTO LABEL_ISSUE_RELATIONSHIP ", err)
		}
	}

}

func insertUserDataAndRelationshipWithIssue(db *sql.Tx, repo *github.Repository, issueWithComments crawler.IssueWithComments) {
	for _, node := range issueWithComments.Assignees.Nodes {
		_, err := db.Exec(
			`INSERT INTO USER (LOGIN_NAME, EMAIL)VALUES (?,?);`,
			node.Login, node.Email)
		if err != nil {
			fmt.Println("INSERT INTO USER ", err)
		}

		_, err = db.Exec(
			`INSERT INTO ASSIGNEE (USER_ID, ISSUE_ID)
				SELECT USER.ID,ISSUE.ID 
				from USER,ISSUE where USER.LOGIN_NAME = ?
				                   and ISSUE.REPOSITORY_ID = ? 
				                   and ISSUE.NUMBER = ?;`,
			node.Login, *repo.ID, issueWithComments.Number)
		if err != nil {
			fmt.Println("INSERT INTO ASSIGNEE ", err)
		}
	}
}

func insertCommentData(db *sql.Tx, repo *github.Repository, issueWithComments crawler.IssueWithComments) {
	stmt, err := db.Prepare(`INSERT INTO COMMENT (ISSUE_ID, BODY)
		SELECT ISSUE.ID, ? 
		FROM ISSUE where ISSUE.REPOSITORY_ID = ? 
		             and ISSUE.NUMBER = ?;`)
	if err != nil {
		fmt.Println("INSERT INTO COMMENT ", err)
		return
	}
	_, err = stmt.Exec(issueWithComments.Body, *repo.ID, issueWithComments.Number)
	if err != nil {
		fmt.Println("INSERT INTO COMMENT ", err)
	}
	for _, comment := range *issueWithComments.Comments {
		_, err := stmt.Exec(comment.Body, *repo.ID, issueWithComments.Number)
		if err != nil {
			fmt.Println("INSERT INTO COMMENT ", err)
		}
	}
}
