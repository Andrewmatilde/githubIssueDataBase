package main

import (
	"database/sql"
	"fmt"
	"github.com/PingCAP-QE/libs/crawler"
	"github.com/google/go-github/v32/github"
	"time"
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
    	(ID,NUMBER, REPOSITORY_ID, CLOSED, CLOSED_AT, CREATED_AT, TITLE) 
    	VALUES (?,?,?,?,?,?,?);`,
		issueWithComment.DatabaseId,
		issueWithComment.Number, *repo.ID, issueWithComment.Closed,
		closeAt, issueWithComment.CreatedAt.Time, issueWithComment.Title)
	if err != nil {
		fmt.Println("Insert fail while INSERT INTO ISSUE ", err)
	}
}

func insertLabelDataAndRelationshipWithIssue(db *sql.Tx, issueWithComments *crawler.IssueWithComments) {
	for _, node := range issueWithComments.Labels.Nodes {
		_, err := db.Exec(
			`INSERT INTO LABEL (NAME) VALUES (?);`,
			node.Name)
		if err != nil {
			fmt.Println("INSERT INTO LABEL", err)
		}

		_, err = db.Exec(
			`INSERT INTO LABEL_ISSUE_RELATIONSHIP (LABEL_ID, ISSUE_ID)
				SELECT LABEL.ID,?
				FROM LABEL where LABEL.NAME = ?;`,
			issueWithComments.DatabaseId, node.Name)
		if err != nil {
			fmt.Println("INSERT INTO LABEL_ISSUE_RELATIONSHIP ", err)
		}
	}

}

func insertUserDataAndRelationshipWithIssue(db *sql.Tx, issueWithComments *crawler.IssueWithComments) {
	for _, node := range issueWithComments.Assignees.Nodes {
		_, err := db.Exec(
			`INSERT INTO USER (LOGIN_NAME, EMAIL)VALUES (?,?);`,
			node.Login, node.Email)
		if err != nil {
			fmt.Println("INSERT INTO USER ", err)
		}

		_, err = db.Exec(
			`INSERT INTO ASSIGNEE (USER_ID, ISSUE_ID)
				SELECT USER.ID,?
				from USER where USER.LOGIN_NAME = ?;`,
			issueWithComments.DatabaseId, node.Login)
		if err != nil {
			fmt.Println("INSERT INTO ASSIGNEE ", err)
		}
	}
}

func insertCommentData(db *sql.Tx, issueWithComments *crawler.IssueWithComments) {
	stmt, err := db.Prepare(`INSERT INTO COMMENT (ISSUE_ID, BODY) VALUES (?,?)`)
	if err != nil {
		fmt.Println("INSERT INTO COMMENT ", err)
		return
	}
	_, err = stmt.Exec(issueWithComments.DatabaseId, issueWithComments.Body)
	if err != nil {
		fmt.Println("INSERT INTO COMMENT ", err)
	}
	for _, comment := range *issueWithComments.Comments {
		_, err := stmt.Exec(issueWithComments.DatabaseId, comment.Body)
		if err != nil {
			fmt.Println("INSERT INTO COMMENT ", err)
		}
	}
}

func insertCrossReferenceEvent(db *sql.Tx, issueWithComments *crawler.IssueWithComments) {
	for _, Node := range (*issueWithComments).TimelineItems.Nodes {
		if Node.Typename == "CrossReferenceEvent" {
			_, err := db.Exec(`INSERT INTO Cross_Referenced_Event (USER_ID,CREATE_AT,ISSUE_ID) 		
				SELECT USER.ID,?,?
				from USER where USER.LOGIN_NAME = ?;`,
				Node.CrossReferencedEvent.CreatedAt.Time,
				issueWithComments.DatabaseId,
				Node.CrossReferencedEvent.Actor.Login)
			if err != nil {
				fmt.Println("INSERT INTO COMMENT ", err)
			}
		}
	}
}

func insertAssignedIssueNumTimeLine(db *sql.Tx, repo *github.Repository, issueWithComments *[]crawler.IssueWithComments) {
	repoCreateTime := ParseDate(repo.CreatedAt.Time)
	assignedIssueNumTimeLine := time.Now().Sub(repoCreateTime)
	hours := assignedIssueNumTimeLine.Hours()
	assignedIssueNums := make([]int, int(hours/24)+1)
	dateTimes := make([]time.Time, int(hours/24)+1)
	for tempTime, i := repoCreateTime, 0; i < len(assignedIssueNums); i++ {
		dateTimes[i] = tempTime
		for _, issueWithComment := range *issueWithComments {
			if issueNumAssignBeforeDateTime(tempTime, &issueWithComment) {
				assignedIssueNums[i]++
			}
		}
		tempTime = tempTime.AddDate(0, 0, 1)
	}
	_, err := db.Exec(`INSERT INTO ASSIGNED_ISSUE_NUM_TIMELINE (DATETIME,ASSIGNED_ISSUE_NUM) VALUES (?,?)`, dateTimes, assignedIssueNums)
	if err != nil {
		fmt.Println("INSERT INTO ASSIGNED_ISSUE_NUM_TIMELINE ", err)
	}
}

func issueNumAssignBeforeDateTime(dateTime time.Time, issueWithComment *crawler.IssueWithComments) bool {

	assigneeMap := make(map[string]bool)
	if issueWithComment.CreatedAt.Before(dateTime) {
		for _, node := range issueWithComment.TimelineItems.Nodes {
			switch node.Typename {
			case "AssignedEvent":
				if node.AssignedEvent.CreatedAt.Before(dateTime) {
					assigneeMap[string(node.AssignedEvent.Assignee.User.Login)] = true
				}
			case "UnassignedEvent":
				if node.UnassignedEvent.CreatedAt.Before(dateTime) {
					assigneeMap[string(node.AssignedEvent.Assignee.User.Login)] = false
				}
			}
		}
	}
	for _, Assigned := range assigneeMap {
		if Assigned {
			return true
		}
	}
	return false
}

func ParseDate(t time.Time) time.Time {
	year, month, day := t.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}
