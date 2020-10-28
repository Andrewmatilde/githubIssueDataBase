package main

import (
	"database/sql"
	"fmt"
	"github.com/PingCAP-QE/libs/crawler"
)

func deleteIssueData(db *sql.Tx, issueWithComment *crawler.IssueWithComments) {
	_, err := db.Exec(
		`DELETE from ISSUE where ISSUE.ID = ?;`, issueWithComment.DatabaseId)
	if err != nil {
		fmt.Println("Delete fail while DELETE from ISSUE where NUMBER = ?:", err)
	}
}
