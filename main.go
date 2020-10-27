package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/PingCAP-QE/libs/crawler"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/go-github/v32/github"
	"log"
	"os"
	"strings"
	"time"
)

var db *sql.DB
var err error

func init() {
	MYSQLEnvString := os.Getenv("MYSQL_URI")
	db, err = sql.Open("mysql", MYSQLEnvString)
	if err != nil {
		panic(err)
	}
	db.SetConnMaxLifetime(100)
	db.SetMaxIdleConns(100)

	if err := db.Ping(); err != nil {
		fmt.Println("open database fail")
		return
	}
	fmt.Println("connnect success")
}

func initClient() (crawler.ClientV4, *github.Client) {
	tokenEnvString := os.Getenv("GITHUB_TOKEN")
	tokens := strings.Split(tokenEnvString, ":")
	crawler.InitGithubV4Client(tokens)
	clientV4 := crawler.NewGithubV4Client()
	client := crawler.NewGithubClient(tokens[0])
	return clientV4, client
}

func insertData(owner, repoName string) {
	clientV4, client := initClient()

	repo, _, err := client.Repositories.Get(context.Background(), owner, repoName)
	if err != nil {
		log.Fatal(err)
	}

	insertRepositoryData(db, repo)

	issueWithComments, errs := crawler.FetchIssueWithCommentsByLabels(clientV4, owner, repoName, []string{"type/bug"})
	if errs != nil {
		log.Fatal(errs[0])
	}

	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: 0,
		ReadOnly:  false,
	})

	for _, issueWithComment := range *issueWithComments {
		insertIssueData(tx, repo, &issueWithComment)
		insertUserDataAndRelationshipWithIssue(tx, repo, issueWithComment)
		insertLabelDataAndRelationshipWithIssue(tx, repo, issueWithComment)
		insertCommentData(tx, repo, issueWithComment)
	}
	err = tx.Commit()
	fmt.Println(err)
}

func main() {
	t := time.Now()
	insertData("pingcap", "tidb")
	fmt.Println(time.Until(t))
}
