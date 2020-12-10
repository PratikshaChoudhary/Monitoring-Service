package main
import (
	"context"
	"os"
	"os/signal"
	"fmt"
	"log"
	"time"
	"sync"
	"syscall"
	"net/http"
	"encoding/json"
	"database/sql"
	 _ "github.com/go-sql-driver/mysql"
)

type Data struct{
	UserId int 					`json:"userId"`
	StatusCode int 				`json:"statuscode"` 		
	ServiceType int 			`json:"serviceType"`
	ServiceApi string 			`json:"serviceApi"`
	ServiceApiUrl string 		`json:"serviceApiUrl"`
	ResponseBody string 		`json:"responceBody"`
	RequestBody string 			`json:"requestBody"`
}

var c int 

func checkErr(err error) {
	if err != nil {
        panic(err)
	}
}

func withContextFunc(ctx context.Context, f func()) context.Context {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		defer signal.Stop(c)

		select {
		case <-ctx.Done():
		case <-c:
			cancel()
			f()
		}
	}()

	return ctx
}

func add(db *sql.DB, s []Data){
	vals := []interface{}{}

	sqlstr := "INSERT INTO data (userId,statuscode,serviceType,serviceApi,serviceApiUrl,responceBody,requestBody) VALUES"

	for _, l := range s{
	 	sqlstr += "(?,?,?,?,?,?,?),"
	 	vals = append(vals,l.UserId,l.StatusCode,l.ServiceType,l.ServiceApi,l.ServiceApiUrl,l.ResponseBody,l.RequestBody )
	}
	
	sqlstr = sqlstr[0:len(sqlstr)-1]

	stmt, err := db.Prepare(sqlstr)
	checkErr(err)
	stmt.Exec(vals...)

}

func call_add(db *sql.DB, ch chan Data, l int ,wg *sync.WaitGroup){
	defer wg.Done()
	var s []Data
	for i:=0;i<l;i++{
		s = append(s,<-ch)
	}
	fmt.Println(len(s))
	add(db,s)
	fmt.Println(len(s),"entries saved")
}

func background(ctx context.Context,db *sql.DB,ch chan Data,full chan bool, wg *sync.WaitGroup){
	defer wg.Done()
	ticker := time.NewTicker(60*time.Second)
	wg1 := &sync.WaitGroup{}
	for {
			select {
				case <- ticker.C :
						if (c > 0){
						x := c
						c = c-x
						wg1.Add(1)
						go call_add(db, ch, x, wg1)
						fmt.Println("1 min")
						}
				case <- full :
						if( c > 0){
						c = c-1000
						wg1.Add(1)
						go call_add(db, ch, 1000, wg1)
						fmt.Println("1000 entries")
						}
				case <- ctx.Done():
						if(c > 0){
						wg1.Add(1)
						go call_add(db, ch, c, wg1)
						fmt.Println("done channel")
						}
						wg1.Wait()
						return				
			}
	}
}

func main() {

	finished := make(chan int)
	wg := &sync.WaitGroup{}
	c = 0 

	ctx := withContextFunc(context.Background(),func(){
			log.Println("cancle from ctrl+c event")
			wg.Wait()
			close(finished)
	})
	
	//create db connection
	db, err :=sql.Open("mysql","root:Mysql@tcp(127.0.0.1:3306)/monitoring")
	checkErr(err)

	ch := make(chan Data,40000)
	full := make(chan bool, 1)

	go func(){
		http.HandleFunc("/add",func(w http.ResponseWriter, r *http.Request){

			var d Data
			
			err:= json.NewDecoder(r.Body).Decode(&d)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}	
			if(len(d.ResponseBody)>100){
			d.ResponseBody = d.ResponseBody[0:99]
			}
			if(len(d.RequestBody)>100){
			d.RequestBody = d.RequestBody[0:99]
			}
			ch <- d
			c++

			if(c==1000) {
				full <- true
			}

		})
		//Starting server at por	 t 8080
		log.Fatal(http.ListenAndServe(":3001",nil))
	}()
	
	wg.Add(1)
	go background(ctx,db,ch,full,wg)
	
	<- finished
}