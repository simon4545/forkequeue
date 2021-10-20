package main

import (
	"forkequeue/api"
	"forkequeue/queue"
	levelAdmin "github.com/qjues/leveldb-admin"
	"log"
	"math/rand"
	"net/http"
)

var LQ *queue.LevelQueue

func init() {
	var err error
	LQ, err = queue.CreateQueue("D:\\goproject\\forkequeue\\example\\data", []byte("test"))
	if err != nil {
		log.Fatalln("LevelDB create error:", err)
		return
	}
	go http.ListenAndServe(":4333", nil)
	levelAdmin.GetLevelAdmin().Register(LQ.Db, "data").SetServerMux(http.DefaultServeMux).Start()
	return
}

func main() {
	handler := api.NewRouter(LQ)
	s := http.Server{
		Addr:    ":8989",
		Handler: handler,
	}

	err := s.ListenAndServe()
	log.Println(err)
	/*rand.Seed(time.Now().UnixNano())
	start := time.Now()
	rpos, wpos := LQ.GetReadPosition(), LQ.GetWritePosition()
	fmt.Println(rpos, wpos)
	for i := wpos; i < wpos+20000; i++ {
		_, err := LQ.Push([]byte(fmt.Sprintf("%d", i)))
		if err != nil {
			log.Println(err)
		}
	}

	for j := 0; j < 100; j++ {
		data, err := LQ.Pop()
		fmt.Println(string(data), err)
	}
	fmt.Println(LQ.GetReadPosition(), LQ.GetWritePosition())

	fmt.Println(time.Since(start))
	select {}*/
}

func RandomString(n int, allowedChars ...[]rune) []byte {
	var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}

	return b
}
