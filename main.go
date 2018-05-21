package main

import (
	"github.com/stellar/go/clients/horizon"
	"net/http"
	"encoding/json"
	"context"
	"time"
	"github.com/go-chi/chi"
	"sync"
	"strconv"
)
var (
	mu      sync.Mutex
	seq int32
	mem []pair
)

type pair struct {
	Time time.Time `json:"time"`
	Off horizon.Offer `json:"off"`
}

type LedgerPage struct{
	Links struct {
		Self horizon.Link `json:"self"`
		Next horizon.Link `json:"next"`
		Prev horizon.Link `json:"prev"`
	} `json:"_links"`
	Embedded struct {
		Records []horizon.Ledger `json:"records"`
	} `json:"_embedded"`
}

func dbGetOffer(start, end string) *pair{
	var p *pair
	s, err := time.Parse(time.RFC3339,end)
	if err != nil{
		panic(err)
	}
	e, err := time.Parse(time.RFC3339, start)
	if err != nil{
		panic(err)
	}
	mu.Lock()
	for i := range mem{
		if s.Before(mem[i].Time) && e.After(mem[i].Time){
			p = &mem[i]
			break
		}
	}
	mu.Unlock()
	return p
}

func offerCtx(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	var offer *pair
	var err error
	if start, end := chi.URLParam(r, "start"), chi.URLParam(r, "end"); start != "" && end != ""{
		offer = dbGetOffer(start, end)
	} else {
		w.Write([]byte("Not found"))
		return
	}
	if err != nil {
		w.Write([]byte("Not found"))
		return
	}

	ctx := context.WithValue(r.Context(), "offer", offer)
	next.ServeHTTP(w, r.WithContext(ctx))
})
}

func GetOffer(w http.ResponseWriter, r *http.Request){
	offer := r.Context().Value("offer").(*pair)
	js, err := json.Marshal(offer)
	if err != nil{
		panic(err)
	}
	w.Write(js)
}

func main(){


	client := &horizon.Client{
		HTTP: http.DefaultClient,
		URL:"http://127.0.0.1:8000",
	}
	buffer := make([]byte, 4096)

	resp, err := client.HTTP.Get(client.URL+"/ledgers?limit=1")
	if err != nil{
		panic(err)
	}

	size, _ := resp.Body.Read(buffer)
	var page LedgerPage
	err = json.Unmarshal(buffer[0:size], &page)
	if err != nil{
		panic(err)
	}
	//client.StreamTransactions()
	cur := horizon.Cursor(string(page.Embedded.Records[0].PT))
	go client.StreamLedgers(context.TODO(),
		&cur,
		func(ledger horizon.Ledger){
			mu.Lock()
			seq = ledger.Sequence
			mu.Unlock()

			if err != nil{
				panic(err)
			}

			if ledger.OperationCount != 0{
				resp, err := client.HTTP.Get(ledger.Links.Operations.Href)
				if err != nil{
					panic(err)
				}
				size, _ := resp.Body.Read(buffer)
				var op horizon.OffersPage
				var OpType struct{
					TypeI int `json:"type_i"`
				}
				err = json.Unmarshal(buffer[0:size], OpType)
				if err != nil{
					panic(err)
				}
				if OpType.TypeI != 3{
					return
				}
				err = json.Unmarshal(buffer[0:size],&op)
				if err != nil{
					panic(err)
				}

				for index := range op.Embedded.Records {
					mu.Lock()
					mem = append(mem, pair{Time: ledger.ClosedAt, Off: op.Embedded.Records[index]})
					mu.Unlock()
				}
			} else {
				time.Sleep(time.Second)
				return
			}
		})

	router := chi.NewRouter()
	router.Get("/ledger", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(strconv.FormatInt(int64(seq), 10)))
	})

	router.Get("/offers{?start,end}", func(w http.ResponseWriter, r *http.Request) {
		router.With(offerCtx)
		router.Get("/", GetOffer)
	})

	http.ListenAndServe(":3000", router)

}
