package main

import (
	"context"
	"encoding/json"
	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/render"
	"github.com/stellar/go/clients/horizon"
	"github.com/stellar/go/support/log"
	"io/ioutil"
	"net/http"
	"sync"
	"time"
)

var (
	mutex          sync.Mutex
	sequenceNumber int32
	memory         []pair
)

const horizonAddress = "http://127.0.0.1:8000"
const port = ":3000"

var client = &horizon.Client{
	HTTP: http.DefaultClient,
	URL:  horizonAddress,
}

type pair struct {
	Time  time.Time     `json:"time"`
	Offer horizon.Offer `json:"offer"`
}

type Response struct {
	Ledger  string `json:"ledger,omitempty"`
	Records []pair `json:"records,omitempty"`
	Errors  string `json:"errors,omitempty"`
}

type LedgerPage struct {
	Links struct {
		Self horizon.Link `json:"self"`
		Next horizon.Link `json:"next"`
		Prev horizon.Link `json:"prev"`
	} `json:"_links"`
	Embedded struct {
		Records []horizon.Ledger `json:"records"`
	} `json:"_embedded"`
}

func dbGetOffer(start, end string) []pair {
	print("parsing time")
	var result []pair

	s, err := time.Parse(time.RFC3339, start)
	if err != nil {
		log.Error(err)
	}
	e, err := time.Parse(time.RFC3339, end)
	if err != nil {
		log.Error(err)
	}

	mutex.Lock()
	for _, v := range memory {
		if s.Before(v.Time) && e.After(v.Time) {
			result = append(result, v)
		}
	}
	mutex.Unlock()
	return result
}

func getCursor(client *horizon.Client) string {
	resp, err := client.HTTP.Get(client.URL + "/ledgers?limit=1")
	if err != nil {
		log.Error(err)
	}
	buffer, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Error(err)
	}
	var page LedgerPage
	err = json.Unmarshal(buffer, &page)
	if err != nil {
		log.Error(err)
	}

	return page.Embedded.Records[0].PT
}

func offersExist(offer horizon.Offer) bool {
	if offer.Seller != "" && offer.Price != "" {
		return true
	} else {
		return false
	}
}

func addOffers(buffer []byte, closed time.Time) {
	var offersPage horizon.OffersPage

	err := json.Unmarshal(buffer, &offersPage)
	if err != nil {
		log.Error(err)
	}
	mutex.Lock()
	for _, offer := range offersPage.Embedded.Records {
		if offersExist(offer) {
			memory = append(memory, pair{Time: closed, Offer: offer})
		} else {
			continue
		}
	}
	mutex.Unlock()
}

func operationHandler(link string, closed time.Time) {
	resp, err := client.HTTP.Get(link)
	if err != nil {
		log.Error(err)
	}
	buffer, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Error(err)
	}

	addOffers(buffer, closed)

}

func ledgerHandler(ledger horizon.Ledger) {
	mutex.Lock()
	sequenceNumber = ledger.Sequence
	mutex.Unlock()

	if ledger.OperationCount != 0 {
		operationHandler(ledger.Links.Operations.Href, ledger.ClosedAt)
	} else {
		return
	}
}

func fetchLedgers() {

	pagingToken := getCursor(client)

	cur := horizon.Cursor(pagingToken)

	client.StreamLedgers(context.TODO(), &cur, ledgerHandler)
}

func main() {
	go fetchLedgers()

	router := chi.NewRouter()
	router.Use(middleware.Logger)
	router.Use(middleware.Recoverer)
	router.Use(render.SetContentType(render.ContentTypeJSON))

	router.Get("/ledger", func(w http.ResponseWriter, r *http.Request) {
		var js Response
		js.Ledger = string(sequenceNumber)
		resp, err := json.Marshal(js)

		if err != nil {
			log.Error(err)
		}

		w.Write(resp)
	})

	router.Get("/offers", func(w http.ResponseWriter, r *http.Request) {
		start, end := r.URL.Query().Get("start"), r.URL.Query().Get("end")
		var js Response
		js.Records = dbGetOffer(start, end)
		if len(js.Records) == 0 {
			js.Errors = "404: Not found"
			w.WriteHeader(404)
		}
		resp, err := json.Marshal(js)
		if err != nil {
			log.Error(err)
		}
		w.Write(resp)
	})

	http.ListenAndServe(port, router)

}
