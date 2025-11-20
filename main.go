package main

import (
	"encoding/json"
	"net/http"

	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	qs := NewQueueService(rdb, "black-friday-2025", 10)

	// /enter?user_id=xx
	http.HandleFunc("/enter", func(w http.ResponseWriter, r *http.Request) {
		userID := r.URL.Query().Get("user_id")

		if userID == "" {
			http.Error(w, "user_id is required", http.StatusBadRequest)
			return
		}

		status, err := qs.Enter(userID)

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		writeJSON(w, status)
	})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
