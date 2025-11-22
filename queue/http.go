package queue

import (
	"encoding/json"
	"net/http"
)

type HTTPHandler struct {
	q *RedisQueue
}

func NewHTTPHandler(q *RedisQueue) *HTTPHandler {
	return &HTTPHandler{
		q: q,
	}
}

func getOrSetUserKey(w http.ResponseWriter, r *http.Request) string {
	const cookieName = "queue_user"
	c, err := r.Cookie(cookieName)
	if err == nil && c.Value != "" {
		return c.Value
	}
	// 새 발급
	key := newToken()
	http.SetCookie(w, &http.Cookie{
		Name:     cookieName,
		Value:    key,
		Path:     "/",
		HttpOnly: true,
	})
	return key
}

func (h *HTTPHandler) JoinQueue(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	userKey := getOrSetUserKey(w, r) // 쿠키 기반

	res, err := h.q.Join(ctx, userKey)
	if err != nil {
		http.Error(w, "queue error", http.StatusInternalServerError)
		return
	}

	resp := map[string]interface{}{
		"position":   res.Position,
		"etaSeconds": res.ETASeconds,
		"userKey":    userKey,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
