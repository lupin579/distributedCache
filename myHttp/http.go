package myHttp

//type htp int
//
//func (h *htp) ServeHTTP(w http.ResponseWriter, r *http.Request) {
//	bytes, err := ioutil.ReadAll(r.Body)
//	if err != nil {
//		http.Error(w, "server busy", http.StatusInternalServerError)
//		return
//	}
//	body := string(bytes)
//	key := strings.Split(body, ":")[1]
//	result := lru.LruCache.Query(key)
//	str := strconv.Itoa(result)
//	w.Write([]byte(str))
//}
