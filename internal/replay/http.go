package replay

type StartResponse struct {
	OK   bool         `json:"ok"`
	Task TaskSnapshot `json:"task"`
}

type ActionResponse struct {
	OK   bool         `json:"ok"`
	Task TaskSnapshot `json:"task"`
}

type StatusResponse struct {
	Task TaskSnapshot `json:"task"`
}
