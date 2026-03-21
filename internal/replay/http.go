package replay

type StartResponse struct {
	// OK 表示启动请求是否成功。
	OK bool `json:"ok"`
	// Task 是启动后的任务快照。
	Task TaskSnapshot `json:"task"`
}

type ActionResponse struct {
	// OK 表示暂停、恢复或停止动作是否成功。
	OK bool `json:"ok"`
	// Task 是动作执行后的任务快照。
	Task TaskSnapshot `json:"task"`
}

type StatusResponse struct {
	// Task 是当前回放任务快照。
	Task TaskSnapshot `json:"task"`
}
