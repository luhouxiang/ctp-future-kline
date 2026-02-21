package db

func OnDuplicateAssignments(cols []string) string {
	if len(cols) == 0 {
		return ""
	}
	out := ""
	for i, c := range cols {
		if i > 0 {
			out += ",\n  "
		}
		out += `"` + c + `" = VALUES("` + c + `")`
	}
	return out
}
