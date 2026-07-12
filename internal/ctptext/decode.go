package ctptext

import (
	"strings"
	"unicode/utf8"

	"golang.org/x/text/encoding/simplifiedchinese"
)

// Decode converts text returned by the CTP native API to UTF-8.
// CTP character arrays are normally GBK/GB18030, while some simulators and
// wrappers already return UTF-8. Preserve valid UTF-8 and decode only invalid
// input so both sources are handled without double conversion.
func Decode(value string) string {
	value = strings.Trim(value, "\x00 \t\r\n")
	if value == "" || utf8.ValidString(value) {
		return value
	}
	decoded, err := simplifiedchinese.GB18030.NewDecoder().Bytes([]byte(value))
	if err != nil {
		return strings.ToValidUTF8(value, "�")
	}
	return strings.Trim(string(decoded), "\x00 \t\r\n")
}
