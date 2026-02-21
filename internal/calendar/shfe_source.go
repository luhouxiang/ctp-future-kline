package calendar

import (
	"context"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"ctp-go-demo/internal/logger"
	"github.com/chromedp/chromedp"
)

const wafMarker = "WEB 应用防火墙"

type AnnouncementMeta struct {
	Title       string
	URL         string
	PublishedAt time.Time
}

type Announcement struct {
	Year       int
	ClosedDays []time.Time
	Meta       AnnouncementMeta
}

type SHFESource interface {
	FetchLatest(ctx context.Context) (Announcement, error)
}

type SourceOptions struct {
	EnableBrowserFallback bool
	BrowserPath           string
	BrowserHeadless       bool
}

type shfeSource struct {
	entryURL string
	client   *http.Client
	opts     SourceOptions

	mu       sync.Mutex
	pageCache map[string]cachedPage
	hostWAF   map[string]int
}

type cachedPage struct {
	body     string
	finalURL string
}

func NewSHFESource(entryURL string) SHFESource {
	return NewSHFESourceWithOptions(entryURL, SourceOptions{
		EnableBrowserFallback: true,
		BrowserHeadless:       true,
	})
}

func NewSHFESourceWithOptions(entryURL string, opts SourceOptions) SHFESource {
	entryURL = strings.TrimSpace(entryURL)
	if entryURL == "" {
		entryURL = "https://www.shfe.com.cn/"
	}
	jar, _ := cookiejar.New(nil)
	if !opts.EnableBrowserFallback {
		opts.BrowserHeadless = true
	}
	return &shfeSource{
		entryURL: entryURL,
		client:   &http.Client{Timeout: 20 * time.Second, Jar: jar},
		opts:     opts,
		pageCache: make(map[string]cachedPage),
		hostWAF:   make(map[string]int),
	}
}

func (s *shfeSource) FetchLatest(ctx context.Context) (Announcement, error) {
	entryCandidates := buildEntryCandidates(s.entryURL)
	var page, finalURL string
	var err error
	for _, u := range entryCandidates {
		page, finalURL, err = s.fetchPage(ctx, u)
		if err == nil {
			break
		}
		logger.Error("shfe fetch entry failed", "url", u, "error", err)
	}
	if err != nil {
		return Announcement{}, err
	}
	logger.Info("shfe fetch entry success", "entry_url", s.entryURL, "final_url", finalURL, "body_len", len(page))

	links := extractLinks(page, finalURL)
	candidates := make([]linkCandidate, 0, len(links)+1)
	seenURL := make(map[string]struct{})
	if isAnnouncementLike(page) {
		seenURL[finalURL] = struct{}{}
		candidates = append(candidates, linkCandidate{URL: finalURL, Title: extractTitle(page), Body: page})
	}
	for _, ln := range links {
		if !containsAny(ln.Text, []string{"休市", "交易日历", "交易日"}) {
			continue
		}
		if _, ok := seenURL[ln.URL]; ok {
			continue
		}
		body, u, e := s.fetchPage(ctx, ln.URL)
		if e != nil {
			continue
		}
		finalKey := strings.TrimSpace(u)
		if finalKey == "" {
			finalKey = ln.URL
		}
		if _, ok := seenURL[finalKey]; ok {
			continue
		}
		seenURL[ln.URL] = struct{}{}
		seenURL[u] = struct{}{}
		candidates = append(candidates, linkCandidate{URL: u, Title: ln.Text, Body: body})
	}
	if len(candidates) == 0 {
		return Announcement{}, fmt.Errorf("no shfe announcement candidates found")
	}
	logger.Info("shfe announcement candidates", "count", len(candidates))

	type parsed struct {
		ann   Announcement
		score int
	}
	parsedItems := make([]parsed, 0, len(candidates))
	for _, c := range candidates {
		ann, e := parseAnnouncement(c.Body, c.URL, c.Title)
		if e != nil {
			continue
		}
		score := len(ann.ClosedDays)
		if strings.Contains(ann.Meta.Title, "休市") {
			score += 10
		}
		parsedItems = append(parsedItems, parsed{ann: ann, score: score})
	}
	if len(parsedItems) == 0 {
		return Announcement{}, fmt.Errorf("parse shfe announcement failed")
	}
	sort.Slice(parsedItems, func(i, j int) bool {
		if parsedItems[i].ann.Year == parsedItems[j].ann.Year {
			return parsedItems[i].score > parsedItems[j].score
		}
		return parsedItems[i].ann.Year > parsedItems[j].ann.Year
	})
	return parsedItems[0].ann, nil
}

func buildEntryCandidates(entryURL string) []string {
	candidates := []string{entryURL}
	if !strings.Contains(strings.ToLower(entryURL), "shfe.com.cn") {
		return candidates
	}
	defaults := []string{
		"https://www.shfe.com.cn/",
		"https://www.shfe.com.cn/services/calender/",
	}
	seen := map[string]struct{}{strings.TrimSpace(entryURL): {}}
	for _, u := range defaults {
		if _, ok := seen[u]; ok {
			continue
		}
		seen[u] = struct{}{}
		candidates = append(candidates, u)
	}
	return candidates
}

type linkCandidate struct {
	URL   string
	Title string
	Body  string
}

type parsedLink struct {
	URL  string
	Text string
}

func (s *shfeSource) fetchPage(ctx context.Context, rawURL string) (string, string, error) {
	if body, final, ok := s.getPageCache(rawURL); ok {
		logger.Info("shfe fetch page cache hit", "url", rawURL, "final_url", final, "body_len", len(body))
		return body, final, nil
	}

	host := hostOf(rawURL)
	if s.opts.EnableBrowserFallback && s.getHostWAFCount(host) >= 2 {
		logger.Info("shfe waf threshold reached, use browser directly", "url", rawURL, "host", host)
		text, final, err := s.fetchPageByBrowser(ctx, rawURL)
		if err != nil {
			logger.Error("shfe browser fallback failed", "url", rawURL, "error", err)
			return "", "", err
		}
		s.setPageCache(rawURL, text, final)
		return text, final, nil
	}

	var lastErr error
	for attempt := 1; attempt <= 2; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if err != nil {
			return "", "", err
		}
		applyBrowserLikeHeaders(req)
		resp, err := s.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			lastErr = readErr
			continue
		}
		text := string(body)
		title := extractTitle(text)
		logger.Info("shfe fetch page",
			"url", rawURL,
			"attempt", attempt,
			"status", resp.StatusCode,
			"final_url", resp.Request.URL.String(),
			"content_type", resp.Header.Get("Content-Type"),
			"body_len", len(text),
			"title", title,
		)
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			lastErr = fmt.Errorf("http status not ok: %d", resp.StatusCode)
			continue
		}
		if isWAFPage(text) {
			lastErr = fmt.Errorf("shfe website blocked by waf challenge page")
			count := s.incHostWAFCount(host)
			logger.Info("shfe waf detected", "url", rawURL, "host", host, "count", count, "attempt", attempt)
			if s.opts.EnableBrowserFallback && count >= 2 {
				break
			}
			continue
		}
		s.setPageCache(rawURL, text, resp.Request.URL.String())
		return text, resp.Request.URL.String(), nil
	}

	if s.opts.EnableBrowserFallback {
		text, final, bErr := s.fetchPageByBrowser(ctx, rawURL)
		if bErr == nil {
			s.setPageCache(rawURL, text, final)
			return text, final, nil
		}
		logger.Error("shfe browser fallback failed", "url", rawURL, "error", bErr)
		if lastErr != nil {
			return "", "", fmt.Errorf("%v; browser=%w", lastErr, bErr)
		}
		return "", "", bErr
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("fetch page failed")
	}
	return "", "", lastErr
}

func (s *shfeSource) getPageCache(rawURL string) (string, string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.pageCache[rawURL]
	if !ok {
		return "", "", false
	}
	return item.body, item.finalURL, true
}

func (s *shfeSource) setPageCache(rawURL string, body string, finalURL string) {
	s.mu.Lock()
	s.pageCache[rawURL] = cachedPage{body: body, finalURL: finalURL}
	if finalURL != "" && finalURL != rawURL {
		s.pageCache[finalURL] = cachedPage{body: body, finalURL: finalURL}
	}
	s.mu.Unlock()
}

func (s *shfeSource) getHostWAFCount(host string) int {
	if host == "" {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hostWAF[host]
}

func (s *shfeSource) incHostWAFCount(host string) int {
	if host == "" {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hostWAF[host]++
	return s.hostWAF[host]
}

func hostOf(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Host)
}

func (s *shfeSource) fetchPageByBrowser(ctx context.Context, rawURL string) (string, string, error) {
	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.NoFirstRun,
		chromedp.NoDefaultBrowserCheck,
		chromedp.UserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"),
		chromedp.Flag("headless", s.opts.BrowserHeadless),
		chromedp.Flag("disable-blink-features", "AutomationControlled"),
		chromedp.Flag("disable-gpu", true),
	)
	if p := strings.TrimSpace(s.opts.BrowserPath); p != "" {
		opts = append(opts, chromedp.ExecPath(p))
	}

	allocCtx, cancelAlloc := chromedp.NewExecAllocator(ctx, opts...)
	defer cancelAlloc()
	browserCtx, cancelBrowser := chromedp.NewContext(allocCtx)
	defer cancelBrowser()

	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		var bodyHTML string
		var finalURL string
		var title string
		err := chromedp.Run(browserCtx,
			chromedp.Navigate(rawURL),
			chromedp.Sleep(5*time.Second),
			chromedp.Title(&title),
			chromedp.OuterHTML("html", &bodyHTML, chromedp.ByQuery),
			chromedp.Evaluate(`window.location.href`, &finalURL),
		)
		if err != nil {
			lastErr = err
			continue
		}
		if strings.TrimSpace(finalURL) == "" {
			finalURL = rawURL
		}
		logger.Info("shfe browser fetch page",
			"url", rawURL,
			"attempt", attempt,
			"final_url", finalURL,
			"body_len", len(bodyHTML),
			"title", title,
		)
		if isWAFPage(bodyHTML) {
			lastErr = fmt.Errorf("shfe website blocked by waf challenge page")
			if attempt < 3 {
				time.Sleep(2 * time.Second)
			}
			continue
		}
		return bodyHTML, finalURL, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("browser fetch failed")
	}
	return "", "", lastErr
}

func applyBrowserLikeHeaders(req *http.Request) {
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8")
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Pragma", "no-cache")
	req.Header.Set("Upgrade-Insecure-Requests", "1")
	req.Header.Set("Referer", "https://www.shfe.com.cn/")
}

func parseAnnouncement(htmlBody string, pageURL string, titleHint string) (Announcement, error) {
	plain := htmlToText(htmlBody)
	year := detectAnnouncementYear(titleHint + "\n" + plain)
	if year == 0 {
		return Announcement{}, fmt.Errorf("announcement year not found")
	}
	closed := extractClosedDays(year, plain)
	if len(closed) == 0 {
		return Announcement{}, fmt.Errorf("no closed days parsed")
	}
	out := make([]time.Time, 0, len(closed))
	for _, d := range closed {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Before(out[j]) })

	title := strings.TrimSpace(titleHint)
	if title == "" {
		title = extractTitle(htmlBody)
	}
	return Announcement{Year: year, ClosedDays: out, Meta: AnnouncementMeta{Title: title, URL: pageURL}}, nil
}

func ParseAnnouncementText(htmlBody string, pageURL string, titleHint string) (Announcement, error) {
	return parseAnnouncement(htmlBody, pageURL, titleHint)
}

func detectAnnouncementYear(text string) int {
	re := regexp.MustCompile(`20\d{2}年`)
	matches := re.FindAllString(text, -1)
	if len(matches) == 0 {
		return 0
	}
	years := make([]int, 0, len(matches))
	for _, m := range matches {
		n, _ := strconv.Atoi(strings.TrimSuffix(m, "年"))
		if n >= 2000 && n <= 2100 {
			years = append(years, n)
		}
	}
	if len(years) == 0 {
		return 0
	}
	sort.Ints(years)
	return years[len(years)-1]
}

func extractClosedDays(year int, text string) map[string]time.Time {
	result := make(map[string]time.Time)
	sentences := splitSentences(text)
	for _, s := range sentences {
		if !strings.Contains(s, "休市") {
			continue
		}
		segment := s
		if idx := strings.LastIndex(segment, "休市"); idx >= 0 {
			segment = segment[:idx+len("休市")]
		}
		for _, d := range parseRangeDates(year, segment) {
			result[d.Format(dateLayout)] = d
		}
		for _, d := range parseSingleDates(year, segment) {
			result[d.Format(dateLayout)] = d
		}
	}
	return result
}

func parseRangeDates(year int, s string) []time.Time {
	out := make([]time.Time, 0)
	re1 := regexp.MustCompile(`(\d{1,2})月(\d{1,2})日[^\d]{0,20}至[^\d]{0,20}(\d{1,2})月(\d{1,2})日`)
	for _, m := range re1.FindAllStringSubmatch(s, -1) {
		sm, _ := strconv.Atoi(m[1])
		sd, _ := strconv.Atoi(m[2])
		em, _ := strconv.Atoi(m[3])
		ed, _ := strconv.Atoi(m[4])
		out = append(out, expandDateRange(year, sm, sd, em, ed)...)
	}
	re2 := regexp.MustCompile(`(\d{1,2})月(\d{1,2})日[^\d]{0,20}至[^\d]{0,20}(\d{1,2})日`)
	for _, m := range re2.FindAllStringSubmatch(s, -1) {
		sm, _ := strconv.Atoi(m[1])
		sd, _ := strconv.Atoi(m[2])
		em := sm
		ed, _ := strconv.Atoi(m[3])
		out = append(out, expandDateRange(year, sm, sd, em, ed)...)
	}
	return out
}

func parseSingleDates(year int, s string) []time.Time {
	out := make([]time.Time, 0)
	re := regexp.MustCompile(`(\d{1,2})月(\d{1,2})日`)
	for _, m := range re.FindAllStringSubmatch(s, -1) {
		mm, _ := strconv.Atoi(m[1])
		dd, _ := strconv.Atoi(m[2])
		d := time.Date(year, time.Month(mm), dd, 0, 0, 0, 0, time.Local)
		if int(d.Month()) == mm && d.Day() == dd {
			out = append(out, d)
		}
	}
	return out
}

func expandDateRange(year, sm, sd, em, ed int) []time.Time {
	start := time.Date(year, time.Month(sm), sd, 0, 0, 0, 0, time.Local)
	end := time.Date(year, time.Month(em), ed, 0, 0, 0, 0, time.Local)
	if start.After(end) {
		return nil
	}
	out := make([]time.Time, 0)
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		out = append(out, d)
	}
	return out
}

func splitSentences(text string) []string {
	text = strings.ReplaceAll(text, "\r", "\n")
	repl := strings.NewReplacer("。", "\n", "；", "\n", ";", "\n")
	text = repl.Replace(text)
	parts := strings.Split(text, "\n")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func htmlToText(s string) string {
	reTag := regexp.MustCompile(`(?is)<script.*?</script>|<style.*?</style>|<[^>]+>`)
	s = reTag.ReplaceAllString(s, "\n")
	s = html.UnescapeString(s)
	s = strings.ReplaceAll(s, "\u00a0", " ")
	s = strings.ReplaceAll(s, "\r", "\n")
	return s
}

func extractTitle(s string) string {
	re := regexp.MustCompile(`(?is)<title[^>]*>(.*?)</title>`)
	m := re.FindStringSubmatch(s)
	if len(m) < 2 {
		return ""
	}
	return strings.TrimSpace(htmlToText(m[1]))
}

func extractLinks(body string, base string) []parsedLink {
	re := regexp.MustCompile(`(?is)<a[^>]*href=["']([^"']+)["'][^>]*>(.*?)</a>`)
	matches := re.FindAllStringSubmatch(body, -1)
	out := make([]parsedLink, 0, len(matches))
	baseURL, _ := url.Parse(base)
	for _, m := range matches {
		href := strings.TrimSpace(m[1])
		if href == "" || strings.HasPrefix(strings.ToLower(href), "javascript:") {
			continue
		}
		u, err := url.Parse(href)
		if err != nil {
			continue
		}
		if baseURL != nil {
			u = baseURL.ResolveReference(u)
		}
		text := strings.TrimSpace(htmlToText(m[2]))
		out = append(out, parsedLink{URL: u.String(), Text: text})
	}
	return out
}

func isAnnouncementLike(htmlBody string) bool {
	plain := htmlToText(htmlBody)
	return strings.Contains(plain, "休市") && (strings.Contains(plain, "安排") || strings.Contains(plain, "交易日历"))
}

func isWAFPage(text string) bool {
	low := strings.ToLower(text)
	return strings.Contains(text, wafMarker) || strings.Contains(low, "web application firewall")
}

func containsAny(s string, keys []string) bool {
	for _, k := range keys {
		if strings.Contains(s, k) {
			return true
		}
	}
	return false
}
