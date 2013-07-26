package s3meta

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"io/ioutil"
	"net/http"
	"sort"
	"strings"
	"time"
)

type Bucket struct {
	Name   string // "com-awesome-dev-bucket"
	Base   string // ".s3.amazonaws.com/"
	Key    string
	Secret string
}

func (b *Bucket) HeadS3ObjectResponse(key string) (*http.Response, error) {
	fullPath := "http://" + b.Name + b.Base + key
	req, err := http.NewRequest("HEAD", fullPath, nil)
	if err != nil {
		return &http.Response{}, err
	}

	resp, err := b.authDoRequest(req)

	return resp, err
}

func (b *Bucket) HeadS3Object(key string) (bl bool, err error) {
	bl = false
	resp, err := b.HeadS3ObjectResponse(key)
	if err != nil {
		return
	}

	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		return
	}

	if resp.StatusCode != 200 {
		err = errors.New(resp.Status)
		return
	}

	bl = true

	return
}

func (b *Bucket) HeadS3ObjectWithMetaData(key string) (bl bool, data map[string]string, err error) {
	bl = false
	resp, err := b.HeadS3ObjectResponse(key)
	if err != nil {
		return
	}

	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		return
	}

	if resp.StatusCode != 200 {
		err = errors.New(resp.Status)
		return
	}

	bl = true
	data = extractMetaDataFrom(resp.Header)

	return
}

func (b *Bucket) GetS3ObjectResponse(key string) (*http.Response, error) {
	fullPath := "http://" + b.Name + b.Base + key
	req, err := http.NewRequest("GET", fullPath, nil)
	if err != nil {
		return &http.Response{}, err
	}
	resp, err := b.authDoRequest(req)

	return resp, err
}

// GetS3Object Get object from s3 bucket
func (b *Bucket) GetS3Object(key string) (string, error) {
	resp, err := b.GetS3ObjectResponse(key)
	if err != nil {
		return "", err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	if resp.StatusCode != 200 {
		return "", errors.New(string(body))
	}

	return string(body), nil
}

func (b *Bucket) GetS3ObjectWithMetaData(key string) (str string, data map[string]string, err error) {
	resp, err := b.GetS3ObjectResponse(key)
	if err != nil {
		return
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}

	if resp.StatusCode != 200 {
		err = errors.New(string(body))
		return
	}

	str = string(body)
	data = extractMetaDataFrom(resp.Header)

	return
}

func (b *Bucket) GetS3BucketResponse(prefix string) (*http.Response, error) {
	fullPath := "http://" + b.Name + b.Base + "?prefix=" + prefix
	req, err := http.NewRequest("GET", fullPath, nil)
	if err != nil {
		return &http.Response{}, err
	}
	resp, err := b.authDoRequest(req)

	return resp, err
}

type BucketItem struct {
	Key          string
	LastModified time.Time
	Body         string
}

type ListBucketResult struct {
	Contents []BucketItem
}

func (b *Bucket) GetS3Bucket(prefix string) (result *ListBucketResult, err error) {
	resp, err := b.GetS3BucketResponse(prefix)
	if err != nil {
		return
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}

	if resp.StatusCode != 200 {
		err = errors.New(string(body))
		return
	}

	result = &ListBucketResult{}

	err = xml.Unmarshal(body, result)
	if err != nil {
		return
	}

	return
}

func (b *Bucket) PutS3ObjectResponse(key string, body []byte) (*http.Response, error) {
	fullPath := "http://" + b.Name + b.Base + key
	req, err := http.NewRequest("PUT", fullPath, nil)
	if err != nil {
		return &http.Response{}, err
	}

	req.Header.Add("Content-Type", "text/plain")

	req.Body = ioutil.NopCloser(bytes.NewReader(body))
	req.ContentLength = int64(len(body))

	return b.authDoRequest(req)
}

// PutObject from s3 bucket
func (b *Bucket) PutS3Object(key string, bs []byte) error {
	resp, err := b.PutS3ObjectResponse(key, bs)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return errors.New(resp.Status + string(body))
	}

	return nil
}

func (b *Bucket) PutS3ObjectMetaDataResponse(key string, body []byte, data map[string]string) (*http.Response, error) {
	fullPath := "http://" + b.Name + b.Base + key
	req, err := http.NewRequest("PUT", fullPath, nil)
	if err != nil {
		return &http.Response{}, err
	}

	req.Header.Add("Content-Type", "text/plain")

	for k, v := range data {
		req.Header.Add("x-amz-meta-"+k, v)
	}

	req.Body = ioutil.NopCloser(bytes.NewReader(body))
	req.ContentLength = int64(len(body))

	return b.authDoRequest(req)
}

// PutObject from s3 bucket
func (b *Bucket) PutS3ObjectWithMetaData(key string, bs []byte, data map[string]string) error {
	resp, err := b.PutS3ObjectMetaDataResponse(key, bs, data)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return errors.New(resp.Status + string(body))
	}

	return nil
}

func (b *Bucket) authDoRequest(request *http.Request) (*http.Response, error) {
	b.authRequest(request)

	return http.DefaultClient.Do(request)
}

// http://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html#ConstructingTheAuthenticationHeader
func (b *Bucket) authRequest(request *http.Request) {
	if request.Header.Get("Date") == "" {
		date := time.Now().UTC().Format(time.RFC1123Z)
		request.Header.Add("Date", date)
	}

	// canonicalizedResource needs uri without query
	url := *request.URL
	url.RawQuery = ""
	uri := url.RequestURI()

	canonicalizedResource := strings.Join([]string{
		"/",
		b.Name,
		uri,
	}, "")

	stringToSign := strings.Join([]string{
		request.Method, "\n\n", // no MD5
		request.Header.Get("Content-Type"), "\n",
		request.Header.Get("Date"), "\n",
		canonicalizedAmzHeaders(request.Header),
		canonicalizedResource,
	}, "")

	h := hmac.New(sha1.New, []byte(b.Secret))
	h.Write([]byte(stringToSign))

	signature := base64.StdEncoding.EncodeToString(h.Sum([]byte{}))

	request.Header.Add("Host", "http://"+b.Name+b.Base)
	request.Header.Add("Authorization", "AWS "+b.Key+":"+signature)

	return
}

// http://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html#RESTAuthenticationConstructingCanonicalizedAmzHeaders
func canonicalizedAmzHeaders(headers http.Header) string {
	var keys []string
	for k, _ := range headers {
		k_lower := strings.ToLower(k)
		if strings.HasPrefix(k_lower, "x-amz-") {
			keys = append(keys, k)
		}
	}

	sort.Strings(keys)
	var buffer bytes.Buffer

	for _, k := range keys {
		buffer.WriteString(strings.ToLower(k) + ":" + headers.Get(k) + "\n")
	}

	return buffer.String()
}

func extractMetaDataFrom(headers http.Header) map[string]string {
	data := make(map[string]string)

	for k, _ := range headers {
		k_lower := strings.ToLower(k)
		if strings.HasPrefix(k_lower, "x-amz-meta-") {
			key := strings.TrimPrefix(k_lower, "x-amz-meta-")
			data[key] = headers.Get(k)
		}
	}

	return data
}