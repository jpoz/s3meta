package s3meta

import (
	"github.com/hoisie/web"
	"io/ioutil"
	"testing"
	"time"
)

var S3Server *web.Server
var FakeS3 map[string]string
var TestBucket *Bucket

func S3GetHandler(ctx *web.Context, key string) (ret string) {
	val := FakeS3[key]
  if val == "" {
    ctx.Abort(404, "Not Found")
    return
  } else if val == "FAIL" {
    ctx.Redirect(301, "htttttttp://idon'twork")
    return
	} else {
		return val
	}
}

func S3SetHandler(ctx *web.Context, key string) (ret string) {
	body, _ := ioutil.ReadAll(ctx.Request.Body)
	FakeS3[key] = string(body)
	return
}

func init() {
	FakeS3 = make(map[string]string)
	TestBucket = &Bucket{
		"localhost:7777",
		"/",
		"WhatEvenISComputerz",
		"ADogWalkedInToABarAndOrderADrinkJKHePoopedHesADog",
	}

	S3Server = web.NewServer()
	S3Server.Get("/(.*)", S3GetHandler)
	S3Server.Put("/(.*)", S3SetHandler)

	go S3Server.Run("0.0.0.0:7777")
}

func TestHeadS3Object(t *testing.T) {
	FakeS3["taco"] = "waffle"

	bl, err := TestBucket.HeadS3Object("taco")
	if !bl {
		t.Errorf("Got false from HeadRequest")
	}
	if err != nil {
		t.Errorf("Got an error '%s'", err)
	}
}

func TestGetS3Object(t *testing.T) {
	FakeS3["chris"] = "schepman"

	str, err := TestBucket.GetS3Object("chris")
	if str != "schepman" {
		t.Errorf("Got %s from GetS3Object not %s", str, "schepman")
	}
	if err != nil {
		t.Errorf("Got an error '%s'", err)
	}
}

func TestRetrys(t *testing.T) {
  // Make FakeS3 give a failure
	FakeS3["matt"] = "FAIL"

  // key matt will 404 on the first attempt
  // 400ms after the key will be set
  // 3 rety should get a 200
  go func() {
    time.Sleep(time.Millisecond * 200)
	  FakeS3["matt"] = "sacks"
  }()

	str, err := TestBucket.GetS3Object("matt")
	if str != "sacks" {
		t.Errorf("Got %s from GetS3Object not %s", str, "sacks")
	}
	if err != nil {
		t.Errorf("Got an error '%s'", err)
	}

}
