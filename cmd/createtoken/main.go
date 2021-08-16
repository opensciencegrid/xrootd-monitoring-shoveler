package main

import (
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/golang-jwt/jwt"
)

func main() {
	// Read in the private key from the command line
	if len(os.Args) != 2 {
		fmt.Println("You must include the private key location as the first argument")
		os.Exit(1)
	}

	// Read in the private key
	pemString, err := ioutil.ReadFile(os.Args[1])
	if err != nil {
		fmt.Println("Failed to read in private key:", os.Args[1], ":", err)
		os.Exit(1)
	}
	block, _ := pem.Decode([]byte(pemString))
    privateKey, _ := x509.ParsePKCS1PrivateKey(block.Bytes)


	type MyCustomClaims struct {
		Scope string `json:"scope"`
		jwt.StandardClaims
	}

	// Create the Claims
	claims := MyCustomClaims{
		"my_rabbit_server.write:xrd-mon/shoveled-xrd",
		jwt.StandardClaims{
			ExpiresAt: time.Now().Add(time.Hour * 1).Unix(),
			Issuer:    "test",
			Audience:  "my_rabbit_server",
			Subject:   "shoveler",
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = "xrdshoveler"
	ss, err := token.SignedString(privateKey)
	fmt.Printf("%v", ss)

}

