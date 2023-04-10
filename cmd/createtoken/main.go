package main

import (
	"crypto/x509"
	"encoding/pem"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/golang-jwt/jwt"
)

func main() {

	hoursPtr := flag.Int("hours", 1, "Number of hours the token should be valid")
	exchangePtr := flag.String("exchange", "shoveled-xrd", "Exchange to set")

	flag.Parse()
	// Read in the private key from the command line
	if len(flag.Args()) != 1 {
		fmt.Println("You must include the private key location as the first argument")
		os.Exit(1)
	}

	// Read in the private key
	pemString, err := ioutil.ReadFile(flag.Args()[0])
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

	// scopes
	// "my_rabbit_server.write:xrd-mon/" + *exchangePtr + " my_rabbit_server.read:xrd-mon/" + *exchangePtr + " my_rabbit_server.configure:xrd-mon/" + *exchangePtr,

	// Create the Claims
	claims := MyCustomClaims{
		"my_rabbit_server.write:xrd-mon/" + *exchangePtr,
		jwt.StandardClaims{
			ExpiresAt: time.Now().Add(time.Hour * time.Duration(*hoursPtr)).Unix(),
			Issuer:    "test",
			Audience:  "my_rabbit_server",
			Subject:   "shoveler",
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = "xrdshoveler"
	ss, err := token.SignedString(privateKey)
	if err != nil {
		fmt.Println("Failed to sign token:", err)
		os.Exit(1)
	}
	fmt.Printf("%v", ss)

}
