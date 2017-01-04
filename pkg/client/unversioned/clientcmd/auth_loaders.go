/*
Copyright 2014 The Kubernetes Authors All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package clientcmd

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"golang.org/x/crypto/ssh/terminal"
	clientauth "k8s.io/kubernetes/pkg/client/unversioned/auth"
)

// AuthLoaders are used to build clientauth.Info objects.
type AuthLoader interface {
	// LoadAuth takes a path to a config file and can then do anything it needs in order to return a valid clientauth.Info
	LoadAuth(path string) (*clientauth.Info, error)
}

// default implementation of an AuthLoader
type defaultAuthLoader struct{}

// LoadAuth for defaultAuthLoader simply delegates to clientauth.LoadFromFile
func (*defaultAuthLoader) LoadAuth(path string) (*clientauth.Info, error) {
	return clientauth.LoadFromFile(path)
}

type PromptingAuthLoader struct {
	reader io.Reader
}

// LoadAuth parses an AuthInfo object from a file path. It prompts user and creates file if it doesn't exist.
func (a *PromptingAuthLoader) LoadAuth(path string) (*clientauth.Info, error) {
	var auth clientauth.Info
	// Prompt for user/pass and write a file if none exists.
	if _, err := os.Stat(path); os.IsNotExist(err) {
		auth = *a.Prompt()
		data, err := json.Marshal(auth)
		if err != nil {
			return &auth, err
		}
		err = ioutil.WriteFile(path, data, 0600)
		return &auth, err
	}
	authPtr, err := clientauth.LoadFromFile(path)
	if err != nil {
		return nil, err
	}
	return authPtr, nil
}

// Prompt pulls the user and password from a reader
func (a *PromptingAuthLoader) Prompt() *clientauth.Info {
	auth := &clientauth.Info{}
	auth.User = promptForString("Username", a.reader)
	auth.Password = getPasswd(a.reader)
	return auth
}

func getPasswd(r io.Reader) string {
	var pass []byte

	if r == os.Stdin {
		fmt.Printf("Please enter Password: ")
		if oldState, err := terminal.MakeRaw(int(os.Stdin.Fd())); err != nil {
			return ""
		} else {
			defer func() {
				terminal.Restore(int(os.Stdin.Fd()), oldState)
				fmt.Println()
			}()
		}
	} else {
		return promptForString("Password", r)
	}
	for {
		buf := make([]byte, 1)
		if n, err := os.Stdin.Read(buf); n == 0 || err != nil {
			return ""
		}
		if buf[0] == 127 || buf[0] == 8 {
			if l := len(pass); l > 0 {
				pass = pass[:l-1]
			}
		} else if buf[0] == 13 || buf[0] == 10 {
			break
		} else if buf[0] == 3 {
			break
		} else if buf[0] != 0 {
			pass = append(pass, buf[0])
		}
	}

	return string(pass)
}

func promptForString(field string, r io.Reader) string {
	fmt.Printf("Please enter %s: ", field)
	var result string
	fmt.Fscan(r, &result)
	return result
}

// NewPromptingAuthLoader is an AuthLoader that parses an AuthInfo object from a file path. It prompts user and creates file if it doesn't exist.
func NewPromptingAuthLoader(reader io.Reader) *PromptingAuthLoader {
	return &PromptingAuthLoader{reader}
}

// NewDefaultAuthLoader returns a default implementation of an AuthLoader that only reads from a config file
func NewDefaultAuthLoader() AuthLoader {
	return &defaultAuthLoader{}
}
