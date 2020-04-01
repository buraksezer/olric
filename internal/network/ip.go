// Copyright 2018-2020 Burak Sezer
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/*Package network provides utility functions for network-related tasks*/
package network

import (
	"fmt"
	"net"

	"github.com/hashicorp/go-sockaddr"
)

func ParseOrLookupIP(addr string) (net.IP, error) {
	if ip := net.ParseIP(addr); ip != nil {
		return ip, nil
	}

	ans, err := net.LookupIP(addr)
	if err != nil {
		return nil, err
	}
	// Fail early.
	if len(ans) == 0 {
		return nil, fmt.Errorf("no IP address found for %s", addr)
	}
	return ans[0], nil
}

func AddrToIP(addr string) (net.IP, error) {
	ip, err := ParseOrLookupIP(addr)
	if err != nil {
		return nil, err
	}
	ipStr := ip.String()

	var advertiseAddr net.IP
	if ipStr == "0.0.0.0" {
		// Otherwise, if we're not bound to a specific IP, let's
		// use a suitable private IP address.
		var err error
		ipStr, err = sockaddr.GetPrivateIP()
		if err != nil {
			return nil, fmt.Errorf("failed to get interface addresses: %v", err)
		}
		if ipStr == "" {
			return nil, fmt.Errorf("no private IP address found, and explicit IP not provided")
		}

		advertiseAddr = net.ParseIP(ipStr)
		if advertiseAddr == nil {
			return nil, fmt.Errorf("failed to parse advertise address: %q", ipStr)
		}
	} else {
		// If they've supplied an address, use that.
		advertiseAddr = net.ParseIP(ipStr)
		if advertiseAddr == nil {
			return nil, fmt.Errorf("failed to parse advertise address %q", ipStr)
		}

		// Ensure IPv4 conversion if necessary.
		if ip4 := advertiseAddr.To4(); ip4 != nil {
			advertiseAddr = ip4
		}
	}
	return advertiseAddr, nil
}
