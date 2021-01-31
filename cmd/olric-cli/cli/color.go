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

package cli

import "fmt"

// Borrowed from https://github.com/chzyer/readline/blob/master/example/readline-pass-strength/readline-pass-strength.go
const (
	Cyan          = 36
	Green         = 32
	Magenta       = 35
	Red           = 31
	Yellow        = 33
	BackgroundRed = 41
)

// Reset sequence
var ColorResetEscape = "\033[0m"

// ColorResetEscape translates a ANSI color number to a color escape.
func ColorEscape(color int) string {
	return fmt.Sprintf("\033[0;%dm", color)
}

// Colorize the msg using ANSI color escapes
func Colorize(msg string, color int) string {
	return ColorEscape(color) + msg + ColorResetEscape
}
