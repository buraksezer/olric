// Copyright 2018 Burak Sezer
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

package protocol

import "io"

type Pipeline struct {
	messages []Message
}

func NewPipeline() *Pipeline {
	return &Pipeline{}
}

func (p *Pipeline) Append(m Message) {
	p.messages = append(p.messages, m)
}

func (p *Pipeline) Read(conn io.Reader) error {
}

func (p *Pipeline) Write(conn io.Writer) error {
}
