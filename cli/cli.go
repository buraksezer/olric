package cli

import "flag"

func BoolVar(f *flag.FlagSet, p *bool, name string, short string, value bool) {
	f.BoolVar(p, short, value, "")
	f.BoolVar(p, name, value, "")
}

func StringVar(f *flag.FlagSet, p *string, name string, short string, value string) {
	f.StringVar(p, short, value, "")
	f.StringVar(p, name, value, "")
}

func IntVar(f *flag.FlagSet, p *int, name string, short string, value int) {
	f.IntVar(p, short, value, "")
	f.IntVar(p, name, value, "")
}

