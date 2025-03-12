package identifier

import (
	"emperror.dev/errors"
	"fmt"
	"golang.org/x/exp/constraints"
	"io/fs"
	"path"
	"path/filepath"
	"regexp"
	"strings"
)

var directCleanRuleAll = regexp.MustCompile("[\u0000-\u001f\u007f\u0020\u0085\u00a0\u1680\u2000-\u200f\u2028\u2029\u202f\u205f\u3000\n\t*?:\\[\\]\"<>|(){}&'!\\;#@]")
var directCleanRuleWhitespace = regexp.MustCompile("[\u0009\u000a-\u000d\u0020\u0085\u00a0\u1680\u2000-\u200f\u2028\u2029\u202f\u205f\u3000]")
var directCleanRuleEqual = regexp.MustCompile("=(u[a-zA-Z0-9]{4})")

// var directCleanRule_1_5 = regexp.MustCompile("[\u0000-\u001F\u007F\n\r\t*?:\\[\\]\"<>|(){}&'!\\;#@]")
var directCleanRule_1_5 = regexp.MustCompile("[\u0000-\u001F\u007F\n\r\t*:<>|{}]")
var directCleanQuotationSingle = regexp.MustCompile("[‘’`]")
var directCleanQuotationSpecial = regexp.MustCompile("[‘’“”]")
var directCleanQuotationDouble = regexp.MustCompile("[“”]")
var directCleanMultiBlank = regexp.MustCompile(" +")
var directCleanDash = regexp.MustCompile("—")

// var directCleanRule_2_4_6 = regexp.MustCompile("^[\\-~\u0009\u000a-\u000d\u0020\u0085\u00a0\u1680\u2000-\u200f\u2028\u2029\u202f\u205f\u3000]*(.*?)[\u0009\u000a-\u000d\u0020\u0085\u00a0\u1680\u2000-\u20a0\u2028\u2029\u202f\u205f\u3000]*$")
var directCleanRule_2_4_6 = regexp.MustCompile("^[\u0009\u000a-\u000d\u0020\u0085\u00a0\u1680\u2000-\u200f\u2028\u2029\u202f\u205f\u3000]*(.*?)[\u0009\u000a-\u000d\u0020\u0085\u00a0\u1680\u2000-\u20a0\u2028\u2029\u202f\u205f\u3000]*$")
var directCleanRulePeriods = regexp.MustCompile("^\\..")
var directCleanRuleTilde = regexp.MustCompile("^~.")
var directCleanRulePrivateUse = regexp.MustCompile("[\uE000-\uF8FF]")

func min[T constraints.Ordered](a, b T) T {
	if a < b {
		return a
	}
	return b
}

func max[T constraints.Ordered](a, b T) T {
	if a > b {
		return a
	}
	return b
}

func encodeUTFCode(s string) string {
	return "=u" + strings.Trim(fmt.Sprintf("%U", []rune(s)), "U+[]")
}

const (
	replacementString           = "_"
	whitespaceReplacementString = "_"
)

func build(fname string, utfEncode bool) string {

	fname = strings.ToValidUTF8(fname, replacementString)

	names := strings.Split(fname, "/")
	result := []string{}

	for _, n := range names {
		if len(n) == 0 {
			continue
		}
		if utfEncode {
			n = directCleanRuleEqual.ReplaceAllString(n, "=u003D$1")
			n = directCleanRuleAll.ReplaceAllStringFunc(n, encodeUTFCode)
			if n[0] == '~' || directCleanRulePeriods.MatchString(n) {
				n = encodeUTFCode(string(n[0])) + n[1:]
			}
		} else {
			//n = directCleanRuleWhitespace.ReplaceAllString(n, whitespaceReplacementString)
			n = directCleanRule_1_5.ReplaceAllString(n, replacementString)
			n = directCleanRulePrivateUse.ReplaceAllString(n, "")
			n = directCleanRule_2_4_6.ReplaceAllString(n, "$1")
			n = directCleanQuotationDouble.ReplaceAllString(n, "\"")
			n = directCleanQuotationSingle.ReplaceAllString(n, "\"")
			n = directCleanMultiBlank.ReplaceAllString(n, " ")
			n = directCleanDash.ReplaceAllString(n, "-")
			if directCleanRulePeriods.MatchString(n) {
				n = replacementString + n[1:]
			}
			if directCleanRuleTilde.MatchString(n) {
				n = "-" + n[1:]
			}
		}

		if len(n) > 0 {
			//n = strings.TrimRight(n, replacementString+whitespaceReplacementString)
			if len(n) == 0 {
				n = replacementString
			}
			result = append(result, n)
		}
	}
	fname = strings.Join(result, "/")

	return fname
}

func NewPathElement(name string, dir bool, parent *pathElement) *pathElement {
	return &pathElement{name: name, dir: dir, parent: parent, subs: []*pathElement{}}
}

type pathElement struct {
	name      string
	clearName string
	dir       bool
	subs      []*pathElement
	parent    *pathElement
}

func (p *pathElement) AddSub(name string, dir bool) *pathElement {
	for _, sub := range p.subs {
		if sub.name == name {
			return sub
		}
	}
	sub := NewPathElement(name, dir, p)
	p.subs = append(p.subs, sub)
	return sub
}

func (p *pathElement) String() string {
	if p.parent == nil {
		return p.name
	}
	return p.parent.String() + "/" + p.name
}

func (p *pathElement) ClearString() string {
	if p.parent == nil {
		clearName, _ := p.ClearName()
		return clearName
	}
	return p.parent.ClearString() + "/" + p.name
}

func (p *pathElement) Name() string {
	return p.name
}

func (p *pathElement) ClearName() (string, bool) {
	if p.clearName == "" {
		p.clearName = build(p.name, false)
	}
	return p.clearName, p.clearName != p.name
}

func (p *pathElement) ClearIterator(yield func(string, string) bool) {
	for _, sub := range p.subs {
		sub.ClearIterator(yield)
	}
	clearName, changed := p.ClearName()
	if changed {
		newName := ""
		if p.parent != nil {
			newName = p.parent.String() + "/" + clearName
		} else {
			newName = clearName
		}
		if !yield(strings.TrimPrefix(p.String(), "/"), strings.TrimPrefix(newName, "/")) {
			return
		}
	}
}

func (p *pathElement) PathIterator(yield func(string) bool) {
	for _, sub := range p.subs {
		sub.PathIterator(yield)
	}
	if !yield(strings.TrimPrefix(p.String(), "/")) {
		return
	}
}

func (p *pathElement) FindBasename(re *regexp.Regexp) func(func(string) bool) {
	return func(yield func(string) bool) {
		for _, sub := range p.subs {
			sub.FindBasename(re)(yield)
		}
		if !p.dir && re.MatchString(p.name) {
			if !yield(strings.TrimPrefix(p.String(), "/")) {
				return
			}
		}
	}
}

func (p *pathElement) FindDirname(re *regexp.Regexp) func(func(string) bool) {
	return func(yield func(string) bool) {
		if p.dir {
			if re.MatchString(p.name) {
				if !yield(strings.TrimPrefix(p.String(), "/")) {
					return
				}
				return
			}
		}
		for _, sub := range p.subs {
			sub.FindDirname(re)(yield)
		}
	}
}

func BuildPath(fsys fs.FS) (*pathElement, error) {
	root := NewPathElement("", true, nil)
	if err := fs.WalkDir(fsys, ".", func(pathStr string, d fs.DirEntry, err error) error {
		if err != nil {
			return errors.Wrapf(err, "cannot walk %s/%s", fsys, pathStr)
		}
		pathStr = path.Clean(filepath.ToSlash(pathStr))
		pathParts := strings.Split(pathStr, "/")
		curr := root
		for _, pathPart := range pathParts {
			if pathPart == "." || pathPart == "" {
				continue
			}
			curr = curr.AddSub(pathPart, d.IsDir())
		}
		if d.IsDir() {
			//fmt.Printf("[d] %s/%s\n", fsys, pathStr)
			return nil
		}

		return nil
	}); err != nil {
		return nil, errors.Wrapf(err, "cannot walkd folder %v", fsys)
	}
	return root, nil
}
