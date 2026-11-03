package exclude

import (
	"fmt"
	"path/filepath"
	"strings"
)

func RuleMatchMe(rule *Rule, path []string, isDir bool) (bool, bool, error) {
	var matched bool
	var err error

	if rule.Anchored {
		matched, err = rule.simpleMatch(path, isDir)
	} else {
		matched, err = rule.globMatch(path, isDir)
	}
	if err != nil {
		return false, false, err
	}
	if matched {
		return true, !rule.Negate, nil
	}
	return false, false, nil
}

func (rule *Rule) simpleMatchMe(path []string, isDir bool) (bool, error) {
	for i, name := range path {
		if match, err := filepath.Match(rule.Path[0], name); err != nil {
			return false, err
		} else if !match {
			continue
		}
		if rule.DirOnly && !isDir && i == len(path)-1 {
			return false, nil
		}
		return true, nil
	}
	return false, nil
}

func (rule *Rule) globMatchMe(path []string, isDir bool) (bool, error) {
	matched := false
	canTraverse := false
	last := len(rule.Path) - 1
	for i, pattern := range rule.Path {
		if pattern == "" {
			canTraverse = false
			continue
		}
		if pattern == "**" {
			if i == last {
				break
			}
			canTraverse = true
			continue
		}
		if strings.Contains(pattern, "**") {
			return false, fmt.Errorf("invalid pattern")
		}
		if len(path) == 0 {
			return false, nil
		}
		if canTraverse {
			canTraverse = false
			for len(path) > 0 {
				e := path[0]
				path = path[1:]
				if match, err := filepath.Match(pattern, e); err != nil {
					return false, err
				} else if match {
					matched = true
					break
				} else if len(path) == 0 {
					// if nothing left then fail
					matched = false
				}
			}
		} else {
			if match, err := filepath.Match(pattern, path[0]); err != nil || !match {
				return false, err
			}
			matched = true
			path = path[1:]
			// files matching dir globs, don't match
			if len(path) == 0 && i < last {
				matched = false
			}
		}
	}
	if matched && rule.DirOnly && !isDir && len(path) == 0 {
		matched = false
	}
	return matched, nil
}
