package identifier

import "regexp"

// Predicate represents a boolean test for an entity of type T.
type Predicate[T any] func(item *T) bool

// And returns a composite predicate that represents the logical AND of the provided predicates.
func And[T any](preds ...Predicate[T]) Predicate[T] {
	return func(item *T) bool {
		for _, p := range preds {
			if !p(item) {
				return false
			}
		}
		return true
	}
}

// Or returns a composite predicate that represents the logical OR of the provided predicates.
func Or[T any](preds ...Predicate[T]) Predicate[T] {
	return func(item *T) bool {
		for _, p := range preds {
			if p(item) {
				return true
			}
		}
		return false
	}
}

// BuildFileFilter returns a Predicate for FileData evaluating standard filter flags:
// empty files (size == 0), duplicate status, regex match on basename, or all files if no filter is active.
func BuildFileFilter(empty, duplicates bool, regex *regexp.Regexp) Predicate[FileData] {
	return func(fData *FileData) bool {
		if fData == nil || fData.Basename == "" || fData.Indexer == nil {
			return false
		}
		if empty && fData.Size == 0 {
			return true
		}
		if duplicates && fData.Duplicate {
			return true
		}
		if regex != nil && regex.MatchString(fData.Basename) {
			return true
		}
		if !empty && !duplicates && regex == nil {
			return true
		}
		return false
	}
}
