// The psort package implements sorting in parallel to take advantage of multi-core CPUs.
package psort

import (
	"runtime"
	"sort"
)

var (
	cpus  = runtime.NumCPU()
	fcpus = float32(runtime.NumCPU())
)

// Sort breaks list into one sublist per logical
// CPU core, sorts them in parallel, and then
// merges them in linear time and space.
func Sort(list sort.Interface) {
	length := list.Len()

	if cpus == 1 {
		// s(list)
		sort.Sort(list)
		return
	}

	listSize := round32(float32(length) / fcpus)
	numLists := length / listSize
	report := make(chan struct{})

	// Pointers to the beginning of each list;
	// initialize them now to avoid using
	// a redundant for loop. Make the slice
	// available for resizing for edge cases
	// (see if statement below)
	ptrs := make([]int, numLists, numLists+1)

	for i := 0; i < numLists; i++ {
		ptrs[i] = i * listSize
		go func() {
			sort.Sort(&sortable{list, i * listSize, (i + 1) * listSize})
			report <- struct{}{}
		}()
	}

	i := 0

	// One extra sort routine for cases 
	// where (length % listSize) != 0
	if numLists*listSize < length {
		// // The for loop has to wait for 
		// // one extra goroutine to halt
		// i = -1
		numLists++
		ptrs = append(ptrs, numLists*listSize)
		go func() {
			sort.Sort(&sortable{list, numLists * listSize, length - 1})
			report <- struct{}{}
		}()
	}

	// Wait for all goroutines to finish
	for ; i < numLists; _, i = <-report, i+1 {
	}


	// Merge list segments, storing the
	// order of their values rather than
	// the values themselves
	result := make([]int, length)
	for i := 0; i < length; i++ {
		j := 1
		var smallInd int
		var smallVal int
		
		// Find first non-exhausted
		// list segment
		for ; ; j++ {
			if ptrs[j] != -1 {
				smallInd = j
				smallVal = ptrs[j]
				break
			}
		}
		for ; j < listSize; j++ {
			if ptrs[j] != -1 && list.Less(ptrs[j], smallVal) {
				smallVal = ptrs[j]
				smallInd = j
			}
		}
		result[i] = ptrs[j]
		
		// If the pointer has already incremented
		// through its entire list segment
		if ptrs[j] == (i + 1) * listSize - 1 {
			ptrs[j] = -1
		} else {
			ptrs[j]++
		}
	}
	
	// Put the list in the order determined above
	
}

func round32(f float32) int {
	intf := int(f)
	if f-float32(intf) < float32(0.5) {
		return intf
	}
	return intf + 1
}

type sortable struct {
	list   sort.Interface
	start  int
	length int
}

func (s *sortable) Len() int           { return s.length }
func (s *sortable) Less(i, j int) bool { return s.list.Less(i+s.start, j+s.start) }
func (s *sortable) Swap(i, j int)      { s.list.Swap(i+s.start, j+s.start) }
