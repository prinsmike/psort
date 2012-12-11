// Copyright 2012 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// The psort package implements sorting in parallel to take advantage of multi-core CPUs.
package main

import (
	"runtime"
	"sort"
)

// Do getters and conversions
// ahead of time if possible
// to reduce runtime
var (
	cpus  = runtime.NumCPU()
	fcpus = float32(runtime.NumCPU())
)

// Sort breaks list into one sublist per logical
// CPU core, sorts them in parallel, and then
// merges them in linear time and space.
func Sort(list sort.Interface) {
	length := list.Len()

	if length < 1024 || cpus == 1 {
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
		go par(i, listSize, list, report)
	}

	i := 0

	// One extra sort routine for cases 
	// where (length % listSize) != 0
	if numLists*listSize < length {
		ptrs = append(ptrs, numLists*listSize)
		go func() {
			sort.Sort(&sortable{list, numLists * listSize, length - (numLists * listSize)})
			report <- struct{}{}
		}()
		// The for loop has to wait for 
		// one extra goroutine to halt
		numLists++
	}

	// Wait for all goroutines to finish
	for ; i < numLists; _, i = <-report, i+1 {
	}

	// fmt.Println(list)

	// Merge list segments, storing the
	// order of their values rather than
	// the values themselves
	result := make([]int, length)
	for i := 0; i < length; i++ {
		j := 0
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

		for ; j < numLists; j++ {
			if ptrs[j] != -1 && list.Less(ptrs[j], smallVal) {
				smallVal = ptrs[j]
				smallInd = j
			}
		}
		result[smallVal] = i

		// If the pointer has already incremented
		// through its entire list segment
		if smallVal == (smallInd+1)*listSize-1 || smallVal == length-1 {
			ptrs[smallInd] = -1
		} else {
			ptrs[smallInd]++
		}
	}

	// Put the list in the order determined above
	for i := 0; i != length; i++ {
		for result[i] != i {
			tmp := result[i]
			list.Swap(i, tmp)
			result[i] = result[tmp]
			result[tmp] = tmp
		}
	}

}

func par(i, listSize int, list sort.Interface, report chan struct{}) {
	sort.Sort(&sortable{list, i * listSize, listSize})
	report <- struct{}{}
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
