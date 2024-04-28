package main

import (
	"fmt"
	"github.com/dolthub/swiss"
	"os"
	"reflect"
	"syscall"
	"unsafe"
)

func main() {
	//// Create a CPU profile file
	//cpuProfileFile, err := os.Create("cpu.prof")
	//if err != nil {
	//	panic(err)
	//}
	//defer cpuProfileFile.Close()
	//
	//// Start CPU profiling
	//if err := pprof.StartCPUProfile(cpuProfileFile); err != nil {
	//	panic(err)
	//}
	//defer pprof.StopCPUProfile()
	// Read the entire file in memory at an offset of 0 => the entire thing
	f, _ := os.Open("/home/nander/1brc/measurements2.txt")
	//f, _ := os.Open("measurements.csv")
	fi, _ := f.Stat()
	size := fi.Size()
	p, _ := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)

	// Hardcode the number of jobs and start them up
	nThreads := 16
	jobs := jobCreater(p, nThreads)
	outputChannel := make(chan *swiss.Map[string, *result])
	for i := 0; i < nThreads; i++ {
		go runJob(jobs[i], p, outputChannel)
	}

	// Wait for the results and group them
	res := make(map[string]result)
	for i := 0; i < nThreads; i++ {
		o := <-outputChannel
		o.Iter(func(k string, v *result) bool {
			oth, ok := res[k]
			if !ok {
				res[k] = *v
				return false
			}
			oth.min = min(oth.min, v.min)
			oth.max = max(oth.max, v.max)
			oth.total += v.total
			oth.count += v.count
			res[k] = oth
			return false
		})
	}

	// Now parse the results onto the output format
	for k, v := range res {
		fmt.Printf("{station:\"%s\", \"min\":%.1f, \"avg\":%.1f,  \"max\":%.1f}\n", k, float64(v.min)/10, float64(v.total/v.count)/10, float64(v.max)/10)
	}

}

// The jobInput struct describes
type jobInput struct {
	startIndex int
	endIndex   int
}

type result struct {
	min   int64
	max   int64
	total int64
	count int64
}

// Find start and endpoints for the tasks for the separate threads
// This way we ensure that the threads don't need to communicate about their workloads
// Observe that the (in my opinion, valid) assumption is made that the workload for handling each line is similar.
func jobCreater(p []byte, nJobs int) []jobInput {
	var start = 0
	jobs := make([]jobInput, 0)

	for i := 0; i < nJobs; i++ {
		x := (i + 1) * (len(p) / nJobs)
		for p[x] != '\n' {
			x++
		}
		jobs = append(jobs, jobInput{startIndex: start, endIndex: x})
		start = x + 1
	}
	return jobs
}

// The runJob function is responsible for solving part of the problem.
func runJob(j jobInput, p []byte, c chan *swiss.Map[string, *result]) {
	type entry struct {
		station []byte
		res     *result
	}
	const (
		nEntries = 1 << 17
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	o := swiss.NewMap[string, *result](100)
	v := p[j.startIndex:j.endIndex]
	startIndex := 0
	semicolonLoc := 0
	newLineLoc := 0
	buff := make([]entry, nEntries)
	for i, b := range v {
		if b == ';' {
			semicolonLoc = i
		} else if b == '\n' {
			newLineLoc = i
			townPart := v[startIndex:semicolonLoc]
			temperaturePart := v[semicolonLoc+1 : newLineLoc]
			var neg = 1
			if temperaturePart[0] == '-' {
				neg = -1
				temperaturePart = temperaturePart[1:]
			}

			var val int64
			if temperaturePart[0] == '.' {
				val = int64(neg) * int64(temperaturePart[1]-'0')
			}
			if temperaturePart[1] == '.' {
				val = int64(neg) * (10*int64(temperaturePart[0]-'0') + int64(temperaturePart[2]-'0'))
			}
			if temperaturePart[2] == '.' {
				val = int64(neg) * (100*int64(temperaturePart[0]-'0') + 10*int64(temperaturePart[1]-'0') + int64(temperaturePart[2]-'0'))
			}
			hash := uint64(offset64)
			for _, v := range townPart {
				hash ^= uint64(v)
				hash *= uint64(prime64)
			}
			hash16bit := hash & (nEntries - 1)

			ok := false
			for {
				if buff[hash16bit].station == nil {
					break
				}
				//tempHeader = (*reflect.SliceHeader)(unsafe.Pointer(&buff[hash16bit].station))
				//stringHeader = reflect.StringHeader{Data: tempHeader.Data, Len: tempHeader.Len}
				//tpn := *(*string)(unsafe.Pointer(&stringHeader))
				if townPart[0] == buff[hash16bit].station[0] {
					ok = true
					break
				}
				hash16bit++
			}
			if !ok {

				res := &result{min: val, max: val, total: val, count: 1}

				buff[hash16bit] = entry{station: townPart, res: res}
				tempHeader := (*reflect.SliceHeader)(unsafe.Pointer(&townPart))
				stringHeader := reflect.StringHeader{Data: tempHeader.Data, Len: tempHeader.Len}
				tp := *(*string)(unsafe.Pointer(&stringHeader))
				o.Put(tp, res)
			} else {
				res := buff[hash16bit].res
				res.min = min(res.min, val)
				res.max = max(res.max, val)
				res.total = res.total + val
				res.count = res.count + 1
			}

			startIndex = i + 1
		}
	}
	c <- o
}
