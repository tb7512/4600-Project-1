package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
)

func main() {
	// CLI args
	f, closeFile, err := openProcessingFile(os.Args...)
	if err != nil {
		log.Fatal(err)
	}
	defer closeFile()

	// Load and parse processes
	processes, err := loadProcesses(f)
	if err != nil {
		log.Fatal(err)
	}

	FCFSSchedule(os.Stdout, "First-come, first-serve", processes)
	SJFSchedule(os.Stdout, "Shortest-job-first", processes)
	SJFPrioritySchedule(os.Stdout, "Priority", processes)
	RRSchedule(os.Stdout, "Round-robin", processes)
}

func openProcessingFile(args ...string) (*os.File, func(), error) {
	if len(args) != 2 {
		return nil, nil, fmt.Errorf("%w: must give a scheduling file to process", ErrInvalidArgs)
	}
	// Read in CSV process CSV file
	f, err := os.Open(args[1])
	if err != nil {
		return nil, nil, fmt.Errorf("%v: error opening scheduling file", err)
	}
	closeFn := func() {
		if err := f.Close(); err != nil {
			log.Fatalf("%v: error closing scheduling file", err)
		}
	}

	return f, closeFn, nil
}

type (
	Process struct {
		ProcessID     int64
		ArrivalTime   int64
		BurstDuration int64
		Priority      int64
	}
	TimeSlice struct {
		PID   int64
		Start int64
		Stop  int64
	}
)

//region Schedulers

// FCFSSchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
// • an output writer
// • a title for the chart
// • a slice of processes
func FCFSSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)
	for i := range processes {
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime
		}
		totalWait += float64(waitingTime)

		start := waitingTime + processes[i].ArrivalTime

		turnaround := processes[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += processes[i].BurstDuration

		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

// SJFPrioritySchedule outputs a schedule of processes in a GANTT chart and a table of timing given where the process with the highest priority is completed first:
// • an output writer
// • a title for the chart
// • a slice of processes
func SJFPrioritySchedule(w io.Writer, title string, processes []Process) {
	var burst []int64 = make([]int64, 0)
	var priorites []int64 = make([]int64, 0)
	var startTime int64 = 0
	var processIndex int64 = 0
	var totalWait float64 = 0
	var totalTurnaround float64 = 0
	var finalCompleteion float64 = 0
	done := 0
	var time int64 = 0
	amountToComplete := len(processes)
	var schedule = make([][]string, len(processes))
	var gantt = make([]TimeSlice, 0)
	tempProcessIndex := 0
	var newBurst int64 = 0

	for i := 0; i < amountToComplete; i++ { //create a second array to keep track of modified burst times and arrived priorities but initialize it with an unrealistic number to overrite when process arrives
		burst = append(burst, -10)
		priorites = append(priorites, -10)
	}

	for done != amountToComplete { //used this as my time function, each itteration of the 'while' incremented item
		for i := range processes {
			if (processes[i].ArrivalTime <= time) && (burst[i] == -10) { //only populate the second burst and priority array if a new process arrived
				burst[i] = processes[i].BurstDuration
				priorites[i] = processes[i].Priority
			}
		}
		for i := range processes { //check each process priorities to see if one arrived that has a higher (lower number) priority
			if tempProcessIndex > 0 {
				if (priorites[tempProcessIndex] > priorites[i]) && (burst[i] > 0) {
					newBurst = burst[i]
					tempProcessIndex = i
				}
			} else if (priorites[i] != -10) && (int64(i) != processIndex) && (burst[i] > 0) && (priorites[processIndex] > priorites[i]) {
				newBurst = burst[i]
				tempProcessIndex = i
			}
		}
		if newBurst > 0 { //continuation of checking the priority array, if statement is only true if there is a new process with a higher priority
			if time > 0 { //only append this gantt if a process has started already, only works if a process arrives at 0 :)
				gantt = append(gantt, TimeSlice{
					PID:   processes[processIndex].ProcessID,
					Start: startTime,
					Stop:  time,
				})
			}
			processIndex = int64(tempProcessIndex)
			startTime = time
		}

		if burst[processIndex] == 0 { //when the process burst has finished push the gantt table and add to the scheduler and then find the next highest priority to start on
			finalCompleteion = float64(processes[processIndex].BurstDuration + processes[processIndex].ArrivalTime + (time - (processes[processIndex].ArrivalTime + processes[processIndex].BurstDuration)))
			schedule[done] = []string{
				fmt.Sprint(processes[processIndex].ProcessID),
				fmt.Sprint(processes[processIndex].Priority),
				fmt.Sprint(processes[processIndex].BurstDuration),
				fmt.Sprint(processes[processIndex].ArrivalTime),
				fmt.Sprint(time - (processes[processIndex].ArrivalTime + processes[processIndex].BurstDuration)), //waiting
				fmt.Sprint(time - processes[processIndex].ArrivalTime),                                           //turnaround
				fmt.Sprint(time), //total time
			}
			totalWait += float64(time - (processes[processIndex].ArrivalTime + processes[processIndex].BurstDuration))
			totalTurnaround += float64(time - processes[processIndex].ArrivalTime)

			gantt = append(gantt, TimeSlice{
				PID:   processes[processIndex].ProcessID,
				Start: startTime,
				Stop:  time,
			})
			done++
			tempProcessIndex = 0
			newBurst = 0
			if !(done == amountToComplete) { //dont select a new process if all process are done (causes a seggie :D )
				for i := range processes {
					if tempProcessIndex > 0 {
						if (priorites[tempProcessIndex] > priorites[i]) && (burst[i] > 0) { //TODO : Something in this loop doesnt pick the next highest priority
							tempProcessIndex = i
						}
					} else if (priorites[i] != -10) && (burst[i] != 0) {
						tempProcessIndex = i
					}
				}
				processIndex = int64(tempProcessIndex)
				startTime = time
			}
		}
		burst[processIndex]--
		tempProcessIndex = 0
		newBurst = 0
		time++
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / finalCompleteion
	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

// SJFSchedule outputs a schedule of processes based on which one has the shortest bust time, in a GANTT chart and a table of timing given:
// • an output writer
// • a title for the chart
// • a slice of processes
func SJFSchedule(w io.Writer, title string, processes []Process) {
	var burst []int64 = make([]int64, 0)
	var startTime int64 = 0
	var processIndex int64 = 0
	var totalWait float64 = 0
	var totalTurnaround float64 = 0
	var finalCompleteion float64 = 0
	done := 0
	var time int64 = 0
	amountToComplete := len(processes)
	var schedule = make([][]string, len(processes))
	var gantt = make([]TimeSlice, 0)
	tempProcessIndex := -10
	var newBurst int64 = -10

	for i := 0; i < amountToComplete; i++ { //create a second array to keep track of modified burst times but initialize it with an unrealistic number to overrite when process arrives
		burst = append(burst, -10)
	}

	for done != amountToComplete { //used this as my time function, each itteration of the 'while' incremented item
		for i := range processes {
			if (processes[i].ArrivalTime <= time) && (burst[i] == -10) { //only populate the second burst array if a new process arrived
				burst[i] = processes[i].BurstDuration
			}
		}

		for i := range processes { //check each process burst time to see if a new one should be started
			if (burst[processIndex] > processes[i].BurstDuration) && (burst[i] != 0) {
				if (newBurst != -10) && (newBurst > burst[i]) { //if there are multple bursts that can be done at the same time, find the shortest
					newBurst = burst[i]
					tempProcessIndex = i
				} else if (newBurst == -10) && (int64(i) != processIndex) { //first new burst found that isnt the current burst
					newBurst = burst[i]
					tempProcessIndex = i
				}
			}
		}
		if newBurst != -10 { //continuation of checking the burst array for a new one with lower, if one is found that is shorter, start that process and append the old ones gantt table
			if time > 0 { //only append this gantt if a process has started already, only works if a process arrives at 0 :)
				gantt = append(gantt, TimeSlice{
					PID:   processes[processIndex].ProcessID,
					Start: startTime,
					Stop:  time,
				})
			}
			processIndex = int64(tempProcessIndex)
			startTime = time
		}

		if burst[processIndex] == 0 { //when the process burst has finished push the gantt table and add to the scheduler and then add a new process to the queue to be worked on
			finalCompleteion = float64(processes[processIndex].BurstDuration + processes[processIndex].ArrivalTime + (time - (processes[processIndex].ArrivalTime + processes[processIndex].BurstDuration)))
			schedule[done] = []string{
				fmt.Sprint(processes[processIndex].ProcessID),
				fmt.Sprint(processes[processIndex].Priority),
				fmt.Sprint(processes[processIndex].BurstDuration),
				fmt.Sprint(processes[processIndex].ArrivalTime),
				fmt.Sprint(time - (processes[processIndex].ArrivalTime + processes[processIndex].BurstDuration)), //waiting
				fmt.Sprint(time - processes[processIndex].ArrivalTime),                                           //turnaround
				fmt.Sprint(time), //total time
			}
			totalWait += float64(time - (processes[processIndex].ArrivalTime + processes[processIndex].BurstDuration))
			totalTurnaround += float64(time - processes[processIndex].ArrivalTime)

			gantt = append(gantt, TimeSlice{
				PID:   processes[processIndex].ProcessID,
				Start: startTime,
				Stop:  time,
			})
			done++
			tempProcessIndex = -10
			newBurst = -10
			if !(done == amountToComplete) { //dont select a new process if all process are done (causes a seggie :D )
				for i := range processes {
					if (burst[i] > 0) && (newBurst > burst[i]) {
						newBurst = burst[i]
						tempProcessIndex = i
					} else if burst[i] > 0 {
						newBurst = burst[i]
						tempProcessIndex = i
					}
				}
				processIndex = int64(tempProcessIndex)
				startTime = time
			}
		}
		burst[processIndex]--
		tempProcessIndex = -10
		newBurst = -10
		time++
	}
	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / finalCompleteion
	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func RRSchedule(w io.Writer, title string, processes []Process) {
	const timeQuantum int64 = 2
	var processTime int64 = 0
	var burst []int64 = make([]int64, 0)
	var startTime int64 = 0
	var processIndex int64 = 0
	var totalWait float64 = 0
	var totalTurnaround float64 = 0
	var finalCompleteion float64 = 0
	done := 0
	var time int64 = 0
	amountToComplete := len(processes)
	var schedule = make([][]string, len(processes))
	var gantt = make([]TimeSlice, 0)
	tempProcessIndex := 0

	for i := 0; i < amountToComplete; i++ { //create a second array to keep track of modified burst times and arrived priorities but initialize it with an unrealistic number to overrite when process arrives
		burst = append(burst, -10)
	}

	for done != amountToComplete {
		for i := range processes {
			if (processes[i].ArrivalTime <= time) && (burst[i] == -10) { //only populate the second burst and priority array if a new process arrived
				burst[i] = processes[i].BurstDuration
			}
		}

		if burst[processIndex] == 0 { //if remaining process burst time is 0, schedule the process and mark is as complete. pick the next process
			finalCompleteion = float64(processes[processIndex].BurstDuration + processes[processIndex].ArrivalTime + (time - (processes[processIndex].ArrivalTime + processes[processIndex].BurstDuration)))
			schedule[done] = []string{
				fmt.Sprint(processes[processIndex].ProcessID),
				fmt.Sprint(processes[processIndex].Priority),
				fmt.Sprint(processes[processIndex].BurstDuration),
				fmt.Sprint(processes[processIndex].ArrivalTime),
				fmt.Sprint(time - (processes[processIndex].ArrivalTime + processes[processIndex].BurstDuration)), //waiting
				fmt.Sprint(time - processes[processIndex].ArrivalTime),                                           //turnaround
				fmt.Sprint(time), //total time
			}
			totalWait += float64(time - (processes[processIndex].ArrivalTime + processes[processIndex].BurstDuration))
			totalTurnaround += float64(time - processes[processIndex].ArrivalTime)

			gantt = append(gantt, TimeSlice{
				PID:   processes[processIndex].ProcessID,
				Start: startTime,
				Stop:  time,
			})
			done++
			tempProcessIndex = 0
			if !(done == amountToComplete) { //dont select a new process if all process are done (causes a seggie :D )
				for i := range processes {
					if tempProcessIndex > 0 { //TODO : Select next process that hasnt finished, based on arrival time
						if (processes[i].ArrivalTime > processes[tempProcessIndex].ArrivalTime) && (burst[i] > 0) && (i != int(processIndex)) {
							tempProcessIndex = i
						}
					} else if (burst[i] > 0) && (i != int(processIndex)) { // just pick an arrived process
						tempProcessIndex = i
					}
				}
				processTime = 0
				processIndex = int64(tempProcessIndex)
				startTime = time
			}
		}

		tempProcessIndex = 0
		if processTime >= timeQuantum { //if the time quantum has elapsed, pick next process if the current process isnt the only process
			for i := 0; i < len(processes); i++ {
				if i+int(processIndex) < amountToComplete { //if the next index to check is not out of bounds of array
					if (burst[processIndex+int64(i)] > 0) && (i != 0) { //if the next index to check hasnt been completed and the index isnt the same as the current process
						gantt = append(gantt, TimeSlice{
							PID:   processes[processIndex].ProcessID,
							Start: startTime,
							Stop:  time,
						})
						if i == 0 {
							processIndex++
							startTime = time
							i = len(processes)
						} else {
							processIndex += int64(i)
							startTime = time
							i = len(processes)
						}
					}
				} else {
					if burst[tempProcessIndex] > 0 { //if the next index to check starts from the beginning of the array and the process hasnt been finished
						gantt = append(gantt, TimeSlice{
							PID:   processes[processIndex].ProcessID,
							Start: startTime,
							Stop:  time,
						})
						processIndex = int64(tempProcessIndex)
						startTime = time
						i = len(processes)
					}
					tempProcessIndex++
				}
			}
			processTime = 0
		}
		burst[processIndex]--
		tempProcessIndex = 0
		time++
		processTime++
	}
	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / finalCompleteion
	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

//region Output helpers

func outputTitle(w io.Writer, title string) {
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
	_, _ = fmt.Fprintln(w, strings.Repeat(" ", len(title)/2), title)
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
}

func outputGantt(w io.Writer, gantt []TimeSlice) {
	_, _ = fmt.Fprintln(w, "Gantt schedule")
	_, _ = fmt.Fprint(w, "|")
	for i := range gantt {
		pid := fmt.Sprint(gantt[i].PID)
		padding := strings.Repeat(" ", (8-len(pid))/2)
		_, _ = fmt.Fprint(w, padding, pid, padding, "|")
	}
	_, _ = fmt.Fprintln(w)
	for i := range gantt {
		_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Start), "\t")
		if len(gantt)-1 == i {
			_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Stop))
		}
	}
	_, _ = fmt.Fprintf(w, "\n\n")
}

func outputSchedule(w io.Writer, rows [][]string, wait, turnaround, throughput float64) {
	_, _ = fmt.Fprintln(w, "Schedule table")
	table := tablewriter.NewWriter(w)
	table.SetHeader([]string{"ID", "Priority", "Burst", "Arrival", "Wait", "Turnaround", "Exit"})
	table.AppendBulk(rows)
	table.SetFooter([]string{"", "", "", "",
		fmt.Sprintf("Average\n%.2f", wait),
		fmt.Sprintf("Average\n%.2f", turnaround),
		fmt.Sprintf("Throughput\n%.2f/t", throughput)})
	table.Render()
}

//endregion

//region Loading processes.

var ErrInvalidArgs = errors.New("invalid args")

func loadProcesses(r io.Reader) ([]Process, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%w: reading CSV", err)
	}

	processes := make([]Process, len(rows))
	for i := range rows {
		processes[i].ProcessID = mustStrToInt(rows[i][0])
		processes[i].BurstDuration = mustStrToInt(rows[i][1])
		processes[i].ArrivalTime = mustStrToInt(rows[i][2])
		if len(rows[i]) == 4 {
			processes[i].Priority = mustStrToInt(rows[i][3])
		}
	}

	return processes, nil
}

func mustStrToInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	return i
}

//endregion
