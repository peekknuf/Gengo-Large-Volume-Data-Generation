package medical

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"time"

	gf "github.com/brianvoe/gofakeit/v6"
	"github.com/peekknuf/Gengo/internal/formats"
	"github.com/peekknuf/Gengo/internal/models/medical"
)

// generateAppointmentsChunk is a worker function that generates a chunk of appointments.
func generateAppointmentsChunk(startID, count int, patients []medical.Patient, doctors []medical.Doctor, clinics []medical.Clinic) []medical.Appointment {
	if count <= 0 || len(patients) == 0 || len(doctors) == 0 || len(clinics) == 0 {
		return []medical.Appointment{}
	}
	appointments := make([]medical.Appointment, count)
	diagnoses := []string{"Common Cold", "Hypertension", "Diabetes", "Routine Check-up", "Injury"}

	for i := 0; i < count; i++ {
		patient := patients[rand.Intn(len(patients))]
		doctor := doctors[rand.Intn(len(doctors))]
		clinic := clinics[rand.Intn(len(clinics))]
		appDate := gf.DateRange(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), time.Now())

		appointments[i] = medical.Appointment{
			AppointmentID:   int64(startID + i),
			PatientID:       patient.PatientID,
			DoctorID:        doctor.DoctorID,
			ClinicID:        clinic.ClinicID,
			AppointmentDate: appDate,
			Diagnosis:       diagnoses[rand.Intn(len(diagnoses))],
		}
	}
	return appointments
}

// generateAppointmentsConcurrently generates the appointment fact data in parallel.
func generateAppointmentsConcurrently(count int, patients []medical.Patient, doctors []medical.Doctor, clinics []medical.Clinic) []medical.Appointment {
	if count <= 0 {
		return []medical.Appointment{}
	}

	numWorkers := runtime.NumCPU()
	appointmentsPerWorker := (count + numWorkers - 1) / numWorkers

	var wg sync.WaitGroup
	resultsChan := make(chan []medical.Appointment, numWorkers)

	for i := 0; i < numWorkers; i++ {
		startID := (i * appointmentsPerWorker) + 1
		numToGen := appointmentsPerWorker

		if startID+numToGen > count+1 {
			numToGen = count - startID + 1
		}

		if numToGen > 0 {
			wg.Add(1)
			go func(sID, c int) {
				defer wg.Done()
				resultsChan <- generateAppointmentsChunk(sID, c, patients, doctors, clinics)
			}(startID, numToGen)
		}
	}

	wg.Wait()
	close(resultsChan)

	finalAppointments := make([]medical.Appointment, 0, count)
	for result := range resultsChan {
		finalAppointments = append(finalAppointments, result...)
	}

	return finalAppointments
}

type MedicalRowCounts struct {
	Patients     int
	Doctors      int
	Clinics      int
	Appointments int
}

func GenerateMedicalModelData(counts MedicalRowCounts, patients []medical.Patient, doctors []medical.Doctor, clinics []medical.Clinic, format string, outputDir string) error {
	// Generate and write appointments
	appointments := generateAppointmentsConcurrently(counts.Appointments, patients, doctors, clinics)
	if err := formats.WriteSliceData(appointments, "fact_appointments", format, outputDir); err != nil {
		return fmt.Errorf("error generating appointments: %w", err)
	}

	return nil
}
