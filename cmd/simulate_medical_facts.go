// cmd/simulate_medical_facts.go
package cmd

import (
	"math/rand"
	"time"

	gf "github.com/brianvoe/gofakeit/v6"
)

// generateAppointments creates a slice of Appointment structs.
func generateAppointments(count int, patients []Patient, doctors []Doctor, clinics []Clinic) []Appointment {
	if count <= 0 || len(patients) == 0 || len(doctors) == 0 || len(clinics) == 0 {
		return []Appointment{}
	}
	appointments := make([]Appointment, count)
	diagnoses := []string{"Common Cold", "Hypertension", "Diabetes", "Routine Check-up", "Injury"}

	for i := 0; i < count; i++ {
		patient := patients[rand.Intn(len(patients))]
		doctor := doctors[rand.Intn(len(doctors))]
		clinic := clinics[rand.Intn(len(clinics))]
		appDate := gf.DateRange(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), time.Now())

		appointments[i] = Appointment{
			AppointmentID:   int64(i + 1),
			PatientID:       patient.PatientID,
			DoctorID:        doctor.DoctorID,
			ClinicID:        clinic.ClinicID,
			AppointmentDate: appDate,
			Diagnosis:       diagnoses[rand.Intn(len(diagnoses))],
		}
	}
	return appointments
}
