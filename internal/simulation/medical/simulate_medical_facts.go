// cmd/simulate_medical_facts.go
package medical

import (
	"fmt"
	"math/rand"
	"time"

	gf "github.com/brianvoe/gofakeit/v6"
	"github.com/peekknuf/Gengo/internal/formats"
	"github.com/peekknuf/Gengo/internal/models/medical"
)

// generateAppointments creates a slice of Appointment structs.
func generateAppointments(count int, patients []medical.Patient, doctors []medical.Doctor, clinics []medical.Clinic) []medical.Appointment {
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

type MedicalRowCounts struct {
	Patients     int
	Doctors      int
	Clinics      int
	Appointments int
}

func GenerateMedicalModelData(counts MedicalRowCounts, patients []medical.Patient, doctors []medical.Doctor, clinics []medical.Clinic, format string, outputDir string) error {
	// Generate and write appointments
	appointments := generateAppointments(counts.Appointments, patients, doctors, clinics)
	if err := formats.WriteSliceData(appointments, "fact_appointments", format, outputDir); err != nil {
		return fmt.Errorf("error generating appointments: %w", err)
	}

	return nil
}