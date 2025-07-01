// cmd/simulate_medical_dims.go
package cmd

import (
	"fmt"
	"math/rand"
	"time"

	gf "github.com/brianvoe/gofakeit/v6"
)

// generatePatients creates a slice of Patient structs.
func generatePatients(count int) []Patient {
	if count <= 0 {
		return []Patient{}
	}
	patients := make([]Patient, count)
	for i := 0; i < count; i++ {
		dob := gf.DateRange(time.Date(1940, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC))
		patients[i] = Patient{
			PatientID:   i + 1,
			PatientName: gf.Name(),
			DateOfBirth: dob,
			Gender:      gf.Gender(),
		}
	}
	return patients
}

// generateDoctors creates a slice of Doctor structs.
func generateDoctors(count int) []Doctor {
	if count <= 0 {
		return []Doctor{}
	}
	doctors := make([]Doctor, count)
	specializations := []string{"Cardiology", "Neurology", "Pediatrics", "General Practice", "Oncology"}
	for i := 0; i < count; i++ {
		doctors[i] = Doctor{
			DoctorID:       i + 1,
			DoctorName:     fmt.Sprintf("Dr. %s", gf.Name()),
			Specialization: specializations[rand.Intn(len(specializations))],
		}
	}
	return doctors
}

// generateClinics creates a slice of Clinic structs.
func generateClinics(count int) []Clinic {
	if count <= 0 {
		return []Clinic{}
	}
	clinics := make([]Clinic, count)
	for i := 0; i < count; i++ {
		addr := gf.Address()
		clinics[i] = Clinic{
			ClinicID:   i + 1,
			ClinicName: fmt.Sprintf("%s Clinic", gf.LastName()),
			Address:    fmt.Sprintf("%s, %s, %s %s", addr.Address, addr.City, addr.State, addr.Zip),
		}
	}
	return clinics
}
