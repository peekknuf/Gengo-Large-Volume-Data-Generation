package medical

import (
	"fmt"
	"math/rand"
	"time"

	gf "github.com/brianvoe/gofakeit/v6"

	"github.com/peekknuf/Gengo/internal/models/medical"
)

func GeneratePatients(count int) []medical.Patient {
	if count <= 0 {
		return []medical.Patient{}
	}
	patients := make([]medical.Patient, count)
	for i := 0; i < count; i++ {
		dob := gf.DateRange(time.Date(1940, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC))
		patients[i] = medical.Patient{
			PatientID:   i + 1,
			PatientName: gf.Name(),
			DateOfBirth: dob,
			Gender:      gf.Gender(),
		}
	}
	return patients
}

func GenerateDoctors(count int) []medical.Doctor {
	if count <= 0 {
		return []medical.Doctor{}
	}
	doctors := make([]medical.Doctor, count)
	specializations := []string{"Cardiology", "Neurology", "Pediatrics", "General Practice", "Oncology"}
	for i := 0; i < count; i++ {
		doctors[i] = medical.Doctor{
			DoctorID:       i + 1,
			DoctorName:     fmt.Sprintf("Dr. %s", gf.Name()),
			Specialization: specializations[rand.Intn(len(specializations))],
		}
	}
	return doctors
}

func GenerateClinics(count int) []medical.Clinic {
	if count <= 0 {
		return []medical.Clinic{}
	}
	clinics := make([]medical.Clinic, count)
	for i := 0; i < count; i++ {
		addr := gf.Address()
		clinics[i] = medical.Clinic{
			ClinicID:   i + 1,
			ClinicName: fmt.Sprintf("%s Clinic", gf.LastName()),
			Address:    fmt.Sprintf("%s, %s, %s %s", addr.Address, addr.City, addr.State, addr.Zip),
		}
	}
	return clinics
}
