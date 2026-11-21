package medical

import (
	"fmt"
	"time"

	"github.com/peekknuf/Gengo/internal/generation"
	"github.com/peekknuf/Gengo/internal/models/medical"
)

func GeneratePatients(count int, options ...generation.Config) []medical.Patient {
	cfg := generation.Options(options)
	fake := cfg.Faker("medical.GeneratePatients")
	rng := cfg.Rand("medical.patient_dates", 0)
	start := cfg.AsOf.AddDate(-86, 0, 0).UnixMicro()
	span := cfg.AsOf.AddDate(-8, 0, 0).UnixMicro() - start
	if count <= 0 {
		return []medical.Patient{}
	}
	patients := make([]medical.Patient, count)
	for i := 0; i < count; i++ {
		dob := time.UnixMicro(start + rng.Int63n(span)).UTC()
		patients[i] = medical.Patient{
			PatientID:   i + 1,
			PatientName: fake.Name(),
			DateOfBirth: dob.UTC().Truncate(time.Microsecond),
			Gender:      fake.Gender(),
		}
	}
	return patients
}

func GenerateDoctors(count int, options ...generation.Config) []medical.Doctor {
	cfg := generation.Options(options)
	rng := cfg.Rand("medical.GenerateDoctors", 0)
	fake := cfg.Faker("medical.GenerateDoctors")
	if count <= 0 {
		return []medical.Doctor{}
	}
	doctors := make([]medical.Doctor, count)
	specializations := []string{"Cardiology", "Neurology", "Pediatrics", "General Practice", "Oncology"}
	for i := 0; i < count; i++ {
		doctors[i] = medical.Doctor{
			DoctorID:       i + 1,
			DoctorName:     fmt.Sprintf("Dr. %s", fake.Name()),
			Specialization: specializations[rng.Intn(len(specializations))],
		}
	}
	return doctors
}

func GenerateClinics(count int, options ...generation.Config) []medical.Clinic {
	cfg := generation.Options(options)
	fake := cfg.Faker("medical.GenerateClinics")
	if count <= 0 {
		return []medical.Clinic{}
	}
	clinics := make([]medical.Clinic, count)
	for i := 0; i < count; i++ {
		addr := fake.Address()
		clinics[i] = medical.Clinic{
			ClinicID:   i + 1,
			ClinicName: fmt.Sprintf("%s Clinic", fake.LastName()),
			Address:    addr.Address,
		}
	}
	return clinics
}
