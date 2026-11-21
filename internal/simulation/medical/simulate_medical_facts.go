package medical

import (
	"context"
	"fmt"
	"time"

	"github.com/peekknuf/Gengo/internal/formats"
	"github.com/peekknuf/Gengo/internal/generation"
	"github.com/peekknuf/Gengo/internal/models/medical"
)

func generateAppointmentsChunk(startID, count int, patients []medical.Patient, doctors []medical.Doctor, clinics []medical.Clinic, cfg generation.Config) []medical.Appointment {
	appointments := make([]medical.Appointment, count)
	diagnoses := []string{"Common Cold", "Hypertension", "Diabetes", "Routine Check-up", "Injury"}
	rng := cfg.Rand("medical.appointments", uint64(startID-1))
	start := cfg.AsOf.AddDate(-5, 0, 0).UnixMicro()
	span := cfg.AsOf.AddDate(0, 0, 1).UnixMicro() - start
	for i := range appointments {
		appointments[i] = medical.Appointment{
			AppointmentID:   int64(startID + i),
			PatientID:       patients[rng.Intn(len(patients))].PatientID,
			DoctorID:        doctors[rng.Intn(len(doctors))].DoctorID,
			ClinicID:        clinics[rng.Intn(len(clinics))].ClinicID,
			AppointmentDate: time.UnixMicro(start + rng.Int63n(span)).UTC(),
			Diagnosis:       diagnoses[rng.Intn(len(diagnoses))],
		}
	}
	return appointments
}

type MedicalRowCounts struct{ Patients, Doctors, Clinics, Appointments int }

func GenerateMedicalModelData(counts MedicalRowCounts, patients []medical.Patient, doctors []medical.Doctor, clinics []medical.Clinic, format, outputDir string, options ...generation.Config) error {
	cfg := generation.Options(options)
	return generation.Partitions(context.Background(), counts.Appointments, cfg, func(ctx context.Context, p generation.Partition) error {
		rows := generateAppointmentsChunk(p.Start+1, p.Count, patients, doctors, clinics, cfg)
		if err := ctx.Err(); err != nil {
			return err
		}
		name := fmt.Sprintf("fact_appointments_%d", p.Index)
		if err := formats.WriteSliceData(rows, name, format, outputDir); err != nil {
			return err
		}
		return cfg.RecordFile(name+"."+format, len(rows), rows[0].AppointmentID, rows[len(rows)-1].AppointmentID)
	})
}
