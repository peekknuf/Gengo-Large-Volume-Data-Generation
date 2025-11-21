package generators

import (
	"context"
	"fmt"
	"time"

	gf "github.com/brianvoe/gofakeit/v6"
	"github.com/peekknuf/Gengo/internal/common"
	"github.com/peekknuf/Gengo/internal/models/medical"
)

// MedicalGenerator implements DataGenerator interface for medical domain
type MedicalGenerator struct {
	*BaseGenerator
}

// NewMedicalGenerator creates a new medical domain generator
func NewMedicalGenerator(config common.GenerationConfig) *MedicalGenerator {
	return &MedicalGenerator{
		BaseGenerator: NewBaseGenerator(config),
	}
}

// GenerateDimensions generates all dimension data for medical domain
func (m *MedicalGenerator) GenerateDimensions(ctx context.Context, config common.GenerationConfig) ([]common.DimensionData, error) {
	var dimensions []common.DimensionData

	// Generate patients
	patients, err := m.generatePatients(ctx, 1000) // Default count, should come from sizing
	if err != nil {
		return nil, fmt.Errorf("failed to generate patients: %w", err)
	}
	dimensions = append(dimensions, common.DimensionData{
		Type:  "patients",
		Data:  patients,
		Count: len(patients),
	})

	// Generate doctors
	doctors, err := m.generateDoctors(ctx, 100) // Default count
	if err != nil {
		return nil, fmt.Errorf("failed to generate doctors: %w", err)
	}
	dimensions = append(dimensions, common.DimensionData{
		Type:  "doctors",
		Data:  doctors,
		Count: len(doctors),
	})

	// Generate clinics
	clinics, err := m.generateClinics(ctx, 50) // Default count
	if err != nil {
		return nil, fmt.Errorf("failed to generate clinics: %w", err)
	}
	dimensions = append(dimensions, common.DimensionData{
		Type:  "clinics",
		Data:  clinics,
		Count: len(clinics),
	})

	return dimensions, nil
}

// GenerateFacts generates all fact data for medical domain
func (m *MedicalGenerator) GenerateFacts(ctx context.Context, config common.GenerationConfig, dimensions []common.DimensionData) ([]common.FactData, error) {
	// Extract dimension data for efficient lookups
	patients := m.extractPatients(dimensions)
	doctors := m.extractDoctors(dimensions)
	clinics := m.extractClinics(dimensions)

	if len(patients) == 0 || len(doctors) == 0 || len(clinics) == 0 {
		return nil, fmt.Errorf("missing required dimension data")
	}

	// Generate appointments using common patterns
	appointments, err := m.generateAppointments(ctx, 5000, patients, doctors, clinics)
	if err != nil {
		return nil, fmt.Errorf("failed to generate appointments: %w", err)
	}

	return []common.FactData{
		{
			Type:   "appointments",
			Data:   appointments,
			Count:  len(appointments),
			Chunks: m.partitionAppointments(appointments),
		},
	}, nil
}

// generatePatients generates patient dimension data
func (m *MedicalGenerator) generatePatients(ctx context.Context, count int) ([]medical.Patient, error) {
	generatorFunc := func() interface{} {
		return medical.Patient{
			PatientID:   int(m.GetIDGenerator().NextID()),
			PatientName: gf.FirstName() + " " + gf.LastName(),
			DateOfBirth: gf.DateRange(time.Date(1950, time.January, 1, 0, 0, 0, time.UTC), time.Date(2005, time.January, 1, 0, 0, 0, time.UTC)),
			Gender:      gf.Gender(),
		}
	}

	data, err := m.GenerateDimensionData("patients", count, generatorFunc)
	if err != nil {
		return nil, err
	}

	return m.convertToPatients(data), nil
}

// generateDoctors generates doctor dimension data
func (m *MedicalGenerator) generateDoctors(ctx context.Context, count int) ([]medical.Doctor, error) {
	specialties := []string{"General Practice", "Cardiology", "Dermatology", "Pediatrics", "Orthopedics"}

	generatorFunc := func() interface{} {
		return medical.Doctor{
			DoctorID:       int(m.GetIDGenerator().NextID()),
			DoctorName:     gf.FirstName() + " " + gf.LastName(),
			Specialization: specialties[m.GetRNG().Intn(len(specialties))],
		}
	}

	data, err := m.GenerateDimensionData("doctors", count, generatorFunc)
	if err != nil {
		return nil, err
	}

	return m.convertToDoctors(data), nil
}

// generateClinics generates clinic dimension data
func (m *MedicalGenerator) generateClinics(ctx context.Context, count int) ([]medical.Clinic, error) {
	generatorFunc := func() interface{} {
		return medical.Clinic{
			ClinicID:   int(m.GetIDGenerator().NextID()),
			ClinicName: fmt.Sprintf("%s Medical Center", gf.Company()),
			Address:    fmt.Sprintf("%s, %s, %s %s", gf.Address(), gf.City(), gf.State(), gf.Zip()),
		}
	}

	data, err := m.GenerateDimensionData("clinics", count, generatorFunc)
	if err != nil {
		return nil, err
	}

	return m.convertToClinics(data), nil
}

// generateAppointments generates appointment fact data using common patterns
func (m *MedicalGenerator) generateAppointments(ctx context.Context, count int, patients []medical.Patient, doctors []medical.Doctor, clinics []medical.Clinic) ([]medical.Appointment, error) {
	diagnoses := []string{"Common Cold", "Hypertension", "Diabetes", "Routine Check-up", "Injury"}

	// Create weighted samplers for realistic distributions
	patientIDs := m.extractPatientIDs(patients)
	doctorIDs := m.extractDoctorIDs(doctors)
	clinicIDs := m.extractClinicIDs(clinics)

	patientSampler, _ := common.NewUnifiedWeightedSampler(patientIDs)
	doctorSampler, _ := common.NewUnifiedWeightedSampler(doctorIDs)
	clinicSampler, _ := common.NewUnifiedWeightedSampler(clinicIDs)

	generatorFunc := func() interface{} {
		return medical.Appointment{
			AppointmentID:   m.GetIDGenerator().NextID(),
			PatientID:       patientSampler.Sample(m.GetRNG()),
			DoctorID:        doctorSampler.Sample(m.GetRNG()),
			ClinicID:        clinicSampler.Sample(m.GetRNG()),
			AppointmentDate: gf.DateRange(time.Date(2020, 1, 1, 0, 0, 0, time.UTC), time.Now()),
			Diagnosis:       diagnoses[m.GetRNG().Intn(len(diagnoses))],
		}
	}

	data, err := m.GenerateDimensionData("appointments", count, generatorFunc)
	if err != nil {
		return nil, err
	}

	return m.convertToAppointments(data), nil
}

// Helper methods for data conversion and extraction

func (m *MedicalGenerator) convertToPatients(data []interface{}) []medical.Patient {
	patients := make([]medical.Patient, len(data))
	for i, item := range data {
		patients[i] = item.(medical.Patient)
	}
	return patients
}

func (m *MedicalGenerator) convertToDoctors(data []interface{}) []medical.Doctor {
	doctors := make([]medical.Doctor, len(data))
	for i, item := range data {
		doctors[i] = item.(medical.Doctor)
	}
	return doctors
}

func (m *MedicalGenerator) convertToClinics(data []interface{}) []medical.Clinic {
	clinics := make([]medical.Clinic, len(data))
	for i, item := range data {
		clinics[i] = item.(medical.Clinic)
	}
	return clinics
}

func (m *MedicalGenerator) convertToAppointments(data []interface{}) []medical.Appointment {
	appointments := make([]medical.Appointment, len(data))
	for i, item := range data {
		appointments[i] = item.(medical.Appointment)
	}
	return appointments
}

func (m *MedicalGenerator) extractPatients(dimensions []common.DimensionData) []medical.Patient {
	for _, dim := range dimensions {
		if dim.Type == "patients" {
			return m.convertToPatients(dim.Data.([]interface{}))
		}
	}
	return nil
}

func (m *MedicalGenerator) extractDoctors(dimensions []common.DimensionData) []medical.Doctor {
	for _, dim := range dimensions {
		if dim.Type == "doctors" {
			return m.convertToDoctors(dim.Data.([]interface{}))
		}
	}
	return nil
}

func (m *MedicalGenerator) extractClinics(dimensions []common.DimensionData) []medical.Clinic {
	for _, dim := range dimensions {
		if dim.Type == "clinics" {
			return m.convertToClinics(dim.Data.([]interface{}))
		}
	}
	return nil
}

func (m *MedicalGenerator) extractPatientIDs(patients []medical.Patient) []int {
	ids := make([]int, len(patients))
	for i, patient := range patients {
		ids[i] = patient.PatientID
	}
	return ids
}

func (m *MedicalGenerator) extractDoctorIDs(doctors []medical.Doctor) []int {
	ids := make([]int, len(doctors))
	for i, doctor := range doctors {
		ids[i] = doctor.DoctorID
	}
	return ids
}

func (m *MedicalGenerator) extractClinicIDs(clinics []medical.Clinic) []int {
	ids := make([]int, len(clinics))
	for i, clinic := range clinics {
		ids[i] = clinic.ClinicID
	}
	return ids
}

func (m *MedicalGenerator) partitionAppointments(appointments []medical.Appointment) [][]interface{} {
	// Partition into chunks for parallel processing
	chunkSize := 1000
	var chunks [][]interface{}

	for i := 0; i < len(appointments); i += chunkSize {
		end := i + chunkSize
		if end > len(appointments) {
			end = len(appointments)
		}

		chunk := make([]interface{}, end-i)
		for j := i; j < end; j++ {
			chunk[j-i] = appointments[j]
		}
		chunks = append(chunks, chunk)
	}

	return chunks
}
